import abc
from typing import List, Dict, Callable, Collection, Tuple, Iterable

from lmdb_storage.file_object import FileObject
from lmdb_storage.tree_structure import Objects, TreeObject, ObjectID
from util import group_to_dict


class ObjectsByRoot:
    def __init__(self, allowed_roots: List[str], roots_to_object: Collection[Tuple[str, ObjectID]] = ()):
        self.allowed_roots = allowed_roots
        self._roots_to_object = dict((k, v) for k, v in roots_to_object if v is not None)
        for child_name in self._roots_to_object:
            assert child_name in self.allowed_roots, f"Child name '{child_name}' not found in allowed roots list"

    def new(self) -> "ObjectsByRoot":
        return ObjectsByRoot(self.allowed_roots)

    def __len__(self) -> int:  # fixme why do we need to get length? there is no unambiguous answer
        return len(self._roots_to_object.values())

    def assigned_values(self) -> Iterable[ObjectID]:
        return self._roots_to_object.values()

    def assigned(self) -> Dict[str, ObjectID]:  # fixme deprecate, too powerful
        return self._roots_to_object

    def get_if_present(self, child_name: str, default: ObjectID | None = None) -> ObjectID | None:
        assert child_name in self.allowed_roots, f"Can't get child '{child_name}'!"
        return self._roots_to_object.get(child_name, default)

    def __setitem__(self, child_name: str, value: ObjectID | None):
        assert child_name in self.allowed_roots, f"Can't set child '{child_name}'!"
        if value is None:
            del self._roots_to_object[child_name]  # setting to None deletes the value if set
        else:
            self._roots_to_object[child_name] = value

    def __contains__(self, child_name: str) -> bool:
        assert child_name in self.allowed_roots, f"Can't check if contains a child '{child_name}'!"
        return child_name in self._roots_to_object

    @classmethod
    def singleton(cls, name, file):
        return ObjectsByRoot([name], ((name, file),))

    @classmethod
    def from_map(cls, dictionary: Dict[str, ObjectID]) -> "ObjectsByRoot":
        return ObjectsByRoot(list(dictionary), dictionary.items())


class Merge[F]:
    objects: Objects[F]

    @abc.abstractmethod
    def combine(self, path: List[str], merged: ObjectsByRoot, original: ObjectsByRoot) -> ObjectsByRoot:
        """Calculates values for the combined path by working on trees and files that are attached to this path."""
        pass

    @abc.abstractmethod
    def should_drill_down(self, path: List[str], trees: ObjectsByRoot, files: ObjectsByRoot) -> bool:
        pass

    @abc.abstractmethod
    def allowed_roots(self, objects_by_root: ObjectsByRoot) -> List[str]:
        pass

    def merge_trees[F](self, obj_ids: ObjectsByRoot) -> ObjectsByRoot:
        assert isinstance(obj_ids, ObjectsByRoot)

        obj_ids = ObjectsByRoot(self.allowed_roots(obj_ids), obj_ids.assigned().items())
        return merge_trees_recursively(self, [], obj_ids)


class TakeOneFile[F](Merge[F]):
    def allowed_roots(self, objects_by_root: ObjectsByRoot) -> List[str]:
        return objects_by_root.allowed_roots + ["MERGED"]

    def __init__(self, objects: Objects[F]):
        self.objects = objects

    def combine(self, path: List[str], merged: ObjectsByRoot, original: ObjectsByRoot) -> ObjectsByRoot:
        """Take the first value that is a file object as the resolved combined value."""
        if len(merged.assigned()) > 0:
            return ObjectsByRoot.singleton("MERGED", merged.assigned()["MERGED"])

        files = [f_id for f_id in original.assigned_values() if isinstance(self.objects[f_id], FileObject)]
        assert len(files) > 0  # prioritize taking the first file
        file = next(iter(files))  # fixme take with priority
        return ObjectsByRoot.singleton("MERGED", file)

    def should_drill_down(self, path: List[str], trees: ObjectsByRoot, files: ObjectsByRoot) -> bool:
        return len(files) == 0  # as we prioritize taking the first file




def split_by_object_type[F](objects: Objects[F], obj_ids: ObjectsByRoot) -> (ObjectsByRoot, ObjectsByRoot):
    files = ObjectsByRoot(
        obj_ids.allowed_roots,
        [(name, obj_id) for name, obj_id in obj_ids.assigned().items() if type(objects[obj_id]) is not TreeObject])
    trees = ObjectsByRoot(
        obj_ids.allowed_roots,
        [(name, obj_id) for name, obj_id in obj_ids.assigned().items() if type(objects[obj_id]) is TreeObject])
    return trees, files


def remap[A, B, C](dictionary: Dict[A, B], key: Callable[[B], C]) -> Dict[A, C]:
    return dict((k, key(v)) for k, v in dictionary.items())


def merge_trees_recursively[F](merge: Merge[F], path: List[str], obj_ids: ObjectsByRoot) -> ObjectsByRoot:
    trees, files = split_by_object_type(merge.objects, obj_ids)

    should_drill_down = merge.should_drill_down(path, trees, files)
    merged_objects = merge_children(merge, path, trees, should_drill_down)
    return merge.combine(path, merged_objects, obj_ids)


def merge_children[F](
        merge: Merge[F], path: List[str], trees: ObjectsByRoot, should_drill_down) -> ObjectsByRoot:
    trees_objects = remap(trees.assigned(), lambda obj_id: merge.objects[obj_id])

    # merging child folders first
    all_children = [
        (child_name, tree_root, child_obj_id)
        for tree_root, tree_obj in trees_objects.items()
        for child_name, child_obj_id in tree_obj.children.items()]

    # group by child name first
    merged_children: Dict[str, TreeObject] = dict()

    child_name_to_tree_root_and_obj = group_to_dict(all_children, lambda cto: cto[0], map_to=lambda cto: cto[1:])
    for child_name, tree_root_to_obj_id in child_name_to_tree_root_and_obj.items():
        all_objects_in_name: ObjectsByRoot = ObjectsByRoot(trees.allowed_roots, tree_root_to_obj_id)

        merged_child_by_roots: ObjectsByRoot = merge_trees_recursively(merge, path + [child_name], all_objects_in_name) \
            if should_drill_down else all_objects_in_name

        for root_name, obj_id in merged_child_by_roots.assigned().items():
            if root_name not in merged_children:
                merged_children[root_name] = TreeObject({})

            merged_children[root_name].children[child_name] = obj_id

    result = ObjectsByRoot(trees.allowed_roots)
    for root_name, child_tree in merged_children.items():
        new_child_id = child_tree.id
        merge.objects[new_child_id] = child_tree
        result[root_name] = new_child_id

    return result
