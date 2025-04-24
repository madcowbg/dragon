import abc
from typing import List, Dict, Callable, Collection, Tuple, Iterable

from lmdb_storage.tree_structure import Objects, TreeObject, ObjectID
from util import group_to_dict


class ObjectsByRoot:
    def __init__(self, roots: List[str], children: Collection[Tuple[str, ObjectID]] = ()):
        self.roots = roots
        self.children = dict(children)

    def new(self) -> "ObjectsByRoot":
        return ObjectsByRoot(self.roots)

    def __len__(self) -> int:  # fixme why do we need to get length? there is no unambiguous answer
        return len(self.children.values())

    def assigned_values(self) -> Iterable[ObjectID]:
        return self.children.values()

    def assigned(self) -> Dict[str, ObjectID]:  # fixme deprecate, too powerful
        return self.children

    def get_if_present(self, child_name: str) -> ObjectID | None:
        return self.children.get(child_name, None)  # fixme filter by allowed roots

    def __setitem__(self, key: str, value: ObjectID):
        self.children[key] = value  # fixme do not add nulls and filter by allowed

    def __contains__(self, key: str) -> bool:
        return key in self.children  # fixme filter by allowed roots

    @classmethod
    def singleton(cls, name, file):
        return ObjectsByRoot([name], ((name, file),))

    @classmethod
    def from_map(cls, dictionary: Dict[str, ObjectID]) -> "ObjectsByRoot":
        return ObjectsByRoot(list(dictionary), dictionary.items())


class Merge[F]:
    objects: Objects[F]

    @abc.abstractmethod
    def combine(self, path: List[str], children: Dict[str, ObjectsByRoot], files: ObjectsByRoot) -> ObjectsByRoot:
        """Calculates values for the combined path by working on trees and files that are attached to this path."""
        pass

    @abc.abstractmethod
    def should_drill_down(self, path: List[str], trees: ObjectsByRoot, files: ObjectsByRoot) -> bool:
        pass


class TakeOneFile[F](Merge[F]):
    def __init__(self, objects: Objects[F]):
        self.objects = objects

    def combine(self, path: List[str], children: Dict[str, ObjectsByRoot], files: ObjectsByRoot) -> ObjectsByRoot:
        """Take the first value that is a file object as the resolved combined value."""
        if len(files) > 0:  # prioritize taking the first file
            file = next(iter(files.assigned_values()))  # fixme take with priority
            return ObjectsByRoot.singleton("MERGED", file)
        else:  # we are merging a tree
            assert len(children) > 0, f"Should not have drilled down as we already have files: {files}"

            result = TreeObject(dict())
            for child_name, merged_child in children.items():
                result.children[child_name] = merged_child.get_if_present("MERGED")

            result_id = result.id
            self.objects[result_id] = result
            return ObjectsByRoot.singleton("MERGED", result_id)

    def should_drill_down(self, path: List[str], trees: ObjectsByRoot, files: ObjectsByRoot) -> bool:
        return len(files) == 0  # as we prioritize taking the first file


def split_by_object_type[F](objects: Objects[F], obj_ids: ObjectsByRoot) -> (ObjectsByRoot, ObjectsByRoot):
    files = ObjectsByRoot.from_map(
        dict((name, obj_id) for name, obj_id in obj_ids.assigned().items() if type(objects[obj_id]) is not TreeObject))
    trees = ObjectsByRoot.from_map(
        dict((name, obj_id) for name, obj_id in obj_ids.assigned().items() if type(objects[obj_id]) is TreeObject))
    return trees, files


def remap[A, B, C](dictionary: Dict[A, B], key: Callable[[B], C]) -> Dict[A, C]:
    return dict((k, key(v)) for k, v in dictionary.items())


def merge_trees_recursively[F](
        merge: Merge[F], path: List[str], trees: ObjectsByRoot, files: ObjectsByRoot) -> ObjectsByRoot:
    trees_objects = remap(trees.assigned(), lambda obj_id: merge.objects[obj_id])

    # merging child folders first
    all_children = [
        (child_name, tree_root, child_obj_id)
        for tree_root, tree_obj in trees_objects.items()
        for child_name, child_obj_id in tree_obj.children.items()]

    # group by child name first
    merged_children: Dict[str, ObjectsByRoot] = dict()
    child_name_to_tree_root_and_obj = group_to_dict(all_children, lambda cto: cto[0], map_to=lambda cto: cto[1:])
    for child_name, tree_root_to_obj_id in child_name_to_tree_root_and_obj.items():
        root_names: List[str] = list(map((lambda t: t[0]), tree_root_to_obj_id))
        all_objects_in_name: ObjectsByRoot = ObjectsByRoot(root_names, tree_root_to_obj_id)

        if merge.should_drill_down(path, trees, files):
            sub_trees, sub_files = split_by_object_type(merge.objects, all_objects_in_name)
            merged_children[child_name] = merge_trees_recursively(merge, path + [child_name], sub_trees, sub_files)
        else:
            merged_children[child_name] = all_objects_in_name

    return merge.combine(path, merged_children, files)


def merge_trees[F](obj_ids: ObjectsByRoot, merge: Merge[F]) -> ObjectsByRoot:
    assert isinstance(obj_ids, ObjectsByRoot)

    trees, files = split_by_object_type(merge.objects, obj_ids)

    return merge_trees_recursively(merge, [], trees, files)
