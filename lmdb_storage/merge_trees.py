import abc
from typing import List, Dict, Callable, Collection, Tuple, Iterable, Type

from lmdb_storage.file_object import FileObject
from lmdb_storage.tree_structure import Objects, TreeObject, ObjectID


class ByRoot[V]:
    def __init__(self, allowed_roots: List[str], roots_to_object: Iterable[Tuple[str, V | None]] = ()):
        self.allowed_roots = allowed_roots
        self._roots_to_object = dict((k, v) for k, v in roots_to_object if v is not None)
        for child_name in self._roots_to_object:
            assert child_name in self.allowed_roots, f"Child name '{child_name}' not found in allowed roots list"

    def new(self) -> "ByRoot[V]":
        return ByRoot[V](self.allowed_roots)

    def __len__(self) -> int:  # fixme why do we need to get length? there is no unambiguous answer
        return len(self._roots_to_object.values())

    def assigned_values(self) -> Iterable[ObjectID]:
        return self._roots_to_object.values()

    def get_if_present(self, child_name: str, default: ObjectID | None = None) -> V | None:
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

    def copy(self) -> "ByRoot[ObjectID]":
        return ByRoot[ObjectID](self.allowed_roots, self._roots_to_object.items())

    def map[R](self, mapper: Callable[[V], R]) -> "ByRoot[R]":
        return ByRoot[R](self.allowed_roots, remap(self._roots_to_object, mapper).items())

    def values(self) -> Collection[V]:
        return self._roots_to_object.values()

    def items(self) -> Collection[Tuple[str, V]]:
        return self._roots_to_object.items()

    def assigned_keys(self) -> Collection[str]:
        return self._roots_to_object.keys()

    def filter_type[T](self, selected_type: Type[T], exclude: bool = False):
        return ByRoot[T](
            self.allowed_roots,
            remap(self._roots_to_object, lambda obj: obj if (exclude ^ (type(obj) is selected_type)) else None).items())


class ObjectsByRoot:
    @classmethod
    def singleton(cls, name, file):
        return ByRoot[ObjectID]([name], ((name, file),))

    @classmethod
    def from_map(cls, dictionary: Dict[str, ObjectID]) -> "ByRoot[ObjectID]":
        return ByRoot[ObjectID](list(dictionary), dictionary.items())


class Merge[F]:
    objects: Objects[F]
    allowed_roots: List[str]

    @abc.abstractmethod
    def combine(self, path: List[str], merged: ByRoot[ObjectID], original: ByRoot[TreeObject | FileObject]) -> ByRoot[ObjectID]:
        """Calculates values for the combined path by working on trees and files that are attached to this path."""
        pass

    @abc.abstractmethod
    def should_drill_down(self, path: List[str], trees: ByRoot[TreeObject], files: ByRoot[FileObject]) -> bool:
        pass

    def merge_trees(self, obj_ids: ByRoot[ObjectID]) -> ByRoot[ObjectID]:
        assert isinstance(obj_ids, ByRoot)
        return self.merge_trees_recursively([], obj_ids)

    def merge_trees_recursively(self, path: List[str], obj_ids: ByRoot[ObjectID]) -> ByRoot[ObjectID]:
        all_original: ByRoot[TreeObject | FileObject] = obj_ids.map(lambda obj_id: self.objects[obj_id])

        trees = all_original.filter_type(TreeObject)
        files = all_original.filter_type(FileObject)

        should_drill_down = self.should_drill_down(path, trees, files)
        merged_objects = self.merge_children(path, trees, should_drill_down)
        return self.combine(path, merged_objects, all_original)

    def merge_children(
            self, path: List[str], trees: ByRoot[TreeObject], should_drill_down) -> ByRoot[ObjectID]:

        all_children_names = list(sorted(set(
            child_name for tree_obj in trees.values() for child_name in tree_obj.children)))

        # group by child name first
        merged_children: Dict[str, TreeObject] = dict()

        for child_name in all_children_names:
            all_objects_in_child_name = trees.map(lambda obj: obj.children.get(child_name))

            merged_child_by_roots: ByRoot[ObjectID] = \
                self.merge_trees_recursively(path + [child_name], all_objects_in_child_name) \
                    if should_drill_down else all_objects_in_child_name

            for root_name, obj_id in merged_child_by_roots.items():
                if root_name not in merged_children:
                    merged_children[root_name] = TreeObject({})

                merged_children[root_name].children[child_name] = obj_id

        # store potential new objects
        for root_name, child_tree in merged_children.items():
            new_child_id = child_tree.id
            self.objects[new_child_id] = child_tree

        result = ByRoot[ObjectID](self.allowed_roots)
        for root_name, child_tree in merged_children.items():
            result[root_name] = child_tree.id

        return result


class TakeOneFile[F](Merge[F]):
    def __init__(self, objects: Objects[F], allowed_roots: Collection[str]):
        self.objects = objects
        self.allowed_roots = list(allowed_roots) + ['MERGED']  # fixme only return merged

    def combine(self, path: List[str], merged: ByRoot[ObjectID], original: ByRoot[TreeObject | FileObject] ) -> ByRoot[ObjectID]:
        """Take the first value that is a file object as the resolved combined value."""
        if len(merged.values()) > 0:
            return ObjectsByRoot.singleton("MERGED", merged.get_if_present("MERGED"))

        files = [file for file in original.assigned_values() if isinstance(file, FileObject)]
        assert len(files) > 0  # prioritize taking the first file
        file = next(iter(files))  # fixme take with priority
        return ObjectsByRoot.singleton("MERGED", file.file_id)

    def should_drill_down(self, path: List[str], trees: ByRoot[TreeObject], files: ByRoot[FileObject]) -> bool:
        return len(files) == 0  # as we prioritize taking the first file


def remap[A, B, C](dictionary: Dict[A, B], key: Callable[[B], C]) -> Dict[A, C]:
    return dict((k, key(v)) for k, v in dictionary.items())
