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

    def __add__(self, other: "ByRoot[V]") -> "ByRoot[V]":
        assert isinstance(other, ByRoot)
        assert set(self.allowed_roots) == set(other.allowed_roots)
        return ByRoot[V](
            self.allowed_roots + other.allowed_roots,
            list(self._roots_to_object.items()) + list(other._roots_to_object.items()))


class ObjectsByRoot:
    @classmethod
    def singleton(cls, name, file):
        return ByRoot[ObjectID]([name], ((name, file),))

    @classmethod
    def from_map(cls, dictionary: Dict[str, ObjectID]) -> "ByRoot[ObjectID]":
        return ByRoot[ObjectID](list(dictionary), dictionary.items())


class MergeResult[F, R]:
    @abc.abstractmethod
    def add_for_child(self, child_name: str, merged_child_by_roots: R) -> None:
        pass

    @abc.abstractmethod
    def get_value(self) -> R:
        pass


class SeparateRootsMergeResult[F](MergeResult[F, ByRoot[ObjectID]]):
    def __init__(self, allowed_roots: List[str], objects: Objects[F]):
        self.allowed_roots = allowed_roots
        self.objects = objects

        self._merged_children: Dict[str, TreeObject] = dict()

    def add_for_child(self, child_name: str, merged_child_by_roots: ByRoot[ObjectID]) -> None:
        for root_name, obj_id in merged_child_by_roots.items():
            if root_name not in self._merged_children:
                self._merged_children[root_name] = TreeObject({})

            self._merged_children[root_name].children[child_name] = obj_id

    def get_value(self) -> ByRoot[ObjectID]:
        # store potential new objects
        for root_name, child_tree in self._merged_children.items():
            new_child_id = child_tree.id
            self.objects[new_child_id] = child_tree

        result = ByRoot[ObjectID](
            self.allowed_roots,
            ((root_name, child_tree.id) for root_name, child_tree in self._merged_children.items()))

        return result


class Merge[F, R]:
    objects: Objects[F]
    allowed_roots: List[str]

    @abc.abstractmethod
    def combine(self, path: List[str], merged: R, original: ByRoot[TreeObject | FileObject]) -> R:
        """Calculates values for the combined path by working on trees and files that are attached to this path."""
        pass

    @abc.abstractmethod
    def should_drill_down(self, path: List[str], trees: ByRoot[TreeObject], files: ByRoot[FileObject]) -> bool:
        pass

    @abc.abstractmethod
    def create_merge_result(self) -> MergeResult[F, R]:
        pass

    @abc.abstractmethod
    def combine_non_drilldown(self, path: List[str], original: ByRoot[TreeObject | FileObject]) -> R:
        pass

    def merge_trees(self, obj_ids: ByRoot[ObjectID]) -> R:
        assert isinstance(obj_ids, ByRoot)
        return self.merge_trees_recursively([], obj_ids)

    def merge_trees_recursively(self, path: List[str], obj_ids: ByRoot[ObjectID]) -> R:
        all_original: ByRoot[TreeObject | FileObject] = obj_ids.map(lambda obj_id: self.objects[obj_id])

        trees = all_original.filter_type(TreeObject)
        files = all_original.filter_type(FileObject)

        if self.should_drill_down(path, trees, files):
            all_children_names = list(sorted(set(
                child_name for tree_obj in trees.values() for child_name in tree_obj.children)))

            merge_result: MergeResult[F, R] = self.create_merge_result()
            for child_name in all_children_names:
                all_objects_in_child_name = trees.map(lambda obj: obj.children.get(child_name))
                merged_child_by_roots: R = self.merge_trees_recursively(path + [child_name], all_objects_in_child_name)
                merge_result.add_for_child(child_name, merged_child_by_roots)

            merged_objects: R = merge_result.get_value()
            return self.combine(path, merged_objects, all_original)
        else:
            return self.combine_non_drilldown(path, all_original)


class TakeOneFile[F](Merge[F, ObjectID]):
    class TakeOneMergeResult[F](MergeResult[F, ObjectID]):
        def __init__(self, objects: Objects[F]):
            self.objects = objects
            self._result = TreeObject({})

        def add_for_child(self, child_name: str, merged_child_by_roots: ObjectID) -> None:
            self._result.children[child_name] = merged_child_by_roots

        def get_value(self) -> ObjectID:
            self.objects[self._result.id] = self._result
            return self._result.id

        def add_for_unmerged(self, child_name: str, all_objects_in_child_name: ByRoot[ObjectID]) -> None:
            raise NotImplementedError()

    def __init__(self, objects: Objects[F]):
        self.objects = objects

    def combine(self, path: List[str], merged: ObjectID, original: ByRoot[TreeObject | FileObject]) -> ObjectID:
        """Take the first value that is a file object as the resolved combined value."""
        return merged

    def should_drill_down(self, path: List[str], trees: ByRoot[TreeObject], files: ByRoot[FileObject]) -> bool:
        return len(files) == 0  # as we prioritize taking the first file

    def create_merge_result(self) -> MergeResult[F, ObjectID]:
        return TakeOneFile.TakeOneMergeResult(self.objects)

    def combine_non_drilldown(self, path: List[str], original: ByRoot[TreeObject | FileObject]) -> ObjectID:
        files = original.filter_type(FileObject)
        assert len(files.values()) > 0, len(files.values())
        return next(files.values().__iter__()).id


def remap[A, B, C](dictionary: Dict[A, B], key: Callable[[B], C]) -> Dict[A, C]:
    return dict((k, key(v)) for k, v in dictionary.items())
