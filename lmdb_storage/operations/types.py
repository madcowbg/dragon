import abc
from typing import List, Iterable

from lmdb_storage.file_object import FileObject
from lmdb_storage.operations.util import ByRoot, Transformed
from lmdb_storage.tree_structure import Objects, TreeObject, ObjectID


class Transformation[F, S, R](abc.ABC):
    objects: Objects[F]

    @abc.abstractmethod
    def combine(self, state: S, merged: Transformed[F, R], original: ByRoot[TreeObject | FileObject]) -> R:
        """Calculates values for the combined path by working on trees and files that are attached to this path."""
        pass

    @abc.abstractmethod
    def should_drill_down(self, state: S, trees: ByRoot[TreeObject], files: ByRoot[FileObject]) -> bool:
        pass

    @abc.abstractmethod
    def create_merge_result(self) -> Transformed[F, R]:
        pass

    @abc.abstractmethod
    def combine_non_drilldown(self, state: S, original: ByRoot[TreeObject | FileObject]) -> R:
        pass

    def execute(self, obj_ids: ByRoot[ObjectID]) -> R:
        assert isinstance(obj_ids, ByRoot)
        return self._execute_recursively(self.initial_state(obj_ids), obj_ids)

    def _execute_recursively(self, merge_state: S, obj_ids: ByRoot[ObjectID]) -> R:
        all_original: ByRoot[TreeObject | FileObject] = obj_ids.map(lambda obj_id: self.objects[obj_id])

        trees = all_original.filter_type(TreeObject)
        files = all_original.filter_type(FileObject)

        if self.should_drill_down(merge_state, trees, files):
            all_children_names = list(sorted(set(
                child_name for tree_obj in trees.values() for child_name in tree_obj.children)))

            merge_result: Transformed[F, R] = self.create_merge_result()
            for child_name in all_children_names:
                all_objects_in_child_name = trees.map(lambda obj: obj.children.get(child_name))
                merged_child_by_roots: R = self._execute_recursively(
                    self.drilldown_state(child_name, merge_state),
                    all_objects_in_child_name)
                merge_result.add_for_child(child_name, merged_child_by_roots)

            return self.combine(merge_state, merge_result, all_original)
        else:
            return self.combine_non_drilldown(merge_state, all_original)

    @abc.abstractmethod
    def initial_state(self, obj_ids: ByRoot[ObjectID]) -> S:
        pass

    @abc.abstractmethod
    def drilldown_state(self, child_name: str, merge_state: S) -> S:
        pass


class EmptyTransformed[F](Transformed[F, None]):
    def add_for_child(self, child_name: str, merged_child_by_roots: None) -> None:
        return None

    def get_value(self) -> None:
        return None


_empty_merge_result = EmptyTransformed()


class Procedure[F](Transformation[F, List[str], None]):
    @abc.abstractmethod
    def run_on_level(self, state: List[str], original: ByRoot[TreeObject | FileObject]): pass

    def combine(self, state: List[str], merged: None, original: ByRoot[TreeObject | FileObject]) -> None:
        self.run_on_level(state, original)

    def create_merge_result(self) -> Transformed[F, None]:
        return _empty_merge_result

    def combine_non_drilldown(self, state: List[str], original: ByRoot[TreeObject | FileObject]) -> None:
        self.run_on_level(state, original)

    def initial_state(self, obj_ids: ByRoot[ObjectID]) -> List[str]:
        return []

    def drilldown_state(self, child_name: str, merge_state: List[str]) -> List[str]:
        return merge_state + [child_name]


class GeneratorTransformed[F, R](Transformed[F, Iterable[R]]):
    def __init__(self):
        self._child_res_list = list()

    def add_for_child(self, child_name: str, merged_child_by_roots: Iterable[R]) -> None:
        self._child_res_list.append(merged_child_by_roots)

    def get_value(self) -> Iterable[R]:
        for child_res in self._child_res_list:
            yield from child_res


class TreeGenerator[F, R](Transformation[F, List[str], Iterable[R]]):
    @abc.abstractmethod
    def compute_on_level(self, path: List[str], original: ByRoot[TreeObject | FileObject]) -> Iterable[R]: pass

    def combine(self, state: List[str], merged: GeneratorTransformed[F, R], original: ByRoot[TreeObject | FileObject]) -> Iterable[R]:
        yield from merged.get_value()
        yield from self.compute_on_level(state, original)

    def create_merge_result(self) -> Transformed[F, Iterable[R]]:
        return GeneratorTransformed()

    def combine_non_drilldown(self, state: List[str], original: ByRoot[TreeObject | FileObject]) -> Iterable[R]:
        yield from self.compute_on_level(state, original)

    def initial_state(self, obj_ids: ByRoot[ObjectID]) -> List[str]:
        return []

    def drilldown_state(self, child_name: str, merge_state: List[str]) -> List[str]:
        return merge_state + [child_name]
