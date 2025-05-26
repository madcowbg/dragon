import abc
from typing import List

from lmdb_storage.file_object import BlobObject
from lmdb_storage.operations.util import ByRoot, Transformed
from lmdb_storage.tree_structure import Objects, ObjectID
from lmdb_storage.tree_object import StoredObject, TreeObject


class Transformation[S, R](abc.ABC):
    objects: Objects

    @abc.abstractmethod
    def combine(self, state: S, merged: Transformed[R], original: ByRoot[StoredObject]) -> R:
        """Calculates values for the combined path by working on trees and files that are attached to this path."""
        pass

    @abc.abstractmethod
    def should_drill_down(self, state: S, trees: ByRoot[TreeObject], files: ByRoot[BlobObject]) -> bool:
        pass

    @abc.abstractmethod
    def create_merge_result(self) -> Transformed[R]:
        pass

    @abc.abstractmethod
    def combine_non_drilldown(self, state: S, original: ByRoot[StoredObject]) -> R:
        pass

    def execute(self, obj_ids: ByRoot[ObjectID]) -> R:
        assert isinstance(obj_ids, ByRoot)
        return self._execute_recursively(self.initial_state(obj_ids), obj_ids)

    def _execute_recursively(self, merge_state: S, obj_ids: ByRoot[ObjectID]) -> R:
        all_original: ByRoot[StoredObject] = obj_ids.map(lambda obj_id: self.objects[obj_id])

        trees = all_original.filter_type(TreeObject)
        files = all_original.filter_type(BlobObject)

        if self.should_drill_down(merge_state, trees, files):
            all_children_names = list(sorted(set(
                child_name for tree_obj in trees.values() for child_name, _ in tree_obj.children)))

            merge_result: Transformed[R] = self.create_merge_result()
            for child_name in all_children_names:
                all_objects_in_child_name = trees.map(lambda obj: obj.get(child_name))
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


class EmptyTransformed(Transformed[None]):
    def add_for_child(self, child_name: str, merged_child_by_roots: None) -> None:
        return None


_empty_merge_result = EmptyTransformed()


class Procedure(Transformation[List[str], None]):
    @abc.abstractmethod
    def run_on_level(self, state: List[str], original: ByRoot[StoredObject]): pass

    def combine(self, state: List[str], merged: None, original: ByRoot[StoredObject]) -> None:
        self.run_on_level(state, original)

    def create_merge_result(self) -> Transformed[None]:
        return _empty_merge_result

    def combine_non_drilldown(self, state: List[str], original: ByRoot[StoredObject]) -> None:
        self.run_on_level(state, original)

    def initial_state(self, obj_ids: ByRoot[ObjectID]) -> List[str]:
        return []

    def drilldown_state(self, child_name: str, merge_state: List[str]) -> List[str]:
        return merge_state + [child_name]


