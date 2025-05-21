import abc
import dataclasses
from types import NoneType
from typing import List, Collection, Dict

from lmdb_storage.file_object import FileObject
from lmdb_storage.operations.types import Transformation
from lmdb_storage.operations.util import ByRoot, Transformed
from lmdb_storage.tree_structure import Objects, TreeObject, ObjectID, MaybeObjectID


class TransformedRoots:
    # def __init__(self, roots_names_idxs: Dict[str, int]):
    #     self.roots_names_idxs = roots_names_idxs
    def __init__(self, result: ByRoot[ObjectID]):
        self.result = result

    def get_value(self) -> ByRoot[ObjectID]:
        return self.result

    def get_if_present(self, root_name: str) -> MaybeObjectID:
        return self.result.get_if_present(root_name)

    def assigned_keys(self):
        return self.result.assigned_keys()


class MergePreferences:

    @abc.abstractmethod
    def combine_both_existing(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject],
            staging_original: FileObject, base_original: FileObject, roots_to_merge: List[str]) -> TransformedRoots:
        pass

    @abc.abstractmethod
    def combine_base_only(
            self, path: List[str], repo_name: str, original_roots: ByRoot[TreeObject | FileObject],
            base_original: FileObject, roots_to_merge: List[str]) -> TransformedRoots:
        return TransformedRoots(original_roots.new())

    @abc.abstractmethod
    def combine_staging_only(
            self, path: List[str], repo_name, original_roots: ByRoot[TreeObject | FileObject],
            staging_original: FileObject, roots_to_merge: List[str]) -> TransformedRoots:
        pass

    @abc.abstractmethod
    def merge_missing(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject],
            roots_to_merge: List[str]) -> TransformedRoots:
        pass


class NaiveMergePreferences(MergePreferences):
    def __init__(self, to_modify: Collection[str]):
        self.to_modify = list(to_modify)

    def where_to_apply_diffs(self, original_roots: List[str]) -> List[str]:
        return list(set(original_roots + self.to_modify))

    def where_to_apply_adds(self, original_roots: List[str]) -> List[str]:
        return list(set(original_roots + self.to_modify))

    def combine_both_existing(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject],
            staging_original: FileObject, base_original: FileObject, roots_to_merge: List[str]) -> TransformedRoots:
        result: ByRoot[ObjectID] = original_roots.new()
        for merge_name in (self.where_to_apply_diffs(list(original_roots.assigned_keys()))):
            result[merge_name] = staging_original.file_id
        return TransformedRoots(result)

    def combine_base_only(
            self, path: List[str], repo_name: str, original_roots: ByRoot[TreeObject | FileObject],
            base_original: FileObject, roots_to_merge) -> TransformedRoots:

        return TransformedRoots(original_roots.new())

    def combine_staging_only(
            self, path: List[str], repo_name: str, original_roots: ByRoot[TreeObject | FileObject],
            staging_original: FileObject, roots_to_merge: List[str]) -> TransformedRoots:
        assert type(staging_original) is FileObject

        result: ByRoot[ObjectID] = original_roots.new()
        for merge_name in (self.where_to_apply_adds(list(original_roots.assigned_keys()))):
            result[merge_name] = staging_original.file_id
        return TransformedRoots(result)

    def merge_missing(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject],
            roots_to_merge: List[str]) -> TransformedRoots:
        return TransformedRoots(original_roots.map(lambda obj: obj.id))


@dataclasses.dataclass
class ThreewayMergeState:
    path: List[str]
    base: FileObject | TreeObject | None
    staging: FileObject | TreeObject | None


class CombinedRoots[F](Transformed[F, ByRoot[ObjectID]]):
    def __init__(self, allowed_roots: List[str], objects: Objects[F]):
        self.allowed_roots = allowed_roots
        self.objects = objects

        self._merged_children: Dict[str, TreeObject] = dict()

    def add_for_child(self, child_name: str, merged_child_by_roots: TransformedRoots) -> None:
        assert isinstance(merged_child_by_roots, TransformedRoots), type(merged_child_by_roots)
        for root_name, obj_id in merged_child_by_roots.result.items():
            if root_name not in self._merged_children:
                self._merged_children[root_name] = TreeObject({})

            self._merged_children[root_name].children[child_name] = obj_id

    def get_value(self) -> TransformedRoots:
        # store potential new objects
        for root_name, child_tree in self._merged_children.items():
            new_child_id = child_tree.id
            self.objects[new_child_id] = child_tree

        result = ByRoot[ObjectID](
            self.allowed_roots,
            ((root_name, child_tree.id) for root_name, child_tree in self._merged_children.items()))

        return TransformedRoots(result)


class ThreewayMerge(Transformation[FileObject, ThreewayMergeState, TransformedRoots]):
    def object_or_none(self, object_id: ObjectID) -> FileObject | TreeObject | None:
        return self.objects[object_id] if object_id is not None else None

    def initial_state(self, obj_ids: ByRoot[ObjectID]) -> ThreewayMergeState:
        base_id = self.current_id
        staging_id = self.staging_id
        return ThreewayMergeState([], self.object_or_none(base_id), self.object_or_none(staging_id))

    def drilldown_state(self, child_name: str, merge_state: ThreewayMergeState) -> ThreewayMergeState:
        base_obj = merge_state.base
        staging_obj = merge_state.staging
        assert type(base_obj) in (NoneType, FileObject, TreeObject)
        return ThreewayMergeState(
            merge_state.path + [child_name],
            self.object_or_none(base_obj.children.get(child_name)) if isinstance(base_obj, TreeObject) else None,
            # fixme handle files
            self.object_or_none(staging_obj.children.get(child_name)) if isinstance(staging_obj, TreeObject) else None)

    def __init__(
            self, objects: Objects[FileObject], current_id: ObjectID | None, staging_id: ObjectID | None,
            repo_name: str, roots_to_merge: List[str], merge_prefs: MergePreferences):
        self.objects = objects
        self.current_id = current_id
        self.staging_id = staging_id

        self.repo_name = repo_name
        self.roots_to_merge = roots_to_merge

        self.merge_prefs = merge_prefs

        self.allowed_roots = None  # fixme pass as argument maybe

    def execute(self, obj_ids: ByRoot[ObjectID]) -> TransformedRoots:
        assert self.allowed_roots is None
        self.allowed_roots = obj_ids.allowed_roots
        try:
            return super().execute(obj_ids)
        finally:
            self.allowed_roots = None

    def should_drill_down(
            self, state: ThreewayMergeState, trees: ByRoot[TreeObject], files: ByRoot[FileObject]) -> bool:
        # we have trees and the current and staging trees are different
        return len(trees) > 0 and state.base != state.staging

    def create_merge_result(self) -> Transformed[FileObject, ByRoot[ObjectID]]:
        return CombinedRoots[FileObject](self.allowed_roots, self.objects)

    def combine_non_drilldown(self, state: ThreewayMergeState, original: ByRoot[TreeObject | FileObject]) -> TransformedRoots:
        # we are on file level
        base_original = state.base
        staging_original = state.staging
        if base_original == staging_original:  # no diffs
            return TransformedRoots(original.map(lambda obj: obj.id))

        assert staging_original is None or isinstance(staging_original, FileObject)
        assert base_original is None or isinstance(base_original, FileObject)

        if staging_original and base_original:
            # left and right both exist, apply difference to the other roots
            return self.merge_prefs.combine_both_existing(
                state.path, original, staging_original, base_original, self.roots_to_merge)

        elif base_original:
            # file is deleted in staging
            return self.merge_prefs.combine_base_only(
                state.path, self.repo_name, original, base_original, self.roots_to_merge)

        elif staging_original:
            # is added in staging
            return self.merge_prefs.combine_staging_only(
                state.path, self.repo_name, original, staging_original, self.roots_to_merge)

        else:
            # current and staging are not in original, retain what was already there
            return self.merge_prefs.merge_missing(state.path, original, self.roots_to_merge)

    def combine(
            self, state: ThreewayMergeState, merged: CombinedRoots[FileObject],
            original: ByRoot[TreeObject | FileObject]) -> TransformedRoots:
        # tree-level, just return the merged
        # # fixme this is needed because empty folders get dropped in "merged" - should fix that problem
        # merged = merged.copy()
        # for root_name, original_obj in original.items():
        #     if merged.get_if_present(root_name) is None:
        #         merged[root_name] = original_obj.id
        return merged.get_value()
