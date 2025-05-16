import abc
import dataclasses
from types import NoneType
from typing import List, Collection, Tuple

from lmdb_storage.file_object import FileObject
from lmdb_storage.merge_trees import Merge, ByRoot, MergeResult, SeparateRootsMergeResult
from lmdb_storage.tree_structure import Objects, TreeObject, ObjectID


class MergePreferences:

    @abc.abstractmethod
    def combine_both_existing(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject], staging_name: str, base_name: str,
            staging_original: FileObject, base_original: FileObject) -> ByRoot[ObjectID]:
        pass

    @abc.abstractmethod
    def combine_base_only(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject], staging_name: str, base_name: str,
            base_original: FileObject) -> ByRoot[ObjectID]:
        return original_roots.new()

    @abc.abstractmethod
    def combine_staging_only(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject], staging_name: str, base_name: str,
            staging_original: FileObject) -> ByRoot[ObjectID]:
        pass

    @abc.abstractmethod
    def merge_missing(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject], staging_name: str,
            base_name: str) -> ByRoot[ObjectID]:
        pass


class NaiveMergePreferences(MergePreferences):
    def __init__(self, to_modify: Collection[str]):
        self.to_modify = list(to_modify)

    def where_to_apply_diffs(self, original_roots: List[str]) -> List[str]:
        return list(set(original_roots + self.to_modify))

    def where_to_apply_adds(self, original_roots: List[str]) -> List[str]:
        return list(set(original_roots + self.to_modify))

    def combine_both_existing(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject], staging_name: str, base_name: str,
            staging_original: FileObject, base_original: FileObject) -> ByRoot[ObjectID]:
        result: ByRoot[ObjectID] = original_roots.new()
        for merge_name in (self.where_to_apply_diffs(list(original_roots.assigned_keys()))):
            result[merge_name] = staging_original.file_id
        return result

    def combine_base_only(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject], staging_name: str, base_name: str,
            base_original: FileObject) -> ByRoot[ObjectID]:

        return original_roots.new()

    def combine_staging_only(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject], staging_name: str, base_name: str,
            staging_original: FileObject) -> ByRoot[ObjectID]:
        assert type(staging_original) is FileObject

        result: ByRoot[ObjectID] = original_roots.new()
        for merge_name in (self.where_to_apply_adds(list(original_roots.assigned_keys()))):
            result[merge_name] = staging_original.file_id
        return result

    def merge_missing(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject], staging_name: str,
            base_name: str) -> ByRoot[ObjectID]:
        return original_roots.map(lambda obj: obj.id)


@dataclasses.dataclass
class ThreewayMergeState:
    path: List[str]
    base: FileObject | TreeObject | None
    staging: FileObject | TreeObject | None


class ThreewayMerge(Merge[FileObject, ThreewayMergeState, ByRoot[ObjectID]]):
    def object_or_none(self, object_id: ObjectID) -> FileObject | TreeObject | None:
        return self.objects[object_id] if object_id is not None else None

    def initial_state(self, obj_ids: ByRoot[ObjectID]) -> ThreewayMergeState:
        base_id = obj_ids.get_if_present(self.current)
        staging_id = obj_ids.get_if_present(self.staging)
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
            self, objects: Objects[FileObject], current: str, staging: str, others: List[str],
            merge_prefs: MergePreferences):
        self.objects = objects
        self.current = current
        self.staging = staging
        self.others = others
        self.merge_prefs = merge_prefs

        self.allowed_roots = [current, staging] + others

    def should_drill_down(self, state: ThreewayMergeState, trees: ByRoot[TreeObject],
                          files: ByRoot[FileObject]) -> bool:
        # we have trees and the current and staging trees are different
        return len(trees) > 0 and state.base != state.staging

    def create_merge_result(self) -> MergeResult[FileObject, ByRoot[ObjectID]]:
        return SeparateRootsMergeResult[FileObject](self.allowed_roots, self.objects)

    def combine_non_drilldown(self, state: ThreewayMergeState, original: ByRoot[TreeObject | FileObject]) -> ByRoot[
        ObjectID]:
        # we are on file level
        base_original = state.base
        staging_original = state.staging
        if base_original == staging_original:  # no diffs
            return original.map(lambda obj: obj.id)

        assert staging_original is None or isinstance(staging_original, FileObject)
        assert base_original is None or isinstance(base_original, FileObject)

        if staging_original and base_original:
            # left and right both exist, apply difference to the other roots
            return self.merge_prefs.combine_both_existing(
                state.path, original, self.staging, self.current, staging_original, base_original)

        elif base_original:
            # file is deleted in staging
            return self.merge_prefs.combine_base_only(
                state.path, original, self.staging, self.current, base_original)

        elif staging_original:
            # is added in staging
            return self.merge_prefs.combine_staging_only(
                state.path, original, self.staging, self.current, staging_original)

        else:
            # current and staging are not in original, retain what was already there
            return self.merge_prefs.merge_missing(state.path, original, self.staging, self.current)

    def combine(self, state: ThreewayMergeState, merged: ByRoot[ObjectID], original: ByRoot[TreeObject | FileObject]) -> \
            ByRoot[ObjectID]:
        # tree-level, just return the merged
        # # fixme this is needed because empty folders get dropped in "merged" - should fix that problem
        # merged = merged.copy()
        # for root_name, original_obj in original.items():
        #     if merged.get_if_present(root_name) is None:
        #         merged[root_name] = original_obj.id
        return merged
