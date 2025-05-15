import abc
from typing import List, Collection

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
        for merge_name in (
                [staging_name, base_name] + self.where_to_apply_diffs(list(original_roots.assigned_keys()))):
            result[merge_name] = staging_original.file_id
        return result

    def combine_base_only(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject], staging_name: str, base_name: str,
            base_original: FileObject) -> ByRoot[ObjectID]:

        return original_roots.new()

    def combine_staging_only(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject], staging_name: str, base_name: str,
            staging_original: FileObject) -> ByRoot[ObjectID]:
        result: ByRoot[ObjectID] = original_roots.new()
        for merge_name in (
                [base_name, staging_name] + self.where_to_apply_adds(list(original_roots.assigned_keys()))):
            result[merge_name] = staging_original.file_id
        return result

    def merge_missing(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject], staging_name: str,
            base_name: str) -> ByRoot[ObjectID]:
        return original_roots.map(lambda obj: obj.id)


class ThreewayMerge(Merge[FileObject, ByRoot[ObjectID]]):
    def __init__(
            self, objects: Objects[FileObject], current: str, staging: str, others: List[str],
            merge_prefs: MergePreferences):
        self.objects = objects
        self.current = current
        self.staging = staging
        self.others = others
        self.merge_prefs = merge_prefs

        self.allowed_roots = [current, staging] + others

    def should_drill_down(self, path: List[str], trees: ByRoot[TreeObject], files: ByRoot[FileObject]) -> bool:
        # we have trees and the current and staging trees are different
        return len(trees) > 0 and trees.get_if_present(self.current) != trees.get_if_present(self.staging)

    def create_merge_result(self) -> MergeResult[FileObject, ByRoot[ObjectID]]:
        return SeparateRootsMergeResult[FileObject](self.allowed_roots, self.objects)

    def combine_non_drilldown(self, path: List[str], original: ByRoot[TreeObject | FileObject]) -> ByRoot[ObjectID]:
        # we are on file level
        base_original = original.get_if_present(self.current)
        staging_original = original.get_if_present(self.staging)
        if base_original == staging_original: # no diffs
            return original.map(lambda obj: obj.id)

        assert staging_original is None or not isinstance(staging_original, TreeObject)  # is file or None
        assert base_original is None or not isinstance(base_original, TreeObject)  # is file or None

        if staging_original and base_original:
            # left and right both exist, apply difference to the other roots
            return self.merge_prefs.combine_both_existing(
                path, original, self.staging, self.current, staging_original, base_original)

        elif base_original:
            # file is deleted in staging
            return self.merge_prefs.combine_base_only(
                path, original, self.staging, self.current, base_original)

        elif staging_original:
            # is added in staging
            return self.merge_prefs.combine_staging_only(
                path, original, self.staging, self.current, staging_original)

        else:
            # current and staging are not in original, retain what was already there
            return self.merge_prefs.merge_missing(path, original, self.staging, self.current)

    def combine(self, path: List[str], merged: ByRoot[ObjectID], original: ByRoot[TreeObject | FileObject]) -> ByRoot[
        ObjectID]:
        # tree-level, just return the merged
        # # fixme this is needed because empty folders get dropped in "merged" - should fix that problem
        # merged = merged.copy()
        # for root_name, original_obj in original.items():
        #     if merged.get_if_present(root_name) is None:
        #         merged[root_name] = original_obj.id
        return merged
