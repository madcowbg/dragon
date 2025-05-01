import abc
from typing import List, Set, Dict, Collection

from lmdb_storage.file_object import FileObject
from lmdb_storage.merge_trees import Merge, ObjectsByRoot
from lmdb_storage.roots import Root
from lmdb_storage.tree_structure import Objects, TreeObject


class MergePreferences:
    @abc.abstractmethod
    def where_to_apply_diffs(self, original_roots: List[str]) -> List[str]: pass

    @abc.abstractmethod
    def where_to_apply_adds(self, original_roots: List[str]) -> List[str]: pass


class NaiveMergePreferences(MergePreferences):
    def __init__(self, to_modify: Collection[str]):
        self.to_modify = list(to_modify)

    def where_to_apply_diffs(self, original_roots: List[str]) -> List[str]:
        return list(set(original_roots + self.to_modify))

    def where_to_apply_adds(self, original_roots: List[str]) -> List[str]:
        return list(set(original_roots + self.to_modify))


class ThreewayMerge(Merge[FileObject]):
    def __init__(
            self, objects: Objects[FileObject], current: str, staging: str, others: List[str],
            merge_prefs: MergePreferences):
        self.objects = objects
        self.current = current
        self.staging = staging
        self.others = others
        self.merge_prefs = merge_prefs

    def allowed_roots(self, objects_by_root: ObjectsByRoot) -> List[str]:
        return objects_by_root.allowed_roots + [self.staging, self.current]

    def should_drill_down(self, path: List[str], trees: ObjectsByRoot, files: ObjectsByRoot) -> bool:
        # we have trees and the current and staging trees are different
        return len(trees) > 0 and trees.get_if_present(self.current) != trees.get_if_present(self.staging)

    def combine(self, path: List[str], merged: ObjectsByRoot, original: ObjectsByRoot) -> ObjectsByRoot:
        if len(merged) > 0:  # tree-level, just return the merged
            # fixme this is needed because empty folders get dropped in "merged" - should fix that problem
            merged = ObjectsByRoot(merged.allowed_roots, merged.assigned().items())
            for merged_name, original_obj_id in original.assigned().items():
                if merged.get_if_present(merged_name) is None:
                    merged[merged_name] = original_obj_id
            return merged

        # we are on file level
        base_original = original.get_if_present(self.current)
        staging_original = original.get_if_present(self.staging)
        assert not isinstance(staging_original, TreeObject)  # is file or None
        assert not isinstance(base_original, TreeObject)  # is file or None

        if staging_original and base_original:
            # left and right both exist, apply difference to the other roots
            result: ObjectsByRoot = merged.new()
            for merge_name in [self.current, self.staging] + self.merge_prefs.where_to_apply_diffs(
                    list(original.assigned().keys())):
                result[merge_name] = staging_original

            return result

        elif base_original:
            # file is deleted in staging
            return merged.new()

        elif staging_original:
            # is added in staging
            result: ObjectsByRoot = merged.new()
            for merge_name in [self.current, self.staging] + self.merge_prefs.where_to_apply_adds(
                    list(original.assigned().keys())):
                result[merge_name] = staging_original
            return result

        else:
            # current and staging are not in original, retain what was already there
            return original
