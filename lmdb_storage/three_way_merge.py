from typing import List, Set, Dict

from lmdb_storage.file_object import FileObject
from lmdb_storage.merge_trees import Merge, ObjectsByRoot
from lmdb_storage.tree_structure import Objects, TreeObject


class ThreewayMerge(Merge[FileObject]):
    def __init__(
            self, objects: Objects[FileObject], current: str, staging: str, others: List[str],
            fetch_new: Set[str]):
        self.objects = objects
        self.current = current
        self.staging = staging
        self.others = others
        self.fetch_new = fetch_new

    def allowed_roots(self, objects_by_root: ObjectsByRoot) -> List[str]:
        return objects_by_root.allowed_roots + [self.staging, self.current]

    def should_drill_down(self, path: List[str], trees: ObjectsByRoot, files: ObjectsByRoot) -> bool:
        # we have trees and the current and staging trees are different
        return len(trees) > 0 and trees.get_if_present(self.current) != trees.get_if_present(self.staging)

    def combine(self, path: List[str], merged: ObjectsByRoot, original: ObjectsByRoot) -> ObjectsByRoot:
        if len(merged) > 0:  # tree-level, just return the merged
            return merged

        # we are on file level
        current_original = original.get_if_present(self.current)
        staging_original = original.get_if_present(self.staging)
        assert not isinstance(staging_original, TreeObject) # is file or None
        assert not isinstance(current_original, TreeObject)  # is file or None

        if staging_original and current_original:
            # left and right both exist, apply difference to the other roots
            result: ObjectsByRoot = merged.new()
            for merge_name in [self.current, self.staging] + list(original.assigned().keys()) + list(self.fetch_new):
                result[merge_name] = staging_original

            return result

        elif current_original:
            # file is deleted in staging
            return merged.new()

        elif staging_original:
            # is added in staging
            result: ObjectsByRoot = merged.new()
            for merge_name in [self.current, self.staging] + list(original.assigned().keys()) + list(self.fetch_new):
                result[merge_name] = staging_original
            return result

        else:
            # current and staging are not in original, retain what was already there
            return original