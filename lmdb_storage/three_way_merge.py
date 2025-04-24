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

    def should_drill_down(self, path: List[str], trees: ObjectsByRoot, files: ObjectsByRoot) -> bool:
        # we have trees and the current and staging trees are different
        return len(trees) > 0 and trees.get_if_present(self.current) != trees.get_if_present(self.staging)

    def combine(self, path: List[str], children: Dict[str, ObjectsByRoot], files: ObjectsByRoot) -> ObjectsByRoot:
        if self.current in files and self.staging in files:
            # left and right are both files, apply difference to the other roots
            result: ObjectsByRoot = files.new()
            for merge_name in [self.current, self.staging] + self.others:
                result[merge_name] = files.get_if_present(self.staging)
            return result

        elif self.current in files:
            # file is deleted in staging
            return files.new()
        elif self.staging in files:
            # file is added in staging
            result: ObjectsByRoot = files.new()
            for merge_name in [self.current, self.staging] + self.others:
                if merge_name == self.current or merge_name in self.fetch_new:
                    result[merge_name] = files.get_if_present(self.staging)
                elif merge_name in files:
                    result[merge_name] = files.get_if_present(merge_name)
            return result
        else:
            # current and staging are not in files. this means that we have a partially-merged folders in children
            result: ObjectsByRoot = files.new()
            for merge_name in [self.current, self.staging] + self.others:
                if merge_name in files:
                    result[merge_name] = files.get_if_present(merge_name)  # and we know it is already in objects
                else:
                    merge_result = TreeObject({})
                    for child_name, child_objects in children.items():
                        if merge_name in child_objects:
                            merge_result.children[child_name] = child_objects.get_if_present(merge_name)

                    if len(merge_result.children) > 0:
                        # add to objects
                        self.objects[merge_result.id] = merge_result
                        result[merge_name] = merge_result.id

            return result
