from typing import List, Dict

from lmdb_storage.file_object import BlobObject
from lmdb_storage.object_serialization import construct_tree_object
from lmdb_storage.operations.types import Transformation
from lmdb_storage.operations.util import ByRoot, Transformed
from lmdb_storage.tree_structure import ObjectID, Objects
from lmdb_storage.tree_object import StoredObject, TreeObject


class TakeOneFile(Transformation[List[str], ObjectID]):
    def initial_state(self, obj_ids: ByRoot[ObjectID]) -> List[str]:
        return []

    def drilldown_state(self, child_name: str, merge_state: List[str]) -> List[str]:
        return merge_state + [child_name]

    def __init__(self, objects: Objects):
        self.objects = objects

    def combine(self, state: List[str], merged: Dict[str, ObjectID], original: ByRoot[StoredObject]) -> ObjectID:
        """Take the first value that is a file object as the resolved combined value."""
        result_tree = construct_tree_object(merged)
        self.objects[result_tree.id] = result_tree
        return result_tree.id

    def should_drill_down(self, state: List[str], trees: ByRoot[TreeObject], files: ByRoot[BlobObject]) -> bool:
        return len(files) == 0  # as we prioritize taking the first file

    def combine_non_drilldown(self, state: List[str], original: ByRoot[StoredObject]) -> ObjectID:
        files = original.filter_type(BlobObject)
        assert len(files.values()) > 0, len(files.values())
        return next(files.values().__iter__()).id
