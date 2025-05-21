from typing import List

from lmdb_storage.file_object import FileObject
from lmdb_storage.operations.types import Transformation
from lmdb_storage.operations.util import ByRoot, Transformed
from lmdb_storage.tree_structure import ObjectID, Objects, TreeObject


class TakeOneFile[F](Transformation[F, List[str], ObjectID]):
    def initial_state(self, obj_ids: ByRoot[ObjectID]) -> List[str]:
        return []

    def drilldown_state(self, child_name: str, merge_state: List[str]) -> List[str]:
        return merge_state + [child_name]

    class TakeOneMergeResult[F](Transformed[F, ObjectID]):
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

    def combine(self, state: List[str], merged: ObjectID, original: ByRoot[TreeObject | FileObject]) -> ObjectID:
        """Take the first value that is a file object as the resolved combined value."""
        return merged

    def should_drill_down(self, state: List[str], trees: ByRoot[TreeObject], files: ByRoot[FileObject]) -> bool:
        return len(files) == 0  # as we prioritize taking the first file

    def create_merge_result(self) -> Transformed[F, ObjectID]:
        return TakeOneFile.TakeOneMergeResult(self.objects)

    def combine_non_drilldown(self, state: List[str], original: ByRoot[TreeObject | FileObject]) -> ObjectID:
        files = original.filter_type(FileObject)
        assert len(files.values()) > 0, len(files.values())
        return next(files.values().__iter__()).id
