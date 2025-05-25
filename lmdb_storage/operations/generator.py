import abc
from functools import lru_cache
from typing import List, Iterable

from lmdb_storage.file_object import FileObject
from lmdb_storage.operations.three_way_merge import TransformedRoots
from lmdb_storage.operations.fast_association import FastAssociation
from lmdb_storage.operations.util import ByRoot
from lmdb_storage.tree_structure import Objects, TreeObject, ObjectID, ObjectType, StoredObject


class TreeGenerator[F, R]:
    objects: Objects

    @abc.abstractmethod
    def should_drill_down(
            self, state: List[str], trees: FastAssociation[TreeObject], files: FastAssociation[FileObject]) -> bool:
        pass

    @abc.abstractmethod
    def compute_on_level(self, path: List[str], original: FastAssociation[StoredObject]) -> Iterable[R]: pass

    def execute(self, obj_ids: ByRoot[ObjectID]) -> R:
        assert isinstance(obj_ids, ByRoot)
        return self._execute_recursively([], TransformedRoots.HACK_create(obj_ids).map(self.get_objects))

    @lru_cache(maxsize=1<<16)
    def get_objects(self, obj_id: ObjectID) -> StoredObject | None:
        return self.objects[obj_id] if obj_id is not None else None

    def _execute_recursively(self, merge_state: List[str], all_original: FastAssociation[StoredObject]) -> Iterable[R]:
        trees = all_original.filter(lambda v: v.object_type == ObjectType.TREE)
        files = all_original.filter(lambda v: v.object_type == ObjectType.BLOB)

        if self.should_drill_down(merge_state, trees, files):
            all_children_names = list(sorted(set(
                child_name for tree_obj in trees.values() for child_name in tree_obj.children)))

            for child_name in all_children_names:
                all_objects_in_child_name = trees.map(lambda obj: obj.children.get(child_name)).map(self.get_objects)
                yield from self._execute_recursively(
                    merge_state + [child_name],
                    all_objects_in_child_name)

        yield from self.compute_on_level(merge_state, all_original)
