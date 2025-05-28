import abc
import dataclasses
from typing import List, Iterable, Tuple, Dict

from lmdb_storage.file_object import BlobObject
from lmdb_storage.object_serialization import construct_tree_object
from lmdb_storage.operations.fast_association import FastAssociation
from lmdb_storage.operations.types import Transformation
from lmdb_storage.operations.util import ByRoot, Transformed
from lmdb_storage.tree_object import ObjectType, StoredObject, TreeObject, ObjectID, MaybeObjectID, TreeObjectBuilder
from lmdb_storage.tree_structure import Objects


class TransformedRoots(FastAssociation[ObjectID]):
    def __init__(self, keys: Tuple[str], values: List[MaybeObjectID]):
        super().__init__(keys, values)

    @staticmethod
    def HACK_create(result: ByRoot[ObjectID]) -> "TransformedRoots":
        _keys: List[str] = list()
        _values: List[MaybeObjectID] = list()
        for key in result.allowed_roots:
            _keys.append(key)
            _values.append(result.get_if_present(key))
        return TransformedRoots(_keys, _values)

    def HACK_items(self) -> Iterable[Tuple[str, ObjectID]]:
        for key, value in zip(self._keys, self._values):
            if value is not None:
                yield key, value

    def HACK_custom_available_items(self, _keys: Tuple[str]) -> Iterable[Tuple[int, ObjectID]]:
        for key, value in self.HACK_items():
            if key in _keys:
                yield _keys.index(key), value

    @staticmethod
    def wrap(inner: FastAssociation[ObjectID]) -> "TransformedRoots":
        return TransformedRoots(inner._keys, inner._values)

    def HACK_maybe_set_by_key(self, key: str, value: ObjectID):
        if key in self._keys:
            self[self._keys.index(key)] = value


class MergePreferences:
    empty_association: FastAssociation

    @abc.abstractmethod
    def combine_both_existing(
            self, path: List[str], original_roots: ByRoot[StoredObject],
            staging_original: BlobObject, base_original: BlobObject) -> TransformedRoots:
        pass

    @abc.abstractmethod
    def combine_base_only(
            self, path: List[str], repo_name: str, original_roots: ByRoot[StoredObject],
            base_original: BlobObject) -> TransformedRoots:
        pass

    @abc.abstractmethod
    def combine_staging_only(
            self, path: List[str], repo_name, original_roots: ByRoot[StoredObject],
            staging_original: BlobObject) -> TransformedRoots:
        pass

    @abc.abstractmethod
    def merge_missing(self, path: List[str], original_roots: ByRoot[StoredObject]) -> TransformedRoots:
        pass


@dataclasses.dataclass
class ThreewayMergeState:
    path: List[str]
    base: BlobObject | TreeObject | None
    staging: BlobObject | TreeObject | None


class ThreewayMerge(Transformation[ThreewayMergeState, TransformedRoots]):
    def object_or_none(self, object_id: ObjectID) -> BlobObject | TreeObject | None:
        return self.objects[object_id] if object_id is not None else None

    def initial_state(self, obj_ids: ByRoot[ObjectID]) -> ThreewayMergeState:
        base_id = self.current_id
        staging_id = self.staging_id
        return ThreewayMergeState([], self.object_or_none(base_id), self.object_or_none(staging_id))

    def drilldown_state(self, child_name: str, merge_state: ThreewayMergeState) -> ThreewayMergeState:
        base_obj = merge_state.base
        staging_obj = merge_state.staging
        assert not base_obj or base_obj.object_type == ObjectType.BLOB or base_obj.object_type == ObjectType.TREE
        return ThreewayMergeState(
            merge_state.path + [child_name],
            self.object_or_none(
                base_obj.get(child_name)) if base_obj and base_obj.object_type == ObjectType.TREE else None,
            # fixme handle files
            self.object_or_none(
                staging_obj.get(child_name)) if staging_obj and staging_obj.object_type == ObjectType.TREE else None)

    def __init__(
            self, objects: Objects, current_id: ObjectID | None, staging_id: ObjectID | None,
            repo_name: str, merge_prefs: MergePreferences):
        self.objects = objects
        self.current_id = current_id
        self.staging_id = staging_id

        self.repo_name = repo_name

        self.merge_prefs = merge_prefs

        self.allowed_roots = None  # fixme pass as argument maybe

    def execute(self, obj_ids: ByRoot[ObjectID]) -> FastAssociation[ObjectID]:
        assert self.allowed_roots is None
        self.allowed_roots = obj_ids.allowed_roots
        try:
            return super().execute(obj_ids)
        finally:
            self.allowed_roots = None

    def should_drill_down(
            self, state: ThreewayMergeState, trees: ByRoot[TreeObject], files: ByRoot[BlobObject]) -> bool:
        # we have trees and the current and staging trees are different
        return len(trees) > 0 and state.base != state.staging

    def combine_non_drilldown(
            self, state: ThreewayMergeState, original: ByRoot[StoredObject]) -> FastAssociation[ObjectID]:
        # we are on file level
        base_original = state.base
        staging_original = state.staging
        if base_original == staging_original:  # no diffs
            return TransformedRoots.HACK_create(original.map(lambda obj: obj.id))

        assert not staging_original or staging_original.object_type == ObjectType.BLOB
        assert not base_original or base_original.object_type == ObjectType.BLOB

        if staging_original and base_original:
            # left and right both exist, apply difference to the other roots
            return self.merge_prefs.combine_both_existing(state.path, original, staging_original, base_original)

        elif base_original:
            # file is deleted in staging
            return self.merge_prefs.combine_base_only(state.path, self.repo_name, original, base_original)

        elif staging_original:
            # is added in staging
            return self.merge_prefs.combine_staging_only(state.path, self.repo_name, original, staging_original)

        else:
            # current and staging are not in original, retain what was already there
            return self.merge_prefs.merge_missing(state.path, original)

    def combine(
            self, state: ThreewayMergeState, merged: Dict[str, FastAssociation[ObjectID]],
            original: ByRoot[StoredObject]) -> FastAssociation[ObjectID]:

        merged_children: FastAssociation[TreeObjectBuilder] = self.merge_prefs.empty_association.new()

        for child_name, merged_child_by_roots in merged.items():
            if isinstance(merged_child_by_roots, TransformedRoots):
                # fixme remove this case, needed to reduce the available items
                for root_idx, obj_id in merged_child_by_roots.HACK_custom_available_items(merged_children._keys):
                    if merged_children[root_idx] is None:
                        merged_children[root_idx] = {}

                    merged_children[root_idx][child_name] = obj_id
            else:
                assert isinstance(merged_child_by_roots, FastAssociation), type(merged_child_by_roots)
                for root_idx, obj_id in merged_child_by_roots.available_items():
                    if merged_children[root_idx] is None:
                        merged_children[root_idx] = {}

                    merged_children[root_idx][child_name] = obj_id

        constructed = merged_children.map(construct_tree_object)

        # store potential new objects
        for _, child_tree in constructed.available_items():
            self.objects[child_tree.id] = child_tree

        return constructed.map(lambda obj: obj.id)
