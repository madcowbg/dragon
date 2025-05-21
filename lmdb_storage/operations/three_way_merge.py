import abc
import dataclasses
from types import NoneType
from typing import List, Dict, Iterable, Tuple

from lmdb_storage.file_object import FileObject
from lmdb_storage.operations.types import Transformation
from lmdb_storage.operations.util import ByRoot, Transformed
from lmdb_storage.tree_structure import Objects, TreeObject, ObjectID, MaybeObjectID


class FastAssociation[V]:
    def __init__(self, keys: Tuple[str], values: Tuple[V | None]):
        self._keys: Tuple[str] = keys
        self._values: Tuple[V | None] = values

    def get_if_present(self, root_name: str) -> V | None:
        return self._values[self._keys.index(root_name)]

    def assigned_keys(self) -> Iterable[str]:
        for key, value in zip(self._keys, self._values):
            if value is not None:
                yield key

    def available_items(self) -> Iterable[Tuple[int, V]]:
        for i, value in enumerate(self._values):
            if value is not None:
                yield i, value


class TransformedRoots(FastAssociation[ObjectID]):
    def __init__(self, keys: Tuple[str], values: Tuple[MaybeObjectID]):
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


class MergePreferences:

    @abc.abstractmethod
    def combine_both_existing(
            self, path: List[str], original_roots: ByRoot[TreeObject | FileObject],
            staging_original: FileObject, base_original: FileObject) -> TransformedRoots:
        pass

    @abc.abstractmethod
    def combine_base_only(
            self, path: List[str], repo_name: str, original_roots: ByRoot[TreeObject | FileObject],
            base_original: FileObject) -> TransformedRoots:
        pass

    @abc.abstractmethod
    def combine_staging_only(
            self, path: List[str], repo_name, original_roots: ByRoot[TreeObject | FileObject],
            staging_original: FileObject) -> TransformedRoots:
        pass

    @abc.abstractmethod
    def merge_missing(self, path: List[str], original_roots: ByRoot[TreeObject | FileObject]) -> TransformedRoots:
        pass

    @abc.abstractmethod
    # fixme remove allowed_roots
    def create_result(self, allowed_roots: List[str], objects: Objects[FileObject]):
        pass


@dataclasses.dataclass
class ThreewayMergeState:
    path: List[str]
    base: FileObject | TreeObject | None
    staging: FileObject | TreeObject | None


class CombinedRoots[F](Transformed[F, ByRoot[ObjectID]]):
    def __init__(self, allowed_roots: List[str], empty_association: FastAssociation[ObjectID], objects: Objects[F]):
        self.allowed_roots = allowed_roots
        self.objects = objects

        self._merged_children: Dict[str, TreeObject] = dict()
        self.empty_association = empty_association
        # self._merged_children = FastAssociation[]

    def add_for_child(self, child_name: str, merged_child_by_roots: TransformedRoots) -> None:
        assert isinstance(merged_child_by_roots, TransformedRoots), type(merged_child_by_roots)
        for root_name, obj_id in merged_child_by_roots.HACK_items():
            if root_name not in self._merged_children:
                self._merged_children[root_name] = TreeObject({})

            self._merged_children[root_name].children[child_name] = obj_id

    def get_value(self) -> TransformedRoots:
        # store potential new objects
        for root_name, child_tree in self._merged_children.items():
            new_child_id = child_tree.id
            self.objects[new_child_id] = child_tree

        result = ByRoot[ObjectID](
            # self.empty_association._keys,
            self.allowed_roots,
            ((root_name, child_tree.id) for root_name, child_tree in self._merged_children.items()))

        return TransformedRoots.HACK_create(result)


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
            repo_name: str, merge_prefs: MergePreferences):
        self.objects = objects
        self.current_id = current_id
        self.staging_id = staging_id

        self.repo_name = repo_name

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
        return self.merge_prefs.create_result(self.allowed_roots, self.objects)

    def combine_non_drilldown(
            self, state: ThreewayMergeState, original: ByRoot[TreeObject | FileObject]) -> TransformedRoots:
        # we are on file level
        base_original = state.base
        staging_original = state.staging
        if base_original == staging_original:  # no diffs
            return TransformedRoots.HACK_create(original.map(lambda obj: obj.id))

        assert staging_original is None or isinstance(staging_original, FileObject)
        assert base_original is None or isinstance(base_original, FileObject)

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
            self, state: ThreewayMergeState, merged: CombinedRoots[FileObject],
            original: ByRoot[TreeObject | FileObject]) -> TransformedRoots:
        # tree-level, just return the merged
        # # fixme this is needed because empty folders get dropped in "merged" - should fix that problem
        # merged = merged.copy()
        # for root_name, original_obj in original.items():
        #     if merged.get_if_present(root_name) is None:
        #         merged[root_name] = original_obj.id
        return merged.get_value()
