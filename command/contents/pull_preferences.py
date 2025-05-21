import enum
import logging
from typing import List

from command.content_prefs import ContentPrefs
from command.fast_path import FastPosixPath
from config import HoardRemotes, CaveType
from contents.hoard import HoardContents
from contents.repo_props import FileDesc
from lmdb_storage.file_object import FileObject
from lmdb_storage.operations.three_way_merge import MergePreferences, TransformedRoots, FastAssociation, CombinedRoots
from lmdb_storage.operations.util import ByRoot
from lmdb_storage.tree_structure import TreeObject, ObjectID, Objects


class PullIntention(enum.Enum):
    FAIL = None
    ADD_TO_HOARD = "add_to_hoard"
    IGNORE = "ignore"
    CLEANUP = "cleanup"
    ADD_TO_HOARD_AND_CLEANUP = "add_to_hoard_and_cleanup"
    RESTORE_FROM_HOARD = "restore_from_hoard"
    MOVE_IN_HOARD = "move_in_hoard"
    DELETE_FROM_HOARD = "delete_from_hoard"
    ACCEPT_FROM_HOARD = "accept_from_hoard"


class PullPreferences:
    def __init__(
            self, local_uuid: str, on_same_file_is_present: PullIntention, on_file_added_or_present: PullIntention,
            on_file_is_different_and_modified: PullIntention, on_file_is_different_and_added: PullIntention,
            on_file_is_different_but_present: PullIntention, on_hoard_only_local_deleted: PullIntention,
            on_hoard_only_local_unknown: PullIntention, on_hoard_only_local_moved: PullIntention,
            force_fetch_local_missing: bool, force_reset_with_local_contents: bool, remote_type: CaveType):
        self.remote_type = remote_type
        self.local_uuid = local_uuid

        self.on_file_added_or_present = on_file_added_or_present

        self.force_fetch_local_missing = force_fetch_local_missing
        self.force_reset_with_local_contents = force_reset_with_local_contents


class PullMergePreferences(MergePreferences):
    def __init__(
            self, preferences: PullPreferences, content_prefs: ContentPrefs,
            remote_uuid: str, remote_type: CaveType, uuid_roots: List[str],
            roots_to_merge: List[str]):
        self.remote_uuid = remote_uuid
        self.remote_type = remote_type

        self.preferences = preferences
        self.content_prefs = content_prefs

        self._where_to_apply_adds = ["HOARD"] + uuid_roots

        self.roots_to_merge = roots_to_merge

        result_roots = tuple(set(self.roots_to_merge))
        self.empty_association = FastAssociation(result_roots, (None,) * len(result_roots))

    def create_result(self, objects: Objects[FileObject]):
        return CombinedRoots[FileObject](self.empty_association, objects)

    def where_to_apply_adds(self, path: List[str], staging_original: FileObject) -> List[str]:
        file_path = FastPosixPath("/" + "/".join(path))
        file_desc = FileDesc(staging_original.size, staging_original.fasthash, None)  # fixme add md5
        repos_to_add = self.content_prefs.repos_to_add(
            file_path,
            file_desc,
            None)
        base_to_add = ["HOARD", self.remote_uuid] if self.remote_type == CaveType.PARTIAL else ["HOARD"]
        return base_to_add + [r for r in repos_to_add if r in self._where_to_apply_adds]

    def combine_both_existing(self, path: List[str], original_roots: ByRoot[TreeObject | FileObject],
                              staging_original: FileObject, base_original: FileObject) -> TransformedRoots:
        original_roots = original_roots.subset(self.roots_to_merge)

        if staging_original.file_id == base_original.file_id:
            logging.error(f"Both staging and base staging for %s are identical, returns as-is.", path)
            return TransformedRoots.HACK_create(original_roots.map(lambda obj: obj.id))

        assert staging_original.file_id != base_original.file_id, staging_original.file_id

        if self.remote_type == CaveType.BACKUP:  # fixme use PullPreferences
            logging.error("Ignoring changes to %s coming from a backup repo!", path)

            result: ByRoot[ObjectID] = original_roots.map(lambda obj: obj.id)
            return result  # ignore changes coming from backups

        if self.remote_type == CaveType.INCOMING:  # fixme use PullPreferences
            # fixme lower
            logging.error("Ignoring changes to %s coming from an incoming repo!", path)
            return TransformedRoots.HACK_create(original_roots.map(lambda obj: obj.id))  # ignore changes coming from incoming

        result: ByRoot[ObjectID] = ByRoot(self.roots_to_merge)
        for merge_name in ["HOARD"] + list(original_roots.assigned_keys()):
            if merge_name in self.roots_to_merge:
                result[merge_name] = staging_original.file_id
        return TransformedRoots.HACK_create(result)

    def combine_base_only(self, path: List[str], repo_name: str, original_roots: ByRoot[TreeObject | FileObject],
                          base_original: FileObject) -> TransformedRoots:
        original_roots = original_roots.subset(self.roots_to_merge)

        if self.remote_type == CaveType.BACKUP:  # fixme use PullPreferences
            # fixme lower
            logging.error("Ignoring changes to %s coming from a backup repo!", path)
            return TransformedRoots.HACK_create(original_roots.map(lambda obj: obj.id))  # ignore changes coming from backups

        if self.preferences.force_fetch_local_missing:
            hoard_object = original_roots.get_if_present("HOARD")
            if hoard_object is not None:
                result: ByRoot[ObjectID] = original_roots.map(lambda obj: obj.id)
                result[repo_name] = hoard_object.id
                return TransformedRoots.HACK_create(result)

        return self.empty_association

    def combine_staging_only(
            self, path: List[str], repo_name: str, original_roots: ByRoot[TreeObject | FileObject],
            staging_original: FileObject) -> TransformedRoots:
        original_roots = original_roots.subset(self.roots_to_merge)

        hoard_object = original_roots.get_if_present("HOARD")
        if self.remote_type == CaveType.BACKUP:
            # fixme use PullPreferences
            logging.error("Ignoring changes to %s coming from a backup repo!", path)
            result: ByRoot[ObjectID] = original_roots.map(lambda obj: obj.id)

            if hoard_object is not None:  # there is a hoard object already
                result[repo_name] = hoard_object.id  # reset desired to currently existing object

            return TransformedRoots.HACK_create(result)  # ignore changes coming from backups
        elif self.remote_type == CaveType.INCOMING:
            if hoard_object is not None:
                return TransformedRoots.HACK_create(original_roots.map(lambda obj: obj.id))  # ignore from incoming
            else:
                return self.add_or_update_object(original_roots, path, staging_original)
        else:  # for partials, update object
            if hoard_object is not None and hoard_object.id == staging_original.id:
                # the repo is just recognizing it already has the object
                result: ByRoot[ObjectID] = original_roots.map(lambda obj: obj.id)
                result[repo_name] = staging_original.id
                return TransformedRoots.HACK_create(result)
            else:
                return self.add_or_update_object(original_roots, path, staging_original)

    def add_or_update_object(
            self, original_roots: ByRoot[TreeObject | FileObject], path: List[str],
            staging_original: FileObject) -> TransformedRoots:
        result: ByRoot[ObjectID] = original_roots.new()
        for merge_name in self.where_to_apply_adds(path, staging_original) + list(original_roots.assigned_keys()):
            result[merge_name] = staging_original.file_id
        return TransformedRoots.HACK_create(result)

    def merge_missing(self, path: List[str], original_roots: ByRoot[TreeObject | FileObject]) -> TransformedRoots:
        original_roots = original_roots.subset(self.roots_to_merge)
        return TransformedRoots.HACK_create(original_roots.map(lambda obj: obj.id))
