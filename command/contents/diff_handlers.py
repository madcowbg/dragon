import logging
from abc import abstractmethod
from io import StringIO
from typing import List

from contents_diff import FileOnlyInLocalAdded, FileOnlyInLocalPresent, FileIsSame, FileContentsDiffer, \
    FileOnlyInHoardLocalDeleted, \
    FileOnlyInHoardLocalUnknown, FileOnlyInHoardLocalMoved
from contents.hoard import HoardContents
from contents.hoard_props import HoardFileStatus, HoardFileProps


class DiffHandler:
    def __init__(self, remote_uuid: str, hoard: HoardContents):
        self.remote_uuid = remote_uuid
        self.hoard = hoard

    @abstractmethod
    def handle_local_only(self, diff: FileOnlyInLocalAdded | FileOnlyInLocalPresent, out: StringIO): pass

    @abstractmethod
    def handle_file_is_same(self, diff: "FileIsSame", out: StringIO): pass

    @abstractmethod
    def handle_file_contents_differ(self, diff: FileContentsDiffer, out: StringIO): pass

    @abstractmethod
    def handle_hoard_only_deleted(self, diff: FileOnlyInHoardLocalDeleted, out: StringIO): pass

    @abstractmethod
    def handle_hoard_only_unknown(self, diff: FileOnlyInHoardLocalUnknown, out: StringIO): pass

    @abstractmethod
    def handle_hoard_only_moved(
            self, diff: FileOnlyInHoardLocalMoved, hoard_new_path: str, hoard_new_path_props: HoardFileProps,
            other_remotes_wanting_new_file: List[str], out: StringIO): pass


class BackupDiffHandler(DiffHandler):
    def handle_local_only(self, diff: FileOnlyInLocalAdded | FileOnlyInLocalPresent, out: StringIO):
        logging.info(f"skipping obsolete file from backup: {diff.hoard_file}")
        out.write(f"?{diff.hoard_file}\n")

    def handle_file_is_same(self, diff: FileIsSame, out: StringIO):
        logging.info(f"file already backed up ... skipping.")

        goal_status = diff.hoard_props.get_status(self.remote_uuid)
        if goal_status == HoardFileStatus.GET or goal_status == HoardFileStatus.COPY or goal_status == HoardFileStatus.UNKNOWN:
            diff.hoard_props.mark_available(self.remote_uuid)
            out.write(f"={diff.hoard_file}\n")

    def handle_file_contents_differ(self, diff: FileContentsDiffer, out: StringIO):
        goal_status = diff.hoard_props.get_status(self.remote_uuid)
        if goal_status == HoardFileStatus.AVAILABLE:  # was backed-up here, get it again
            props = diff.hoard_props
            props.mark_to_get([self.remote_uuid])

            out.write(f"g{diff.hoard_file}\n")

    def handle_hoard_only_deleted(self, diff: FileOnlyInHoardLocalDeleted | FileOnlyInHoardLocalUnknown, out: StringIO):
        goal_status = diff.hoard_props.get_status(self.remote_uuid)
        if goal_status == HoardFileStatus.AVAILABLE:  # was backed-up here, get it again
            props = diff.hoard_props
            props.mark_to_get([self.remote_uuid])

            out.write(f"g{diff.hoard_file}\n")
        elif goal_status == HoardFileStatus.CLEANUP:  # file already deleted
            diff.hoard_props.remove_status(self.remote_uuid)
        elif goal_status == HoardFileStatus.GET or goal_status == HoardFileStatus.COPY:
            pass
        elif goal_status == HoardFileStatus.UNKNOWN:
            logging.info("File not recognized by this backup, skipping")
        else:
            raise NotImplementedError(f"Unrecognized goal status {goal_status}")

    def handle_hoard_only_unknown(self, diff: FileOnlyInHoardLocalUnknown, out: StringIO):
        self.handle_hoard_only_deleted(diff, out)  # todo implement different case, e.g. do not add or remove

    def handle_hoard_only_moved(
            self, diff: FileOnlyInHoardLocalMoved, hoard_new_path: str, hoard_new_path_props: HoardFileProps,
            other_remotes_wanting_new_file: List[str], out: StringIO):
        raise NotImplementedError()
