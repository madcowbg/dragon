import logging
from abc import abstractmethod
from io import StringIO
from typing import List

from config import HoardRemote
from contents_diff import FileMissingInHoard, FileIsSame, FileContentsDiffer, FileMissingInLocal
from contents.hoard import HoardContents
from contents.props import FileStatus
from command.pathing import HoardPathing


class DiffHandler:
    def __init__(self, remote_uuid: str, hoard: HoardContents):
        self.remote_uuid = remote_uuid
        self.hoard = hoard

    @abstractmethod
    def handle_local_only(self, diff: "FileMissingInHoard", out: StringIO): pass

    @abstractmethod
    def handle_file_is_same(self, diff: "FileIsSame", out: StringIO): pass

    @abstractmethod
    def handle_file_contents_differ(self, diff: FileContentsDiffer, out: StringIO): pass

    @abstractmethod
    def handle_hoard_only(self, diff: FileMissingInLocal, out: StringIO): pass


def filter_accessible(pathing: HoardPathing, repos: List[HoardRemote], hoard_file: str) -> List[str]:
    return [r.uuid for r in repos if pathing.in_hoard(hoard_file).at_local(r.uuid) is not None]


class PartialDiffHandler(DiffHandler):
    def __init__(
            self, remote_uuid: str, hoard: HoardContents, repos_to_add_new_files: List[HoardRemote],
            fetch_new: bool, pathing: HoardPathing, force_fetch_local_missing: bool):
        super().__init__(remote_uuid, hoard)
        self.repos_to_add_new_files = repos_to_add_new_files
        self.fetch_new = fetch_new
        self.pathing = pathing
        self.force_fetch_local_missing = force_fetch_local_missing

    def handle_local_only(self, diff: "FileMissingInHoard", out: StringIO):
        out.write(f"+{diff.hoard_file}\n")
        self.hoard.fsobjects.add_new_file(
            diff.hoard_file, diff.local_props,
            current_uuid=self.remote_uuid,
            repos_to_add_new_files=filter_accessible(self.pathing, self.repos_to_add_new_files, diff.hoard_file))

    def handle_file_is_same(self, diff: "FileIsSame", out: StringIO):
        goal_status = diff.hoard_props.status(self.remote_uuid)
        if goal_status == FileStatus.CLEANUP:
            logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
            out.write(f"?{diff.hoard_file}\n")
        elif goal_status == FileStatus.GET or goal_status == FileStatus.UNKNOWN:
            logging.info(f"mark {diff.hoard_file} as available here!")
            diff.hoard_props.mark_available(self.remote_uuid)
            out.write(f"={diff.hoard_file}\n")
        elif goal_status == FileStatus.AVAILABLE:
            pass
        else:
            raise ValueError(f"unrecognized hoard state for {diff.hoard_file}: {goal_status}")

    def handle_file_contents_differ(self, diff: FileContentsDiffer, out: StringIO):
        goal_status = diff.hoard_props.status(self.remote_uuid)
        if goal_status == FileStatus.CLEANUP:
            logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
            out.write(f"?{diff.hoard_file}\n")
        elif goal_status == FileStatus.GET:
            logging.info(f"current file is out of date and was marked for restore: {diff.hoard_file}")
            out.write(f"g{diff.hoard_file}\n")
        elif goal_status == FileStatus.AVAILABLE:  # file was changed in-place
            diff.hoard_props.replace_file(diff.local_props, self.remote_uuid)
            out.write(f"u{diff.hoard_file}\n")
            diff.hoard_props.mark_to_get(self.remote_uuid)

    def handle_hoard_only(self, diff: FileMissingInLocal, out: StringIO):
        goal_status = diff.hoard_props.status(self.remote_uuid)
        if goal_status == FileStatus.CLEANUP:
            logging.info(f"file had been deleted.")
            diff.hoard_props.remove_status(self.remote_uuid)
        elif goal_status == FileStatus.AVAILABLE:  # file was here, is no longer
            if self.force_fetch_local_missing:
                logging.info(f"file {diff.hoard_file} is missing, restoring due to --force-fetch-local-missing")

                diff.hoard_props.mark_to_get(self.remote_uuid)
                out.write(f"g{diff.hoard_file}\n")
            else:
                logging.info(f"deleting file {diff.hoard_file} as is no longer in local")
                diff.hoard_props.mark_to_delete()
                diff.hoard_props.remove_status(self.remote_uuid)
                out.write(f"-{diff.hoard_file}\n")
        elif goal_status == FileStatus.GET:
            logging.info(f"file fetch had been scheduled already.")
        elif goal_status == FileStatus.UNKNOWN:
            logging.info(f"file not related to repo, skipping!")
        else:
            raise NotImplementedError(f"Unrecognized goal status {goal_status}")


class IncomingDiffHandler(DiffHandler):
    def __init__(
            self, remote_uuid: str, hoard: HoardContents,
            repos_to_add_new_files: List[HoardRemote], pathing: HoardPathing):
        super().__init__(remote_uuid, hoard)
        self.repos_to_add_new_files = repos_to_add_new_files
        self.pathing = pathing

    def handle_local_only(self, diff: FileMissingInHoard, out: StringIO):
        out.write(f"<+{diff.hoard_file}\n")
        hoard_file = self.hoard.fsobjects.add_new_file(
            diff.hoard_file, diff.local_props,
            current_uuid=self.remote_uuid,
            repos_to_add_new_files=filter_accessible(self.pathing, self.repos_to_add_new_files, diff.hoard_file))
        logging.info(f"marking {diff.hoard_file} for cleanup from {self.remote_uuid}")
        hoard_file.mark_for_cleanup(repo_uuid=self.remote_uuid)

    def handle_file_is_same(self, diff: FileIsSame, out: StringIO):
        logging.info(f"incoming file is already recorded in hoard.")
        logging.info(f"marking {diff.hoard_file} for cleanup from {self.remote_uuid}")
        out.write(f"-{diff.hoard_file}\n")
        diff.hoard_props.mark_for_cleanup(repo_uuid=self.remote_uuid)

    def handle_file_contents_differ(self, diff: FileContentsDiffer, out: StringIO):
        goal_status = diff.hoard_props.status(self.remote_uuid)
        if goal_status == FileStatus.CLEANUP:  # is already marked for deletion
            logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
            out.write(f"?{diff.hoard_file}\n")
        else:  # file was changed in-place
            diff.hoard_props.replace_file(diff.local_props, self.remote_uuid)
            out.write(f"u{diff.hoard_file}\n")
            diff.hoard_props.mark_for_cleanup(self.remote_uuid)

    def handle_hoard_only(self, diff: FileMissingInLocal, out: StringIO):
        logging.info(f"skipping file not in local.")
        if diff.hoard_props.status(self.remote_uuid) == FileStatus.CLEANUP:
            diff.hoard_props.remove_status(self.remote_uuid)


class BackupDiffHandler(DiffHandler):
    def handle_local_only(self, diff: FileMissingInHoard, out: StringIO):
        logging.info(f"skipping obsolete file from backup: {diff.hoard_file}")
        out.write(f"?{diff.hoard_file}\n")

    def handle_file_is_same(self, diff: FileIsSame, out: StringIO):
        logging.info(f"file already backed up ... skipping.")

        goal_status = diff.hoard_props.status(self.remote_uuid)
        if goal_status == FileStatus.GET or goal_status == FileStatus.UNKNOWN:
            diff.hoard_props.mark_available(self.remote_uuid)
            out.write(f"={diff.hoard_file}\n")

    def handle_file_contents_differ(self, diff: FileContentsDiffer, out: StringIO):
        goal_status = diff.hoard_props.status(self.remote_uuid)
        if goal_status == FileStatus.AVAILABLE:  # was backed-up here, get it again
            out.write(f"g{diff.hoard_file}\n")
            diff.hoard_props.mark_to_get(self.remote_uuid)

    def handle_hoard_only(self, diff: FileMissingInLocal, out: StringIO):
        goal_status = diff.hoard_props.status(self.remote_uuid)
        if goal_status == FileStatus.AVAILABLE:  # was backed-up here, get it again
            out.write(f"g{diff.hoard_file}\n")
            diff.hoard_props.mark_to_get(self.remote_uuid)
        elif goal_status == FileStatus.CLEANUP:  # file already deleted
            diff.hoard_props.remove_status(self.remote_uuid)
        elif goal_status == FileStatus.GET:
            pass
        elif goal_status == FileStatus.UNKNOWN:
            diff.hoard_props.mark_to_get(self.remote_uuid)  # fixme make into a backup set
        else:
            raise NotImplementedError(f"Unrecognized goal status {goal_status}")
