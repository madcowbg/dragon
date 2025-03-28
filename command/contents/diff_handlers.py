import logging
from abc import abstractmethod
from io import StringIO

from command.content_prefs import ContentPrefs
from contents_diff import FileMissingInHoard, FileIsSame, FileContentsDiffer, FileMissingInLocal
from contents.hoard import HoardContents
from contents.repo_props import RepoFileProps
from contents.hoard_props import HoardFileStatus, HoardFileProps


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


def reset_local_as_current(
        hoard: HoardContents, remote_uuid: str, hoard_file: str, hoard_props: HoardFileProps,
        local_props: RepoFileProps):
    past_available = hoard_props.by_statuses(HoardFileStatus.AVAILABLE, HoardFileStatus.GET, HoardFileStatus.COPY)

    hoard_props = hoard.fsobjects.add_or_replace_file(hoard_file, local_props)
    hoard_props.mark_to_get(past_available)
    hoard_props.mark_available(remote_uuid)


class PartialDiffHandler(DiffHandler):
    def __init__(
            self, remote_uuid: str, hoard: HoardContents, content_prefs: ContentPrefs,
            force_fetch_local_missing: bool, assume_current: bool):
        super().__init__(remote_uuid, hoard)
        self.content_prefs = content_prefs
        self.force_fetch_local_missing = force_fetch_local_missing
        self.assume_current = assume_current

    def handle_local_only(self, diff: "FileMissingInHoard", out: StringIO):
        hoard_props = self.hoard.fsobjects.add_or_replace_file(diff.hoard_file, diff.local_props)

        # add status for new repos
        hoard_props.set_status(self.content_prefs.repos_to_add(diff.hoard_file, diff.local_props), HoardFileStatus.GET)

        # set status here
        hoard_props.mark_available(self.remote_uuid)

        out.write(f"+{diff.hoard_file}\n")

    def handle_file_is_same(self, diff: "FileIsSame", out: StringIO):
        goal_status = diff.hoard_props.get_status(self.remote_uuid)
        if goal_status == HoardFileStatus.CLEANUP:
            logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
            out.write(f"?{diff.hoard_file}\n")
        elif goal_status == HoardFileStatus.GET or goal_status == HoardFileStatus.COPY or goal_status == HoardFileStatus.UNKNOWN:
            logging.info(f"mark {diff.hoard_file} as available here!")
            diff.hoard_props.mark_available(self.remote_uuid)
            out.write(f"={diff.hoard_file}\n")
        elif goal_status == HoardFileStatus.AVAILABLE:
            pass
        else:
            raise ValueError(f"unrecognized hoard state for {diff.hoard_file}: {goal_status}")

    def handle_file_contents_differ(self, diff: FileContentsDiffer, out: StringIO):
        goal_status = diff.hoard_props.get_status(self.remote_uuid)
        if goal_status == HoardFileStatus.AVAILABLE:
            # file was changed in-place, but is different now FIXME should that always happen?
            reset_local_as_current(self.hoard, self.remote_uuid, diff.hoard_file, diff.hoard_props, diff.local_props)

            out.write(f"u{diff.hoard_file}\n")
        elif goal_status == HoardFileStatus.UNKNOWN:  # fixme this should disappear if we track repository contents
            if self.assume_current:
                # file is added as different than what is in the hoard
                reset_local_as_current(
                    self.hoard, self.remote_uuid, diff.hoard_file, diff.hoard_props, diff.local_props)

                out.write(f"RESETTING {diff.hoard_file}\n")
            else:
                logging.info(f"Current file is different, but won't be added because --assume-current == False")
                out.write(f"IGNORE_DIFF {diff.hoard_file}\n")
        elif goal_status == HoardFileStatus.CLEANUP:
            if self.assume_current:
                reset_local_as_current(
                    self.hoard, self.remote_uuid, diff.hoard_file, diff.hoard_props, diff.local_props)
                out.write(f"RESETTING {diff.hoard_file}\n")
            else:
                logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
                out.write(f"?{diff.hoard_file}\n")
        elif goal_status == HoardFileStatus.GET or goal_status == HoardFileStatus.COPY:
            if self.assume_current:
                reset_local_as_current(
                    self.hoard, self.remote_uuid, diff.hoard_file, diff.hoard_props, diff.local_props)
                out.write(f"RESETTING {diff.hoard_file}\n")
            else:
                logging.info(f"current file is out of date and was marked for restore: {diff.hoard_file}")
                out.write(f"g{diff.hoard_file}\n")

    def handle_hoard_only(self, diff: FileMissingInLocal, out: StringIO):
        goal_status = diff.hoard_props.get_status(self.remote_uuid)
        if goal_status == HoardFileStatus.CLEANUP:
            logging.info(f"file had been deleted.")
            diff.hoard_props.remove_status(self.remote_uuid)
        elif goal_status == HoardFileStatus.AVAILABLE:  # file was here, is no longer
            if self.force_fetch_local_missing:
                logging.info(f"file {diff.hoard_file} is missing, restoring due to --force-fetch-local-missing")

                diff.hoard_props.mark_to_get([self.remote_uuid])
                out.write(f"g{diff.hoard_file}\n")
            else:
                logging.info(f"deleting file {diff.hoard_file} as is no longer in local")
                diff.hoard_props.mark_to_delete_everywhere()
                diff.hoard_props.remove_status(self.remote_uuid)
                out.write(f"-{diff.hoard_file}\n")
        elif goal_status == HoardFileStatus.GET:
            logging.info(f"file fetch had been scheduled already.")
        elif goal_status == HoardFileStatus.UNKNOWN:
            logging.info(f"file not related to repo, skipping!")
        else:
            raise NotImplementedError(f"Unrecognized goal status {goal_status}")


class IncomingDiffHandler(DiffHandler):
    def __init__(self, remote_uuid: str, hoard: HoardContents, content_prefs: ContentPrefs):
        super().__init__(remote_uuid, hoard)
        self.content_prefs = content_prefs

    def handle_local_only(self, diff: FileMissingInHoard, out: StringIO):
        self._move_to_other_caves(diff, out)

        out.write(f"<+{diff.hoard_file}\n")

    def _move_to_other_caves(self, diff: FileMissingInHoard | FileContentsDiffer, out: StringIO):
        hoard_props = self.hoard.fsobjects.add_or_replace_file(diff.hoard_file, diff.local_props)

        # add status for new repos
        hoard_props.set_status(list(self.content_prefs.repos_to_add(diff.hoard_file, diff.local_props)), HoardFileStatus.GET)

        self._safe_mark_for_cleanup(diff, hoard_props, out)

    def _safe_mark_for_cleanup(self, diff: FileMissingInHoard | FileContentsDiffer | FileIsSame, hoard_file: HoardFileProps, out: StringIO):
        logging.info(f"safe marking {diff.hoard_file} for cleanup from {self.remote_uuid}")

        repos_to_get_file = hoard_file.by_statuses(HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.AVAILABLE)
        if self.remote_uuid in repos_to_get_file:
            repos_to_get_file.remove(self.remote_uuid)
        if len(repos_to_get_file) > 0:
            logging.info(f"marking {diff.hoard_file} for cleanup from {self.remote_uuid}")
            hoard_file.mark_for_cleanup([self.remote_uuid])
        else:
            logging.error(f"No place to get {diff.hoard_file}, will NOT cleanup.")
            hoard_file.mark_available(self.remote_uuid)

            out.write(f"~{diff.hoard_file}\n")

    def handle_file_is_same(self, diff: FileIsSame, out: StringIO):
        logging.info(f"incoming file is already recorded in hoard.")

        # add status for new repos
        already_available = diff.hoard_props.by_status(HoardFileStatus.AVAILABLE)
        repos_to_add = [
            uuid for uuid in self.content_prefs.repos_to_add(diff.hoard_file, diff.local_props)
            if uuid not in already_available]

        # add status for new repos
        diff.hoard_props.set_status(repos_to_add, HoardFileStatus.GET)

        self._safe_mark_for_cleanup(diff, diff.hoard_props, out)
        out.write(f"-{diff.hoard_file}\n")

    def handle_file_contents_differ(self, diff: FileContentsDiffer, out: StringIO):
        logging.info(f"incoming file has different contents.")

        self._move_to_other_caves(diff, out)
        out.write(f"u{diff.hoard_file}\n")

    def handle_hoard_only(self, diff: FileMissingInLocal, out: StringIO):
        logging.info(f"skipping file not in local.")
        goal_status = diff.hoard_props.get_status(self.remote_uuid)
        if goal_status == HoardFileStatus.CLEANUP:
            diff.hoard_props.remove_status(self.remote_uuid)
        elif goal_status == HoardFileStatus.UNKNOWN:
            pass  # ignore file
        else:
            logging.error(f"File in hoard only, but status is not {HoardFileStatus.CLEANUP}")
            out.write(f"E{diff.hoard_file}\n")


class BackupDiffHandler(DiffHandler):
    def handle_local_only(self, diff: FileMissingInHoard, out: StringIO):
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

    def handle_hoard_only(self, diff: FileMissingInLocal, out: StringIO):
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
