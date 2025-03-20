import logging
from abc import abstractmethod
from io import StringIO
from typing import List, Generator, Optional, Iterable

from config import HoardRemote, HoardConfig, CaveType
from contents_diff import FileMissingInHoard, FileIsSame, FileContentsDiffer, FileMissingInLocal
from contents.hoard import HoardContents
from contents.props import FileStatus, RepoFileProps, HoardFileProps
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


class BackupSet:
    def __init__(self, backups: List[HoardRemote], pathing: HoardPathing):
        self.backups = dict((backup.uuid, backup) for backup in backups)
        self.pathing = pathing

        self.num_backup_copies_desired = min(1, len(self.backups))
        if self.num_backup_copies_desired:
            logging.warning("No backups are defined.")

    def repos_to_backup_to(
            self, hoard_file: str, local_props: RepoFileProps,
            hoard_props: Optional[HoardFileProps] = None) -> Iterable[HoardRemote]:

        past_backups = list(self._find_past_backups(hoard_file, hoard_props))

        logging.info(f"Got {len(past_backups)} currently requested backups for {hoard_file}.")
        if len(past_backups) >= self.num_backup_copies_desired:
            logging.info(
                f"Skipping {hoard_file}, requested backups {len(past_backups)} >= {self.num_backup_copies_desired}")
            return []

        num_backups_to_request = self.num_backup_copies_desired - len(past_backups)

        new_possible_backups = list(self._find_new_backups(hoard_file, hoard_props, past_backups))

        if len(new_possible_backups) < num_backups_to_request:
            logging.error(
                f"Need at least {num_backups_to_request} backup media to satisfy, has only {len(new_possible_backups)} remaining.")

        logging.info(
            f"Returning {min(num_backups_to_request, len(new_possible_backups))} new backups "
            f"from requested {num_backups_to_request}.")
        return map(lambda r: r.uuid, new_possible_backups[:num_backups_to_request])

    def _find_past_backups(
            self, hoard_file: str, hoard_props: Optional[HoardFileProps]) -> Iterable[HoardRemote]:
        if hoard_props is None:
            return

        for uuid in hoard_props.repos_having_status(FileStatus.GET, FileStatus.COPY, FileStatus.AVAILABLE):
            if uuid not in self.backups:
                continue

            if not is_path_available(self.pathing, hoard_file, uuid):
                continue

            yield self.backups[uuid]

    def _find_new_backups(self, hoard_file: str, hoard_props: Optional[HoardFileProps],
                          past_backups: List[HoardRemote]) -> Iterable[HoardRemote]:

        # FIXME implement balancing logic e.g. by ordering as % empty and checking file size
        for uuid, backup in self.backups.items():
            if not is_path_available(self.pathing, hoard_file, uuid):
                continue

            if backup in past_backups:
                continue

            yield backup


class ContentPrefs:
    def __init__(self, config: HoardConfig, pathing: HoardPathing):
        self.config = config
        self._partials_with_fetch_new: List[HoardRemote] = [
            r for r in config.remotes.all() if
            r.type == CaveType.PARTIAL and r.fetch_new]

        self._backup_set = BackupSet([r for r in config.remotes.all() if r.type == CaveType.BACKUP], pathing)
        self.pathing = pathing

    def repos_to_add(
            self, hoard_file: str, local_props: RepoFileProps,
            hoard_props: Optional[HoardFileProps] = None) -> Generator[HoardRemote, None, None]:
        for r in self._partials_with_fetch_new:
            if is_path_available(self.pathing, hoard_file, r.uuid):
                yield r.uuid

        yield from self._backup_set.repos_to_backup_to(hoard_file, local_props, hoard_props)


def is_path_available(pathing, hoard_file: str, repo: str) -> bool:
    return pathing.in_hoard(hoard_file).at_local(repo) is not None


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
        hoard_props.set_status(self.content_prefs.repos_to_add(diff.hoard_file, diff.local_props), FileStatus.GET)

        # set status here
        hoard_props.mark_available(self.remote_uuid)

        out.write(f"+{diff.hoard_file}\n")

    def handle_file_is_same(self, diff: "FileIsSame", out: StringIO):
        goal_status = diff.hoard_props.get_status(self.remote_uuid)
        if goal_status == FileStatus.CLEANUP:
            logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
            out.write(f"?{diff.hoard_file}\n")
        elif goal_status == FileStatus.GET or goal_status == FileStatus.COPY or goal_status == FileStatus.UNKNOWN:
            logging.info(f"mark {diff.hoard_file} as available here!")
            diff.hoard_props.mark_available(self.remote_uuid)
            out.write(f"={diff.hoard_file}\n")
        elif goal_status == FileStatus.AVAILABLE:
            pass
        else:
            raise ValueError(f"unrecognized hoard state for {diff.hoard_file}: {goal_status}")

    def handle_file_contents_differ(self, diff: FileContentsDiffer, out: StringIO):
        goal_status = diff.hoard_props.get_status(self.remote_uuid)
        if goal_status == FileStatus.AVAILABLE:
            # file was changed in-place, but is different now FIXME should that always happen?
            self._reset_local_as_current(diff)

            out.write(f"u{diff.hoard_file}\n")
        elif goal_status == FileStatus.UNKNOWN:  # fixme this should disappear if we track repository contents
            if self.assume_current:
                # file is added as different than what is in the hoard
                self._reset_local_as_current(diff)

                out.write(f"RESETTING {diff.hoard_file}\n")
            else:
                logging.info(f"Current file is different, but won't be added because --assume-current == False")
                out.write(f"IGNORE_DIFF {diff.hoard_file}\n")
        elif goal_status == FileStatus.CLEANUP:
            if self.assume_current:
                self._reset_local_as_current(diff)
                out.write(f"RESETTING {diff.hoard_file}\n")
            else:
                logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
                out.write(f"?{diff.hoard_file}\n")
        elif goal_status == FileStatus.GET or goal_status == FileStatus.COPY:
            if self.assume_current:
                self._reset_local_as_current(diff)
                out.write(f"RESETTING {diff.hoard_file}\n")
            else:
                logging.info(f"current file is out of date and was marked for restore: {diff.hoard_file}")
                out.write(f"g{diff.hoard_file}\n")

    def _reset_local_as_current(self, diff: FileContentsDiffer):
        past_available = diff.hoard_props.by_statuses(FileStatus.AVAILABLE, FileStatus.GET, FileStatus.COPY)

        hoard_props = self.hoard.fsobjects.add_or_replace_file(diff.hoard_file, diff.local_props)
        hoard_props.mark_to_get(past_available)
        hoard_props.mark_available(self.remote_uuid)

    def handle_hoard_only(self, diff: FileMissingInLocal, out: StringIO):
        goal_status = diff.hoard_props.get_status(self.remote_uuid)
        if goal_status == FileStatus.CLEANUP:
            logging.info(f"file had been deleted.")
            diff.hoard_props.remove_status(self.remote_uuid)
        elif goal_status == FileStatus.AVAILABLE:  # file was here, is no longer
            if self.force_fetch_local_missing:
                logging.info(f"file {diff.hoard_file} is missing, restoring due to --force-fetch-local-missing")

                diff.hoard_props.mark_to_get([self.remote_uuid])
                out.write(f"g{diff.hoard_file}\n")
            else:
                logging.info(f"deleting file {diff.hoard_file} as is no longer in local")
                diff.hoard_props.mark_to_delete_everywhere()
                diff.hoard_props.remove_status(self.remote_uuid)
                out.write(f"-{diff.hoard_file}\n")
        elif goal_status == FileStatus.GET:
            logging.info(f"file fetch had been scheduled already.")
        elif goal_status == FileStatus.UNKNOWN:
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
        hoard_file = self.hoard.fsobjects.add_or_replace_file(diff.hoard_file, diff.local_props)
        # add status for new repos
        hoard_file.set_status(self.content_prefs.repos_to_add(diff.hoard_file, diff.local_props), FileStatus.GET)
        logging.info(f"marking {diff.hoard_file} for cleanup from {self.remote_uuid}")
        hoard_file.mark_for_cleanup([self.remote_uuid])

    def handle_file_is_same(self, diff: FileIsSame, out: StringIO):
        logging.info(f"incoming file is already recorded in hoard.")
        logging.info(f"marking {diff.hoard_file} for cleanup from {self.remote_uuid}")
        diff.hoard_props.mark_for_cleanup([self.remote_uuid])

        out.write(f"-{diff.hoard_file}\n")

    def handle_file_contents_differ(self, diff: FileContentsDiffer, out: StringIO):
        self._move_to_other_caves(diff, out)

        out.write(f"u{diff.hoard_file}\n")

    def handle_hoard_only(self, diff: FileMissingInLocal, out: StringIO):
        logging.info(f"skipping file not in local.")
        goal_status = diff.hoard_props.get_status(self.remote_uuid)
        if goal_status == FileStatus.CLEANUP:
            diff.hoard_props.remove_status(self.remote_uuid)
        elif goal_status == FileStatus.UNKNOWN:
            pass  # ignore file
        else:
            logging.error(f"File in hoard only, but status is not {FileStatus.CLEANUP}")
            out.write(f"E{diff.hoard_file}\n")


class BackupDiffHandler(DiffHandler):
    def handle_local_only(self, diff: FileMissingInHoard, out: StringIO):
        logging.info(f"skipping obsolete file from backup: {diff.hoard_file}")
        out.write(f"?{diff.hoard_file}\n")

    def handle_file_is_same(self, diff: FileIsSame, out: StringIO):
        logging.info(f"file already backed up ... skipping.")

        goal_status = diff.hoard_props.get_status(self.remote_uuid)
        if goal_status == FileStatus.GET or goal_status == FileStatus.UNKNOWN:
            diff.hoard_props.mark_available(self.remote_uuid)
            out.write(f"={diff.hoard_file}\n")

    def handle_file_contents_differ(self, diff: FileContentsDiffer, out: StringIO):
        goal_status = diff.hoard_props.get_status(self.remote_uuid)
        if goal_status == FileStatus.AVAILABLE:  # was backed-up here, get it again
            out.write(f"g{diff.hoard_file}\n")
            props = diff.hoard_props
            props.mark_to_get([self.remote_uuid])

    def handle_hoard_only(self, diff: FileMissingInLocal, out: StringIO):
        goal_status = diff.hoard_props.get_status(self.remote_uuid)
        if goal_status == FileStatus.AVAILABLE:  # was backed-up here, get it again
            out.write(f"g{diff.hoard_file}\n")
            props = diff.hoard_props
            props.mark_to_get([self.remote_uuid])
        elif goal_status == FileStatus.CLEANUP:  # file already deleted
            diff.hoard_props.remove_status(self.remote_uuid)
        elif goal_status == FileStatus.GET:
            pass
        elif goal_status == FileStatus.UNKNOWN:
            file_props = diff.hoard_props
            file_props.mark_to_get([self.remote_uuid])
        else:
            raise NotImplementedError(f"Unrecognized goal status {goal_status}")
