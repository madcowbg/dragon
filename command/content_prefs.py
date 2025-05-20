import logging
from io import StringIO

from propcache import cached_property

from command.fast_path import FastPosixPath
from typing import List, Optional, Generator, Dict

from command.pathing import HoardPathing, is_path_available
from config import HoardRemote, HoardConfig, CaveType
from contents.hoard import HoardContents
from contents.repo_props import FileDesc
from contents.hoard_props import HoardFileStatus, HoardFileProps
from util import format_percent, format_size

MIN_REPO_PERC_FREE = 0.02


class Size:
    def __init__(self, backup: HoardRemote, hoard: HoardContents):
        self.backup = backup

        # fixme make it more dynamic, as usage can change over time
        self.reserved = 0
        self.total_size = max(1, hoard.config.max_size(backup.uuid))
        used_size = hoard.fsobjects.used_size(backup.uuid)
        self.current_free_size = self.total_size - used_size

        logging.debug(
            f"Space for {backup.name}: "
            f"{format_percent(self.current_free_size / float(self.total_size))} = "
            f"{format_size(self.current_free_size)} / {format_size(self.total_size)}")

    @property
    def remaining_pct(self):
        remaining_size = self.current_free_size - self.reserved
        total_size = self.total_size

        return remaining_size / float(total_size)


class BackupSizes:
    def __init__(self, backups: Dict[str, HoardRemote], hoard: HoardContents):
        self.backups = backups
        self.sizes = dict((b.uuid, Size(b, hoard)) for b in self.backups.values())

    def reserve_size(self, remote: HoardRemote, size: int):
        logging.debug(f"Reserving {format_size(size)} on {remote}")
        self.sizes[remote.uuid].reserved += size

    def remaining_pct(self, remote: HoardRemote) -> float:
        return self.sizes[remote.uuid].remaining_pct


class BackupSet:
    def __init__(self, mounted_at: FastPosixPath, backups: List[HoardRemote], pathing: HoardPathing,
                 hoard: HoardContents, available_remotes: List[str]):
        self.backups = dict((backup.uuid, backup) for backup in backups)
        self.available_backups = set(backup.uuid for backup in backups if backup.uuid in available_remotes)

        self.uuids = sorted(list(self.backups.keys()))
        self.pathing = pathing

        self.backup_sizes = BackupSizes(self.backups, hoard)

        self.mounted_at = mounted_at

        self.num_backup_copies_desired = min(1, len(self.backups))
        if self.num_backup_copies_desired == 0:
            logging.warning("No backups are defined.")

    def repos_to_backup_to(
            self, hoard_file: FastPosixPath, hoard_props: Optional[HoardFileProps], file_size: int,
            available_only: bool) -> List[HoardRemote]:

        past_backups = self.currently_scheduled_backups(hoard_file, hoard_props) if hoard_props is not None else []

        logging.info(f"Got {len(past_backups)} currently requested backups for {hoard_file}.")
        if len(past_backups) >= self.num_backup_copies_desired:
            logging.info(
                f"Skipping {hoard_file}, requested backups {len(past_backups)} >= {self.num_backup_copies_desired}")
            return []

        return self.reserve_new_backups(hoard_file, file_size, past_backups, available_only)

    def repos_to_clean(self, hoard_file: FastPosixPath, hoard_props: Optional[HoardFileProps], file_size: int) -> List[
        HoardRemote]:
        assert hoard_file.is_absolute()

        past_backups = self.currently_scheduled_backups(hoard_file, hoard_props) if hoard_props is not None else []

        logging.info(f"Got {len(past_backups)} currently requested backups for {hoard_file}.")

        if len(past_backups) <= self.num_backup_copies_desired:
            logging.info(
                f"Retaining {hoard_file}, has only {len(past_backups)} backups "
                f"out of requested {self.num_backup_copies_desired}.")
            return []

        num_backups_to_remove = len(past_backups) - self.num_backup_copies_desired
        assert num_backups_to_remove > 0

        def _available_are_largest(backup: HoardRemote) -> (float, str):
            current_status = hoard_props.get_status(backup.uuid)
            if current_status == HoardFileStatus.CLEANUP:
                return 0.0, backup.uuid
            elif current_status == HoardFileStatus.GET or current_status == HoardFileStatus.COPY:
                return 1.0, backup.uuid
            elif current_status == HoardFileStatus.AVAILABLE:
                return (
                    10 - self.backup_sizes.remaining_pct(backup),  # 9 means empty remote, 10 means full
                    backup.uuid)
            else:
                raise ValueError(f"Unknown backup status {current_status} for backup UUID {backup.uuid}")

        sorted_to_remove = sorted(past_backups, key=_available_are_largest)
        logging.info(f"{hoard_file} has {len(sorted_to_remove)} backups to remove from.")

        remotes_to_remove = sorted_to_remove[:num_backups_to_remove]
        for remote in remotes_to_remove:
            self.backup_sizes.reserve_size(remote, -file_size)
        return remotes_to_remove

    def currently_scheduled_backups(self, hoard_file: FastPosixPath, hoard_props: HoardFileProps) -> List[HoardRemote]:
        return [
            self.backups[uuid] for uuid in hoard_props.repos_having_status(*STATUSES_DECLARED_TO_FETCH)
            if uuid in self.backups and is_path_available(self.pathing, hoard_file, uuid)]

    def reserve_new_backups(
            self, hoard_file: FastPosixPath, file_size: int, past_backups: List[HoardRemote],
            available_only: bool) -> List[HoardRemote]:

        allowed_backups = [
            backup for uuid, backup in self.backups.items()
            if is_path_available(self.pathing, hoard_file, uuid)
               and backup not in past_backups
               and (not available_only or backup.uuid in self.available_backups)]

        new_possible_backups = sorted(
            allowed_backups, key=self.backup_sizes.remaining_pct, reverse=True)

        num_backups_to_request = self.num_backup_copies_desired - len(past_backups)

        if len(new_possible_backups) < num_backups_to_request:
            logging.error(
                f"Need at least {num_backups_to_request} backup media to satisfy, has only {len(new_possible_backups)} remaining.")

        logging.info(
            f"Returning {min(num_backups_to_request, len(new_possible_backups))} new backups "
            f"from requested {num_backups_to_request}.")

        reserved_remotes = new_possible_backups[:num_backups_to_request]
        good_remotes = []
        for remote in reserved_remotes:
            self.backup_sizes.reserve_size(remote, file_size)
            projected_free_space = self.backup_sizes.remaining_pct(remote)
            if projected_free_space < MIN_REPO_PERC_FREE:
                logging.error(
                    f"Free space on {remote.name} projected to become {format_percent(projected_free_space)}!")
                logging.error(
                    f"Not enough free space to reserve on {remote.name}, "
                    f"projected {format_percent(projected_free_space)}!")
                self.backup_sizes.reserve_size(remote, -file_size)  # unreserve space ...
            else:
                good_remotes.append(remote)

        return good_remotes

    @staticmethod
    def all(config: HoardConfig, pathing: HoardPathing, hoard: HoardContents, available_remotes: List[str]) -> List["BackupSet"]:
        sets: Dict[FastPosixPath, List[HoardRemote]] = dict()
        for remote in config.remotes.all():
            if remote.type == CaveType.BACKUP:
                if remote.mounted_at not in sets:
                    sets[remote.mounted_at] = []
                sets[remote.mounted_at].append(remote)
        return [BackupSet(mounted_at, s, pathing, hoard, available_remotes) for mounted_at, s in sets.items()]


STATUSES_DECLARED_TO_FETCH = [HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.MOVE, HoardFileStatus.AVAILABLE]


class ContentPrefs:
    def __init__(self, config: HoardConfig, pathing: HoardPathing, hoard: HoardContents, available_remotes: List[str]):
        self.config = config
        self._partials_with_fetch_new: List[HoardRemote] = [
            r for r in config.remotes.all() if
            r.type == CaveType.PARTIAL and r.fetch_new]

        self._backup_sets = BackupSet.all(config, pathing, hoard, available_remotes)
        self.pathing = pathing
        self.hoard = hoard

    def repos_to_add(
            self, hoard_file: FastPosixPath, local_props: FileDesc,
            hoard_props: Optional[HoardFileProps] = None) -> Generator[str, None, None]:
        for r in self._partials_with_fetch_new:
            if is_path_available(self.pathing, hoard_file, r.uuid):
                yield r.uuid

        for b in self._backup_sets:
            yield from map(
                lambda remote: remote.uuid, b.repos_to_backup_to(hoard_file, hoard_props, local_props.size, True))

    def can_cleanup(
            self, hoard_file: FastPosixPath, hoard_props: HoardFileProps, repo_uuid: str, out: StringIO) -> bool:
        to_be_got = hoard_props.by_status(HoardFileStatus.GET)
        is_available = hoard_props.by_status(HoardFileStatus.AVAILABLE)

        local_file_to_delete = self.pathing.in_hoard(hoard_file).at_local(repo_uuid).as_pure_path.as_posix()

        if hoard_props.fasthash in self.files_to_copy:  # fixme that copy functionality is kinda pointless and complicating
            logging.info(f"file with fasthash {hoard_props.fasthash} to be copied, retaining")
            out.write(f"~h {local_file_to_delete}\n")
            return False

        required_min_copies = self.config.remotes[repo_uuid].min_copies_before_cleanup
        if len(to_be_got) > 0 and len(is_available) < required_min_copies:
            logging.info(
                f"file needs to be copied in {len(to_be_got)} places, "
                f"has {len(is_available)} < {required_min_copies} - will retain here.")
            names_to_get = list(sorted(self.config.remotes[uuid].name for uuid in to_be_got))
            out.write(f"NEEDS_MORE_COPIES ({len(is_available)}) {names_to_get} {local_file_to_delete}\n")
            return False

        return True

    @cached_property
    def files_to_copy(self) -> Dict[str, List[str]]:
        return _find_files_to_copy(self.hoard)


def _find_files_to_copy(hoard: HoardContents) -> Dict[str, List[str]]:
    fasthashes_to_copy = [
        props.fasthash for filepath, props in hoard.fsobjects
        if len(props.by_status(HoardFileStatus.COPY)) > 0]

    files_to_copy: Dict[str, List[str]] = dict((h, []) for h in fasthashes_to_copy)
    for filepath, props in hoard.fsobjects:
        assert isinstance(props, HoardFileProps)
        if props.fasthash in fasthashes_to_copy:
            files_to_copy[props.fasthash].append(filepath.as_posix())

    return files_to_copy
