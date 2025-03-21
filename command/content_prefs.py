import logging
from typing import List, Optional, Generator, Dict

from command.pathing import HoardPathing, is_path_available
from config import HoardRemote, HoardConfig, CaveType
from contents.hoard import HoardContents
from contents.props import RepoFileProps, HoardFileProps, FileStatus
from util import format_percent, format_size

MIN_REPO_PERC_FREE = 0.02


class Size:
    def __init__(self, backup: HoardRemote, pathing: HoardPathing):
        self.backup = backup

        # fixme make it independent of whether we have the media available
        # fixme make it work with multiple caves on a single media, to be per-media instead of per-cave
        # fixme make it more dynamic, as usage can change over time
        self.reserved = 0
        try:
            self.total_size = pathing.total_size_on(backup.uuid)
            self.current_free_size = pathing.free_size_on(backup.uuid)
        except FileNotFoundError as e:
            self.total_size = 1
            self.current_free_size = 0

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
    def __init__(self, backups: Dict[str, HoardRemote], pathing: HoardPathing, hoard: HoardContents):
        self.backups = backups
        self.sizes = dict((b.uuid, Size(b, pathing)) for b in self.backups.values())

    def reserve_size(self, remote: HoardRemote, size: int):
        logging.debug(f"Reserving {format_size(size)} on {remote}")
        self.sizes[remote.uuid].reserved += size

    def remaining_pct(self, remote: HoardRemote) -> float:
        return self.sizes[remote.uuid].remaining_pct


class BackupSet:
    def __init__(self, mounted_at: str, backups: List[HoardRemote], pathing: HoardPathing, hoard: HoardContents):
        self.backups = dict((backup.uuid, backup) for backup in backups)
        self.uuids = sorted(list(self.backups.keys()))
        self.pathing = pathing

        self.backup_sizes = BackupSizes(self.backups, self.pathing, hoard)

        self.mounted_at = mounted_at

        self.num_backup_copies_desired = min(1, len(self.backups))
        if self.num_backup_copies_desired == 0:
            logging.warning("No backups are defined.")

    def repos_to_backup_to(
            self, hoard_file: str, hoard_props: Optional[HoardFileProps], file_size: int) -> List[HoardRemote]:

        past_backups = self.currently_scheduled_backups(hoard_file, hoard_props) if hoard_props is not None else []

        logging.info(f"Got {len(past_backups)} currently requested backups for {hoard_file}.")
        if len(past_backups) >= self.num_backup_copies_desired:
            logging.info(
                f"Skipping {hoard_file}, requested backups {len(past_backups)} >= {self.num_backup_copies_desired}")
            return []

        return self.reserve_new_backups(hoard_file, file_size, past_backups)

    def currently_scheduled_backups(self, hoard_file: str, hoard_props: HoardFileProps) -> List[HoardRemote]:
        return [
            self.backups[uuid] for uuid in hoard_props.repos_having_status(*STATUSES_DECLARED_TO_FETCH)
            if uuid in self.backups and is_path_available(self.pathing, hoard_file, uuid)]

    def reserve_new_backups(
            self, hoard_file: str, file_size: int, past_backups: List[HoardRemote]) -> List[HoardRemote]:

        allowed_backups = [
            backup for uuid, backup in self.backups.items()
            if is_path_available(self.pathing, hoard_file, uuid) and backup not in past_backups]

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
        for remote in reserved_remotes:
            self.backup_sizes.reserve_size(remote, file_size)
            projected_free_space = self.backup_sizes.remaining_pct(remote)
            if projected_free_space < MIN_REPO_PERC_FREE:
                logging.error(
                    f"Free space on {remote.name} projected to become {format_percent(projected_free_space)}!")
                raise ValueError(
                    f"Not enough free space to reserve on {remote.name}, "
                    f"projected {format_percent(projected_free_space)}!")

        return reserved_remotes

    @staticmethod
    def all(config: HoardConfig, pathing: HoardPathing, hoard: HoardContents) -> List["BackupSet"]:
        sets: Dict[str, List[HoardRemote]] = dict()
        for remote in config.remotes.all():
            if remote.type == CaveType.BACKUP:
                if remote.mounted_at not in sets:
                    sets[remote.mounted_at] = []
                sets[remote.mounted_at].append(remote)
        return [BackupSet(mounted_at, s, pathing, hoard) for mounted_at, s in sets.items()]


STATUSES_DECLARED_TO_FETCH = [FileStatus.GET, FileStatus.COPY, FileStatus.AVAILABLE]


class ContentPrefs:
    def __init__(self, config: HoardConfig, pathing: HoardPathing, hoard: HoardContents):
        self.config = config
        self._partials_with_fetch_new: List[HoardRemote] = [
            r for r in config.remotes.all() if
            r.type == CaveType.PARTIAL and r.fetch_new]

        self._backup_sets = BackupSet.all(config, pathing, hoard)
        self.pathing = pathing

    def repos_to_add(
            self, hoard_file: str, local_props: RepoFileProps,
            hoard_props: Optional[HoardFileProps] = None) -> Generator[HoardRemote, None, None]:
        for r in self._partials_with_fetch_new:
            if is_path_available(self.pathing, hoard_file, r.uuid):
                yield r.uuid

        for b in self._backup_sets:
            yield from map(lambda remote: remote.uuid, b.repos_to_backup_to(hoard_file, hoard_props, local_props.size))
