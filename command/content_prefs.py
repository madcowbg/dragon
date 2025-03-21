import logging
from typing import List, Optional, Iterable, Generator, Dict

from command.pathing import HoardPathing, is_path_available
from config import HoardRemote, HoardConfig, CaveType
from contents.props import RepoFileProps, HoardFileProps, FileStatus


class BackupSet:
    def __init__(self, mounted_at: str, backups: List[HoardRemote], pathing: HoardPathing):
        self.backups = dict((backup.uuid, backup) for backup in backups)
        self.pathing = pathing

        self.mounted_at = mounted_at

        self.num_backup_copies_desired = min(1, len(self.backups))
        if self.num_backup_copies_desired:
            logging.warning("No backups are defined.")

    def repos_to_backup_to(
            self, hoard_file: str, local_props: RepoFileProps,
            hoard_props: Optional[HoardFileProps] = None) -> Iterable[HoardRemote]:

        past_backups = self.currently_scheduled_backups(hoard_file, hoard_props) if hoard_props is not None else []

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

    def currently_scheduled_backups(self, hoard_file: str, hoard_props: HoardFileProps) -> List[HoardRemote]:
        return [
            self.backups[uuid] for uuid in hoard_props.repos_having_status(*STATUSES_DECLARED_TO_FETCH)
            if uuid in self.backups and is_path_available(self.pathing, hoard_file, uuid)]

    def _find_new_backups(
            self, hoard_file: str, hoard_props: Optional[HoardFileProps],
            past_backups: List[HoardRemote]) -> Iterable[HoardRemote]:

        # FIXME implement balancing logic e.g. by ordering as % empty and checking file size
        for uuid, backup in self.backups.items():
            if not is_path_available(self.pathing, hoard_file, uuid):
                continue

            if backup in past_backups:
                continue

            yield backup

    @staticmethod
    def all(config: HoardConfig, pathing: HoardPathing) -> List["BackupSet"]:
        sets: Dict[str, List[HoardRemote]] = dict()
        for remote in config.remotes.all():
            if remote.type == CaveType.BACKUP:
                if remote.mounted_at not in sets:
                    sets[remote.mounted_at] = []
                sets[remote.mounted_at].append(remote)
        return [BackupSet(mounted_at, s, pathing) for mounted_at, s in sets.items()]


STATUSES_DECLARED_TO_FETCH = [FileStatus.GET, FileStatus.COPY, FileStatus.AVAILABLE]


class ContentPrefs:
    def __init__(self, config: HoardConfig, pathing: HoardPathing):
        self.config = config
        self._partials_with_fetch_new: List[HoardRemote] = [
            r for r in config.remotes.all() if
            r.type == CaveType.PARTIAL and r.fetch_new]

        self._backup_sets = BackupSet.all(config, pathing)
        self.pathing = pathing

    def repos_to_add(
            self, hoard_file: str, local_props: RepoFileProps,
            hoard_props: Optional[HoardFileProps] = None) -> Generator[HoardRemote, None, None]:
        for r in self._partials_with_fetch_new:
            if is_path_available(self.pathing, hoard_file, r.uuid):
                yield r.uuid

        for b in self._backup_sets:
            yield from b.repos_to_backup_to(hoard_file, local_props, hoard_props)
