import logging
from io import StringIO
from typing import Dict, Tuple, Callable

from alive_progress import alive_it

import command.fast_path
from command.content_prefs import BackupSet, MIN_REPO_PERC_FREE
from command.hoard import Hoard
from command.pathing import HoardPathing
from command.tree_operations import add_to_desired_tree, \
    remove_from_desired_tree
from config import HoardRemote
from contents.hoard_props import HoardFileStatus, HoardFileProps
from resolve_uuid import resolve_remote_uuid
from util import format_size, format_percent, group_to_dict


class HoardCommandBackups:
    def __init__(self, hoard: Hoard):
        self.hoard = hoard

    async def health(self):  # fixme rewrite output as table
        logging.info("Loading config")
        config = self.hoard.config()
        pathing = HoardPathing(config, self.hoard.paths())

        logging.info(f"Loading hoard...")
        async with self.hoard.open_contents(create_missing=False) as hoard:
            backup_sets = BackupSet.all(config, pathing, hoard, self.hoard.available_remotes())
            backup_media = set(sum((list(b.backups.keys()) for b in backup_sets), []))
            count_backup_media = len(backup_media)

            with StringIO() as out:
                out.write(f"# backup sets: {len(backup_sets)}\n")
                out.write(f"# backups: {count_backup_media}\n")

                print("Iterating over hoard files")

                file_sizes: Dict[str, int] = dict()
                file_stats_copies: Dict[str, Tuple[int, int, int, int, int]] = dict()
                for hoard_file, hoard_props in alive_it(hoard.fsobjects):
                    assert isinstance(hoard_props, HoardFileProps)

                    file_sizes[hoard_file] = hoard_props.size
                    scheduled = 0
                    for backup_set in backup_sets:
                        scheduled += len(backup_set.currently_scheduled_backups(hoard_file, hoard_props))

                    available = sum(
                        1 for uuid in hoard_props.by_status(HoardFileStatus.AVAILABLE) if uuid in backup_media)
                    get_or_copy = len(
                        hoard_props.by_statuses(HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.MOVE))
                    move = len(hoard_props.by_status(HoardFileStatus.MOVE))
                    cleanup = len(hoard_props.by_status(HoardFileStatus.CLEANUP))

                    file_stats_copies[hoard_file] = (scheduled, available, get_or_copy, move, cleanup)

                def pivot_stat(stat_idx, fun: Callable[[str], int]) -> Dict[int, int]:
                    return dict(
                        (stat_value, sum(fun(file) for file, _ in file_stats_fstat))
                        for stat_value, file_stats_fstat in group_to_dict(
                            file_stats_copies.items(),
                            key=lambda file_to_fstats: file_to_fstats[1][stat_idx]).items())

                for idx, name in [(0, "scheduled"), (1, "available"), (2, "get_or_copy"), (3, "move"), (4, "cleanup")]:
                    out.write(f"{name} count:\n")
                    sizes = pivot_stat(idx, lambda file: file_sizes[file])
                    for num_copies, cnt in sorted(pivot_stat(idx, lambda _: 1).items(), key=lambda x: x[0]):
                        size = sizes[num_copies]
                        out.write(f" {num_copies}: {cnt} files ({format_size(size)})\n")

                out.write("DONE")
                return out.getvalue()

    async def clean(self):
        logging.info("Loading config")
        config = self.hoard.config()
        pathing = HoardPathing(config, self.hoard.paths())

        logging.info(f"Loading hoard...")
        async with self.hoard.open_contents(create_missing=False).writeable() as hoard:
            backup_sets = BackupSet.all(config, pathing, hoard, self.hoard.available_remotes())

            with StringIO() as out:
                for backup_set in backup_sets:
                    removed_cnt: Dict[HoardRemote, int] = dict()
                    removed_size: Dict[HoardRemote, int] = dict()
                    out.write(f"set: {backup_set.mounted_at.as_posix()} with {len(backup_set.backups)} media\n")

                    print(f"Considering backup set at {backup_set.mounted_at} with {len(backup_set.backups)} media")
                    hoard_file: command.fast_path.FastPosixPath
                    for hoard_file, hoard_props in alive_it(
                            [s async for s in hoard.fsobjects.in_folder(backup_set.mounted_at)]):
                        assert hoard_file.is_relative_to(backup_set.mounted_at)
                        assert isinstance(hoard_props, HoardFileProps)

                        repos_to_clean_from = backup_set.repos_to_clean(hoard_file, hoard_props, hoard_props.size)

                        logging.info(f"Cleaning up {hoard_file} from {[r.uuid for r in repos_to_clean_from]}")

                        for repo in repos_to_clean_from:
                            remove_from_desired_tree(hoard, repo.uuid, hoard_file)

                        for repo in repos_to_clean_from:
                            removed_cnt[repo] = removed_cnt.get(repo, 0) + 1
                            removed_size[repo] = removed_size.get(repo, 0) + hoard_props.size

                    for repo, cnt in sorted(removed_cnt.items(), key=lambda rc: rc[0].name):
                        out.write(f" {repo.name} LOST {cnt} files ({format_size(removed_size[repo])})\n")

                out.write("DONE")
                return out.getvalue()

    async def assign(self, available_only: bool):
        logging.info("Loading config")
        config = self.hoard.config()
        pathing = HoardPathing(config, self.hoard.paths())

        logging.info(f"Loading hoard...")
        async with self.hoard.open_contents(create_missing=False).writeable() as hoard:
            backup_sets = BackupSet.all(config, pathing, hoard, self.hoard.available_remotes())

            with StringIO() as out:
                for backup_set in backup_sets:
                    added_cnt: Dict[HoardRemote, int] = dict()
                    added_size: Dict[HoardRemote, int] = dict()
                    out.write(
                        f"set: {backup_set.mounted_at} with {len(backup_set.available_backups)}/{len(backup_set.backups)} media\n")

                    print(f"Considering backup set at {backup_set.mounted_at} with {len(backup_set.backups)} media")
                    hoard_file: command.fast_path.FastPosixPath
                    for hoard_file, hoard_props in alive_it(
                            [s async for s in hoard.fsobjects.in_folder_non_deleted(backup_set.mounted_at)]):
                        assert hoard_file.is_relative_to(backup_set.mounted_at), \
                            f"{hoard_file} not rel to {backup_set.mounted_at}"

                        assert isinstance(hoard_props, HoardFileProps)

                        new_repos_to_backup_to = backup_set.repos_to_backup_to(
                            hoard_file, hoard_props, hoard_props.size, available_only)

                        if len(new_repos_to_backup_to) == 0:
                            logging.info(f"No new backups for {hoard_file}.")
                            continue

                        logging.info(f"Backing up {hoard_file} to {[r.uuid for r in new_repos_to_backup_to]}")
                        for repo in new_repos_to_backup_to:
                            add_to_desired_tree(hoard, repo.uuid, hoard_file.simple, hoard_props)

                        for repo in new_repos_to_backup_to:
                            added_cnt[repo] = added_cnt.get(repo, 0) + 1
                            added_size[repo] = added_size.get(repo, 0) + hoard_props.size

                            projected = backup_set.backup_sizes.remaining_pct(repo)
                            if projected < MIN_REPO_PERC_FREE:
                                # fixme this may be better handled in bulk...
                                out.write(
                                    f"Error: Backup {repo.name} free space is projected to become "
                                    f"{format_percent(projected)} < {format_percent(MIN_REPO_PERC_FREE)}%!\n)")
                                return out.getvalue()

                    for repo, cnt in sorted(added_cnt.items(), key=lambda rc: rc[0].name):
                        out.write(f" {repo.name} <- {cnt} files ({format_size(added_size[repo])})\n")

                out.write("DONE")
                return out.getvalue()

    async def unassign(self, repo: str | None = None, all_unavailable: bool = False):
        logging.info("Loading config")
        config = self.hoard.config()

        if repo is not None:
            assert not all_unavailable, "Either provide a repo or use --all-unavailable."
            repo_uuid = resolve_remote_uuid(self.hoard.config(), repo)
            remote_to_unassign = self.hoard.config().remotes[repo_uuid]
            if remote_to_unassign is None:
                return f"Can't find repo {repo} with uuid {repo_uuid}!"
        else:
            assert all_unavailable == True
            remote_to_unassign = None

        assert (all_unavailable and remote_to_unassign is None) \
               or (remote_to_unassign is not None and not all_unavailable)

        pathing = HoardPathing(config, self.hoard.paths())

        logging.info(f"Loading hoard...")
        async with self.hoard.open_contents(create_missing=False).writeable() as hoard:
            available_remotes = self.hoard.available_remotes()
            backup_sets = BackupSet.all(config, pathing, hoard, available_remotes)

            with StringIO() as out:
                for backup_set in backup_sets:
                    print(f"Considering backup set at {backup_set.mounted_at} with {len(backup_set.backups)} media")

                    for remote in backup_set.backups.values():
                        if all_unavailable and remote.uuid in available_remotes:
                            out.write(f"Remote {remote.name} is available, will not unassign\n")
                            continue

                        if not all_unavailable and remote.uuid != remote_to_unassign.uuid:
                            out.write(f"Skipping {remote.name}!")
                            continue

                        out.write(f"Unassigning from {remote.name}:\n")

                        for hoard_file, hoard_props in hoard.fsobjects.to_get_in_repo(remote.uuid):
                            assert hoard_props.get_status(remote.uuid) == HoardFileStatus.GET

                            remove_from_desired_tree(hoard, remote.uuid, hoard_file.simple)

                            out.write(f"WONT_GET {hoard_file.as_posix()}\n")

                return out.getvalue()
