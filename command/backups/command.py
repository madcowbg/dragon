import logging
from io import StringIO
from typing import Dict, Tuple, Callable, Any, TextIO

from alive_progress import alive_it, alive_bar

import command.fast_path
from command.content_prefs import BackupSet, MIN_REPO_PERC_FREE, Presence
from command.hoard import Hoard
from command.pathing import HoardPathing
from command.pending_file_ops import HACK_create_from_hoard_props
from config import HoardRemote
from contents.hoard import MovesAndCopies, HoardContents
from contents.hoard_props import HoardFileStatus, HoardFileProps
from lmdb_storage.deferred_operations import remove_from_desired_tree, add_to_desired_tree, HoardDeferredOperations, \
    mklist_from_tree
from lmdb_storage.file_object import BlobObject, FileObject
from lmdb_storage.lookup_tables import CompressedPath
from lmdb_storage.object_serialization import construct_tree_object
from lmdb_storage.operations.types import Transformation
from lmdb_storage.operations.util import ByRoot
from lmdb_storage.tree_iteration import zip_trees_dfs, zip_dfs, DiffType
from lmdb_storage.tree_object import ObjectID, StoredObject, TreeObject, MaybeObjectID, ObjectType
from lmdb_storage.tree_structure import Objects
from resolve_uuid import resolve_remote_uuid
from util import format_size, format_percent, group_to_dict, safe_hex


def reuse_already_existing_files(backup_set: BackupSet, hoard: HoardContents, out: TextIO) -> None:
    moves_and_copies = MovesAndCopies(hoard)
    for repo in backup_set.backups.values():
        repo_root = hoard.env.roots(write=False)[repo.uuid]
        mounted_at_trailing_slash = repo.mounted_at.as_posix_folder()

        with hoard.env.objects(write=False) as objects:
            desired_tree_objs = mklist_from_tree(objects, repo_root.desired)

            with alive_bar(title="Computing reassignments") as bar:
                for hoard_path, diff_type, current_id, desired_id, _ in zip_dfs(
                        objects, '', repo_root.current, repo_root.desired, drilldown_same=False):
                    bar()
                    if not hoard_path.startswith(mounted_at_trailing_slash):
                        # fixme optimize this to simply not iterate
                        continue

                    if diff_type == DiffType.RIGHT_MISSING:
                        current_obj = objects[current_id]
                        if current_obj.object_type == ObjectType.TREE:
                            continue

                        logging.debug(
                            "Reconsidering file %s scheduled for deletion in %s.", hoard_path, repo.uuid)

                        requested_paths_in_hoard: list[CompressedPath] = list(
                            moves_and_copies.get_paths_in_hoard(current_id))

                        if len(requested_paths_in_hoard) == 0:
                            logging.debug("File not in hoard, skipping...")
                            continue

                        if len(requested_paths_in_hoard) > 1:
                            # todo maybe spread it around? hoard shouldn't contain many copies, but still
                            logging.info("File %s is %s>1 paths")

                        destination_path = moves_and_copies.resolve_on_hoard(requested_paths_in_hoard[0], objects)
                        logging.debug("Preparing to assign %s to %s", hoard_path, destination_path)

                        out.write(f"REASSIGN [{repo.name}] {hoard_path} to {destination_path.as_posix()}\n")
                        desired_tree_objs[destination_path.as_posix()] = current_obj

        with hoard.env.objects(write=True) as objects:
            new_desired_root = objects.mktree_from_tuples(
                sorted(desired_tree_objs.items()), alive_it=alive_it) \
                if len(desired_tree_objs) > 0 else None

        hoard.env.roots(write=True)[repo.uuid].desired = new_desired_root
    assert not HoardDeferredOperations(hoard).have_deferred_ops()


class HoardCommandBackups:
    def __init__(self, hoard: Hoard):
        self.hoard = hoard

    async def health(self):
        logging.info("Loading config")
        config = self.hoard.config()
        pathing = HoardPathing(config, self.hoard.paths())

        logging.info(f"Loading hoard...")
        async with self.hoard.open_contents(create_missing=False) as hoard:

            backup_sets = BackupSet.all(config, pathing, hoard, self.hoard.available_remotes(), Presence(hoard))
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
                    get_or_copy = len(hoard_props.by_statuses(HoardFileStatus.GET))
                    cleanup = len(hoard_props.by_status(HoardFileStatus.CLEANUP))

                    file_stats_copies[hoard_file] = (scheduled, available, get_or_copy, 0, cleanup)  # fixme remove 0

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
            backup_sets = BackupSet.all(config, pathing, hoard, self.hoard.available_remotes(), Presence(hoard))

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
                            remove_from_desired_tree(
                                hoard, repo.uuid, hoard_file.as_posix(), HACK_create_from_hoard_props(hoard_props))

                        for repo in repos_to_clean_from:
                            removed_cnt[repo] = removed_cnt.get(repo, 0) + 1
                            removed_size[repo] = removed_size.get(repo, 0) + hoard_props.size

                    for repo, cnt in sorted(removed_cnt.items(), key=lambda rc: rc[0].name):
                        out.write(f" {repo.name} LOST {cnt} files ({format_size(removed_size[repo])})\n")

                    logging.info("Running deferred operations...")
                    HoardDeferredOperations(hoard).apply_deferred_queue()

                out.write("DONE")
                return out.getvalue()

    async def assign(self, available_only: bool):
        logging.info("Loading config")
        config = self.hoard.config()
        pathing = HoardPathing(config, self.hoard.paths())

        logging.info(f"Loading hoard...")
        async with self.hoard.open_contents(create_missing=False).writeable() as hoard:
            with StringIO() as out:
                for backup_set in BackupSet.all(config, pathing, hoard, self.hoard.available_remotes(), Presence(hoard)):
                    added_cnt: Dict[HoardRemote, int] = dict()
                    added_size: Dict[HoardRemote, int] = dict()
                    out.write(
                        f"set: {backup_set.mounted_at} with {len(backup_set.available_backups)}/{len(backup_set.backups)} media\n")

                    logging.info("Enabling files that are possibly the result of rename operations.")
                    reuse_already_existing_files(backup_set, hoard, out)

                for backup_set in BackupSet.all(config, pathing, hoard, self.hoard.available_remotes(), Presence(hoard)):
                    logging.info(
                        f"Considering backup set at {backup_set.mounted_at} with {len(backup_set.backups)} media")
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
                            add_to_desired_tree(
                                hoard, repo.uuid, hoard_file.simple, HACK_create_from_hoard_props(hoard_props))
                            out.write(f"BACKUP [{repo.name}]{hoard_file.simple}\n")

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

                    logging.info("Running deferred operations...")
                    HoardDeferredOperations(hoard).apply_deferred_queue()

                    for repo, cnt in sorted(added_cnt.items(), key=lambda rc: rc[0].name):
                        out.write(f" {repo.name} <- {cnt} files ({format_size(added_size[repo])})\n")

                out.write("DONE")
                return out.getvalue()

    async def unassign(self, repo: str | None = None, all_unavailable: bool = False, all: bool = False):
        logging.info("Loading config")
        config = self.hoard.config()

        if repo is not None:
            assert not all_unavailable and not all, "Either provide a repo or use --all-unavailable or --all."
            repo_uuid = resolve_remote_uuid(self.hoard.config(), repo)
            if repo_uuid not in self.hoard.config().remotes:
                return f"Can't find repo {repo} with uuid {repo_uuid}!"

            remotes_to_unassign = [self.hoard.config().remotes[repo_uuid].uuid]
        elif all:
            assert all_unavailable == False
            remotes_to_unassign = [remote.uuid for remote in self.hoard.config().remotes.all()]
        else:
            assert all_unavailable == True
            available_remotes = self.hoard.available_remotes()
            remotes_to_unassign = [
                remote.uuid for remote in self.hoard.config().remotes.all()
                if remote.uuid not in available_remotes]

        assert len(remotes_to_unassign) >= 1

        pathing = HoardPathing(config, self.hoard.paths())

        logging.info(f"Loading hoard...")
        async with self.hoard.open_contents(create_missing=False).writeable() as hoard:
            available_remotes = self.hoard.available_remotes()
            backup_sets = BackupSet.all(config, pathing, hoard, available_remotes, Presence(hoard))

            with StringIO() as out:
                for backup_set in backup_sets:
                    print(f"Considering backup set at {backup_set.mounted_at} with {len(backup_set.backups)} media")

                    for remote in backup_set.backups.values():
                        if all_unavailable and remote.uuid in available_remotes:
                            out.write(f"Remote {remote.name} is available, will not unassign\n")
                            continue

                        if remote.uuid not in remotes_to_unassign:
                            out.write(f"Skipping {remote.name}!")
                            continue

                        repo_root = hoard.env.roots(write=False)[remote.uuid]
                        out.write(f"Unassigning from {remote.name} [{safe_hex(repo_root.desired)[:6]}]:\n")
                        with hoard.env.objects(write=True) as objects:

                            new_root_id = SelectOnlyExisting(objects).execute(ByRoot(
                                ["acceptable", "actual"],
                                {"acceptable": repo_root.current, "actual": repo_root.desired}.items()))

                            for file_path, (new_id, old_id), _ in \
                                    zip_trees_dfs(objects, "", [new_root_id, repo_root.desired], False):
                                assert old_id is not None, f"Can't happen when filtering, {file_path}!"
                                old = objects[old_id]
                                if isinstance(old, BlobObject):
                                    if new_id != old_id:
                                        assert new_id is None
                                        out.write(f"WONT_GET {file_path}\n")

                        repo_root = hoard.env.roots(write=True)[remote.uuid]
                        out.write(
                            f"Desired root for {remote.name} is {safe_hex(new_root_id)[:6]} <- {safe_hex(repo_root.desired)[:6]}\n")
                        repo_root.desired = new_root_id

                return out.getvalue()


class SelectOnlyExisting(Transformation[None, MaybeObjectID]):
    def __init__(self, objects: Objects):
        self.objects = objects

    def combine(self, state: None, merged: Dict[str, MaybeObjectID], original: ByRoot[StoredObject]) -> MaybeObjectID:
        merged = dict((c, v) for c, v in merged.items() if v is not None)
        if len(merged) == 0:
            return None  # skipping empty folders

        new_tree_node = construct_tree_object(merged)

        self.objects[new_tree_node.id] = new_tree_node

        return new_tree_node.id

    def should_drill_down(self, state: None, trees: ByRoot[TreeObject], files: ByRoot[BlobObject]) -> bool:
        return len(trees) > 0

    def combine_non_drilldown(self, state: None, original: ByRoot[StoredObject]) -> MaybeObjectID:
        acceptable_obj = original.get_if_present("acceptable")
        if acceptable_obj is None:
            return None

        actual_obj = original.get_if_present("actual")
        if acceptable_obj == actual_obj:
            return actual_obj.id

        return None

    def initial_state(self, obj_ids: ByRoot[ObjectID]) -> None:
        pass

    def drilldown_state(self, child_name: str, merge_state: None) -> None:
        pass
