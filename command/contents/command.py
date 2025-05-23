import logging
import sys
from io import StringIO
from typing import List, Dict, Any, Optional, Callable, Awaitable, Tuple, TextIO

import humanize
from alive_progress import alive_it

from command.content_prefs import ContentPrefs
from command.contents.comparisons import copy_local_staging_to_hoard
from command.contents.pull_preferences import PullPreferences, PullIntention, PullMergePreferences
from command.fast_path import FastPosixPath
from command.hoard import Hoard
from command.pathing import HoardPathing
from command.tree_operations import remove_from_desired_tree
from config import CaveType, HoardRemote, HoardConfig
from contents.hoard import HoardContents, HoardFile, HoardDir
from contents.hoard_props import HoardFileStatus, HoardFileProps
from contents.repo import RepoContents
from exceptions import MissingRepoContents
from lmdb_storage.file_object import FileObject
from lmdb_storage.operations.three_way_merge import TransformedRoots
from lmdb_storage.operations.util import ByRoot
from lmdb_storage.pull_contents import merge_contents, commit_merged
from lmdb_storage.roots import Root, Roots
from lmdb_storage.tree_iteration import zip_trees_dfs
from lmdb_storage.tree_operations import get_child, graft_in_tree
from lmdb_storage.tree_structure import Objects, ObjectID, TreeObject
from resolve_uuid import resolve_remote_uuid
from util import format_size, custom_isabs, safe_hex


def _file_stats(props: HoardFileProps) -> str:
    a = props.by_status(HoardFileStatus.AVAILABLE)
    g = props.by_status(HoardFileStatus.GET)
    c = props.by_status(HoardFileStatus.CLEANUP)
    x = props.by_status(HoardFileStatus.COPY)
    res: List[str] = []
    if len(a) > 0:
        res.append(f'a:{len(a)}')
    if len(g) > 0:
        res.append(f'g:{len(g)}')
    if len(c) > 0:
        res.append(f'c:{len(c)}')
    if len(x) > 0:
        res.append(f'x:{len(x)}')
    return " ".join(res)


def _init_pull_preferences_partial(
        remote_uuid: str, assume_current: bool = False,
        force_fetch_local_missing: bool = False) -> PullPreferences:
    return PullPreferences(remote_uuid, on_same_file_is_present=PullIntention.ADD_TO_HOARD,
                           on_file_added_or_present=PullIntention.ADD_TO_HOARD,
                           on_file_is_different_and_modified=PullIntention.ADD_TO_HOARD,
                           on_file_is_different_and_added=PullIntention.ADD_TO_HOARD,
                           on_file_is_different_but_present=PullIntention.RESTORE_FROM_HOARD if not assume_current else PullIntention.ADD_TO_HOARD,
                           on_hoard_only_local_deleted=PullIntention.DELETE_FROM_HOARD if not force_fetch_local_missing else PullIntention.RESTORE_FROM_HOARD,
                           on_hoard_only_local_unknown=PullIntention.ACCEPT_FROM_HOARD,
                           on_hoard_only_local_moved=PullIntention.MOVE_IN_HOARD,
                           force_fetch_local_missing=force_fetch_local_missing, force_reset_with_local_contents=False,
                           remote_type=CaveType.PARTIAL)


def _init_pull_preferences_backup(remote_uuid: str) -> PullPreferences:
    return PullPreferences(remote_uuid, on_same_file_is_present=PullIntention.ADD_TO_HOARD,
                           on_file_added_or_present=PullIntention.IGNORE,
                           on_file_is_different_and_modified=PullIntention.RESTORE_FROM_HOARD,
                           on_file_is_different_and_added=PullIntention.RESTORE_FROM_HOARD,
                           on_file_is_different_but_present=PullIntention.RESTORE_FROM_HOARD,
                           on_hoard_only_local_deleted=PullIntention.RESTORE_FROM_HOARD,
                           on_hoard_only_local_unknown=PullIntention.RESTORE_FROM_HOARD,
                           on_hoard_only_local_moved=PullIntention.RESTORE_FROM_HOARD, force_fetch_local_missing=False,
                           force_reset_with_local_contents=False, remote_type=CaveType.BACKUP)


def _init_pull_preferences_incoming(remote_uuid: str) -> PullPreferences:
    return PullPreferences(remote_uuid, on_same_file_is_present=PullIntention.CLEANUP,
                           on_file_added_or_present=PullIntention.ADD_TO_HOARD_AND_CLEANUP,
                           on_file_is_different_and_modified=PullIntention.ADD_TO_HOARD_AND_CLEANUP,
                           on_file_is_different_and_added=PullIntention.ADD_TO_HOARD_AND_CLEANUP,
                           on_file_is_different_but_present=PullIntention.CLEANUP,
                           on_hoard_only_local_deleted=PullIntention.IGNORE,
                           on_hoard_only_local_unknown=PullIntention.IGNORE,
                           on_hoard_only_local_moved=PullIntention.IGNORE, force_fetch_local_missing=False,
                           force_reset_with_local_contents=False, remote_type=CaveType.INCOMING)


def augment_statuses(config, hoard, show_empty, statuses):
    statuses_present = \
        [(config.remotes[uuid].name, uuid, hoard.config.updated(uuid), vals)  # those that have files recorded
         for uuid, vals in statuses.items()] + \
        [(remote.name, remote.uuid, hoard.config.updated(remote.uuid), {})  # those lacking a recorded file
         for remote in config.remotes.all() if show_empty and remote.uuid not in statuses]
    statuses_sorted = sorted(statuses_present)
    available_states = set(sum((list(stats.keys()) for _, _, _, stats in statuses_sorted), []))
    return available_states, statuses_sorted


async def execute_pull(
        hoard: Hoard, preferences: PullPreferences, ignore_epoch: bool, out: StringIO, progress_bar=alive_it):
    config = hoard.config()
    remote_uuid = preferences.local_uuid
    pathing = HoardPathing(config, hoard.paths())

    logging.info(f"Loading hoard contents TOML...")
    async with hoard.open_contents(create_missing=False).writeable() as hoard_contents:
        logging.info(f"Loaded hoard contents TOML!")
        content_prefs = ContentPrefs(config, pathing, hoard_contents, hoard.available_remotes())

        try:
            connected_repo = hoard.connect_to_repo(remote_uuid, require_contents=True)
            current_contents = connected_repo.open_contents(is_readonly=True)
        except MissingRepoContents as e:
            logging.error(e)
            out.write(f"Repo {remote_uuid} has no current contents available!\n")
            return

        with current_contents:
            if current_contents.config.is_dirty:
                logging.error(
                    f"{remote_uuid} is_dirty = TRUE, so the refresh is not complete - can't use current repo.")
                out.write(f"Skipping update as {remote_uuid} is not fully calculated!\n")
                return

            if not ignore_epoch \
                    and hoard_contents.config.remote_epoch(remote_uuid) >= current_contents.config.epoch:
                out.write(f"Skipping update as past epoch {current_contents.config.epoch} "
                          f"is not after hoard epoch {hoard_contents.config.remote_epoch(remote_uuid)}\n")
                return

            logging.info(f"Saving config of remote {remote_uuid}...")
            hoard_contents.config.save_remote_config(current_contents.config)

            copy_local_staging_to_hoard(hoard_contents, current_contents, hoard.config())
            uuid = current_contents.config.uuid

            out.write(f"Pulling {config.remotes[uuid].name}...\n")

            roots = hoard_contents.env.roots(False)
            dump_before_op(roots, uuid, out)

            roots = hoard_contents.env.roots(True)
            all_remote_roots = [roots[remote.uuid] for remote in config.remotes.all()]
            all_remote_roots_old_desired = dict((root.name, root.desired) for root in all_remote_roots)

            hoard_root = roots["HOARD"]
            repo_root = roots[uuid]

            merged_ids = merge_contents(
                hoard_contents.env, repo_root, all_repo_roots=[hoard_root] + all_remote_roots,
                preferences=preferences, content_prefs=content_prefs)

            # print what actually changed for the hoard and the repo todo consider printing other repo changes?
            print_differences(hoard_contents, hoard_root, repo_root, merged_ids, out)

            commit_merged(hoard_root, repo_root, all_remote_roots, merged_ids)

            for root in all_remote_roots:
                old_desired = all_remote_roots_old_desired[root.name]
                if root.desired != old_desired:
                    out.write(
                        f"updated {config.remotes[root.name].name} from {safe_hex(old_desired)[:6]} to {safe_hex(root.desired)[:6]}\n")

            dump_after_op(roots, uuid, out)

            logging.info(f"Updating epoch of {remote_uuid} to {current_contents.config.epoch}")
            hoard_contents.config.mark_up_to_date(
                remote_uuid, current_contents.config.epoch, current_contents.config.updated)

    out.write(f"Sync'ed {config.remotes[remote_uuid].name} to hoard!\n")


def dump_after_op(roots: Roots, uuid: str, out: TextIO):
    repo_current = roots[uuid].current
    repo_staging = roots[uuid].staging
    repo_desired = roots[uuid].desired
    out.write(
        f"After: Hoard [{safe_hex(roots['HOARD'].desired)[:6]}],"
        f" repo [curr: {safe_hex(repo_current)[:6]}, stg: {safe_hex(repo_staging)[:6]}, des: {safe_hex(repo_desired)[:6]}]\n")


def dump_before_op(roots: Roots, uuid: str, out: TextIO):
    repo_current = roots[uuid].current
    repo_staging = roots[uuid].staging
    repo_desired = roots[uuid].desired
    out.write(
        f"Before: Hoard [{safe_hex(roots['HOARD'].desired)[:6]}] "
        f"<- repo [curr: {safe_hex(repo_current)[:6]}, stg: {safe_hex(repo_staging)[:6]}, des: {safe_hex(repo_desired)[:6]}]\n")


def init_pull_preferences(
        remote_obj: HoardRemote, assume_current: bool,
        force_fetch_local_missing: bool) -> PullPreferences:
    if remote_obj.type == CaveType.INCOMING:
        return _init_pull_preferences_incoming(remote_obj.uuid)
    elif remote_obj.type == CaveType.BACKUP:
        return _init_pull_preferences_backup(remote_obj.uuid)
    else:
        assert remote_obj.type == CaveType.PARTIAL
        return _init_pull_preferences_partial(remote_obj.uuid, assume_current, force_fetch_local_missing)


def is_tree_or_none(objects: Objects, obj_id: ObjectID | None) -> bool:
    return True if obj_id is None else isinstance(objects[obj_id], TreeObject)


async def execute_print_differences(hoard: HoardContents, repo_uuid: str, ignore_missing: bool, out: StringIO):
    repo_root = hoard.env.roots(write=False)[repo_uuid]

    with (hoard.env.objects(write=False) as objects):
        for path, (sub_before_hoard_id, sub_repo_current, sub_repo_staging), _ in zip_trees_dfs(
                objects, "", [
                    hoard.env.roots(write=False)["HOARD"].desired,
                    repo_root.current, repo_root.staging],
                drilldown_same=True):

            if sub_before_hoard_id is not None:  # file is in hoard
                if is_tree_or_none(objects, sub_before_hoard_id):
                    continue

                if sub_repo_staging is not None:  # file is in local
                    if is_tree_or_none(objects, sub_repo_staging):
                        continue

                    if sub_before_hoard_id == sub_repo_staging:
                        pass  # skipping same files
                    else:
                        out.write(f"MODIFIED {path}\n")
                else:  # file not in local
                    if sub_repo_current is not None:
                        out.write(f"DELETED {path}\n")
                    else:
                        if not ignore_missing:
                            out.write(f"MISSING {path}\n")
            else:  # not in hoard
                if sub_repo_staging is not None:
                    if is_tree_or_none(objects, sub_repo_staging):
                        continue
                    out.write(f"PRESENT {path}\n")
                else:
                    pass

    out.write("DONE")


def pull_prefs_to_restore_from_hoard(remote_uuid: str, remote_type: CaveType) -> PullPreferences:
    return PullPreferences(remote_uuid, on_same_file_is_present=PullIntention.ADD_TO_HOARD,
                           on_file_added_or_present=PullIntention.CLEANUP,
                           on_file_is_different_and_modified=PullIntention.RESTORE_FROM_HOARD,
                           on_file_is_different_and_added=PullIntention.RESTORE_FROM_HOARD,
                           on_file_is_different_but_present=PullIntention.RESTORE_FROM_HOARD,
                           on_hoard_only_local_deleted=PullIntention.RESTORE_FROM_HOARD,
                           on_hoard_only_local_unknown=PullIntention.RESTORE_FROM_HOARD,
                           on_hoard_only_local_moved=PullIntention.RESTORE_FROM_HOARD, force_fetch_local_missing=False,
                           force_reset_with_local_contents=False, remote_type=remote_type)


async def print_pending_to_pull(
        hoard_contents: HoardContents, content_prefs: ContentPrefs, current_contents: RepoContents, config: HoardConfig,
        preferences: PullPreferences, out):
    with StringIO() as other_out:
        roots = hoard_contents.env.roots(write=False)
        repo_root = roots[current_contents.uuid]
        hoard_root = roots["HOARD"]

        out.write(f"Hoard root: {safe_hex(hoard_contents.env.roots(False)['HOARD'].desired)}:\n")
        out.write(
            f"Repo current={safe_hex(repo_root.current)[:6]} staging={safe_hex(repo_root.staging)[:6]} desired={safe_hex(repo_root.desired)[:6]}\n")
        out.write(f"Repo root: {safe_hex(current_contents.fsobjects.root_id)}:\n")

        merged_ids = merge_contents(
            hoard_contents.env, repo_root, [repo_root, hoard_root],
            preferences=preferences, content_prefs=content_prefs, merge_only=[repo_root.name])

        # raise ValueError()
        print_differences(hoard_contents, hoard_root, repo_root, merged_ids, out)

        logging.debug(other_out.getvalue())


def print_differences(
        hoard_contents: HoardContents, hoard_root: Root, repo_root: Root, merged_ids: TransformedRoots, out: StringIO):
    print_differences_for_id(
        hoard_contents, hoard_root, repo_root,
        merged_ids.get_if_present("HOARD"),
        merged_ids.get_if_present(repo_root.name),
        out)


def print_differences_for_id(
        hoard_contents: HoardContents, hoard_root: Root, repo_root: Root,
        desired_hoard_root_id: ObjectID | None, desired_repo_root_id: ObjectID | None, out: StringIO):
    with hoard_contents.env.objects(write=False) as objects:
        for path, (base_hoard_id, merged_hoard_id, base_current_repo_id, merged_repo_id, staging_repo_id), _ \
                in zip_trees_dfs(
            objects, "", [
                hoard_root.desired, desired_hoard_root_id,
                repo_root.current, desired_repo_root_id, repo_root.staging],
            drilldown_same=True):

            if base_current_repo_id and merged_repo_id:
                if not (is_tree_or_none(objects, base_current_repo_id) or is_tree_or_none(objects, merged_repo_id)):
                    if base_current_repo_id != merged_repo_id:
                        out.write(f"REPO_DESIRED_FILE_CHANGED {path}\n")
            elif merged_repo_id:
                assert base_current_repo_id is None
                if not is_tree_or_none(objects, merged_repo_id):
                    assert merged_repo_id == merged_hoard_id, f"File somehow desired but not in hoard?! {path}"
                    if merged_repo_id == staging_repo_id:
                        # file was added just now
                        out.write(f"REPO_MARK_FILE_AVAILABLE {path}\n")
                    else:
                        if merged_repo_id == base_hoard_id:
                            out.write(f"REPO_DESIRED_FILE_TO_GET {path}\n")
                        else:
                            out.write(f"REPO_DESIRED_FILE_ADDED {path}\n")
            elif base_current_repo_id:
                assert merged_repo_id is None
                if not is_tree_or_none(objects, base_current_repo_id):
                    out.write(f"REPO_FILE_TO_DELETE {path}\n")
            else:
                assert base_current_repo_id is None and merged_repo_id is None
                if merged_hoard_id == staging_repo_id:
                    if not is_tree_or_none(objects, staging_repo_id):
                        # file was added just now
                        out.write(f"REPO_MARK_FILE_AVAILABLE {path}\n")
                logging.debug(f"Ignoring %s as is not in repo past or future", path)
                pass

            if merged_hoard_id and base_hoard_id:
                if not (is_tree_or_none(objects, merged_hoard_id) or is_tree_or_none(objects, base_hoard_id)):
                    if merged_hoard_id != base_hoard_id:
                        out.write(f"HOARD_FILE_CHANGED {path}\n")
            elif merged_hoard_id:
                assert base_hoard_id is None
                if not is_tree_or_none(objects, merged_hoard_id):
                    out.write(f"HOARD_FILE_ADDED {path}\n")
            elif base_hoard_id:
                assert merged_hoard_id is None
                if not is_tree_or_none(objects, base_hoard_id):
                    out.write(f"HOARD_FILE_DELETED {path}\n")
            else:
                assert base_hoard_id is None and merged_hoard_id is None
                logging.debug(f"Ignoring %s as is not in hoard past or future", path)


class HoardCommandContents:
    def __init__(self, hoard: Hoard):
        self.hoard = hoard

    async def pending_pull(self, remote: str):
        config = self.hoard.config()
        remote_uuid = resolve_remote_uuid(config, remote)

        remote_obj = config.remotes[remote_uuid]
        preferences = init_pull_preferences(remote_obj, assume_current=False, force_fetch_local_missing=False)

        logging.info(f"Reading current contents of {remote_uuid}...")
        connected_repo = self.hoard.connect_to_repo(remote_uuid, require_contents=True)
        with connected_repo.open_contents(is_readonly=True) as current_contents:
            logging.info(f"Loading hoard TOML...")
            async with self.hoard.open_contents(create_missing=False) as hoard_contents:
                logging.info(f"Loaded hoard TOML!")
                logging.info(f"Computing status ...")

                with StringIO() as out:
                    out.write(f"Status of {remote_obj.name}:\n")

                    pathing = HoardPathing(config, self.hoard.paths())
                    content_prefs = ContentPrefs(config, pathing, hoard_contents, self.hoard.available_remotes())

                    copy_local_staging_to_hoard(hoard_contents, current_contents, self.hoard.config())

                    await print_pending_to_pull(hoard_contents, content_prefs, current_contents, config, preferences,
                                                out)

                    return out.getvalue()

    async def differences(self, remote: str, ignore_missing: bool = False) -> str:
        remote_uuid = resolve_remote_uuid(self.hoard.config(), remote)

        logging.info(f"Reading current contents of {remote_uuid}...")
        connected_repo = self.hoard.connect_to_repo(remote_uuid, require_contents=True)
        with connected_repo.open_contents(is_readonly=True) as current_contents:
            logging.info(f"Loading hoard TOML...")
            async with self.hoard.open_contents(create_missing=False) as hoard:
                logging.info(f"Loaded hoard TOML!")
                logging.info(f"Computing status ...")

                with StringIO() as out:
                    out.write(f"Root: {safe_hex(hoard.env.roots(False)['HOARD'].desired)}\n")
                    dump_remotes(self.hoard.config(), hoard, out)
                    out.write(f"Status of {self.hoard.config().remotes[remote_uuid].name}:\n")

                    copy_local_staging_to_hoard(hoard, current_contents, self.hoard.config())

                    await execute_print_differences(hoard, current_contents.uuid, ignore_missing, out)
                    return out.getvalue()

    async def status(
            self, path: str | None = None, hide_time: bool = False, hide_disk_sizes: bool = False,
            show_empty: bool = False):
        config = self.hoard.config()
        async with self.hoard.open_contents(create_missing=False) as hoard:
            statuses: Dict[str, Dict[str, Dict[str, Any]]] = hoard.fsobjects.status_by_uuid(
                FastPosixPath(path) if path else None)
            available_states, statuses_sorted = augment_statuses(config, hoard, show_empty, statuses)

            all_stats = ["total", *(s for s in (
                HoardFileStatus.AVAILABLE.value, HoardFileStatus.GET.value,
                HoardFileStatus.COPY.value, HoardFileStatus.MOVE.value,
                HoardFileStatus.CLEANUP.value) if s in available_states)]
            with StringIO() as out:
                out.write(f"Root: {safe_hex(hoard.env.roots(False)['HOARD'].desired)}\n")
                out.write(f"|{'Num Files':<25}|")
                if not hide_time:
                    out.write(f"{'updated':>20}|")
                if not hide_disk_sizes:
                    out.write(f"{'max':>8}|")

                for col in all_stats:
                    out.write(f"{col:<10}|")
                out.write("\n")

                for name, uuid, updated_maybe, uuid_stats in statuses_sorted:
                    out.write(f"|{name:<25}|")
                    if not hide_time:
                        updated = humanize.naturaltime(updated_maybe) if updated_maybe is not None else "never"
                        out.write(f"{updated:>20}|")
                    if not hide_disk_sizes:
                        out.write(f"{format_size(hoard.config.max_size(uuid)):>8}|")

                    for stat in all_stats:
                        nfiles = uuid_stats[stat]["nfiles"] if stat in uuid_stats else ""
                        out.write(f"{nfiles:>10}|")
                    out.write("\n")

                out.write("\n")

                out.write(f"|{'Size':<25}|")
                if not hide_time:
                    out.write(f"{'updated':>20}|")
                if not hide_disk_sizes:
                    out.write(f"{'max':>8}|")

                for col in all_stats:
                    out.write(f"{col:<10}|")
                out.write("\n")
                for name, uuid, updated_maybe, uuid_stats in statuses_sorted:
                    out.write(f"|{name:<25}|")
                    if not hide_time:
                        updated = humanize.naturaltime(updated_maybe) if updated_maybe is not None else "never"
                        out.write(f"{updated:>20}|")
                    if not hide_disk_sizes:
                        out.write(f"{format_size(hoard.config.max_size(uuid)):>8}|")
                    for stat in all_stats:
                        size = format_size(uuid_stats[stat]["size"]) if stat in uuid_stats else ""
                        out.write(f"{size:>10}|")
                    out.write("\n")

                return out.getvalue()

    async def ls(
            self, selected_path: Optional[str] = None, depth: int = None,
            skip_folders: bool = False, show_remotes: int = False):
        logging.info(f"Loading hoard TOML...")
        async with self.hoard.open_contents(create_missing=False) as hoard:
            if depth is None:
                depth = sys.maxsize if selected_path is None else 1

            if selected_path is None:
                selected_path = "/"
            if not custom_isabs(selected_path):
                return f"Use absolute paths, {selected_path} is relative."

            pathing = HoardPathing(self.hoard.config(), self.hoard.paths())

            logging.info(f"Listing files...")
            with StringIO() as out:
                out.write(f"Root: {safe_hex(hoard.env.roots(False)['HOARD'].desired)}\n")
                dump_remotes(self.hoard.config(), hoard, out)

                file: Optional[HoardFile]
                folder: Optional[HoardDir]
                for folder, file in (await hoard.fsobjects.tree).walk(selected_path, depth=depth):
                    if file is not None:
                        stats = _file_stats(file.props)
                        out.write(f"{file.fullname} = {stats}\n")

                    if not skip_folders and folder is not None:
                        if show_remotes:
                            repos_availability = sorted(
                                pathing.repos_availability(folder.fullname).items(),
                                key=lambda v: v[0].name)  # sort by repo name
                            remotes_stats = ", ".join([f"({repo.name}:{path})" for repo, path in repos_availability])

                            appendix = f' => {remotes_stats}' if remotes_stats != '' else ''
                            out.write(f"{folder.fullname}{appendix}\n")
                        else:
                            out.write(f"{folder.fullname}\n")

                out.write("DONE")
                return out.getvalue()

    async def drop(self, repo: str, path: str):
        return await self._run_op(repo, path, _execute_drop, is_readonly=False)

    async def get(self, repo: str, path: str):
        return await self._run_op(repo, path, _execute_get, is_readonly=False)

    async def _run_op(self, repo: str, path: str,
                      fun: Callable[[HoardContents, HoardPathing, str, FastPosixPath], Awaitable[str]],
                      is_readonly: bool):
        config = self.hoard.config()
        if custom_isabs(path):
            return f"Path {path} must be relative, but is absolute."

        pathing = HoardPathing(self.hoard.config(), self.hoard.paths())

        logging.info(f"Loading hoard TOML...")
        conn = self.hoard.open_contents(create_missing=False)
        if not is_readonly:
            conn = conn.writeable()
        async with conn as hoard:
            repo_uuid = resolve_remote_uuid(self.hoard.config(), repo)
            repo_mounted_at = config.remotes[repo_uuid].mounted_at
            logging.info(f"repo {repo} mounted at {repo_mounted_at}")

            return await fun(hoard, pathing, repo_uuid, FastPosixPath(path))

    async def pull(
            self, remote: Optional[str] = None, all: bool = False, ignore_epoch: bool = False,
            force_fetch_local_missing: bool = False, assume_current: bool = False):
        logging.info("Loading config")
        config = self.hoard.config()

        if all:
            assert remote is None
            if assume_current:
                return f"Cannot use --assume-current with --all!"
            logging.info("Pulling from all remotes!")
            remote_uuids = [r.uuid for r in config.remotes.all()]
        else:
            assert remote is not None
            remote_uuids = [remote]

        if assume_current:
            if not ignore_epoch:
                logging.info(f"Forcing --ignore-epoch because --assume-current = True.")
                ignore_epoch = True

        with StringIO() as out:
            for remote_uuid in remote_uuids:
                remote_uuid = resolve_remote_uuid(self.hoard.config(), remote_uuid)
                remote_obj = config.remotes[remote_uuid]
                logging.info(f"Pulling contents of {remote_obj.name}[{remote_uuid}].")

                if remote_obj is None or remote_obj.mounted_at is None:
                    out.write(f"Remote {remote_uuid} is not mounted!\n")
                    continue

                preferences = init_pull_preferences(remote_obj, assume_current, force_fetch_local_missing)
                await execute_pull(self.hoard, preferences, ignore_epoch, out)

            out.write("DONE")
            return out.getvalue()

    async def restore(self, remote: str):
        logging.info("Loading config")
        config = self.hoard.config()

        with StringIO() as out:
            remote_uuid = resolve_remote_uuid(self.hoard.config(), remote)
            remote_obj = config.remotes[remote_uuid]
            logging.info(f"Pulling contents of {remote_obj.name}[{remote_uuid}].")

            if remote_obj is None or remote_obj.mounted_at is None:
                out.write(f"Remote {remote_uuid} is not mounted!\n")
                return

            async with self.hoard.open_contents(create_missing=False).writeable() as hoard_contents:
                roots = hoard_contents.env.roots(True)

                dump_before_op(roots, remote_uuid, out)

                # sets current to whatever is currently available, but do not update desired
                # this effectively discards all changes
                roots[remote_uuid].current = roots[remote_uuid].staging

                repo_root = roots[remote_uuid]
                hoard_root = roots["HOARD"]

                # print what actually changed for the hoard and the repo todo consider printing other repo changes?
                print_differences_for_id(hoard_contents, hoard_root, repo_root, hoard_root.desired,
                                         repo_root.desired, out)

                dump_after_op(roots, remote_uuid, out)

            out.write("DONE")
            return out.getvalue()

    async def reset(self, repo: str):
        config = self.hoard.config()

        repo_uuid = resolve_remote_uuid(config, repo)

        with StringIO() as out:
            await clear_pending_file_ops(self.hoard, repo_uuid, out)

            out.write("DONE")
            return out.getvalue()


def dump_remotes(hoard_config: HoardConfig, hoard: HoardContents, out: TextIO):
    remotes = hoard_config.remotes.all()
    for remote in sorted(remotes, key=lambda r: r.name):
        repo_root = hoard.env.roots(write=False)[remote.uuid]

        out.write(
            f"Remote {remote.name} current={safe_hex(repo_root.current)[:6]} "
            f"staging={safe_hex(repo_root.staging)[:6]} desired={safe_hex(repo_root.desired)[:6]}\n")


async def _execute_get(
        hoard: HoardContents, pathing: HoardPathing, repo_uuid: str, path_in_local: FastPosixPath) -> str:
    mounted_at: FastPosixPath = pathing.mounted_at(repo_uuid)
    path_in_hoard = mounted_at.joinpath(path_in_local)
    with StringIO() as out:
        await execute_get(hoard, pathing, repo_uuid, path_in_hoard, out)
        return out.getvalue()


async def execute_get(
        hoard: HoardContents, pathing: HoardPathing, repo_uuid: str, path_in_hoard: FastPosixPath,
        out: TextIO) -> None:
    assert path_in_hoard.is_absolute()

    old_desired_id = hoard.env.roots(False)[repo_uuid].desired
    with hoard.env.objects(write=True) as objects:
        path_in_tree = path_in_hoard._rem
        hoard_root = hoard.env.roots(False)["HOARD"]
        new_desired_id = graft_in_tree(objects, old_desired_id, path_in_tree, hoard_root.desired)

        considered = dump_changed_files_info(objects, path_in_tree, old_desired_id, new_desired_id, out)

    hoard.env.roots(True)[repo_uuid].desired = new_desired_id

    out.write(f"Considered {considered} files.\n")
    out.write("DONE")


def dump_changed_files_info(objects, path_in_tree: List[str], old_desired_root_id, new_desired_root_id, out):
    old_desired_id = get_child(objects, path_in_tree, old_desired_root_id)
    new_desired_id = get_child(objects, path_in_tree, new_desired_root_id)

    considered = 0
    for file_path, (old_id, new_id), _ in zip_trees_dfs(
            objects, '/' + '/'.join(path_in_tree), [old_desired_id, new_desired_id], True):
        if new_id is not None:
            new_obj = objects[new_id]
            if isinstance(new_obj, FileObject):
                considered += 1

                if old_id != new_id:
                    out.write(f"+{file_path}\n")
    return considered


async def _execute_drop(
        hoard: HoardContents, pathing: HoardPathing, repo_uuid: str, path_in_local: FastPosixPath) -> str:
    mounted_at: FastPosixPath = pathing.mounted_at(repo_uuid)
    path_in_hoard = mounted_at.joinpath(path_in_local)
    with StringIO() as out:
        await execute_drop(hoard, pathing, repo_uuid, path_in_hoard, out)
        return out.getvalue()


async def execute_drop(
        hoard: HoardContents, pathing: HoardPathing, repo_uuid: str, path_in_hoard: FastPosixPath,
        out: TextIO) -> Tuple[int, int, int]:
    assert path_in_hoard.is_absolute()

    old_desired_id = hoard.env.roots(write=False)[repo_uuid].desired

    considered = 0
    cleaned_up, wont_get, skipped = 0, 0, 0

    print(f"Iterating files and folders to see what to drop...")
    hoard_file: FastPosixPath
    for hoard_file, hoard_props in alive_it([s async for s in hoard.fsobjects.in_folder(path_in_hoard)]):
        assert isinstance(hoard_props, HoardFileProps)

        remove_from_desired_tree(hoard, repo_uuid, hoard_file)

    new_desired_id = hoard.env.roots(write=False)[repo_uuid].desired

    with hoard.env.objects(write=False) as objects:
        cleaned_up = dump_dropped_files_info(
            objects, path_in_hoard._rem, old_desired_id, new_desired_id, out)

    out.write(
        f"{cleaned_up} marked for cleanup.\n")
    out.write("DONE")

    return cleaned_up, wont_get, skipped


def dump_dropped_files_info(objects, path_in_tree: List[str], old_desired_root_id, new_desired_root_id, out):
    old_desired_id = get_child(objects, path_in_tree, old_desired_root_id)
    new_desired_id = get_child(objects, path_in_tree, new_desired_root_id)

    cleaned_up = 0
    for file_path, (old_id, new_id), _ in zip_trees_dfs(
            objects, '/' + '/'.join(path_in_tree), [old_desired_id, new_desired_id], True):
        if old_id is not None:
            old_obj = objects[old_id]
            if isinstance(old_obj, FileObject):
                assert new_id is None
                cleaned_up += 1
                out.write(f"DROP {file_path}\n")
    return cleaned_up


STATUSES_ALREADY_ENABLED = [HoardFileStatus.AVAILABLE, HoardFileStatus.GET]
