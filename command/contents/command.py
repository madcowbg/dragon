import logging
import sys
from io import StringIO
from typing import List, Dict, Any, Optional, Callable, Awaitable, Tuple, TextIO

import humanize
from alive_progress import alive_bar, alive_it

from command.content_prefs import ContentPrefs
from command.contents.comparisons import compare_local_to_hoard
from command.contents.handle_pull import PullPreferences, pull_repo_contents_to_hoard, _handle_local_only, \
    reset_local_as_current, PullBehavior
from command.fast_path import FastPosixPath
from command.hoard import Hoard
from command.pathing import HoardPathing
from command.pending_file_ops import GetFile, CopyFile, CleanupFile, get_pending_operations
from config import CaveType, HoardRemote
from contents.hoard import HoardContents, HoardFile, HoardDir
from contents.hoard_props import HoardFileStatus, HoardFileProps
from contents.repo_props import RepoFileProps, RepoFileStatus
from contents_diff import FileIsSame, FileContentsDiffer, FileOnlyInHoardLocalDeleted, FileOnlyInHoardLocalUnknown, \
    FileOnlyInHoardLocalMoved, FileOnlyInLocal
from exceptions import MissingRepoContents
from resolve_uuid import resolve_remote_uuid
from util import format_size, custom_isabs


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
        content_prefs: ContentPrefs, remote_uuid: str,
        assume_current: bool = False, force_fetch_local_missing: bool = False) -> PullPreferences:
    return PullPreferences(
        remote_uuid, content_prefs,
        on_same_file_is_present=PullBehavior.ADD,
        on_file_added_or_present=PullBehavior.ADD,
        on_file_is_different_and_modified=PullBehavior.ADD,
        on_file_is_different_and_added=PullBehavior.ADD,
        on_file_is_different_but_present=
        PullBehavior.RESTORE if not assume_current else PullBehavior.ADD,
        on_hoard_only_local_moved=PullBehavior.MOVE_ON_HOARD,
        on_hoard_only_local_deleted=
        PullBehavior.DELETE_FROM_HOARD if not force_fetch_local_missing else PullBehavior.RESTORE_AS_HOARD,
        on_hoard_only_local_unknown=PullBehavior.ACCEPT_FROM_HOARD)


def _init_pull_preferences_backup(content_prefs: ContentPrefs, remote_uuid: str) -> PullPreferences:
    return PullPreferences(
        remote_uuid, content_prefs,
        on_same_file_is_present=PullBehavior.ADD,
        on_file_added_or_present=PullBehavior.IGNORE,
        on_file_is_different_and_modified=PullBehavior.RESTORE,
        on_file_is_different_and_added=PullBehavior.RESTORE,
        on_file_is_different_but_present=PullBehavior.RESTORE,
        on_hoard_only_local_moved=PullBehavior.RESTORE_AS_HOARD,
        on_hoard_only_local_deleted=PullBehavior.RESTORE_AS_HOARD,
        on_hoard_only_local_unknown=PullBehavior.RESTORE_AS_HOARD)


def _init_pull_preferences_incoming(content_prefs: ContentPrefs, remote_uuid: str) -> PullPreferences:
    return PullPreferences(
        remote_uuid, content_prefs,
        on_same_file_is_present=PullBehavior.ADD_TO_OTHERS_AND_CLEANUP,
        on_file_added_or_present=PullBehavior.ADD_TO_OTHERS_AND_CLEANUP,
        on_file_is_different_and_modified=PullBehavior.ADD_TO_OTHERS_AND_CLEANUP,
        on_file_is_different_and_added=PullBehavior.ADD_TO_OTHERS_AND_CLEANUP,
        on_file_is_different_but_present=PullBehavior.ADD_TO_OTHERS_AND_CLEANUP,
        on_hoard_only_local_moved=PullBehavior.IGNORE,
        on_hoard_only_local_deleted=PullBehavior.IGNORE,
        on_hoard_only_local_unknown=PullBehavior.IGNORE)


def augment_statuses(config, hoard, show_empty, statuses):
    statuses_present = \
        [(config.remotes[uuid].name, uuid, hoard.config.updated(uuid), vals)  # those that have files recorded
         for uuid, vals in statuses.items()] + \
        [(remote.name, remote.uuid, hoard.config.updated(remote.uuid), {})  # those lacking a recorded file
         for remote in config.remotes.all() if show_empty and remote.uuid not in statuses]
    statuses_sorted = sorted(statuses_present)
    available_states = set(sum((list(stats.keys()) for _, _, _, stats in statuses_sorted), []))
    return available_states, statuses_sorted


async def execute_pull(hoard: Hoard, remote_obj: HoardRemote, ignore_epoch: bool, assume_current: bool,
                       force_fetch_local_missing: bool, out: StringIO):
    config = hoard.config()
    pathing = HoardPathing(config, hoard.paths())

    logging.info(f"Loading hoard contents TOML...")
    async with hoard.open_contents(create_missing=False, is_readonly=False) as hoard_contents:
        logging.info(f"Loaded hoard contents TOML!")
        content_prefs = ContentPrefs(config, pathing, hoard_contents)

        try:
            connected_repo = hoard.connect_to_repo(remote_obj.uuid, require_contents=True)
            current_contents = connected_repo.open_contents(is_readonly=True)
        except MissingRepoContents as e:
            logging.error(e)
            out.write(f"Repo {remote_obj.uuid} has no current contents available!\n")
            return

        with current_contents:
            if current_contents.config.is_dirty:
                logging.error(
                    f"{remote_obj.uuid} is_dirty = TRUE, so the refresh is not complete - can't use current repo.")
                out.write(f"Skipping update as {remote_obj.uuid} is not fully calculated!\n")
                return

            if not ignore_epoch and hoard_contents.config.remote_epoch(
                    remote_obj.uuid) >= current_contents.config.epoch:
                out.write(f"Skipping update as past epoch {current_contents.config.epoch} "
                          f"is not after hoard epoch {hoard_contents.config.remote_epoch(remote_obj.uuid)}\n")
                return

            logging.info(f"Saving config of remote {remote_obj.uuid}...")
            hoard_contents.config.save_remote_config(current_contents.config)

            if remote_obj.type == CaveType.INCOMING:
                preferences = _init_pull_preferences_incoming(content_prefs, remote_obj.uuid)
            elif remote_obj.type == CaveType.BACKUP:
                preferences = _init_pull_preferences_backup(content_prefs, remote_obj.uuid)
            else:
                assert remote_obj.type == CaveType.PARTIAL
                preferences = _init_pull_preferences_partial(
                    content_prefs, remote_obj.uuid, assume_current, force_fetch_local_missing)

            await pull_repo_contents_to_hoard(
                hoard_contents, pathing, config, current_contents, preferences, out)

            logging.info(f"Updating epoch of {remote_obj.uuid} to {current_contents.config.epoch}")
            hoard_contents.config.mark_up_to_date(
                remote_obj.uuid, current_contents.config.epoch, current_contents.config.updated)

        clean_dangling_files(hoard_contents, out)

    out.write(f"Sync'ed {remote_obj.name} to hoard!\n")

class HoardCommandContents:
    def __init__(self, hoard: Hoard):
        self.hoard = hoard

    async def pending(self, remote: str, ignore_missing: bool = False):
        remote_uuid = resolve_remote_uuid(self.hoard.config(), remote)

        logging.info(f"Reading current contents of {remote_uuid}...")
        connected_repo = self.hoard.connect_to_repo(remote_uuid, require_contents=True)
        with connected_repo.open_contents(is_readonly=True) as current_contents:
            logging.info(f"Loading hoard TOML...")
            async with self.hoard.open_contents(create_missing=False, is_readonly=True) as hoard:
                logging.info(f"Loaded hoard TOML!")
                logging.info(f"Computing status ...")

                with StringIO() as out:
                    out.write(f"Status of {self.hoard.config().remotes[remote_uuid].name}:\n")

                    async for diff in compare_local_to_hoard(
                            current_contents, hoard, HoardPathing(self.hoard.config(), self.hoard.paths())):
                        if isinstance(diff, FileOnlyInLocal):
                            if diff.is_added:
                                out.write(f"ADDED {diff.hoard_file.as_posix()}\n")
                            else:
                                out.write(f"PRESENT {diff.hoard_file.as_posix()}\n")
                        elif isinstance(diff, FileContentsDiffer):
                            out.write(f"MODIFIED {diff.hoard_file.as_posix()}\n")
                        elif isinstance(diff, FileOnlyInHoardLocalDeleted):
                            out.write(f"DELETED {diff.hoard_file.as_posix()}\n")
                        elif isinstance(diff, FileOnlyInHoardLocalUnknown):
                            if not ignore_missing:
                                out.write(f"MISSING {diff.hoard_file.as_posix()}\n")
                        elif isinstance(diff, FileOnlyInHoardLocalMoved):
                            out.write(f"MOVED {diff.hoard_file.as_posix()}\n")
                        elif isinstance(diff, FileIsSame):
                            pass
                        else:
                            raise ValueError(f"Unused diff class: {type(diff)}")

                    out.write("DONE")
                    return out.getvalue()

    async def status(
            self, path: str | None = None, hide_time: bool = False, hide_disk_sizes: bool = False,
            show_empty: bool = False):
        config = self.hoard.config()
        async with self.hoard.open_contents(create_missing=False, is_readonly=True) as hoard:
            statuses: Dict[str, Dict[str, Dict[str, Any]]] = hoard.fsobjects.status_by_uuid(
                FastPosixPath(path) if path else None)
            available_states, statuses_sorted = augment_statuses(config, hoard, show_empty, statuses)

            all_stats = ["total", *(s for s in (HoardFileStatus.AVAILABLE.value, HoardFileStatus.GET.value,
                                                HoardFileStatus.COPY.value, HoardFileStatus.MOVE.value,
                                                HoardFileStatus.CLEANUP.value) if s in available_states)]
            with StringIO() as out:
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
        async with self.hoard.open_contents(create_missing=False, is_readonly=True) as hoard:
            if depth is None:
                depth = sys.maxsize if selected_path is None else 1

            if selected_path is None:
                selected_path = "/"
            if not custom_isabs(selected_path):
                return f"Use absolute paths, {selected_path} is relative."

            pathing = HoardPathing(self.hoard.config(), self.hoard.paths())

            logging.info(f"Listing files...")
            with StringIO() as out:
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

    async def copy(self, from_path: str, to_path: str):
        assert custom_isabs(from_path), f"From path {from_path} must be absolute path."
        assert custom_isabs(to_path), f"To path {to_path} must be absolute path."

        print(f"Marking files for copy {from_path} to {to_path}...")
        async with self.hoard.open_contents(create_missing=False, is_readonly=False) as hoard:
            with StringIO() as out:
                with alive_bar(len(hoard.fsobjects)) as bar:
                    for hoard_path, _ in hoard.fsobjects:
                        if not hoard_path.is_relative_to(from_path):
                            print(f"Skip copying {hoard_path} as is not in {from_path}...")
                            continue
                        # file or dir is to be copied
                        relpath = hoard_path.relative_to(from_path)
                        to_fullpath = FastPosixPath(to_path).joinpath(relpath)
                        logging.info(f"Copying {hoard_path} to {to_fullpath}")

                        hoard.fsobjects.copy(hoard_path, to_fullpath)
                        out.write(f"c+ {to_fullpath.as_posix()}\n")
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
        async with self.hoard.open_contents(create_missing=False, is_readonly=is_readonly) as hoard:
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

                await execute_pull(self.hoard, remote_obj, ignore_epoch, assume_current, force_fetch_local_missing, out)

            out.write("DONE")
            return out.getvalue()

    async def reset_with_existing(self, repo: str):
        config = self.hoard.config()
        pathing = HoardPathing(config, self.hoard.paths())

        repo_uuid = resolve_remote_uuid(config, repo)
        remote = config.remotes[repo_uuid]

        logging.info(f"Loading hoard contents...")
        async with self.hoard.open_contents(create_missing=False, is_readonly=False) as hoard:
            content_prefs = ContentPrefs(config, pathing, hoard)

            with StringIO() as out:
                out.write(f"{config.remotes[repo_uuid].name}:\n")

                logging.info(f"Iterating over pending ops in {repo_uuid} to reset pending ops")
                connected_repo = self.hoard.connect_to_repo(repo_uuid, True)
                with connected_repo.open_contents(is_readonly=True) as current_contents:
                    for local_file, local_props in alive_it(current_contents.fsobjects.existing()):
                        assert isinstance(local_props, RepoFileProps)

                        hoard_file = pathing.in_local(local_file, repo_uuid).at_hoard().as_pure_path
                        if hoard_file not in hoard.fsobjects:
                            logging.info(f"Local file {local_file} will be handled to hoard.")
                            _handle_local_only(
                                PullPreferences(
                                    remote.uuid, content_prefs,
                                    on_same_file_is_present=PullBehavior.ADD,
                                    on_file_added_or_present=PullBehavior.FAIL,
                                    on_file_is_different_and_modified=PullBehavior.FAIL,
                                    on_file_is_different_and_added=PullBehavior.FAIL,
                                    on_file_is_different_but_present=PullBehavior.FAIL,
                                    on_hoard_only_local_moved=PullBehavior.FAIL,
                                    on_hoard_only_local_unknown=PullBehavior.FAIL,
                                    on_hoard_only_local_deleted=PullBehavior.FAIL,
                                ),
                                FileOnlyInLocal(
                                    local_file, hoard_file, local_props,
                                    local_props.last_status == RepoFileStatus.ADDED),
                                hoard,
                                StringIO())  # fixme make it elegant
                            out.write(f"READD {hoard_file}\n")
                        else:
                            hoard_props = hoard.fsobjects[hoard_file]

                            if hoard_props.get_status(repo_uuid) != HoardFileStatus.AVAILABLE:
                                logging.info(
                                    f"Local file {local_file} is not marked available, will reset its contents in repo")

                                reset_local_as_current(hoard, repo_uuid, hoard_file, hoard_props, local_props)
                                out.write(f"RESET {hoard_file}\n")

                out.write("DONE")
                return out.getvalue()

    async def reset(self, repo: str):
        config = self.hoard.config()
        pathing = HoardPathing(config, self.hoard.paths())

        repo_uuid = resolve_remote_uuid(config, repo)

        logging.info(f"Loading hoard contents...")
        async with self.hoard.open_contents(create_missing=False, is_readonly=False) as hoard:
            with StringIO() as out:
                out.write(f"{config.remotes[repo_uuid].name}:\n")

                logging.info(f"Iterating over pending ops in {repo_uuid} to reset pending ops")

                ops = list(get_pending_operations(hoard, repo_uuid))
                print(f"Clearing {len(ops)} pending operations...")
                for op in alive_it(ops):
                    local_file = pathing.in_hoard(op.hoard_file).at_local(repo_uuid).as_pure_path.as_posix()
                    assert local_file is not None

                    if isinstance(op, GetFile):
                        logging.info(f"File to get {local_file} is already missing, removing status.")
                        op.hoard_props.remove_status(repo_uuid)

                        out.write(f"WONT_GET {op.hoard_file.as_posix()}\n")
                    elif isinstance(op, CopyFile):
                        logging.info(
                            f"File to get {local_file} is already missing, removing status.")
                        op.hoard_props.remove_status(repo_uuid)
                        out.write(f"WONT_COPY {op.hoard_file.as_posix()}\n")
                    elif isinstance(op, CleanupFile):
                        op.hoard_props.remove_status(repo_uuid)

                        out.write(f"WONT_CLEANUP {op.hoard_file.as_posix()}\n")
                    else:
                        raise ValueError(f"Unhandled op type: {type(op)}")

                out.write("DONE")
                return out.getvalue()


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
    considered = 0
    print(f"Iterating over {len(hoard.fsobjects)} files and folders...")
    for hoard_file, hoard_props in alive_it([s async for s in hoard.fsobjects.in_folder(path_in_hoard)]):
        assert isinstance(hoard_props, HoardFileProps)

        local_file = pathing.in_hoard(hoard_file).at_local(repo_uuid)
        assert local_file is not None  # is not addressable here at all

        considered += 1

        if hoard_props.get_status(repo_uuid) not in STATUSES_ALREADY_ENABLED:
            logging.info(f"enabling file {hoard_file} on {repo_uuid}")
            hoard_props.mark_to_get([repo_uuid])
            out.write(f"+{hoard_file}\n")

    out.write(f"Considered {considered} files.\n")
    out.write("DONE")


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

    considered = 0
    cleaned_up, wont_get, skipped = 0, 0, 0

    print(f"Iterating files and folders to see what to drop...")
    hoard_file: FastPosixPath
    for hoard_file, hoard_props in alive_it([s async for s in hoard.fsobjects.in_folder(path_in_hoard)]):
        assert isinstance(hoard_props, HoardFileProps)

        local_file = pathing.in_hoard(hoard_file).at_local(repo_uuid)
        assert local_file is not None  # is not addressable here at all

        considered += 1

        goal_status = hoard_props.get_status(repo_uuid)
        if goal_status == HoardFileStatus.AVAILABLE:
            logging.info(f"File {hoard_file} is available, mapping for removal from {repo_uuid}.")

            hoard_props.mark_for_cleanup([repo_uuid])
            out.write(f"DROP {hoard_file.as_posix()}\n")

            cleaned_up += 1
        elif goal_status == HoardFileStatus.GET or goal_status == HoardFileStatus.COPY:
            logging.info(f"File {hoard_file} is already not in repo, removing status.")

            hoard_props.remove_status(repo_uuid)
            out.write(f"WONT_GET {hoard_file.as_posix()}\n")

            wont_get += 1
        elif goal_status == HoardFileStatus.CLEANUP or goal_status == HoardFileStatus.UNKNOWN:
            logging.info(f"Skipping {hoard_file} as it is already missing.")
            skipped += 1
        else:
            raise ValueError(f"Unexpected status for {hoard_file}: {goal_status}")

    out.write(
        f"Considered {considered} files, {cleaned_up} marked for cleanup, "
        f"{wont_get} won't be downloaded, {skipped} are skipped.\n")
    out.write("DONE")

    return cleaned_up, wont_get, skipped


STATUSES_ALREADY_ENABLED = [HoardFileStatus.AVAILABLE, HoardFileStatus.GET]


def clean_dangling_files(hoard: HoardContents, out: StringIO):  # fixme do this when status is modified, not after
    logging.info("Cleaning dangling files from hoard...")

    for dangling_path, props in hoard.fsobjects.dangling_files:
        assert len(props.presence) == 0
        logging.warning(f"Removing dangling path {dangling_path} from hoard!")
        hoard.fsobjects.delete(dangling_path)
        out.write(f"remove dangling {dangling_path}\n")
