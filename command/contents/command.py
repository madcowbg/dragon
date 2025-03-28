import logging
import os
import pathlib
import sys
from io import StringIO
from typing import List, Dict, Any, Optional, Generator, Callable

import humanize
from alive_progress import alive_bar, alive_it

from command.pending_file_ops import GetFile, CopyFile, CleanupFile, get_pending_operations
from command.hoard import Hoard
from command.contents.diff_handlers import DiffHandler, PartialDiffHandler, BackupDiffHandler, IncomingDiffHandler, \
    reset_local_as_current
from command.content_prefs import ContentPrefs
from command.pathing import HoardPathing
from command.repo import RepoHasNoContents
from config import CaveType, HoardConfig, HoardPaths, HoardRemote
from contents.hoard import HoardContents, HoardFile, HoardDir

from contents.props import HoardFileProps, HoardFileStatus, RepoFileProps, DirProps, RepoDirProps
from contents.repo import RepoContents
from contents_diff import FileMissingInHoard, FileIsSame, FileContentsDiffer, FileMissingInLocal, DirMissingInHoard, \
    Diff, DirIsSame, DirMissingInLocal
from resolve_uuid import resolve_remote_uuid
from util import format_size


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


class HoardCommandContents:
    def __init__(self, hoard: Hoard):
        self.hoard = hoard

    def status(self, hide_time: bool = False):
        config = self.hoard.config()
        with HoardContents.load(self.hoard.hoard_contents_filename()) as hoard:
            statuses: Dict[str, Dict[str, Dict[str, Any]]] = hoard.fsobjects.status_by_uuid
            statuses_sorted = sorted(
                (config.remotes[uuid].name, hoard.updated(uuid), vals) for uuid, vals in statuses.items())
            all_stats = ["total", HoardFileStatus.AVAILABLE.value, HoardFileStatus.GET.value,
                         HoardFileStatus.COPY.value,
                         HoardFileStatus.CLEANUP.value]
            with StringIO() as out:
                out.write(f"|{'Num Files':<25}|")
                if not hide_time:
                    out.write(f"{'updated':>20}|")
                for col in all_stats:
                    out.write(f"{col:<10}|")
                out.write("\n")

                for name, updated_maybe, uuid_stats in statuses_sorted:
                    out.write(f"|{name:<25}|")
                    if not hide_time:
                        updated = humanize.naturaltime(updated_maybe) if updated_maybe is not None else "never"
                        out.write(f"{updated:>20}|")
                    for stat in all_stats:
                        nfiles = uuid_stats[stat]["nfiles"] if stat in uuid_stats else ""
                        out.write(f"{nfiles:>10}|")
                    out.write("\n")

                out.write("\n")

                out.write(f"|{'Size':<25}|")
                if not hide_time:
                    out.write(f"{'updated':>20}|")
                for col in all_stats:
                    out.write(f"{col:<10}|")
                out.write("\n")
                for name, updated_maybe, uuid_stats in statuses_sorted:
                    out.write(f"|{name:<25}|")
                    if not hide_time:
                        updated = humanize.naturaltime(updated_maybe) if updated_maybe is not None else "never"
                        out.write(f"{updated:>20}|")
                    for stat in all_stats:
                        size = format_size(uuid_stats[stat]["size"]) if stat in uuid_stats else ""
                        out.write(f"{size:>10}|")
                    out.write("\n")

                return out.getvalue()

    def ls(
            self, selected_path: Optional[str] = None, depth: int = None,
            skip_folders: bool = False, show_remotes: int = False):
        logging.info(f"Loading hoard TOML...")
        with HoardContents.load(self.hoard.hoard_contents_filename()) as hoard:
            if depth is None:
                depth = sys.maxsize if selected_path is None else 1

            if selected_path is None:
                selected_path = "/"
            if not os.path.isabs(selected_path):
                return f"Use absolute paths, {selected_path} is relative."

            pathing = HoardPathing(self.hoard.config(), self.hoard.paths())

            logging.info(f"Listing files...")
            with StringIO() as out:
                file: Optional[HoardFile]
                folder: Optional[HoardDir]
                for folder, file in hoard.fsobjects.tree.walk(selected_path, depth=depth):
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

    def copy(self, from_path: str, to_path: str):
        assert os.path.isabs(from_path), f"From path {from_path} must be absolute path."
        assert os.path.isabs(to_path), f"To path {to_path} must be absolute path."

        print(f"Marking files for copy {from_path} to {to_path}...")
        with HoardContents.load(self.hoard.hoard_contents_filename()) as hoard:
            with StringIO() as out:
                with alive_bar(len(hoard.fsobjects)) as bar:
                    for hoard_obj, _ in hoard.fsobjects:
                        hoard_path = pathlib.Path(hoard_obj)
                        if not hoard_path.is_relative_to(from_path):
                            print(f"Skip copying {hoard_obj} as is not in {from_path}...")
                            continue
                        # file or dir is to be copied
                        relpath = hoard_path.relative_to(from_path)
                        to_fullpath = pathlib.Path(to_path).joinpath(relpath).as_posix()
                        logging.info(f"Copying {hoard_obj} to {to_fullpath}")

                        hoard.fsobjects.copy(hoard_obj, to_fullpath)
                        out.write(f"c+ {to_fullpath}\n")
                out.write("DONE")
                return out.getvalue()

    def drop(self, repo: str, path: str):
        return self._run_op(repo, path, self._execute_drop)

    def _execute_drop(self, hoard: HoardContents, repo_uuid: str, path: str) -> str:
        pathing = HoardPathing(self.hoard.config(), self.hoard.paths())
        mounted_at = pathing.mounted_at(repo_uuid)

        considered = 0
        cleaned_up, wont_get, skipped = 0, 0, 0
        with StringIO() as out:
            print(f"Iterating files and folders to see what to drop...")
            for hoard_file, hoard_props in alive_it(hoard.fsobjects.in_folder(mounted_at)):
                if not isinstance(hoard_props, HoardFileProps):
                    continue

                local_file = pathing.in_hoard(hoard_file).at_local(repo_uuid)
                assert local_file is not None  # is not addressable here at all

                considered += 1
                if not pathlib.Path(local_file.as_posix()).is_relative_to(path):
                    logging.info(f"file not in {path}: {local_file.as_posix()}, skipping")
                    continue

                goal_status = hoard_props.get_status(repo_uuid)
                if goal_status == HoardFileStatus.AVAILABLE:
                    logging.info(f"File {hoard_file} is available, mapping for removal from {repo_uuid}.")

                    hoard_props.mark_for_cleanup([repo_uuid])
                    out.write(f"DROP {hoard_file}\n")

                    cleaned_up += 1
                elif goal_status == HoardFileStatus.GET or goal_status == HoardFileStatus.COPY:
                    logging.info(f"File {hoard_file} is already not in repo, removing status.")

                    hoard_props.remove_status(repo_uuid)
                    out.write(f"WONT_GET {hoard_file}\n")

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
            return out.getvalue()

    def get(self, repo: str, path: str):
        return self._run_op(repo, path, self._execute_get)

    def _execute_get(self, hoard: HoardContents, repo_uuid: str, path: str) -> str:
        pathing = HoardPathing(self.hoard.config(), self.hoard.paths())

        considered = 0
        with StringIO() as out:
            print(f"Iterating over {len(hoard.fsobjects)} files and folders...")
            for hoard_file, hoard_props in alive_it(hoard.fsobjects):
                if not isinstance(hoard_props, HoardFileProps):
                    continue

                local_file = pathing.in_hoard(hoard_file).at_local(repo_uuid)
                if local_file is None:  # is not addressable here at all
                    continue
                considered += 1
                if not pathlib.Path(local_file.as_posix()).is_relative_to(path):
                    logging.info(f"file not in {path}: {local_file.as_posix()}")
                    continue
                if hoard_props.get_status(repo_uuid) not in STATUSES_ALREADY_ENABLED:
                    logging.info(f"enabling file {hoard_file} on {repo_uuid}")
                    hoard_props.mark_to_get([repo_uuid])
                    out.write(f"+{hoard_file}\n")

            out.write(f"Considered {considered} files.\n")
            out.write("DONE")
            return out.getvalue()

    def _run_op(self, repo: str, path: str, fun: Callable[[HoardContents, str, str], str]):
        config = self.hoard.config()
        if os.path.isabs(path):
            return f"Path {path} must be relative, but is absolute."

        logging.info(f"Loading hoard TOML...")
        with HoardContents.load(self.hoard.hoard_contents_filename()) as hoard:
            repo_uuid = resolve_remote_uuid(self.hoard.config(), repo)
            repo_mounted_at = config.remotes[repo_uuid].mounted_at
            logging.info(f"repo {repo} mounted at {repo_mounted_at}")

            return fun(hoard, repo_uuid, path)

    def pull(
            self, remote: Optional[str] = None, all: bool = False, ignore_epoch: bool = False,
            force_fetch_local_missing: bool = False, assume_current: bool = False):
        logging.info("Loading config")
        config = self.hoard.config()
        pathing = HoardPathing(config, self.hoard.paths())

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
                print(f"Pulling contents of {remote_obj.name}[{remote_uuid}].")

                logging.info(f"Loading hoard TOML...")
                with HoardContents.load(self.hoard.hoard_contents_filename()) as hoard:
                    logging.info(f"Loaded hoard TOML!")
                    content_prefs = ContentPrefs(config, pathing, hoard)

                    remote_op_handler = init_handler(
                        hoard, remote_obj, content_prefs,
                        assume_current, force_fetch_local_missing)

                    try:
                        current_contents = self.hoard.connect_to_repo(remote_uuid, require_contents=True) \
                            .open_contents()
                    except RepoHasNoContents:
                        out.write(f"Repo {remote_uuid} has no current contents available!\n")
                        continue

                    with current_contents:
                        if current_contents.config.is_dirty:
                            logging.error(
                                f"{remote_uuid} is_dirty = TRUE, so the refresh is not complete - can't use current repo.")
                            out.write(f"Skipping update as {remote_uuid} is not fully calculated!\n")
                            continue

                        if not ignore_epoch and hoard.epoch(remote_uuid) >= current_contents.config.epoch:
                            out.write(f"Skipping update as past epoch {current_contents.config.epoch} "
                                      f"is not after hoard epoch {hoard.epoch(remote_uuid)}\n")
                            continue

                        remote_doc = config.remotes[remote_uuid]
                        if remote_doc is None or remote_doc.mounted_at is None:
                            out.write(f"Remote {remote_uuid} is not mounted!\n")
                            continue

                        logging.info("Merging local changes...")
                        for diff in compare_local_to_hoard(current_contents, hoard, config, self.hoard.paths()):
                            if isinstance(diff, FileMissingInHoard):
                                remote_op_handler.handle_local_only(diff, out)
                            elif isinstance(diff, FileIsSame):
                                remote_op_handler.handle_file_is_same(diff, out)
                            elif isinstance(diff, FileContentsDiffer):
                                remote_op_handler.handle_file_contents_differ(diff, out)
                            elif isinstance(diff, FileMissingInLocal):
                                remote_op_handler.handle_hoard_only(diff, out)
                            elif isinstance(diff, DirMissingInHoard):
                                logging.info(f"new dir found: {diff.local_dir}")
                                hoard.fsobjects.add_dir(diff.hoard_dir)
                            else:
                                logging.info(f"skipping diff of type {type(diff)}")

                        logging.info(f"Updating epoch of {remote_uuid} to {current_contents.config.epoch}")
                        hoard.set_epoch(remote_uuid, current_contents.config.epoch, current_contents.config.updated)

                    clean_dangling_files(hoard, out)
                    logging.info("Writing updated hoard contents...")
                    hoard.write()
                    logging.info("Local commit DONE!")

                out.write(f"Sync'ed {config.remotes[remote_uuid].name} to hoard!\n")
            out.write("DONE")
            return out.getvalue()

    def reset_with_existing(self, repo: str):
        config = self.hoard.config()
        pathing = HoardPathing(config, self.hoard.paths())

        repo_uuid = resolve_remote_uuid(config, repo)
        remote = config.remotes[repo_uuid]

        logging.info(f"Loading hoard contents...")
        with HoardContents.load(self.hoard.hoard_contents_filename()) as hoard:
            content_prefs = ContentPrefs(config, pathing, hoard)

            remote_op_handler = init_handler(
                hoard, remote, content_prefs,
                assume_current=True, force_fetch_local_missing=False)

            with StringIO() as out:
                out.write(f"{config.remotes[repo_uuid].name}:\n")

                logging.info(f"Iterating over pending ops in {repo_uuid} to reset pending ops")
                with self.hoard.connect_to_repo(repo_uuid, True).open_contents() as current_contents:
                    for local_file, local_props in alive_it(current_contents.fsobjects.existing()):
                        if not isinstance(local_props, RepoFileProps):
                            continue

                        hoard_file = pathing.in_local(local_file, repo_uuid).at_hoard().as_posix()
                        if hoard_file not in hoard.fsobjects:
                            logging.info(f"Local file {local_file} will be handled to hoard.")
                            remote_op_handler.handle_local_only(
                                FileMissingInHoard(local_file, hoard_file, local_props),
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

    def reset(self, repo: str):
        config = self.hoard.config()
        pathing = HoardPathing(config, self.hoard.paths())

        repo_uuid = resolve_remote_uuid(config, repo)

        logging.info(f"Loading hoard contents...")
        with HoardContents.load(self.hoard.hoard_contents_filename()) as hoard:
            with StringIO() as out:
                out.write(f"{config.remotes[repo_uuid].name}:\n")

                logging.info(f"Iterating over pending ops in {repo_uuid} to reset pending ops")

                ops = list(get_pending_operations(hoard, repo_uuid))
                print(f"Clearing {len(ops)} pending operations...")
                for op in alive_it(ops):
                    local_file = pathing.in_hoard(op.hoard_file).at_local(repo_uuid).as_posix()
                    assert local_file is not None

                    if isinstance(op, GetFile):
                        logging.info(f"File to get {local_file} is already missing, removing status.")
                        op.hoard_props.remove_status(repo_uuid)

                        out.write(f"WONT_GET {op.hoard_file}\n")
                    elif isinstance(op, CopyFile):
                        logging.info(
                            f"File to get {local_file} is already missing, removing status.")
                        op.hoard_props.remove_status(repo_uuid)
                        out.write(f"WONT_COPY {op.hoard_file}\n")
                    elif isinstance(op, CleanupFile):
                        op.hoard_props.remove_status(repo_uuid)

                        out.write(f"WONT_CLEANUP {op.hoard_file}\n")
                    else:
                        raise ValueError(f"Unhandled op type: {type(op)}")

                out.write("DONE")
                return out.getvalue()


def init_handler(
        hoard: HoardContents, remote_obj: HoardRemote, content_prefs: ContentPrefs,
        assume_current: bool, force_fetch_local_missing: bool):
    if remote_obj.type == CaveType.PARTIAL:
        remote_op_handler: DiffHandler = PartialDiffHandler(
            remote_obj.uuid, hoard, content_prefs,
            force_fetch_local_missing=force_fetch_local_missing,
            assume_current=assume_current)
    elif remote_obj.type == CaveType.BACKUP:
        remote_op_handler: DiffHandler = BackupDiffHandler(remote_obj.uuid, hoard)
    elif remote_obj.type == CaveType.INCOMING:
        remote_op_handler: DiffHandler = IncomingDiffHandler(remote_obj.uuid, hoard, content_prefs)
    else:
        raise ValueError(f"FIXME unsupported remote type: {remote_obj.type}")
    return remote_op_handler


STATUSES_ALREADY_ENABLED = [HoardFileStatus.AVAILABLE, HoardFileStatus.GET]


def clean_dangling_files(hoard: HoardContents, out: StringIO):  # fixme do this when status is modified, not after
    logging.info("Cleaning dangling files from hoard...")

    for dangling_path, props in hoard.fsobjects.dangling_files:
        assert len(props.presence) == 0
        logging.warning(f"Removing dangling path {dangling_path} from hoard!")
        hoard.fsobjects.delete(dangling_path)
        out.write(f"remove dangling {dangling_path}\n")


def is_same_file(current: RepoFileProps, hoard: HoardFileProps):
    if current.size != hoard.size:
        return False  # files differ by size

    if current.fasthash != hoard.fasthash:
        return False  # fast hash is different

    return True  # files are the same


def compare_local_to_hoard(local: RepoContents, hoard: HoardContents, config: HoardConfig, paths: HoardPaths) \
        -> Generator[Diff, None, None]:
    pathing = HoardPathing(config, paths)

    print("Comparing current files to hoard:")
    with alive_bar(local.fsobjects.len_existing()) as bar:
        for current_path, props in local.fsobjects.existing():
            bar()
            if isinstance(props, RepoFileProps):
                current_file = current_path
                curr_file_hoard_path = pathing.in_local(current_file, local.config.uuid).at_hoard()
                if curr_file_hoard_path.as_posix() not in hoard.fsobjects:
                    logging.info(f"local file not in hoard: {curr_file_hoard_path.as_posix()}")
                    yield FileMissingInHoard(current_file, curr_file_hoard_path.as_posix(), props)
                elif is_same_file(
                        local.fsobjects.get_existing(current_file),
                        hoard.fsobjects[curr_file_hoard_path.as_posix()]):
                    logging.info(f"same in hoard {current_file}!")
                    yield FileIsSame(current_file, curr_file_hoard_path.as_posix(), props, hoard.fsobjects[
                        curr_file_hoard_path.as_posix()])
                else:
                    logging.info(f"file changes {current_file}")
                    yield FileContentsDiffer(
                        current_file,
                        curr_file_hoard_path.as_posix(), props, hoard.fsobjects[curr_file_hoard_path.as_posix()])

            elif isinstance(props, RepoDirProps):
                current_dir = current_path
                curr_dir_hoard_path = pathing.in_local(current_dir, local.config.uuid).at_hoard()
                if curr_dir_hoard_path.as_posix() not in hoard.fsobjects:
                    logging.info(f"new dir found: {current_dir}")
                    yield DirMissingInHoard(current_dir, curr_dir_hoard_path.as_posix())
                else:
                    yield DirIsSame(current_dir, curr_dir_hoard_path.as_posix())
            else:
                raise ValueError(f"unknown props type: {type(props)}")

    print("Comparing hoard to current files")
    for hoard_file, props in alive_it(list(hoard.fsobjects.in_folder(pathing.mounted_at(local.config.uuid)))):
        if isinstance(props, HoardFileProps):
            curr_file_path_in_local = pathing.in_hoard(hoard_file).at_local(local.config.uuid)
            assert curr_file_path_in_local is not None  # hoard file is not in the mounted location

            if not local.fsobjects.in_existing(curr_file_path_in_local.as_posix()):
                yield FileMissingInLocal(curr_file_path_in_local.as_posix(), hoard_file, props)
            # else file is there, which is handled above
        elif isinstance(props, DirProps):
            hoard_dir = hoard_file
            curr_dir_path_in_local = pathing.in_hoard(hoard_dir).at_local(local.config.uuid)
            assert curr_dir_path_in_local is not None  # hoard dir is not in the mounted location

            if curr_dir_path_in_local.as_posix() not in hoard.fsobjects:
                logging.info(f"missing dir found in hoard: {hoard_dir}")
                yield DirMissingInLocal(curr_dir_path_in_local.as_posix(), hoard_dir)
            else:
                pass  # existing dirs are handled above
        else:
            raise ValueError(f"unknown props type: {type(props)}")
