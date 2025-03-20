import logging
import os
import pathlib
import sys
from io import StringIO
from typing import List, Dict, Any, Optional, Generator, Iterable

import humanize
from alive_progress import alive_bar, alive_it

from command.hoard import Hoard
from command.contents.diff_handlers import DiffHandler, PartialDiffHandler, BackupDiffHandler, IncomingDiffHandler, \
    ContentPrefs, reset_local_as_current
from command.pathing import HoardPathing
from config import CaveType, HoardConfig, HoardPaths
from contents.hoard import HoardContents, HoardFile, HoardDir

from contents.props import HoardFileProps, FileStatus, RepoFileProps, DirProps
from contents.repo import RepoContents
from contents_diff import FileMissingInHoard, FileIsSame, FileContentsDiffer, FileMissingInLocal, DirMissingInHoard, \
    Diff, DirIsSame, DirMissingInLocal
from resolve_uuid import resolve_remote_uuid
from util import format_size


def _file_stats(props: HoardFileProps) -> str:
    a = props.by_status(FileStatus.AVAILABLE)
    g = props.by_status(FileStatus.GET)
    c = props.by_status(FileStatus.CLEANUP)
    x = props.by_status(FileStatus.COPY)
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


type FileOp = GetFile | CopyFile | CleanupFile


class GetFile:  # fixme needs to know if we would fetch or update a current file
    def __init__(self, hoard_file: str, hoard_props: HoardFileProps):
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props


class CopyFile:
    def __init__(self, hoard_file: str, hoard_props: HoardFileProps):
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props


class CleanupFile:
    def __init__(self, hoard_file: str, hoard_props: HoardFileProps):
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props


class HoardCommandContents:
    def __init__(self, hoard: Hoard):
        self.hoard = hoard

    def status(self, hide_time: bool = False):
        config = self.hoard.config()
        with HoardContents.load(self.hoard.hoard_contents_filename()) as hoard:
            statuses: Dict[str, Dict[str, Dict[str, Any]]] = hoard.fsobjects.status_by_uuid
            statuses_sorted = sorted(
                (config.remotes[uuid].name, hoard.updated(uuid), vals) for uuid, vals in statuses.items())
            all_stats = ["total", FileStatus.AVAILABLE.value, FileStatus.GET.value, FileStatus.COPY.value,
                         FileStatus.CLEANUP.value]
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

    def get(self, repo: str, path: str = ""):
        config = self.hoard.config()
        if os.path.isabs(path):
            return f"Path {path} must be relative, but is absolute."

        logging.info(f"Loading hoard TOML...")
        with HoardContents.load(self.hoard.hoard_contents_filename()) as hoard:

            repo_uuid = resolve_remote_uuid(self.hoard.config(), repo)
            repo_mounted_at = config.remotes[repo_uuid].mounted_at
            logging.info(f"repo {repo} mounted at {repo_mounted_at}")

            pathing = HoardPathing(config, self.hoard.paths())

            considered = 0
            with StringIO() as out:
                for hoard_file, hoard_props in hoard.fsobjects:
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
                    content_prefs = ContentPrefs(config, pathing)

                    if remote_obj.type == CaveType.PARTIAL:
                        remote_op_handler: DiffHandler = PartialDiffHandler(
                            remote_uuid, hoard, content_prefs,
                            force_fetch_local_missing=force_fetch_local_missing,
                            assume_current=assume_current)
                    elif remote_obj.type == CaveType.BACKUP:
                        remote_op_handler: DiffHandler = BackupDiffHandler(remote_uuid, hoard)
                    elif remote_obj.type == CaveType.INCOMING:
                        remote_op_handler: DiffHandler = IncomingDiffHandler(remote_uuid, hoard, content_prefs)
                    else:
                        raise ValueError(f"FIXME unsupported remote type: {remote_obj.type}")

                    if not self.hoard[remote_uuid].has_contents():
                        out.write(f"Repo {remote_uuid} has no current contents available!\n")
                        continue

                    with self.hoard[remote_uuid].open_contents() as current_contents:
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

    def pending(self, repo: Optional[str] = None):
        config = self.hoard.config()

        repo_uuids: List[str] = [resolve_remote_uuid(config, repo)] \
            if repo is not None else [r.uuid for r in config.remotes.all()]

        logging.info(f"Loading hoard contents...")
        with HoardContents.load(self.hoard.hoard_contents_filename()) as hoard:
            with StringIO() as out:
                for repo_uuid in repo_uuids:
                    logging.info(f"Iterating over pending ops in {repo_uuid}")
                    out.write(f"{config.remotes[repo_uuid].name}:\n")

                    repos_containing_what_this_one_needs: Dict[str, int] = dict()
                    for op in _get_pending_operations(hoard, repo_uuid):
                        num_available = op.hoard_props.by_status(FileStatus.AVAILABLE)
                        if isinstance(op, GetFile):
                            out.write(f"TO_GET (from {len(num_available)}) {op.hoard_file}\n")
                            for repo in num_available:
                                repos_containing_what_this_one_needs[repo] = \
                                    repos_containing_what_this_one_needs.get(repo, 0) + 1
                        elif isinstance(op, CopyFile):
                            out.write(f"TO_COPY (from {len(num_available)}+?) {op.hoard_file}\n")
                            for repo in num_available:
                                repos_containing_what_this_one_needs[repo] = \
                                    repos_containing_what_this_one_needs.get(repo, 0) + 1
                        elif isinstance(op, CleanupFile):
                            out.write(f"TO_CLEANUP (is in {len(num_available)}) {op.hoard_file}\n")
                        else:
                            raise ValueError(f"Unhandled op type: {type(op)}")
                    nc = sorted(map(
                        lambda uc: (config.remotes[uc[0]].name, uc[1]),  # uuid, count -> name, count
                        repos_containing_what_this_one_needs.items()))
                    for name, count in nc:
                        out.write(f" {name} has {count} files\n")
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

                with self.hoard[repo_uuid].open_contents() as current_contents:
                    ops = list(_get_pending_operations(hoard, repo_uuid))
                    print("Clearing pending operations...")
                    for op in alive_it(ops):
                        local_file = pathing.in_hoard(op.hoard_file).at_local(repo_uuid).as_posix()
                        assert local_file is not None

                        if local_file in current_contents.fsobjects:
                            assert isinstance(op, GetFile) or isinstance(op, CopyFile) or isinstance(op, CleanupFile)
                            local_props = current_contents.fsobjects[local_file]
                            assert isinstance(local_props, RepoFileProps)
                            logging.info(f"Pending {local_file} is available, will reset its contents")

                            reset_local_as_current(hoard, repo_uuid, op.hoard_file, op.hoard_props, local_props)
                            out.write(f"RESET {op.hoard_file}\n")
                        else:  # file is not available in repo
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


def _get_pending_operations(hoard: HoardContents, repo_uuid: str) -> Iterable[FileOp]:
    for hoard_file, hoard_props in hoard.fsobjects.with_pending(repo_uuid):
        goal_status = hoard_props.get_status(repo_uuid)
        if goal_status == FileStatus.GET:
            yield GetFile(hoard_file, hoard_props)
        elif goal_status == FileStatus.COPY:
            yield CopyFile(hoard_file, hoard_props)
        elif goal_status == FileStatus.CLEANUP:
            yield CleanupFile(hoard_file, hoard_props)
        else:
            raise ValueError(f"File {hoard_file} has no pending ops, yet was selected as one that has.")


STATUSES_ALREADY_ENABLED = [FileStatus.AVAILABLE, FileStatus.GET]


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
    with alive_bar(len(local.fsobjects)) as bar:
        for current_path, props in local.fsobjects:
            bar()
            if isinstance(props, RepoFileProps):
                current_file = current_path
                curr_file_hoard_path = pathing.in_local(current_file, local.config.uuid).at_hoard()
                if curr_file_hoard_path.as_posix() not in hoard.fsobjects:
                    logging.info(f"local file not in hoard: {curr_file_hoard_path.as_posix()}")
                    yield FileMissingInHoard(current_file, curr_file_hoard_path.as_posix(), props)
                elif is_same_file(
                        local.fsobjects[current_file],
                        hoard.fsobjects[curr_file_hoard_path.as_posix()]):
                    logging.info(f"same in hoard {current_file}!")
                    yield FileIsSame(current_file, curr_file_hoard_path.as_posix(), props, hoard.fsobjects[
                        curr_file_hoard_path.as_posix()])
                else:
                    logging.info(f"file changes {current_file}")
                    yield FileContentsDiffer(
                        current_file,
                        curr_file_hoard_path.as_posix(), props, hoard.fsobjects[curr_file_hoard_path.as_posix()])

            elif isinstance(props, DirProps):
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
    with alive_bar(len(hoard.fsobjects)) as bar:
        for hoard_file, props in hoard.fsobjects:
            bar()
            if isinstance(props, HoardFileProps):
                curr_file_path_in_local = pathing.in_hoard(hoard_file).at_local(local.config.uuid)
                if curr_file_path_in_local is None:
                    continue  # hoard file is not in the mounted location

                if curr_file_path_in_local.as_posix() not in local.fsobjects:
                    yield FileMissingInLocal(curr_file_path_in_local.as_posix(), hoard_file, props)
                # else file is there, which is handled above
            elif isinstance(props, DirProps):
                hoard_dir = hoard_file
                curr_dir_path_in_local = pathing.in_hoard(hoard_dir).at_local(local.config.uuid)
                if curr_dir_path_in_local is None:
                    continue  # hoard dir is not in the mounted location
                if curr_dir_path_in_local.as_posix() not in hoard.fsobjects:
                    logging.info(f"missing dir found in hoard: {hoard_dir}")
                    yield DirMissingInLocal(curr_dir_path_in_local.as_posix(), hoard_dir)
                else:
                    pass  # existing dirs are handled above
            else:
                raise ValueError(f"unknown props type: {type(props)}")
