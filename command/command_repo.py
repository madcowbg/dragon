import asyncio
import logging
import os
import pathlib
import re
from io import StringIO
from pathlib import PurePosixPath
from typing import Tuple, List, Optional, Dict, Iterable

import aiofiles.os
from alive_progress import alive_bar, alive_it

from command.pathing import HoardPathing
from command.repo import ProspectiveRepo
from contents.repo import RepoContents
from contents.repo_props import RepoFileProps, RepoFileStatus, RepoDirProps
from exceptions import MissingRepo, MissingRepoContents
from hashing import find_hashes, fast_hash_async
from resolve_uuid import load_config, resolve_remote_uuid, load_paths
from util import format_size, run_async_in_parallel, format_percent, group_to_dict

DEFAULT_IGNORE_GLOBS = [
    r".hoard",
    r"**/thumbs.db",
    r"System Volume Information",
    r"$Recycle.Bin",
    r"RECYCLE?",
]


class HoardIgnore:
    def __init__(self, ignore_globs_list: List[str]):
        self.ignore_globs_list = ignore_globs_list

    def matches(self, fullpath: pathlib.Path) -> bool:
        for glob in self.ignore_globs_list:
            if fullpath.full_match(glob, case_sensitive=False):
                return True
        return False


def walk_repo(repo: str, hoard_ignore: HoardIgnore) -> Iterable[Tuple[pathlib.Path | None, pathlib.Path | None]]:
    for dirpath_s, dirnames, filenames in os.walk(repo, topdown=True):
        dirpath = pathlib.Path(dirpath_s)

        for filename in filenames:
            fullpath_file = dirpath.joinpath(filename)
            relpath_file = fullpath_file.relative_to(repo)
            if not hoard_ignore.matches(relpath_file):
                yield fullpath_file, None

        ignored_dirnames = []
        for dirname in dirnames:
            fullpath_dir = dirpath.joinpath(dirname)
            relpath_dir = fullpath_dir.relative_to(repo)
            if hoard_ignore.matches(relpath_dir):
                ignored_dirnames.append(dirname)
            else:
                yield None, fullpath_dir

        for ignored in ignored_dirnames:
            dirnames.remove(ignored)


def _refresh_repo_contents(
        repo_path: str, contents: RepoContents, hoard_ignore: HoardIgnore,
        add_new_with_status: RepoFileStatus, skip_integrity_checks: bool, show_details: bool, out: StringIO):
    print_maybe = (lambda line: out.write(line + "\n")) if show_details else (lambda line: None)

    logging.info(f"Comparing contents and filesystem...")
    files_to_add_or_update: Dict[pathlib.Path, RepoFileStatus] = {}
    files_maybe_removed: List[Tuple[PurePosixPath, RepoFileProps]] = []
    folders_to_add: List[pathlib.Path] = []
    for diff in compute_difference_between_contents_and_filesystem(
            contents, repo_path, hoard_ignore, skip_integrity_checks):
        if isinstance(diff, FileNotInFilesystem):
            logging.info(f"File not found, marking for removal: {diff.filepath}")
            files_maybe_removed.append((diff.filepath, diff.props))
        elif isinstance(diff, DirNotInFilesystem):
            logging.info(f"Removing dir {diff.dirpath}")
            print_maybe("REMOVED_DIR {diff.dirpath}")
            contents.fsobjects.mark_removed(PurePosixPath(diff.dirpath))
        elif isinstance(diff, RepoFileWeakSame):
            assert skip_integrity_checks
            logging.info("Skipping file as size and mtime is the same!!!")
        elif isinstance(diff, RepoFileWeakDifferent):
            assert skip_integrity_checks
            logging.info(f"File {diff.filepath} is weakly different, adding to check.")
            files_to_add_or_update[pathlib.Path(repo_path).joinpath(diff.filepath)] = \
                RepoFileStatus.MODIFIED
        elif isinstance(diff, RepoFileSame):
            logging.info(f"File {diff.filepath} is same.")
        elif isinstance(diff, RepoFileDifferent):
            logging.info(f"File {diff.filepath} is different, adding to check.")
            files_to_add_or_update[pathlib.Path(repo_path).joinpath(diff.filepath)] = \
                RepoFileStatus.MODIFIED
        elif isinstance(diff, FileNotInRepo):
            logging.info(f"File {diff.filepath} not in repo, adding.")
            files_to_add_or_update[pathlib.Path(repo_path).joinpath(diff.filepath)] = \
                add_new_with_status
        elif isinstance(diff, DirIsSameInRepo):
            logging.info(f"Dir {diff.dirpath} is same, skipping")
        elif isinstance(diff, DirNotInRepo):
            logging.info(f"Dir {diff.dirpath} is different, adding...")
            folders_to_add.append(pathlib.Path(repo_path).joinpath(diff.dirpath))
        else:
            raise ValueError(f"unknown diff type: {type(diff)}")
    logging.info(f"Detected {len(files_maybe_removed)} possible deletions.")
    logging.info(f"Hashing {len(files_to_add_or_update)} files to add:")
    file_hashes = asyncio.run(find_hashes([file for file, status in files_to_add_or_update.items()]))
    inverse_hashes: Dict[str, List[Tuple[pathlib.Path, str]]] = group_to_dict(
        file_hashes.items(),
        key=lambda file_to_hash: file_to_hash[1])
    for missing_relpath, missing_file_props in alive_it(files_maybe_removed, title="Detecting moves"):
        candidates_file_to_hash = [
            (file, fasthash) for (file, fasthash) in inverse_hashes.get(missing_file_props.fasthash, [])
            if files_to_add_or_update.get(file, None) == RepoFileStatus.ADDED]

        if len(candidates_file_to_hash) == 0:
            logging.info(f"File {missing_relpath} has no suitable copy, marking as deleted.")
            print_maybe(f"REMOVED_FILE {missing_relpath}")
            contents.fsobjects.mark_removed(PurePosixPath(missing_relpath))
        elif len(candidates_file_to_hash) == 1:
            moved_to_file, moved_file_hash = candidates_file_to_hash[0]
            assert missing_file_props.fasthash == moved_file_hash
            assert files_to_add_or_update[moved_to_file] == RepoFileStatus.ADDED

            moved_to_relpath = pathlib.Path(moved_to_file).relative_to(repo_path).as_posix()
            logging.info(f"{missing_relpath} is moved to {moved_to_relpath} ")

            try:
                size = os.path.getsize(moved_to_file)
                mtime = os.path.getmtime(moved_to_file)
                contents.fsobjects.mark_moved(
                    pathlib.PurePosixPath(missing_relpath),
                    pathlib.PurePosixPath(moved_to_relpath),
                    size=size, mtime=mtime, fasthash=moved_file_hash)

                del files_to_add_or_update[moved_to_file]  # was fixed above
                print_maybe(f"MOVED {missing_relpath} TO {moved_to_relpath}")
            except FileNotFoundError as e:
                logging.error(
                    f"File not found: {moved_to_file}, fallbacks to delete/new")
                contents.fsobjects.mark_removed(PurePosixPath(missing_relpath))
                print_maybe(f"REMOVED_FILE_FALLBACK_ON_ERROR {missing_relpath}")
        else:
            logging.error(
                f"Multiple new file candidates for {missing_relpath}, fallbacks to delete/new")
            contents.fsobjects.mark_removed(PurePosixPath(missing_relpath))
            print_maybe(f"REMOVED_FILE_FALLBACK_TOO_MANY {missing_relpath}")
    for fullpath, requested_status in alive_it(
            files_to_add_or_update.items(), title=f"Adding {len(files_to_add_or_update)} files"):
        relpath = pathlib.PurePosixPath(fullpath).relative_to(repo_path)

        if fullpath not in file_hashes:
            logging.warning(f"Skipping {fullpath} as it doesn't have a computed hash!")
            continue
        try:
            size = os.path.getsize(fullpath)
            mtime = os.path.getmtime(fullpath)
            contents.fsobjects.add_file(
                relpath, size=size, mtime=mtime, fasthash=file_hashes[fullpath], status=requested_status)

            print_maybe(f"{requested_status.value.upper()}_FILE {relpath.as_posix()}")
        except FileNotFoundError as e:
            logging.error("Error while adding file!")
            logging.error(e)
    for fullpath in alive_it(folders_to_add, title=f"Adding {len(folders_to_add)} folders"):
        relpath = pathlib.PurePosixPath(fullpath).relative_to(repo_path)
        contents.fsobjects.add_dir(relpath, status=RepoFileStatus.ADDED)
        print_maybe(f"ADDED_DIR {relpath}")
    logging.info(f"Files read!")
    logging.info("Ends updating, setting is_dirty to FALSE")
    contents.config.end_updating()
    assert not contents.config.is_dirty
    if len(out.getvalue()) == 0:
        print_maybe("NO CHANGES")


class RepoCommand(object):
    def __init__(self, path: str = ".", name: Optional[str] = None):
        if name is not None:  # assume path is a hoard, and the name to use is the provided
            logging.info(f"Finding repo path by hoard assumed at {path}...")

            config = load_config(hoardpath=path, create=False)
            remote_uuid = resolve_remote_uuid(config, name)
            logging.info(f"resolved {name} to assumed uuid {remote_uuid}")

            paths = load_paths(hoardpath=path)
            cave_path = paths[remote_uuid]
            if cave_path is None:
                raise ValueError(f"No repo with uuid {remote_uuid}.")

            print(f"Resolved repo {name} to path {cave_path.find()}.")
            path = cave_path.find()

        self.repo = ProspectiveRepo(pathlib.Path(path).absolute().as_posix())

    def current_uuid(self) -> str:
        return self.repo.open_repo().current_uuid

    def init(self):
        logging.info(f"Creating repo in {self.repo.path}")
        self.repo.init()

        return f"Repo initialized at {self.repo.path}"

    def refresh(self, skip_integrity_checks: bool = False, show_details: bool = True):
        """ Refreshes the cache of the current hoard folder """
        connected_repo = self.repo.open_repo().connect(False)
        if connected_repo is None:
            return f"No initialized repo in {self.repo.path}!"

        current_uuid = connected_repo.current_uuid
        try:
            contents = connected_repo.open_contents(is_readonly=False)
            first_refresh = False
        except MissingRepoContents as e:
            logging.warning("Repo contents missing, creating!")
            first_refresh = True
            contents = connected_repo.create_contents(current_uuid)

        logging.info(f"Refreshing uuid {current_uuid}{', is first refresh' if first_refresh else ''}")
        add_new_with_status = RepoFileStatus.ADDED if not first_refresh else RepoFileStatus.PRESENT

        hoard_ignore = HoardIgnore(DEFAULT_IGNORE_GLOBS)

        with contents:
            logging.info("Start updating, setting is_dirty to TRUE")
            contents.config.start_updating()

            logging.info(f"Bumped epoch to {contents.config.epoch}")

            with StringIO() as out:
                _refresh_repo_contents(
                    self.repo.path, contents, hoard_ignore, add_new_with_status, skip_integrity_checks, show_details,
                    out)
                out.write(f"Refresh done!")
                return out.getvalue()

    def status_index(self, show_files: bool = True, show_dates: bool = True):
        remote_uuid = self.current_uuid()

        logging.info(f"Reading repo {self.repo.path}...")
        with self.repo.open_repo().connect(False).open_contents(is_readonly=True) as contents:
            logging.info(f"Read repo!")

            with StringIO() as out:
                if show_files:
                    for file_or_dir, props in contents.fsobjects.all_status():
                        out.write(f"{file_or_dir.as_posix()}: {props.last_status.value} @ {props.last_update_epoch}\n")
                    out.write("--- SUMMARY ---\n")

                stats = contents.fsobjects.stats_existing
                out.writelines([
                    f"Result for local\n",
                    f"Max size: {format_size(contents.config.max_size)}\n"
                    f"UUID: {remote_uuid}\n",
                    f"Last updated on {contents.config.updated}\n" if show_dates else "",
                    f"  # files = {stats.num_files} of size {format_size(stats.total_size)}\n",
                    f"  # dirs  = {stats.num_dirs}\n", ])
                return out.getvalue()

    def status(self, skip_integrity_checks: bool = False):
        try:
            connected_repo = self.repo.open_repo().connect(False)
            current_uuid = connected_repo.current_uuid
        except MissingRepo:
            return f"Repo is not initialized at {self.repo.path}"

        try:
            contents = connected_repo.open_contents(is_readonly=True)
        except MissingRepoContents:
            return f"Repo {current_uuid} contents have not been refreshed yet!"

        files_same: List[PurePosixPath] = []
        files_new: List[PurePosixPath] = []
        files_mod: List[PurePosixPath] = []
        files_del: List[PurePosixPath] = []

        dir_new = []
        dir_same = []
        dir_deleted = []

        hoard_ignore = HoardIgnore(DEFAULT_IGNORE_GLOBS)

        with contents:
            print("Calculating diffs between repo and filesystem...")
            for diff in compute_difference_between_contents_and_filesystem(
                    contents, self.repo.path, hoard_ignore, skip_integrity_checks):
                if isinstance(diff, FileNotInFilesystem):
                    files_del.append(diff.filepath)
                elif isinstance(diff, RepoFileWeakSame):
                    assert skip_integrity_checks
                    files_same.append(diff.filepath)
                elif isinstance(diff, RepoFileWeakDifferent):
                    assert skip_integrity_checks
                    files_mod.append(diff.filepath)
                elif isinstance(diff, RepoFileSame):
                    files_same.append(diff.filepath)
                elif isinstance(diff, RepoFileDifferent):
                    files_mod.append(diff.filepath)
                elif isinstance(diff, FileNotInRepo):
                    files_new.append(diff.filepath)
                elif isinstance(diff, DirNotInFilesystem):
                    dir_deleted.append(diff.dirpath)
                elif isinstance(diff, DirIsSameInRepo):
                    dir_same.append(diff.dirpath)
                elif isinstance(diff, DirNotInRepo):
                    dir_new.append(diff.dirpath)
                else:
                    raise ValueError(f"unknown diff type: {type(diff)}")

            # assert len(files_same) + len(files_del) + len(files_mod) == contents.fsobjects.num_files
            # assert len(dir_new) + len(dir_same) == contents.fsobjects.num_dirs

            files_current = len(files_new) + len(files_same) + len(files_mod)
            dirs_current = len(dir_same) + len(dir_new)
            with StringIO() as out:
                stats = contents.fsobjects.stats_existing
                out.write(
                    f"{current_uuid}:\n"
                    f"files:\n"
                    f"    same: {len(files_same)} ({format_percent(len(files_same) / files_current)})\n"
                    f"     mod: {len(files_mod)} ({format_percent(len(files_mod) / files_current)})\n"
                    f"     new: {len(files_new)} ({format_percent(len(files_new) / files_current)})\n"
                    f" current: {files_current}\n"
                    f" in repo: {stats.num_files}\n"
                    f" deleted: {len(files_del)} ({format_percent(len(files_del) / stats.num_files)})\n"
                    f"dirs:\n"
                    f"    same: {len(dir_same)}\n"
                    f"     new: {len(dir_new)} ({format_percent(len(dir_new) / dirs_current)})\n"
                    f" current: {dirs_current}\n"
                    f" in repo: {stats.num_dirs}\n"
                    f" deleted: {len(dir_deleted)} ({format_percent(len(dir_deleted) / max(1, stats.num_dirs))})\n")

                return out.getvalue()


class FileNotInFilesystem:
    def __init__(self, filepath: PurePosixPath, props: RepoFileProps):
        assert not filepath.is_absolute()
        self.filepath = filepath
        self.props = props


class DirNotInFilesystem:
    def __init__(self, dirpath: PurePosixPath, props: RepoDirProps):
        assert not dirpath.is_absolute()
        self.dirpath = dirpath
        self.props = props


class RepoFileWeakSame:
    def __init__(self, filepath: PurePosixPath, props: RepoFileProps):
        assert not filepath.is_absolute()
        self.filepath = filepath
        self.props = props


class RepoFileWeakDifferent:
    def __init__(self, filepath: PurePosixPath, props: RepoFileProps, mtime: float, size: int):
        assert not filepath.is_absolute()

        self.filepath = filepath
        self.props = props

        self.mtime = mtime
        self.size = size


class RepoFileDifferent:
    def __init__(self, filepath: PurePosixPath, props: RepoFileProps, mtime: float, size: int, fasthash: str):
        assert not filepath.is_absolute()

        self.filepath = filepath
        self.props = props

        self.mtime = mtime
        self.size = size
        self.fasthash = fasthash


class RepoFileSame:
    def __init__(self, filepath: PurePosixPath, props: RepoFileProps, mtime: float):
        assert not filepath.is_absolute()
        self.filepath = filepath
        self.props = props
        self.mtime = mtime


class DirIsSameInRepo:
    def __init__(self, dirpath: PurePosixPath, props: RepoDirProps):
        assert not dirpath.is_absolute()
        self.dirpath = dirpath
        self.props = props


class DirNotInRepo:
    def __init__(self, dirpath: PurePosixPath):
        assert not dirpath.is_absolute()
        self.dirpath = dirpath


class FileNotInRepo:
    def __init__(self, filepath: PurePosixPath):
        assert not filepath.is_absolute()
        self.filepath = filepath


type RepoDiffs = (
        FileNotInFilesystem | FileNotInRepo
        | RepoFileWeakSame | RepoFileWeakDifferent | RepoFileSame | RepoFileDifferent
        | DirNotInFilesystem | DirIsSameInRepo | DirNotInRepo)


def compute_difference_between_contents_and_filesystem(
        contents: RepoContents, repo_path: str, hoard_ignore: HoardIgnore,
        skip_integrity_checks: bool) -> Iterable[RepoDiffs]:
    current_repo_path = pathlib.Path(repo_path)
    for obj_path, props in alive_it(
            list(contents.fsobjects.existing()),
            title="Checking for deleted files and folders"):
        if isinstance(props, RepoFileProps):
            file_path = current_repo_path.joinpath(obj_path)
            if not file_path.is_file() or hoard_ignore.matches(file_path):
                yield FileNotInFilesystem(obj_path, props)
        elif isinstance(props, RepoDirProps):
            dir_path = current_repo_path.joinpath(obj_path)
            if not dir_path.is_dir() or hoard_ignore.matches(dir_path):
                yield DirNotInFilesystem(obj_path, props)
        else:
            raise ValueError(f"invalid props type: {type(props)}")

    file_path_matches: List[str] = list()
    with alive_bar(total=contents.fsobjects.len_existing(), title="Walking filesystem") as bar:
        for file_path_full, dir_path_full in walk_repo(repo_path, hoard_ignore):
            if file_path_full is not None:
                assert dir_path_full is None
                file_path_local = PurePosixPath(file_path_full.relative_to(repo_path))
                logging.info(f"Checking {file_path_local} for existence...")
                if contents.fsobjects.in_existing(file_path_local):  # file is already in index
                    logging.info(f"File is in contents, adding to check")  # checking size and mtime.")
                    file_path_matches.append(file_path_full.as_posix())
                else:
                    yield FileNotInRepo(file_path_local)
                bar()
            else:
                assert dir_path_full is not None and file_path_full is None
                dir_path_in_local = PurePosixPath(dir_path_full.relative_to(repo_path))
                if contents.fsobjects.in_existing(dir_path_in_local):
                    props = contents.fsobjects.get_existing(dir_path_in_local)
                    assert isinstance(props, RepoDirProps)
                    yield DirIsSameInRepo(dir_path_in_local, props)
                else:
                    yield DirNotInRepo(dir_path_in_local)
                bar()

    with alive_bar(len(file_path_matches), title="Checking maybe mod files") as m_bar:
        async def find_size_mtime_of(file_fullpath: str) -> RepoDiffs:
            try:
                stats = await aiofiles.os.stat(file_fullpath)

                file_path_local = pathlib.PurePosixPath(file_fullpath).relative_to(repo_path)
                props = contents.fsobjects.get_existing(file_path_local)
                if skip_integrity_checks:
                    if props.mtime == stats.st_mtime and props.size == stats.st_size:
                        return RepoFileWeakSame(file_path_local, props)
                    else:
                        return RepoFileWeakDifferent(file_path_local, props, stats.st_mtime, stats.st_size)
                else:
                    fasthash = await fast_hash_async(file_fullpath)
                    if props.fasthash == fasthash:
                        return RepoFileSame(file_path_local, props, stats.st_mtime)
                    else:
                        return RepoFileDifferent(file_path_local, props, stats.st_mtime, stats.st_size, fasthash)
            finally:
                m_bar()

        prop_tuples: List[RepoDiffs] = run_async_in_parallel(
            [(f,) for f in file_path_matches],
            find_size_mtime_of)

        assert len(prop_tuples) == len(file_path_matches)

    yield from alive_it(prop_tuples, title="Returning file diffs")
