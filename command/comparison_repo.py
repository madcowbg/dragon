import asyncio
import logging
import os
import pathlib
from io import StringIO

import command.fast_path
from command.fast_path import FastPosixPath
from typing import Iterable, Tuple, Dict, Optional, List

import aiofiles.os
from alive_progress import alive_it, alive_bar

from command.hoard_ignore import HoardIgnore
from contents.repo import RepoContents
from contents.repo_props import RepoFileStatus, RepoFileProps, RepoDirProps
from hashing import find_hashes, fast_hash_async, fast_hash
from util import group_to_dict, run_async_in_parallel, run_in_separate_loop

type RepoDiffs = (
        FileNotInFilesystem | FileNotInRepo
        | RepoFileWeakSame | RepoFileWeakDifferent | RepoFileSame | RepoFileDifferent
        | DirNotInFilesystem | DirIsSameInRepo | DirNotInRepo)


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


class FileDeleted:
    def __init__(self, missing_relpath: FastPosixPath, details: str = "MOVED"):
        self.missing_relpath = missing_relpath
        self.details = details


class FileMoved:
    def __init__(
            self, missing_relpath: FastPosixPath, moved_to_relpath: FastPosixPath, size: int, mtime: float,
            moved_file_hash: str):
        self.moved_file_hash = moved_file_hash
        self.mtime = mtime
        self.size = size

        self.missing_relpath = missing_relpath
        self.moved_to_relpath = moved_to_relpath


class FileAdded:
    def __init__(
            self, relpath: FastPosixPath, size: int, mtime: float, fasthash: str, requested_status: RepoFileStatus):
        assert not relpath.is_absolute()
        assert requested_status in (RepoFileStatus.ADDED, RepoFileStatus.PRESENT)
        self.relpath = relpath

        self.mtime = mtime
        self.size = size
        self.fasthash = fasthash

        self.requested_status = requested_status


class FileModified:
    def __init__(
            self, relpath: FastPosixPath, size: int, mtime: float, fasthash: str):
        assert not relpath.is_absolute()
        self.relpath = relpath

        self.mtime = mtime
        self.size = size
        self.fasthash = fasthash


class DirAdded:
    def __init__(self, relpath: FastPosixPath):
        self.relpath = relpath


class DirRemoved:
    def __init__(self, dirpath: command.fast_path.FastPosixPath):
        self.dirpath = dirpath


class FileIsSame:
    def __init__(self, relpath: FastPosixPath):
        self.relpath = relpath


class DirIsSame:
    def __init__(self, relpath: FastPosixPath):
        self.relpath = relpath


type RepoChange = FileIsSame | FileDeleted | FileModified | FileMoved | FileAdded | DirIsSame | DirAdded | DirRemoved


def find_repo_changes(
        repo_path: str, contents: RepoContents, hoard_ignore: HoardIgnore,
        add_new_with_status: RepoFileStatus, skip_integrity_checks: bool) -> Iterable[RepoChange]:
    logging.info(f"Comparing contents and filesystem...")
    diffs_stream = compute_difference_between_contents_and_filesystem(
        contents, repo_path, hoard_ignore, skip_integrity_checks)

    yield from compute_changes_from_diffs(diffs_stream, repo_path, add_new_with_status)


def compute_changes_from_diffs(diffs_stream: Iterable[RepoDiffs], repo_path: str, add_new_with_status: RepoFileStatus):
    files_to_add_or_update: Dict[pathlib.Path, Tuple[RepoFileStatus, Optional[RepoFileProps]]] = {}
    files_maybe_removed: List[Tuple[FastPosixPath, RepoFileProps]] = []
    folders_to_add: List[pathlib.Path] = []

    for diff in diffs_stream:
        if isinstance(diff, FileNotInFilesystem):
            logging.debug(f"File not found, marking for removal: {diff.filepath}")
            files_maybe_removed.append((diff.filepath, diff.props))
        elif isinstance(diff, DirNotInFilesystem):
            logging.debug(f"Removing dir {diff.dirpath}")
            yield DirRemoved(diff.dirpath)
        elif isinstance(diff, RepoFileWeakSame):
            logging.debug("Skipping file as size and mtime is the same!!!")
            yield FileIsSame(diff.filepath)
        elif isinstance(diff, RepoFileWeakDifferent):
            logging.debug(f"File {diff.filepath} is weakly different, adding to check.")
            files_to_add_or_update[pathlib.Path(repo_path).joinpath(diff.filepath)] = \
                (RepoFileStatus.MODIFIED, diff.props)
        elif isinstance(diff, RepoFileSame):
            logging.debug(f"File {diff.filepath} is same.")
            yield FileIsSame(diff.filepath)
        elif isinstance(diff, RepoFileDifferent):
            logging.debug(f"File {diff.filepath} is different, adding to check.")
            files_to_add_or_update[pathlib.Path(repo_path).joinpath(diff.filepath)] = \
                (RepoFileStatus.MODIFIED, diff.props)
        elif isinstance(diff, FileNotInRepo):
            logging.debug(f"File {diff.filepath} not in repo, adding.")
            files_to_add_or_update[pathlib.Path(repo_path).joinpath(diff.filepath)] = \
                (add_new_with_status, None)
        elif isinstance(diff, DirIsSameInRepo):
            logging.debug(f"Dir {diff.dirpath} is same, skipping")
            yield DirIsSame(diff.dirpath)
        elif isinstance(diff, DirNotInRepo):
            logging.debug(f"Dir {diff.dirpath} is different, adding...")
            folders_to_add.append(pathlib.Path(repo_path).joinpath(diff.dirpath))
        else:
            raise ValueError(f"unknown diff type: {type(diff)}")

    logging.info(f"Detected {len(files_maybe_removed)} possible deletions.")
    logging.info(f"Hashing {len(files_to_add_or_update)} files to add:")
    file_hashes = run_in_separate_loop(find_hashes(list(files_to_add_or_update.keys())))
    inverse_hashes: Dict[str, List[Tuple[pathlib.Path, str]]] = group_to_dict(
        file_hashes.items(),
        key=lambda file_to_hash: file_to_hash[1])
    for missing_relpath, missing_file_props in alive_it(files_maybe_removed, title="Detecting moves"):
        candidates_file_to_hash = [
            (file, fasthash) for (file, fasthash) in inverse_hashes.get(missing_file_props.fasthash, [])
            if files_to_add_or_update.get(file, (None, None))[0] == RepoFileStatus.ADDED]

        if len(candidates_file_to_hash) == 0:
            logging.info(f"File {missing_relpath} has no suitable copy, marking as deleted.")
            yield FileDeleted(missing_relpath)
        elif len(candidates_file_to_hash) == 1:
            moved_to_file, moved_file_hash = candidates_file_to_hash[0]
            assert missing_file_props.fasthash == moved_file_hash
            assert files_to_add_or_update[moved_to_file][0] == RepoFileStatus.ADDED

            moved_to_relpath = FastPosixPath(moved_to_file.relative_to(repo_path))
            logging.info(f"{missing_relpath} is moved to {moved_to_relpath} ")

            try:
                # fixme maybe reuse the data from the old file?
                size = os.path.getsize(moved_to_file)
                mtime = os.path.getmtime(moved_to_file)
                yield FileMoved(missing_relpath, moved_to_relpath, size, mtime, moved_file_hash)

                del files_to_add_or_update[moved_to_file]  # was fixed above
            except FileNotFoundError as e:
                logging.error(
                    f"File not found: {moved_to_file}, fallbacks to delete/new")
                yield FileDeleted(missing_relpath, details="REMOVED_FILE_FALLBACK_ON_ERROR")
        else:
            logging.error(
                f"Multiple new file candidates for {missing_relpath}, fallbacks to delete/new")
            yield FileDeleted(missing_relpath, details="REMOVED_FILE_FALLBACK_TOO_MANY")

    for fullpath, (requested_status, old_props) in alive_it(
            files_to_add_or_update.items(), title=f"Adding {len(files_to_add_or_update)} files"):
        relpath = command.fast_path.FastPosixPath(fullpath).relative_to(repo_path)

        if fullpath not in file_hashes:
            logging.warning(f"Skipping {fullpath} as it doesn't have a computed hash!")
            continue
        try:
            size = os.path.getsize(fullpath)
            mtime = os.path.getmtime(fullpath)
            if requested_status == RepoFileStatus.MODIFIED:
                if old_props.size == size and old_props.fasthash == file_hashes[fullpath]:
                    yield FileIsSame(relpath)
                else:
                    yield FileModified(relpath, size, mtime, file_hashes[fullpath])
            else:
                assert old_props is None
                yield FileAdded(relpath, size, mtime, file_hashes[fullpath], requested_status)
        except FileNotFoundError as e:
            logging.error("Error while adding file!")
            logging.error(e)

    for fullpath in alive_it(folders_to_add, title=f"Adding {len(folders_to_add)} folders"):
        relpath = command.fast_path.FastPosixPath(fullpath).relative_to(repo_path)
        yield DirAdded(relpath)

    logging.info(f"Files read!")


class FileNotInFilesystem:
    def __init__(self, filepath: FastPosixPath, props: RepoFileProps):
        assert not filepath.is_absolute()
        self.filepath = filepath
        self.props = props


class DirNotInFilesystem:
    def __init__(self, dirpath: FastPosixPath, props: RepoDirProps):
        assert not dirpath.is_absolute()
        self.dirpath = dirpath
        self.props = props


class RepoFileWeakSame:
    def __init__(self, filepath: FastPosixPath, props: RepoFileProps):
        assert not filepath.is_absolute()
        self.filepath = filepath
        self.props = props


class RepoFileWeakDifferent:
    def __init__(self, filepath: FastPosixPath, props: RepoFileProps, mtime: float, size: int):
        assert not filepath.is_absolute()

        self.filepath = filepath
        self.props = props

        self.mtime = mtime
        self.size = size


class RepoFileDifferent:
    def __init__(self, filepath: FastPosixPath, props: RepoFileProps, mtime: float, size: int, fasthash: str):
        assert not filepath.is_absolute()

        self.filepath = filepath
        self.props = props

        self.mtime = mtime
        self.size = size
        self.fasthash = fasthash


class RepoFileSame:
    def __init__(self, filepath: FastPosixPath, props: RepoFileProps, mtime: float):
        assert not filepath.is_absolute()
        self.filepath = filepath
        self.props = props
        self.mtime = mtime


class DirIsSameInRepo:
    def __init__(self, dirpath: FastPosixPath, props: RepoDirProps):
        assert not dirpath.is_absolute()
        self.dirpath = dirpath
        self.props = props


class DirNotInRepo:
    def __init__(self, dirpath: FastPosixPath):
        assert not dirpath.is_absolute()
        self.dirpath = dirpath


class FileNotInRepo:
    def __init__(self, filepath: FastPosixPath):
        assert not filepath.is_absolute()
        self.filepath = filepath


def compute_difference_between_contents_and_filesystem(
        contents: RepoContents, repo_path: str, hoard_ignore: HoardIgnore,
        skip_integrity_checks: bool) -> Iterable[RepoDiffs]:
    current_repo_path = pathlib.Path(repo_path)
    for obj_path, props in alive_it(
            list(contents.fsobjects.existing()),
            title="Checking for deleted files and folders"):
        if isinstance(props, RepoFileProps):
            file_path = current_repo_path.joinpath(obj_path)
            try:
                if not file_path.is_file() or hoard_ignore.matches(file_path):
                    yield FileNotInFilesystem(obj_path, props)
            except OSError as e:
                logging.error(e)  # fixme yield error
        elif isinstance(props, RepoDirProps):
            dir_path = current_repo_path.joinpath(obj_path)
            try:
                if not dir_path.is_dir() or hoard_ignore.matches(dir_path):
                    yield DirNotInFilesystem(obj_path, props)
            except OSError as e:
                logging.error(e)  # fixme yield error
        else:
            raise ValueError(f"invalid props type: {type(props)}")

    file_path_matches: List[str] = list()
    with alive_bar(total=contents.fsobjects.len_existing(), title="Walking filesystem") as bar:
        for file_path_full, dir_path_full in walk_repo(repo_path, hoard_ignore):
            if file_path_full is not None:
                assert dir_path_full is None
                file_path_local = FastPosixPath(file_path_full.relative_to(repo_path))
                logging.debug(f"Checking {file_path_local} for existence...")
                if contents.fsobjects.in_existing(file_path_local):  # file is already in index
                    logging.debug(f"File is in contents, adding to check")  # checking size and mtime.")
                    file_path_matches.append(file_path_full.as_posix())
                else:
                    yield FileNotInRepo(file_path_local)
                bar()
            else:
                assert dir_path_full is not None and file_path_full is None
                dir_path_in_local = FastPosixPath(dir_path_full.relative_to(repo_path))
                if contents.fsobjects.in_existing(dir_path_in_local):
                    props = contents.fsobjects.get_existing(dir_path_in_local)
                    assert isinstance(props, RepoDirProps)
                    yield DirIsSameInRepo(dir_path_in_local, props)
                else:
                    yield DirNotInRepo(dir_path_in_local)
                bar()

    with alive_bar(len(file_path_matches), title="Checking maybe mod files") as m_bar:
        def find_size_mtime_of(file_fullpath: str) -> RepoDiffs:
            try:
                stats = run_in_separate_loop(aiofiles.os.stat(file_fullpath))

                file_path_local = command.fast_path.FastPosixPath(file_fullpath).relative_to(repo_path)
                props = contents.fsobjects.get_existing(file_path_local)
                if skip_integrity_checks:
                    if props.mtime == stats.st_mtime and props.size == stats.st_size:
                        return RepoFileWeakSame(file_path_local, props)
                    else:
                        return RepoFileWeakDifferent(file_path_local, props, stats.st_mtime, stats.st_size)
                else:
                    fasthash = run_in_separate_loop(fast_hash_async(file_fullpath))
                    if props.fasthash == fasthash:
                        return RepoFileSame(file_path_local, props, stats.st_mtime)
                    else:
                        return RepoFileDifferent(file_path_local, props, stats.st_mtime, stats.st_size, fasthash)
            finally:
                m_bar()

        prop_tuples: List[RepoDiffs] = [find_size_mtime_of(f) for f in file_path_matches]

        assert len(prop_tuples) == len(file_path_matches)

    yield from alive_it(prop_tuples, title="Returning file diffs")


def compute_difference_filtered_by_path(
        contents: RepoContents, repo_path: str, hoard_ignore: HoardIgnore,
        allowed_paths: List[str]) -> Iterable[RepoDiffs]:
    for allowed_path in alive_it(allowed_paths, title="Checking updates"):
        path_on_device = pathlib.Path(allowed_path).absolute()

        local_path = FastPosixPath(path_on_device.relative_to(repo_path))
        if hoard_ignore.matches(local_path):
            logging.info("ignoring a file")
            continue

        if contents.fsobjects.in_existing(local_path):
            logging.debug(f"{local_path} is in contents, check")

            existing_prop = contents.fsobjects.get_existing(local_path)
            if isinstance(existing_prop, RepoDirProps):
                if not path_on_device.is_dir():
                    yield DirNotInFilesystem(local_path, existing_prop)
                else:
                    yield DirIsSameInRepo(local_path, existing_prop)
            elif isinstance(existing_prop, RepoFileProps):
                if not path_on_device.is_file():
                    yield FileNotInFilesystem(local_path, existing_prop)
                else:
                    stats = os.stat(path_on_device)
                    fasthash = fast_hash(path_on_device)
                    if existing_prop.fasthash == fasthash:
                        yield RepoFileSame(local_path, existing_prop, stats.st_mtime)
                    else:
                        yield RepoFileDifferent(local_path, existing_prop, stats.st_mtime, stats.st_size, fasthash)
            else:
                raise ValueError(f"invalid props type: {type(existing_prop)}")
        else:
            if path_on_device.is_file():
                yield FileNotInRepo(local_path)
            elif path_on_device.is_dir():
                yield DirNotInRepo(local_path)
            else:
                logging.error(f"Bad type of object in {local_path}.")


def _apply_repo_change_to_contents(
        change: RepoChange, contents: RepoContents, show_details: bool, out: StringIO) -> None:
    print_maybe = (lambda line: out.write(line + "\n")) if show_details else (lambda line: None)

    if isinstance(change, FileIsSame):
        pass
    elif isinstance(change, FileDeleted):
        print_maybe(f"{change.details} {change.missing_relpath}")
        contents.fsobjects.mark_removed(FastPosixPath(change.missing_relpath))
    elif isinstance(change, FileMoved):
        contents.fsobjects.mark_moved(
            change.missing_relpath, change.moved_to_relpath,
            size=change.size, mtime=change.mtime, fasthash=change.moved_file_hash)

        print_maybe(f"MOVED {change.missing_relpath.as_posix()} TO {change.moved_to_relpath.as_posix()}")
    elif isinstance(change, FileAdded):
        contents.fsobjects.add_file(
            change.relpath, size=change.size, mtime=change.mtime, fasthash=change.fasthash,
            status=change.requested_status)

        print_maybe(f"{change.requested_status.value.upper()}_FILE {change.relpath.as_posix()}")
    elif isinstance(change, FileModified):
        contents.fsobjects.add_file(
            change.relpath, size=change.size, mtime=change.mtime, fasthash=change.fasthash,
            status=RepoFileStatus.MODIFIED)

        print_maybe(f"MODIFIED_FILE {change.relpath.as_posix()}")
    elif isinstance(change, DirIsSame):
        pass
    elif isinstance(change, DirAdded):
        contents.fsobjects.add_dir(change.relpath, status=RepoFileStatus.ADDED)
        print_maybe(f"ADDED_DIR {change.relpath}")
    elif isinstance(change, DirRemoved):
        print_maybe("REMOVED_DIR {diff.dirpath}")
        contents.fsobjects.mark_removed(FastPosixPath(change.dirpath))
    else:
        raise TypeError(f"Unexpected change type {type(change)}")
