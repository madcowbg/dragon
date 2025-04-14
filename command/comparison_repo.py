import logging
import os
import pathlib
import random
import traceback
from asyncio import TaskGroup
from io import StringIO
from itertools import batched
from typing import Iterable, Tuple, Dict, Optional, List, AsyncGenerator

import aiofiles.os
from alive_progress import alive_it, alive_bar

import command.fast_path
from command.fast_path import FastPosixPath
from command.hoard_ignore import HoardIgnore
from contents.repo import RepoContents
from contents.repo_props import RepoFileStatus, RepoFileProps
from hashing import find_hashes, fast_hash_async, fast_hash
from util import group_to_dict, run_in_separate_loop

type RepoDiffs = (
        FileNotInFilesystem | FileNotInRepo
        | RepoFileWeakSame | RepoFileWeakDifferent | RepoFileSame | RepoFileDifferent)


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
    def __init__(self, missing_relpath: FastPosixPath, details: str):
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

class FileIsSame:
    def __init__(self, relpath: FastPosixPath):
        self.relpath = relpath



type RepoChange = FileIsSame | FileDeleted | FileModified | FileMoved | FileAdded


async def find_repo_changes(
        repo_path: str, contents: RepoContents, hoard_ignore: HoardIgnore,
        add_new_with_status: RepoFileStatus) -> AsyncGenerator[RepoChange]:
    logging.info(f"Comparing contents and filesystem...")
    diffs_stream = compute_difference_between_contents_and_filesystem(
        contents, repo_path, hoard_ignore)

    async for diff in compute_changes_from_diffs(diffs_stream, repo_path, add_new_with_status):
        yield diff


async def compute_changes_from_diffs(diffs_stream: AsyncGenerator[RepoDiffs], repo_path: str,
                                     add_new_with_status: RepoFileStatus):
    files_to_add_or_update: Dict[pathlib.Path, Tuple[RepoFileStatus, Optional[RepoFileProps]]] = {}
    files_maybe_removed: List[Tuple[FastPosixPath, RepoFileProps]] = []

    async for diff in diffs_stream:
        if isinstance(diff, FileNotInFilesystem):
            logging.debug(f"File not found, marking for removal: {diff.filepath}")
            files_maybe_removed.append((diff.filepath, diff.props))
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
            yield FileDeleted(missing_relpath, "DELETED_NO_COPY")
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

    logging.info(f"Files read!")


class FileNotInFilesystem:
    def __init__(self, filepath: FastPosixPath, props: RepoFileProps):
        assert not filepath.is_absolute()
        self.filepath = filepath
        self.props = props


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

class FileNotInRepo:
    def __init__(self, filepath: FastPosixPath):
        assert not filepath.is_absolute()
        self.filepath = filepath


async def compute_difference_between_contents_and_filesystem(
        contents: RepoContents, repo_path: str, hoard_ignore: HoardIgnore) -> AsyncGenerator[RepoDiffs]:
    current_repo_path = pathlib.Path(repo_path)
    for obj_path, props in alive_it(
            list(contents.fsobjects.existing()),
            title="Checking for deleted files and folders"):
        assert isinstance(props, RepoFileProps)
        file_path = current_repo_path.joinpath(obj_path)
        try:
            if not file_path.is_file() or hoard_ignore.matches(file_path):
                yield FileNotInFilesystem(obj_path, props)
        except OSError as e:
            logging.error(e)  # fixme yield error

    file_path_matches: List[pathlib.Path] = list()
    with alive_bar(total=contents.fsobjects.len_existing(), title="Walking filesystem") as bar:
        for file_path_full, dir_path_full in walk_repo(repo_path, hoard_ignore):
            if file_path_full is not None:
                assert dir_path_full is None
                file_path_local = FastPosixPath(file_path_full.relative_to(repo_path))
                logging.debug(f"Checking {file_path_local} for existence...")
                if contents.fsobjects.in_existing(file_path_local):  # file is already in index
                    logging.debug(f"File is in contents, adding to check")  # checking size and mtime.")
                    file_path_matches.append(file_path_full)
                else:
                    yield FileNotInRepo(file_path_local)
                bar()
            else:
                bar()

    with alive_bar(len(file_path_matches), title="Checking maybe mod files") as m_bar:
        async def find_size_mtime_of(file_fullpath: pathlib.Path) -> AsyncGenerator[RepoDiffs]:
            try:
                stats = await aiofiles.os.stat(file_fullpath)

                file_path_local = command.fast_path.FastPosixPath(file_fullpath).relative_to(repo_path)
                props = contents.fsobjects.get_existing(file_path_local)

                fasthash = await fast_hash_async(file_fullpath)
                if props.fasthash == fasthash:
                    yield RepoFileSame(file_path_local, props, stats.st_mtime)
                else:
                    yield RepoFileDifferent(file_path_local, props, stats.st_mtime, stats.st_size, fasthash)
            except OSError as e:
                logging.error(e)
            finally:
                m_bar()

        async with TaskGroup() as tg:
            tasks = []

            async def run(batch: List[pathlib.Path]):
                return [d for file in batch async for d in find_size_mtime_of(file)]

            random.shuffle(file_path_matches)
            files_batch: List[pathlib.Path]
            batches = list(batched(file_path_matches, max(10, len(file_path_matches) // 32)))
            logging.warning(f"Splitting {len(file_path_matches)} into {len(batches)} batches")
            for files_batch in batches:
                tasks.append(tg.create_task(run(files_batch)))

        for task in tasks:
            for result in await task:
                yield result


async def compute_difference_filtered_by_path(
        contents: RepoContents, repo_path: str, hoard_ignore: HoardIgnore,
        allowed_paths: List[pathlib.PurePosixPath]) -> AsyncGenerator[RepoDiffs]:
    for allowed_path in alive_it(allowed_paths, title="Checking updates"):
        path_on_device = pathlib.Path(allowed_path).absolute()

        local_path = path_on_device.relative_to(repo_path)
        if hoard_ignore.matches(local_path):
            logging.info("ignoring a file")
            continue

        if contents.fsobjects.in_existing(local_path):
            logging.debug(f"{local_path} is in contents, check")

            existing_prop = contents.fsobjects.get_existing(local_path)
            assert isinstance(existing_prop, RepoFileProps)
            if not path_on_device.is_file():
                yield FileNotInFilesystem(FastPosixPath(local_path), existing_prop)
            else:
                stats = os.stat(path_on_device)
                fasthash = fast_hash(path_on_device)
                if existing_prop.fasthash == fasthash:
                    yield RepoFileSame(FastPosixPath(local_path), existing_prop, stats.st_mtime)
                else:
                    yield RepoFileDifferent(FastPosixPath(local_path), existing_prop, stats.st_mtime, stats.st_size, fasthash)
        else:
            if path_on_device.is_file():
                yield FileNotInRepo(FastPosixPath(local_path))
            elif path_on_device.is_dir():
                pass
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
    else:
        raise TypeError(f"Unexpected change type {type(change)}")
