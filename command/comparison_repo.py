import logging
import os
import pathlib
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Iterable, Tuple, Dict, Optional, List, AsyncGenerator

import aiofiles.os
from alive_progress import alive_it, alive_bar

import command.fast_path
from command.fast_path import FastPosixPath
from command.hoard_ignore import HoardIgnore
from contents.repo import RepoContents
from contents.repo_props import RepoFileStatus, RepoFileProps, FileDesc
from hashing import fast_hash_async
from lmdb_storage.file_object import FileObject
from lmdb_storage.tree_iteration import zip_dfs
from lmdb_storage.tree_structure import add_file_object, TreeObject
from util import group_to_dict, process_async

type RepoDiffs = (FileNotInFilesystem | FileNotInRepo | RepoFileSame | RepoFileDifferent | ErrorReadingFilesystem)


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
    files_to_add_or_update: Dict[pathlib.Path, Tuple[RepoFileStatus, Optional[RepoFileProps], FileDesc]] = {}
    files_maybe_removed: List[Tuple[FastPosixPath, RepoFileProps]] = []

    async for diff in diffs_stream:
        if isinstance(diff, FileNotInFilesystem):
            logging.debug(f"File not found, marking for removal: {diff.filepath}")
            files_maybe_removed.append((diff.filepath, diff.repo_props))
        elif isinstance(diff, RepoFileSame):
            logging.debug(f"File {diff.filepath} is same.")
            yield FileIsSame(diff.filepath)
        elif isinstance(diff, RepoFileDifferent):
            logging.debug(f"File {diff.filepath} is different, adding to check.")
            files_to_add_or_update[pathlib.Path(repo_path).joinpath(diff.filepath)] = \
                (RepoFileStatus.MODIFIED, diff.repo_props, diff.filesystem_prop)
        elif isinstance(diff, FileNotInRepo):
            logging.debug(f"File {diff.filepath} not in repo, adding.")
            files_to_add_or_update[pathlib.Path(repo_path).joinpath(diff.filepath)] = \
                (add_new_with_status, None, diff.filesystem_prop)
        elif isinstance(diff, ErrorReadingFilesystem):
            logging.error(f"File {diff.filepath} could not be read! Ignoring...")
        else:
            raise ValueError(f"unknown diff type: {diff}")

    logging.info(f"Detected {len(files_maybe_removed)} possible deletions.")
    _file_hashes: dict[Path, str] = dict(
        (file, file_desc.fasthash) for file, (_, _, file_desc) in files_to_add_or_update.items())
    inverse_hashes: Dict[str, List[Tuple[pathlib.Path, str]]] = group_to_dict(
        _file_hashes.items(),
        key=lambda file_to_hash: file_to_hash[1])
    for missing_relpath, missing_file_props in alive_it(files_maybe_removed, title="Detecting moves"):
        candidates_file_to_hash = [
            (file, fasthash) for (file, fasthash) in inverse_hashes.get(missing_file_props.fasthash, [])
            if files_to_add_or_update.get(file, (None, None, None))[0] == RepoFileStatus.ADDED]

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

    for fullpath, (requested_status, old_props, file_desc) in files_to_add_or_update.items():
        relpath = command.fast_path.FastPosixPath(fullpath).relative_to(repo_path)

        if requested_status == RepoFileStatus.MODIFIED:
            if old_props.fasthash == file_desc.fasthash:
                yield FileIsSame(relpath)
            else:
                yield FileModified(relpath, file_desc.size, file_desc.mtime, file_desc.fasthash)
        else:
            assert old_props is None
            yield FileAdded(relpath, file_desc.size, file_desc.mtime, file_desc.fasthash, requested_status)

    logging.info(f"Files read!")


class FileNotInFilesystem:
    def __init__(self, filepath: FastPosixPath, repo_props: RepoFileProps):
        assert not filepath.is_absolute()
        self.filepath = filepath
        self.repo_props = repo_props


class RepoFileDifferent:
    def __init__(self, filepath: FastPosixPath, repo_props: RepoFileProps, filesystem_prop: FileDesc):
        assert not filepath.is_absolute()

        self.filepath = filepath
        self.repo_props = repo_props

        self.filesystem_prop = filesystem_prop


class RepoFileSame:
    def __init__(self, filepath: FastPosixPath, repo_props: RepoFileProps, filesystem_prop: FileDesc):
        assert not filepath.is_absolute()
        self.filepath = filepath
        self.repo_props = repo_props

        self.filesystem_prop = filesystem_prop


class FileNotInRepo:
    def __init__(self, filepath: FastPosixPath, filesystem_prop: FileDesc):
        assert not filepath.is_absolute()
        self.filepath = filepath
        self.filesystem_prop = filesystem_prop


class ErrorReadingFilesystem:
    def __init__(self, filepath: FastPosixPath, repo_props: RepoFileProps):
        self.filepath = filepath
        self.repo_props = repo_props


class FilesystemState:
    def __init__(self, contents: RepoContents):
        self.contents = contents
        self.state_root_id = None

        self.all_files = dict()

    def mark_file(self, fullpath: FastPosixPath, file_desc: FileDesc) -> None:
        assert not fullpath.is_absolute()
        self.all_files[fullpath] = FileObject.create(file_desc.fasthash, file_desc.size)

    def files_not_found(self) -> Iterable[FastPosixPath]:
        return []  # do nothing ...

    def mark_error(self, fullpath: FastPosixPath, error: str):
        assert not fullpath.is_absolute()
        self.all_files[fullpath] = FileObject.create("", -1)

        assert not fullpath.is_absolute()

    async def diffs(self) -> AsyncGenerator[RepoDiffs]:  # fixme no need for async

        with self.contents.objects as objects:
            for fullpath, diff_type, fo_id, ff_id, skip_children in \
                    zip_dfs(objects, "", self.contents.fsobjects.root_id, self.state_root_id, True):
                ff_obj = objects[ff_id] if ff_id is not None else None
                fo_obj = objects[fo_id] if fo_id is not None else None

                if isinstance(fo_obj, TreeObject) or isinstance(ff_obj, TreeObject):
                    # fixme that is bad logic, filenames can be the same as folder names
                    continue

                fo_props = RepoFileProps(
                    fo_obj.size, 0, fo_obj.fasthash, None, RepoFileStatus.ADDED,
                    -1, None) if fo_obj is not None else None
                ff_props = FileDesc(ff_obj.size, 0, ff_obj.fasthash, None) if ff_obj is not None else None

                fullpath_posix = FastPosixPath(fullpath).relative_to("/")
                if ff_obj is None:
                    yield FileNotInFilesystem(fullpath_posix, fo_props)
                elif fo_obj is None:
                    yield FileNotInRepo(fullpath_posix, ff_props)
                else:  # both are not none
                    if ff_obj.fasthash == "":  # is error
                        yield ErrorReadingFilesystem(fullpath_posix, fo_props)

                    if fo_props.fasthash == ff_props.fasthash:
                        yield RepoFileSame(fullpath_posix, fo_props, ff_props)
                    else:
                        yield RepoFileDifferent(fullpath_posix, fo_props, ff_props)

    def diffs_at(self, allowed_paths: List[FastPosixPath]) -> Iterable[RepoDiffs]:
        raise NotImplementedError()
        # curr = self.contents.conn.cursor()
        # curr.row_factory = build_diff
        #
        # yield from curr.execute(
        #     f"SELECT * FROM filesystem_repo_matched WHERE fullpath in ({','.join('?' * len(allowed_paths))})",
        #     [p.as_posix() for p in allowed_paths])

    async def read_state_from_filesystem(
            self, contents: RepoContents, hoard_ignore: HoardIgnore, repo_path: str, njobs: int = 32):
        async def add_discovered_files(file_path_full: pathlib.Path):
            file_path_local = FastPosixPath(file_path_full.relative_to(repo_path))
            try:
                filesystem_prop = await read_filesystem_desc(file_path_full)
                self.mark_file(file_path_local, filesystem_prop)
            except OSError as e:
                logging.error(e)
                self.mark_error(file_path_local, str(e))

        await process_async(walk_filesystem(contents, hoard_ignore, repo_path), add_discovered_files, njobs=njobs)
        for repo_file in alive_it(self.files_not_found(), title="Verifying unmatched files"):
            try:
                file_on_device = pathlib.Path(repo_path).joinpath(repo_file)
                logging.debug(f"Checking {repo_file} for existence at {file_on_device}...")
                if hoard_ignore.matches(pathlib.PurePosixPath(repo_file)) or not file_on_device.is_file():
                    pass  # will yield as missing
                else:
                    filesystem_prop = await read_filesystem_desc(file_on_device)  # todo how likely are we to get here?
                    self.mark_file(repo_file, filesystem_prop)
            except OSError as e:
                logging.error(e)
                self.mark_error(repo_file, str(e))

        all_files_sorted = list(
            sorted(("/" + filepath.as_posix(), fileobj) for filepath, fileobj in self.all_files.items()))
        with self.contents.objects as objects:
            self.state_root_id = objects.mktree_from_tuples(all_files_sorted, alive_it)


def build_diff(_, row) -> RepoDiffs:  # fixme remove obsolete
    fullpath_s, fo_size, fo_mtime, fo_fasthash, fo_md5, fo_last_status, fo_last_update_epoch, fo_last_related_fullpath, \
        ff_size, ff_mtime, ff_fasthash, ff_md5, ff_error = row

    assert fullpath_s is not None
    fullpath = FastPosixPath(fullpath_s)
    if ff_error is not None:
        return ErrorReadingFilesystem(
            fullpath, RepoFileProps(
                fo_size, fo_mtime, fo_fasthash, fo_md5, fo_last_status,
                fo_last_update_epoch, fo_last_related_fullpath))
    elif fo_size is None:
        assert ff_size is not None
        return FileNotInRepo(fullpath, FileDesc(ff_size, ff_mtime, ff_fasthash, ff_md5))
    elif ff_size is None:
        return FileNotInFilesystem(fullpath, RepoFileProps(
            fo_size, fo_mtime, fo_fasthash, fo_md5, fo_last_status,
            fo_last_update_epoch, fo_last_related_fullpath))
    else:
        props = RepoFileProps(
            fo_size, fo_mtime, fo_fasthash, fo_md5, fo_last_status,
            fo_last_update_epoch, fo_last_related_fullpath)
        filesystem_prop = FileDesc(ff_size, ff_mtime, ff_fasthash, ff_md5)

        if fo_fasthash == ff_fasthash:
            return RepoFileSame(fullpath, props, filesystem_prop)
        else:
            return RepoFileDifferent(fullpath, props, filesystem_prop)


async def compute_difference_between_contents_and_filesystem(
        contents: RepoContents, repo_path: str, hoard_ignore: HoardIgnore,
        njobs: int = 32) -> AsyncGenerator[RepoDiffs]:
    state = FilesystemState(contents)
    await state.read_state_from_filesystem(contents, hoard_ignore, repo_path, njobs)

    async for diff in state.diffs():
        yield diff


def walk_filesystem(contents, hoard_ignore, repo_path) -> Iterable[pathlib.Path]:
    with alive_bar(total=contents.fsobjects.len_existing(), title="Walking filesystem") as bar:
        for file_path_full, dir_path_full in walk_repo(repo_path, hoard_ignore):
            if file_path_full is not None:
                assert dir_path_full is None
                yield file_path_full
                bar()


async def read_filesystem_desc(file_fullpath: pathlib.Path) -> FileDesc:
    stats = await aiofiles.os.stat(file_fullpath)
    fasthash = await fast_hash_async(file_fullpath)
    filesystem_prop = FileDesc(stats.st_size, stats.st_mtime, fasthash, None)
    return filesystem_prop


async def compute_difference_filtered_by_path(
        contents: RepoContents, repo_path: str, hoard_ignore: HoardIgnore,
        allowed_paths: List[pathlib.PurePosixPath]) -> AsyncGenerator[RepoDiffs]:
    state = FilesystemState(contents)

    local_paths: List[Tuple[pathlib.Path, pathlib.Path]] = []
    for allowed_path in alive_it(allowed_paths, title="Checking updates"):
        path_on_device = pathlib.Path(allowed_path).absolute()
        local_path = path_on_device.relative_to(repo_path)

        local_paths.append((path_on_device, local_path))

    for path_on_device, local_path in local_paths:
        file_path_local = FastPosixPath(local_path)
        try:
            if not hoard_ignore.matches(local_path) and path_on_device.is_file():
                filesystem_prop = await read_filesystem_desc(path_on_device)
                state.mark_file(file_path_local, filesystem_prop)
            else:
                pass  # file is not here, and is not a permission error
        except OSError as e:
            logging.error(e)
            state.mark_error(file_path_local, str(e))

    for diff in state.diffs_at([FastPosixPath(file) for _, file in local_paths]):
        yield diff


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
