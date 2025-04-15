import asyncio
import logging
import os
import pathlib
from asyncio import TaskGroup, QueueShutDown
from io import StringIO
from typing import Iterable, Tuple, Dict, Optional, List, AsyncGenerator

import aiofiles.os
from alive_progress import alive_it, alive_bar

import command.fast_path
from command.fast_path import FastPosixPath
from command.hoard_ignore import HoardIgnore
from contents.repo import RepoContents
from contents.repo_props import RepoFileStatus, RepoFileProps, FileDesc
from hashing import find_hashes, fast_hash_async, fast_hash
from util import group_to_dict, run_in_separate_loop

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
    files_to_add_or_update: Dict[pathlib.Path, Tuple[RepoFileStatus, Optional[RepoFileProps]]] = {}
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
                (RepoFileStatus.MODIFIED, diff.repo_props)
        elif isinstance(diff, FileNotInRepo):
            logging.debug(f"File {diff.filepath} not in repo, adding.")
            files_to_add_or_update[pathlib.Path(repo_path).joinpath(diff.filepath)] = \
                (add_new_with_status, None)
        elif isinstance(diff, ErrorReadingFilesystem):
            logging.error(f"File {diff.filepath} could not be read! Ignoring...")
        else:
            raise ValueError(f"unknown diff type: {diff}")

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
    def __init__(self, filepath: FastPosixPath):
        assert not filepath.is_absolute()
        self.filepath = filepath


class ErrorReadingFilesystem:
    def __init__(self, filepath: FastPosixPath, repo_props: RepoFileProps):
        self.filepath = filepath
        self.repo_props = repo_props


class FilesystemState:
    def __init__(self, contents: RepoContents):
        self.contents = contents

    async def __aenter__(self):
        self.contents.conn.execute(
            "CREATE TABLE IF NOT EXISTS temp.filesystem_files ( "
            "  fullpath TEXT NOT NULL UNIQUE, "
            "  size INTEGER, "
            "  mtime REAL, "
            "  fasthash TEXT, "
            "  md5 TEXT, "
            "  error TEXT) ")

        # finds all that are not marked as DELETED or MOVED_FROM, matching against current contents
        self.contents.conn.execute(
            f"CREATE VIEW IF NOT EXISTS temp.filesystem_repo_matched AS "
            f"SELECT IFNULL(fo.fullpath, ff.fullpath) AS fullpath, "
            f"  fo.size as fo_size, fo.mtime, fo.fasthash, fo.md5, fo.last_status, fo.last_update_epoch, fo.last_related_fullpath, "
            f"  ff.size as ff_size, ff.mtime, ff.fasthash, ff.md5, ff.error as ff_error "
            f"FROM ("
            f"  SELECT * FROM fsobject "
            f"  WHERE fsobject.isdir = FALSE "
            f"    AND fsobject.last_status NOT IN ('{RepoFileStatus.DELETED.value}', '{RepoFileStatus.MOVED_FROM.value}')) AS fo "
            f"  FULL OUTER JOIN filesystem_files as ff ON fo.fullpath = ff.fullpath ")

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.contents.conn.execute("DELETE FROM temp.filesystem_files")
        return None

    def mark_file(self, fullpath: FastPosixPath, file_desc: FileDesc) -> None:
        self.contents.conn.execute(
            "INSERT INTO temp.filesystem_files (fullpath, size, mtime, fasthash, md5) VALUES (?, ?, ?, ?, ?)",
            (fullpath.as_posix(), file_desc.size, file_desc.mtime, file_desc.fasthash, file_desc.md5))

    def files_not_found(self) -> Iterable[FastPosixPath]:
        curr = self.contents.conn.cursor()
        curr.row_factory = lambda _, row: FastPosixPath(row[0])

        yield from curr.execute("SELECT fullpath FROM temp.filesystem_repo_matched WHERE ff_size IS NULL AND ff_error IS NULL")

    def mark_error(self, fullpath: FastPosixPath, error: str):
        self.contents.conn.execute(
            "INSERT INTO temp.filesystem_files (fullpath, error) VALUES (?, ?)",
            (fullpath.as_posix(), error))

    def diffs(self) -> Iterable[RepoDiffs]:
        def build_diff(_, row) -> RepoDiffs:
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
                return FileNotInRepo(fullpath)
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

        curr = self.contents.conn.cursor()
        curr.row_factory = build_diff

        yield from curr.execute("SELECT * FROM filesystem_repo_matched ")

async def compute_difference_between_contents_and_filesystem(
        contents: RepoContents, repo_path: str, hoard_ignore: HoardIgnore,
        njobs: int = 32) -> AsyncGenerator[RepoDiffs]:
    async with FilesystemState(contents) as state:
        async with TaskGroup() as tg_walking:
            q = asyncio.Queue(maxsize=njobs)

            async def fill_queue():
                nonlocal q
                for p in walk_filesystem(contents, hoard_ignore, repo_path):
                    await q.put(p)
                await q.join()
                q.shutdown()

            async def process_queue():
                nonlocal q
                while True:
                    try:
                        file_path_full = await q.get()
                        try:
                            file_path_local = FastPosixPath(file_path_full.relative_to(repo_path))
                            try:
                                filesystem_prop = await read_filesystem_desc(file_path_full)
                                state.mark_file(file_path_local, filesystem_prop)
                            except OSError as e:
                                logging.error(e)
                                state.mark_error(file_path_local, str(e))
                        finally:
                            q.task_done()
                    except QueueShutDown:
                        return

            for _ in range(njobs):
                tg_walking.create_task(process_queue())
            tg_walking.create_task(fill_queue())

        for repo_file in alive_it(state.files_not_found(), title="Verifying unmatched files"):
            try:
                file_on_device = pathlib.Path(repo_path).joinpath(repo_file)
                logging.debug(f"Checking {repo_file} for existence at {file_on_device}...")
                if not file_on_device.is_file() or hoard_ignore.matches(file_on_device):
                    pass  # will yield as missing
                else:
                    filesystem_prop = await read_filesystem_desc(file_on_device)
                    state.mark_file(repo_file, filesystem_prop)
            except OSError as e:
                logging.error(e)
                state.mark_error(repo_file, str(e))

        for diff in state.diffs():
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
                file_desc = FileDesc(stats.st_size, stats.st_mtime, fasthash, None)
                if existing_prop.fasthash == fasthash:
                    yield RepoFileSame(FastPosixPath(local_path), existing_prop, file_desc)
                else:
                    yield RepoFileDifferent(FastPosixPath(local_path), existing_prop, file_desc)
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
