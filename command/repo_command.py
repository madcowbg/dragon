import asyncio
import logging
import os
import pathlib
import uuid
from io import StringIO
from os.path import join
from typing import Generator, Tuple, List, Optional

import aiofiles.os
from alive_progress import alive_bar, alive_it

from contents.props import RepoFileProps, DirProps
from contents.repo import RepoContents
from hashing import find_hashes, fast_hash_async
from resolve_uuid import load_config, resolve_remote_uuid, load_paths
from util import format_size, run_async_in_parallel, format_percent

CURRENT_UUID_FILENAME = "current.uuid"


def walk_repo(repo: str) -> Generator[Tuple[str, List[str], List[str]], None, None]:
    for dirpath, dirnames, filenames in os.walk(repo, topdown=True):
        if ".hoard" in dirnames:
            dirnames.remove(".hoard")

        yield dirpath, dirnames, filenames


class ConnectedRepo:
    def __init__(self, path: str, has_contents: bool = True):  # fixme remove default value, force declaration
        self.path = path
        self.has_contents = has_contents

    @property
    def current_uuid(self):
        return _current_uuid(self.path)

    @classmethod
    def connect_if_present(cls, remote_path, require_contents) -> Optional["ConnectedRepo"]:

        if not _has_uuid_filename(remote_path):
            logging.info(f"Repo UUID file not found: {_uuid_filename(remote_path)}")
            return None
        else:
            has_contents = os.path.isfile(join(_config_folder(remote_path), f"{_current_uuid(remote_path)}.contents"))
            if require_contents and not has_contents:
                logging.info(f"Contents file not found.")
                return None
            else:
                return ConnectedRepo(remote_path, has_contents=has_contents)

    def open_contents(self, create_for_uuid: Optional[str] = None) -> RepoContents:
        assert self.has_contents or create_for_uuid is not None
        return RepoContents.load(
            os.path.join(self.config_folder(), f"{self.current_uuid}.contents"),
            create_for_uuid)

    def config_folder(self):
        return _config_folder(self.path)


def _current_uuid(path: str) -> str:
    with open(_uuid_filename(path), "r") as f:
        return f.readline()


def _config_folder(path: str) -> str:
    return os.path.join(path, ".hoard")


def _uuid_filename(path: str) -> str:
    return os.path.join(_config_folder(path), CURRENT_UUID_FILENAME)


def _has_uuid_filename(path: str) -> bool:
    return os.path.isfile(_uuid_filename(path))


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

        self.repo = ConnectedRepo(pathlib.Path(path).absolute().as_posix())

    def current_uuid(self) -> str:
        return self.repo.current_uuid

    def _init_uuid(self):
        with open(os.path.join(self.repo.config_folder(), CURRENT_UUID_FILENAME), "w") as f:
            f.write(str(uuid.uuid4()))

    def _validate_repo(self):
        logging.info(f"Validating {self.repo.path}")
        if not os.path.isdir(self.repo.path):
            raise ValueError(f"folder {self.repo.path} does not exist")
        if not os.path.isdir(self.repo.config_folder()):
            raise ValueError(f"no hoard folder in {self.repo.path}")
        if not os.path.isfile(os.path.join(self.repo.config_folder(), CURRENT_UUID_FILENAME)):
            raise ValueError(f"no hoard guid in {self.repo.path}/.hoard/{CURRENT_UUID_FILENAME}")

    def init(self):
        logging.info(f"Creating repo in {self.repo.path}")

        if not os.path.isdir(self.repo.path):
            raise ValueError(f"folder {self.repo.path} does not exist")

        if not os.path.isdir(self.repo.config_folder()):
            os.mkdir(self.repo.config_folder())

        if not os.path.isfile(os.path.join(self.repo.config_folder(), CURRENT_UUID_FILENAME)):
            self._init_uuid()

        self._validate_repo()
        return f"Repo initialized at {self.repo.path}"

    def refresh(self, skip_integrity_checks: bool = False):
        """ Refreshes the cache of the current hoard folder """
        self._validate_repo()

        current_uuid = self.current_uuid()
        print(f"Refreshing uuid {current_uuid}")

        with self.repo.open_contents(current_uuid) as contents:

            logging.info("Start updating, setting is_dirty to TRUE")
            contents.config.start_updating()

            print(f"Computing diffs.")
            files_to_update: List[str] = []
            folders_to_add: List[str] = []
            for diff in compute_diffs(contents, self.repo.path, skip_integrity_checks):
                if isinstance(diff, FileNotInFilesystem):
                    logging.info(f"Removing file {diff.filepath}")
                    contents.fsobjects.remove(diff.filepath)
                elif isinstance(diff, DirNotInFilesystem):
                    logging.info(f"Removing dir {diff.dirpath}")
                    contents.fsobjects.remove(diff.dirpath)
                elif isinstance(diff, RepoFileWeakSame):
                    assert skip_integrity_checks
                    logging.info("Skipping file as size and mtime is the same!!!")
                elif isinstance(diff, RepoFileWeakDifferent):
                    assert skip_integrity_checks
                    logging.info(f"File {diff.filepath} is weakly different, adding to check.")
                    files_to_update.append(pathlib.Path(self.repo.path).joinpath(diff.filepath).as_posix())
                elif isinstance(diff, RepoFileSame):
                    logging.info(f"File {diff.filepath} is same.")
                elif isinstance(diff, RepoFileDifferent):
                    logging.info(f"File {diff.filepath} is different, adding to check.")
                    files_to_update.append(pathlib.Path(self.repo.path).joinpath(diff.filepath).as_posix())
                elif isinstance(diff, FileNotInRepo):
                    logging.info(f"File {diff.filepath} not in repo, adding.")
                    files_to_update.append(pathlib.Path(self.repo.path).joinpath(diff.filepath).as_posix())
                elif isinstance(diff, DirIsSameInRepo):
                    logging.info(f"Dir {diff.dirpath} is same, skipping")
                elif isinstance(diff, DirNotInRepo):
                    logging.info(f"Dir {diff.dirpath} is different, adding...")
                    folders_to_add.append(pathlib.Path(self.repo.path).joinpath(diff.dirpath).as_posix())
                else:
                    raise ValueError(f"unknown diff type: {type(diff)}")

            print(f"Hashing {len(files_to_update)} files to add:")
            file_hashes = asyncio.run(find_hashes(files_to_update))

            print(f"Adding {len(files_to_update)} files in {self.repo.path}")
            for fullpath in alive_it(files_to_update):
                relpath = pathlib.Path(fullpath).relative_to(self.repo.path).as_posix()

                if fullpath not in file_hashes:
                    logging.warning(f"Skipping {fullpath} as it doesn't have a computed hash!")
                    continue
                try:
                    size = os.path.getsize(fullpath)
                    mtime = os.path.getmtime(fullpath)
                    contents.fsobjects.add_file(relpath, size=size, mtime=mtime, fasthash=file_hashes[fullpath])
                except FileNotFoundError as e:
                    logging.error("Error while adding file!")
                    logging.error(e)

            print(f"Adding {len(folders_to_add)} folders in {self.repo.path}")
            for fullpath in alive_it(folders_to_add):
                relpath = pathlib.Path(fullpath).relative_to(self.repo.path).as_posix()
                contents.fsobjects.add_dir(relpath)

            logging.info(f"Files read!")

            contents.config.touch_updated()
            contents.config.bump_epoch()

            logging.info(f"Bumped epoch to {contents.config.epoch}")

            logging.info("Start updating, setting is_dirty to FALSE")
            contents.config.end_updating()

            assert not contents.config.is_dirty

            return f"Refresh done!"  # fixme add more information on what happened

    def show(self):  # fixme remove in favor of status
        remote_uuid = self.current_uuid()

        logging.info(f"Reading repo {self.repo.path}...")
        with self.repo.open_contents() as contents:
            logging.info(f"Read repo!")

            with StringIO() as out:
                out.writelines([
                    f"Result for local\n",
                    f"UUID: {remote_uuid}\n",
                    f"Last updated on {contents.config.updated}\n",
                    f"  # files = {contents.fsobjects.num_files} of size {format_size(contents.fsobjects.total_size)}\n",
                    f"  # dirs  = {contents.fsobjects.num_dirs}\n", ])
                return out.getvalue()

    def status(self, skip_integrity_checks: bool = False):
        self._validate_repo()

        current_uuid = self.current_uuid()

        files_same = []
        files_new = []
        files_mod = []
        files_del = []

        dir_new = []
        dir_same = []
        dir_deleted = []
        with self.repo.open_contents(current_uuid) as contents:
            print("Calculating diffs between repo and filesystem...")
            for diff in compute_diffs(contents, self.repo.path, skip_integrity_checks):
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
                out.write(
                    f"{current_uuid}:\n"
                    f"files:\n"
                    f"    same: {len(files_same)} ({format_percent(len(files_same) / files_current)})\n"
                    f"     mod: {len(files_mod)} ({format_percent(len(files_mod) / files_current)})\n"
                    f"     new: {len(files_new)} ({format_percent(len(files_new) / files_current)})\n"
                    f" current: {files_current}\n"
                    f" in repo: {contents.fsobjects.num_files}\n"
                    f" deleted: {len(files_del)} ({format_percent(len(files_del) / contents.fsobjects.num_files)})\n"
                    f"dirs:\n"
                    f"    same: {len(dir_same)}\n"
                    f"     new: {len(dir_new)} ({format_percent(len(dir_new) / dirs_current)})\n"
                    f" current: {dirs_current}\n"
                    f" in repo: {contents.fsobjects.num_dirs}\n"
                    f" deleted: {len(dir_deleted)} ({format_percent(len(dir_deleted) / contents.fsobjects.num_dirs)})\n")

                return out.getvalue()


class FileNotInFilesystem:
    def __init__(self, filepath: str, props: RepoFileProps):
        self.filepath = filepath
        self.props = props


class DirNotInFilesystem:
    def __init__(self, dirpath: str, props: DirProps):
        self.dirpath = dirpath
        self.props = props


class RepoFileWeakSame:
    def __init__(self, filepath: str, props: RepoFileProps):
        self.filepath = filepath
        self.props = props


class RepoFileWeakDifferent:
    def __init__(self, filepath: str, props: RepoFileProps, mtime: float, size: int):
        self.filepath = filepath
        self.props = props

        self.mtime = mtime
        self.size = size


class RepoFileDifferent:
    def __init__(self, filepath: str, props: RepoFileProps, mtime: float, size: int, fasthash: str):
        self.filepath = filepath
        self.props = props

        self.mtime = mtime
        self.size = size
        self.fasthash = fasthash


class RepoFileSame:
    def __init__(self, filepath: str, props: RepoFileProps, mtime: float):
        self.filepath = filepath
        self.props = props
        self.mtime = mtime


class DirIsSameInRepo:
    def __init__(self, dirpath: str, props: DirProps):
        self.dirpath = dirpath
        self.props = props


class DirNotInRepo:
    def __init__(self, dirpath: str):
        self.dirpath = dirpath


class FileNotInRepo:
    def __init__(self, filepath: str):
        self.filepath = filepath


type RepoDiffs = (
        FileNotInFilesystem | FileNotInRepo
        | RepoFileWeakSame | RepoFileWeakDifferent | RepoFileSame | RepoFileDifferent
        | DirNotInFilesystem | DirIsSameInRepo | DirNotInRepo)


def compute_diffs(contents: RepoContents, repo_path: str, skip_integrity_checks: bool) -> Generator[
    RepoDiffs, None, None]:
    print("Checking for deleted files and folders...")
    for obj_path, props in alive_it(list(contents.fsobjects)):
        if isinstance(props, RepoFileProps):
            if not pathlib.Path(repo_path).joinpath(obj_path).is_file():
                yield FileNotInFilesystem(obj_path, props)
        elif isinstance(props, DirProps):
            if not pathlib.Path(repo_path).joinpath(obj_path).is_dir():
                yield DirNotInFilesystem(obj_path, props)
        else:
            raise ValueError(f"invalid props type: {type(props)}")

    print("Walking filesystem for added files and changed dirs...")
    file_path_matches: List[str] = list()
    with alive_bar(total=len(contents.fsobjects)) as bar:
        for dirpath_s, dirnames, filenames in walk_repo(repo_path):
            dirpath = pathlib.Path(dirpath_s)
            for filename in filenames:
                file_path_full = dirpath.joinpath(filename)

                file_path_local = file_path_full.relative_to(repo_path).as_posix()
                logging.info(f"Checking {file_path_local} for existence...")
                if file_path_local in contents.fsobjects:  # file is already in index
                    logging.info(f"File is in contents, adding to check")  # checking size and mtime.")
                    file_path_matches.append(file_path_full.as_posix())
                else:
                    yield FileNotInRepo(file_path_local)
                bar()
            for dirname in dirnames:
                dir_path_full = dirpath.joinpath(dirname)
                dir_path_in_local = dir_path_full.relative_to(repo_path).as_posix()
                if dir_path_in_local in contents.fsobjects:
                    props = contents.fsobjects[dir_path_in_local]
                    assert isinstance(props, DirProps)
                    yield DirIsSameInRepo(dir_path_in_local, props)
                else:
                    yield DirNotInRepo(dir_path_in_local)
                bar()

    print(f"Checking {len(file_path_matches)} possibly weakly modified files...")
    with alive_bar(len(file_path_matches)) as m_bar:
        async def find_size_mtime_of(file_fullpath: str) -> RepoDiffs:
            try:
                stats = await aiofiles.os.stat(file_fullpath)

                file_path_local = pathlib.Path(file_fullpath).relative_to(repo_path).as_posix()
                props = contents.fsobjects[file_path_local]
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

    print(f"Returning file diffs for {len(file_path_matches)} files...")
    yield from alive_it(prop_tuples)
