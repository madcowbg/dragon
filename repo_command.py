import asyncio
import logging
import os
import pathlib
import uuid
from io import StringIO
from os.path import join
from typing import Generator, Tuple, List, Dict

import aiofiles.os
from alive_progress import alive_bar, alive_it

from contents_props import RepoFileProps, DirProps
from contents_repo import RepoContents
from hashing import find_hashes, fast_hash, fast_hash_async
from util import format_size, run_async_in_parallel

CURRENT_UUID_FILENAME = "current.uuid"


def walk_repo(repo: str) -> Generator[Tuple[str, List[str], List[str]], None, None]:
    for dirpath, dirnames, filenames in os.walk(repo, topdown=True):
        if ".hoard" in dirnames:
            dirnames.remove(".hoard")

        yield dirpath, dirnames, filenames


class RepoCommand(object):
    def __init__(self, path: str = "."):
        self.repo = pathlib.Path(path).absolute().as_posix()

    def _hoard_folder(self):
        return os.path.join(self.repo, ".hoard")

    def current_uuid(self):
        with open(os.path.join(self._hoard_folder(), CURRENT_UUID_FILENAME), "r") as f:
            return f.readline()

    def _init_uuid(self):
        with open(os.path.join(self._hoard_folder(), CURRENT_UUID_FILENAME), "w") as f:
            f.write(str(uuid.uuid4()))

    def _validate_repo(self):
        logging.info(f"Validating {self.repo}")
        if not os.path.isdir(self.repo):
            raise ValueError(f"folder {self.repo} does not exist")
        if not os.path.isdir(self._hoard_folder()):
            raise ValueError(f"no hoard folder in {self.repo}")
        if not os.path.isfile(os.path.join(self._hoard_folder(), CURRENT_UUID_FILENAME)):
            raise ValueError(f"no hoard guid in {self.repo}/.hoard/{CURRENT_UUID_FILENAME}")

    def init(self):
        logging.info(f"Creating repo in {self.repo}")

        if not os.path.isdir(self.repo):
            raise ValueError(f"folder {self.repo} does not exist")

        if not os.path.isdir(self._hoard_folder()):
            os.mkdir(self._hoard_folder())

        if not os.path.isfile(os.path.join(self._hoard_folder(), CURRENT_UUID_FILENAME)):
            self._init_uuid()

        self._validate_repo()
        return f"Repo initialized at {self.repo}"

    def refresh(self, skip_integrity_checks: bool = False):
        """ Refreshes the cache of the current hoard folder """
        self._validate_repo()

        current_uuid = self.current_uuid()
        logging.info(f"Refreshing uuid {current_uuid}")

        with RepoContents.load(
                self._contents_filename(current_uuid),
                create_for_uuid=current_uuid, write_on_exit=False) as contents:
            contents.config.touch_updated()

            print(f"Computing diffs.")
            files_to_update: List[str] = []
            folders_to_add: List[str] = []
            for diff in compute_diffs(contents, self.repo, skip_integrity_checks):
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
                    files_to_update.append(pathlib.Path(self.repo).joinpath(diff.filepath).as_posix())
                elif isinstance(diff, RepoFileSame):
                    logging.info(f"File {diff.filepath} is same.")
                elif isinstance(diff, RepoFileDifferent):
                    logging.info(f"File {diff.filepath} is different, adding to check.")
                    files_to_update.append(pathlib.Path(self.repo).joinpath(diff.filepath).as_posix())
                elif isinstance(diff, FileNotInRepo):
                    logging.info(f"File {diff.filepath} not in repo, adding.")
                    files_to_update.append(pathlib.Path(self.repo).joinpath(diff.filepath).as_posix())
                elif isinstance(diff, DirIsSameInRepo):
                    logging.info(f"Dir {diff.dirpath} is same, skipping")
                elif isinstance(diff, DirNotInRepo):
                    logging.info(f"Dir {diff.dirpath} is different, adding...")
                    folders_to_add.append(pathlib.Path(self.repo).joinpath(diff.dirpath).as_posix())
                else:
                    raise ValueError(f"unknown diff type: {type(diff)}")

            print(f"Hashing {len(files_to_update)} files to add:")
            file_hashes = asyncio.run(find_hashes(files_to_update))

            print(f"Adding {len(files_to_update)} files in {self.repo}")
            for fullpath in alive_it(files_to_update):
                relpath = pathlib.Path(fullpath).relative_to(self.repo).as_posix()

                contents.fsobjects.add_file(
                    relpath, size=os.path.getsize(fullpath),
                    mtime=os.path.getmtime(fullpath),
                    fasthash=file_hashes.get(fullpath, None))

            print(f"Adding {len(folders_to_add)} folders in {self.repo}")
            for fullpath in alive_it(folders_to_add):
                relpath = pathlib.Path(fullpath).relative_to(self.repo).as_posix()
                contents.fsobjects.add_dir(relpath)

            logging.info(f"Files read!")

            contents.config.bump_epoch()
            logging.info(f"Bumped epoch to {contents.config.epoch}")

            logging.info(f"Writing cache...")
            contents.write()

            return f"Refresh done!"

    def show(self):  # fixme remove in favor of status
        remote_uuid = self.current_uuid()

        logging.info(f"Reading repo {self.repo}...")
        with RepoContents.load(self._contents_filename(remote_uuid), write_on_exit=False) as contents:
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
        with RepoContents.load(
                self._contents_filename(current_uuid),
                create_for_uuid=current_uuid, write_on_exit=False) as contents:
            print("Calculating diffs between repo and filesystem...")
            for diff in compute_diffs(contents, self.repo, skip_integrity_checks):
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
                    f"    same: {len(files_same)} ({fmt_percent(len(files_same) / files_current)})\n"
                    f"     mod: {len(files_mod)} ({fmt_percent(len(files_mod) / files_current)})\n"
                    f"     new: {len(files_new)} ({fmt_percent(len(files_new) / files_current)})\n"
                    f" current: {files_current}\n"
                    f" in repo: {contents.fsobjects.num_files}\n"
                    f" deleted: {len(files_del)} ({fmt_percent(len(files_del) / contents.fsobjects.num_files)})\n"
                    f"dirs:\n"
                    f"    same: {len(dir_same)}\n"
                    f"     new: {len(dir_new)} ({fmt_percent(len(dir_new) / dirs_current)})\n"
                    f" current: {dirs_current}\n"
                    f" in repo: {contents.fsobjects.num_dirs}\n"
                    f" deleted: {len(dir_deleted)} ({fmt_percent(len(dir_deleted) / contents.fsobjects.num_dirs)})\n")

                return out.getvalue()

    def _contents_filename(self, current_uuid):
        return os.path.join(self._hoard_folder(), f"{current_uuid}.contents")


def fmt_percent(num: float): return f"{100 * num:.1f}%"


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
