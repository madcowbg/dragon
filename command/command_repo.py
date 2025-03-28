import asyncio
import logging
import os
import pathlib
from io import StringIO
from typing import Generator, Tuple, List, Optional

import aiofiles.os
from alive_progress import alive_bar, alive_it

from command.repo import ProspectiveRepo
from exceptions import MissingRepo, MissingRepoContents
from contents.repo_props import RepoFileProps, RepoFileStatus, RepoDirProps
from contents.hoard_props import HoardDirProps
from contents.repo import RepoContents
from hashing import find_hashes, fast_hash_async
from resolve_uuid import load_config, resolve_remote_uuid, load_paths
from util import format_size, run_async_in_parallel, format_percent


def walk_repo(repo: str) -> Generator[Tuple[str, List[str], List[str]], None, None]:
    for dirpath, dirnames, filenames in os.walk(repo, topdown=True):
        if ".hoard" in dirnames:
            dirnames.remove(".hoard")

        yield dirpath, dirnames, filenames


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

    def refresh(self, skip_integrity_checks: bool = False):
        """ Refreshes the cache of the current hoard folder """
        connected_repo = self.repo.open_repo().connect(False)
        if connected_repo is None:
            return f"No initialized repo in {self.repo.path}!"

        current_uuid = connected_repo.current_uuid
        try:
            contents = connected_repo.open_contents()
            first_refresh = False
        except MissingRepoContents as e:
            logging.warning("Repo contents missing, creating!")
            first_refresh = True
            contents = connected_repo.create_contents(current_uuid)

        print(f"Refreshing uuid {current_uuid}{', is first refresh' if first_refresh else ''}")
        add_new_with_status = RepoFileStatus.ADDED if not first_refresh else RepoFileStatus.PRESENT

        with contents:
            logging.info("Start updating, setting is_dirty to TRUE")
            contents.config.start_updating()

            logging.info(f"Bumped epoch to {contents.config.epoch}")

            print(f"Computing diffs.")
            files_to_add_or_update: List[Tuple[str, RepoFileStatus]] = []
            folders_to_add: List[str] = []
            for diff in compute_difference_between_contents_and_filesystem(
                    contents, self.repo.path, skip_integrity_checks):
                if isinstance(diff, FileNotInFilesystem):
                    logging.info(f"Removing file {diff.filepath}")
                    contents.fsobjects.mark_removed(diff.filepath)
                elif isinstance(diff, DirNotInFilesystem):
                    logging.info(f"Removing dir {diff.dirpath}")
                    contents.fsobjects.mark_removed(diff.dirpath)
                elif isinstance(diff, RepoFileWeakSame):
                    assert skip_integrity_checks
                    logging.info("Skipping file as size and mtime is the same!!!")
                elif isinstance(diff, RepoFileWeakDifferent):
                    assert skip_integrity_checks
                    logging.info(f"File {diff.filepath} is weakly different, adding to check.")
                    files_to_add_or_update.append(
                        (pathlib.Path(self.repo.path).joinpath(diff.filepath).as_posix(), RepoFileStatus.MODIFIED))
                elif isinstance(diff, RepoFileSame):
                    logging.info(f"File {diff.filepath} is same.")
                elif isinstance(diff, RepoFileDifferent):
                    logging.info(f"File {diff.filepath} is different, adding to check.")
                    files_to_add_or_update.append(
                        (pathlib.Path(self.repo.path).joinpath(diff.filepath).as_posix(), RepoFileStatus.MODIFIED))
                elif isinstance(diff, FileNotInRepo):
                    logging.info(f"File {diff.filepath} not in repo, adding.")
                    files_to_add_or_update.append(
                        (pathlib.Path(self.repo.path).joinpath(diff.filepath).as_posix(), add_new_with_status))
                elif isinstance(diff, DirIsSameInRepo):
                    logging.info(f"Dir {diff.dirpath} is same, skipping")
                elif isinstance(diff, DirNotInRepo):
                    logging.info(f"Dir {diff.dirpath} is different, adding...")
                    folders_to_add.append(pathlib.Path(self.repo.path).joinpath(diff.dirpath).as_posix())
                else:
                    raise ValueError(f"unknown diff type: {type(diff)}")

            print(f"Hashing {len(files_to_add_or_update)} files to add:")
            file_hashes = asyncio.run(find_hashes([file for file, status in files_to_add_or_update]))

            print(f"Adding {len(files_to_add_or_update)} files in {self.repo.path}")
            for fullpath, requested_status in alive_it(files_to_add_or_update):
                relpath = pathlib.Path(fullpath).relative_to(self.repo.path).as_posix()

                if fullpath not in file_hashes:
                    logging.warning(f"Skipping {fullpath} as it doesn't have a computed hash!")
                    continue
                try:
                    size = os.path.getsize(fullpath)
                    mtime = os.path.getmtime(fullpath)
                    contents.fsobjects.add_file(
                        relpath, size=size, mtime=mtime, fasthash=file_hashes[fullpath], status=requested_status)
                except FileNotFoundError as e:
                    logging.error("Error while adding file!")
                    logging.error(e)

            print(f"Adding {len(folders_to_add)} folders in {self.repo.path}")
            for fullpath in alive_it(folders_to_add):
                relpath = pathlib.Path(fullpath).relative_to(self.repo.path).as_posix()
                contents.fsobjects.add_dir(relpath, status=RepoFileStatus.ADDED)

            logging.info(f"Files read!")

            contents.config.touch_updated()

            logging.info("Start updating, setting is_dirty to FALSE")
            contents.config.end_updating()

            assert not contents.config.is_dirty

            return f"Refresh done!"  # fixme add more information on what happened

    def status_index(self, show_files: bool = True, show_dates: bool = True):
        remote_uuid = self.current_uuid()

        logging.info(f"Reading repo {self.repo.path}...")
        with self.repo.open_repo().connect(False).open_contents() as contents:
            logging.info(f"Read repo!")

            with StringIO() as out:
                if show_files:
                    for file_or_dir, props in contents.fsobjects.all_status():
                        out.write(f"{file_or_dir}: {props.last_status.value} @ {props.last_update_epoch}\n")
                    out.write("--- SUMMARY ---\n")

                stats = contents.fsobjects.stats_existing
                out.writelines([
                    f"Result for local\n",
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
            contents = connected_repo.open_contents()
        except MissingRepoContents:
            return f"Repo {current_uuid} contents have not been refreshed yet!"

        files_same = []
        files_new = []
        files_mod = []
        files_del = []

        dir_new = []
        dir_same = []
        dir_deleted = []

        with contents:
            print("Calculating diffs between repo and filesystem...")
            for diff in compute_difference_between_contents_and_filesystem(contents, self.repo.path,
                                                                           skip_integrity_checks):
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
                    f" deleted: {len(dir_deleted)} ({format_percent(len(dir_deleted) / stats.num_dirs)})\n")

                return out.getvalue()


class FileNotInFilesystem:
    def __init__(self, filepath: str, props: RepoFileProps):
        self.filepath = filepath
        self.props = props


class DirNotInFilesystem:
    def __init__(self, dirpath: str, props: HoardDirProps):
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
    def __init__(self, dirpath: str, props: HoardDirProps):
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


def compute_difference_between_contents_and_filesystem(
        contents: RepoContents, repo_path: str, skip_integrity_checks: bool) -> Generator[RepoDiffs, None, None]:
    print("Checking for deleted files and folders...")
    for obj_path, props in alive_it(list(contents.fsobjects.existing())):
        if isinstance(props, RepoFileProps):
            if not pathlib.Path(repo_path).joinpath(obj_path).is_file():
                yield FileNotInFilesystem(obj_path, props)
        elif isinstance(props, RepoDirProps):
            if not pathlib.Path(repo_path).joinpath(obj_path).is_dir():
                yield DirNotInFilesystem(obj_path, props)
        else:
            raise ValueError(f"invalid props type: {type(props)}")

    print("Walking filesystem for added files and changed dirs...")
    file_path_matches: List[str] = list()
    with alive_bar(total=contents.fsobjects.len_existing()) as bar:
        for dirpath_s, dirnames, filenames in walk_repo(repo_path):
            dirpath = pathlib.Path(dirpath_s)
            for filename in filenames:
                file_path_full = dirpath.joinpath(filename)

                file_path_local = file_path_full.relative_to(repo_path).as_posix()
                logging.info(f"Checking {file_path_local} for existence...")
                if contents.fsobjects.in_existing(file_path_local):  # file is already in index
                    logging.info(f"File is in contents, adding to check")  # checking size and mtime.")
                    file_path_matches.append(file_path_full.as_posix())
                else:
                    yield FileNotInRepo(file_path_local)
                bar()
            for dirname in dirnames:
                dir_path_full = dirpath.joinpath(dirname)
                dir_path_in_local = dir_path_full.relative_to(repo_path).as_posix()
                if contents.fsobjects.in_existing(dir_path_in_local):
                    props = contents.fsobjects.get_existing(dir_path_in_local)
                    assert isinstance(props, RepoDirProps)
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

    print(f"Returning file diffs for {len(file_path_matches)} files...")
    yield from alive_it(prop_tuples)
