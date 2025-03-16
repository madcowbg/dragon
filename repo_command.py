import asyncio
import logging
import os
import pathlib
import uuid
from io import StringIO
from os.path import join
from typing import Generator, Tuple, List

from alive_progress import alive_bar

from contents import RepoContents, FileProps, DirProps
from hashing import find_hashes
from util import format_size

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

    def refresh(self, fast_refresh: bool = False):
        """ Refreshes the cache of the current hoard folder """
        self._validate_repo()

        current_uuid = self.current_uuid()
        logging.info(f"Refreshing uuid {current_uuid}")

        with RepoContents.load(
                self._contents_filename(current_uuid),
                create_for_uuid=current_uuid, write_on_exit=False) as contents:
            contents.config.touch_updated()

            print(f"Removing old files and folders.")
            with alive_bar(len(contents.fsobjects)) as bar:
                for file, props in contents.fsobjects:
                    bar()
                    if isinstance(props, FileProps):
                        fullpath = str(os.path.join(self.repo, file))
                        if not os.path.isfile(fullpath):
                            logging.info(f"Removing file {file}")
                            contents.fsobjects.remove(file)
                    elif isinstance(props, DirProps):
                        dirname = file
                        fullpath = str(os.path.join(self.repo, dirname))
                        if not os.path.isdir(fullpath):
                            logging.info(f"Removing dir {dirname}")
                            contents.fsobjects.remove(dirname)
                    else:
                        raise ValueError(f"invalid props type: {type(props)}")

            print("Counting files to add or update...")
            files_to_update: List[str] = []
            folders_to_add: List[str] = []
            with alive_bar(0) as bar:
                for dirpath_s, dirnames, filenames in walk_repo(self.repo):
                    dirpath = pathlib.Path(dirpath_s)
                    for filename in filenames:
                        file_path_full = dirpath.joinpath(filename)
                        if fast_refresh:
                            file_path_local = file_path_full.relative_to(self.repo).as_posix()
                            logging.info(f"Checking {file_path_local} for existence...")
                            if file_path_local in contents.fsobjects:  # file is already in index
                                logging.info(f"File is in contents, checking size and mtime.")
                                props = contents.fsobjects[file_path_local]
                                assert isinstance(props, FileProps)
                                if props.mtime == os.path.getmtime(file_path_full.as_posix()) \
                                        and props.size == os.path.getsize(file_path_full.as_posix()):
                                    logging.info("Skipping file as size and mtime is the same!!!")
                                    continue

                        files_to_update.append(file_path_full.as_posix())
                    for dirname in dirnames:
                        dir_path_full = dirpath.joinpath(dirname)
                        dir_path_in_local = dir_path_full.relative_to(self.repo).as_posix()
                        if fast_refresh and dir_path_in_local in contents.fsobjects:
                            assert isinstance(contents.fsobjects[dir_path_in_local], DirProps)
                            continue
                        folders_to_add.append(dir_path_full.as_posix())

                    bar(len(filenames) + len(dirnames))

            print(f"Hashing {len(files_to_update)} files to add:")
            file_hashes = asyncio.run(find_hashes(files_to_update))

            print(f"Reading all files in {self.repo}")
            with alive_bar(len(files_to_update) + len(folders_to_add)) as bar:
                for fullpath in files_to_update:
                    relpath = pathlib.Path(fullpath).relative_to(self.repo).as_posix()

                    contents.fsobjects.add_file(
                        relpath, size=os.path.getsize(fullpath),
                        mtime=os.path.getmtime(fullpath),
                        fasthash=file_hashes.get(fullpath, None))
                    bar()

                for fullpath in folders_to_add:
                    relpath = pathlib.Path(fullpath).relative_to(self.repo).as_posix()
                    contents.fsobjects.add_dir(relpath)
                    bar()

            logging.info(f"Files read!")

            contents.config.bump_epoch()
            logging.info(f"Bumped epoch to {contents.config.epoch}")

            logging.info(f"Writing cache...")
            contents.write()

            logging.info(f"Refresh done!")
            return f"Refresh done!"

    def show(self):
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

    def _contents_filename(self, current_uuid):
        return os.path.join(self._hoard_folder(), f"{current_uuid}.contents")
