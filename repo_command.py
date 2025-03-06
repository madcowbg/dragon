import asyncio
import logging
import os
import pathlib
import uuid
from io import StringIO
from os.path import join
from typing import Generator, Tuple, List

from alive_progress import alive_bar

from contents import Contents
from hashing import find_hashes

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

    def list_files(self, path: str):  # todo remove
        self._validate_repo()
        for dirpath, dirnames, filenames in walk_repo(path):
            for filename in filenames:
                fullpath = str(os.path.join(dirpath, filename))
                print(fullpath)

    def init(self):
        if not os.path.isdir(self.repo):
            raise ValueError(f"folder {self.repo} does not exist")

        if not os.path.isdir(self._hoard_folder()):
            os.mkdir(self._hoard_folder())

        if not os.path.isfile(os.path.join(self._hoard_folder(), CURRENT_UUID_FILENAME)):
            self._init_uuid()

        self._validate_repo()
        return f"Repo initialized at {self.repo}"

    def refresh(self):
        """ Refreshes the cache of the current hoard folder """
        self._validate_repo()

        current_uuid = self.current_uuid()
        logging.info(f"Refreshing uuid {current_uuid}")

        contents = Contents(
            os.path.join(self._hoard_folder(), f"{current_uuid}.contents"),
            contents_doc={"config": {"uuid": current_uuid}})
        contents.config.touch_updated()

        print("Counting files to add")
        nfiles, nfolders = 0, 0
        file_paths = set()
        with alive_bar(0) as bar:
            for dirpath, dirnames, filenames in walk_repo(self.repo):
                nfiles += len(filenames)
                nfolders += len(dirnames)
                for filename in filenames:
                    file_paths.add(join(dirpath, filename))

                bar(len(filenames) + len(dirnames))

        print("Hashing files to add:")
        file_hashes = asyncio.run(find_hashes(file_paths))

        print(f"Reading all files in {self.repo}")
        with alive_bar(nfiles + nfolders) as bar:
            for dirpath, dirnames, filenames in walk_repo(self.repo):
                for filename in filenames:
                    fullpath = str(os.path.join(dirpath, filename))
                    relpath = pathlib.Path(fullpath).relative_to(self.repo).as_posix()

                    contents.fsobjects.add_file(
                        relpath, size=os.path.getsize(fullpath),
                        mtime=os.path.getmtime(fullpath),
                        fasthash=file_hashes.get(fullpath, None))
                    bar()

                for dirname in dirnames:
                    fullpath = str(os.path.join(dirpath, dirname))
                    relpath = pathlib.Path(fullpath).relative_to(self.repo).as_posix()
                    contents.fsobjects.add_dir(relpath)
                    bar()

        logging.info(f"Files read!")

        logging.info(f"Writing cache...")
        contents.write()

        logging.info(f"Refresh done!")
        return f"Refresh done!"

    def show(self):
        remote_uuid = self.current_uuid()

        logging.info(f"Reading repo {self.repo}...")
        contents = Contents.load(self._contents_filename(remote_uuid))
        logging.info(f"Read repo!")

        with StringIO() as out:
            out.writelines([
                f"Result for local\n",
                f"UUID: {remote_uuid}\n",
                f"Last updated on {contents.config.updated}\n",
                f"  # files = {len(contents.fsobjects.files)} of size {sum(f.size for f in contents.fsobjects.files.values())}\n",
                f"  # dirs  = {len(contents.fsobjects.dirs)}\n", ])
            return out.getvalue()

    def _contents_filename(self, remote_uuid):
        return os.path.join(self._hoard_folder(), f"{remote_uuid}.contents")
