import os
import pathlib
import uuid
from typing import Generator, List, Tuple, Dict, Any, Optional

import fire
import logging
import rtoml
from alive_progress import alive_bar

from contents import FileProps, Contents, HoardFileProps, HoardContents

CONFIG_FILE = "hoard.config"
CURRENT_UUID_FILENAME = "current.uuid"
NONE_TOML = "MISSING"
HOARD_CONTENTS_FILENAME = "hoard.contents"


def validate_repo(repo: str):
    logging.info(f"Validating {repo}")
    if not os.path.isdir(repo):
        raise ValueError(f"folder {repo} does not exist")
    if not os.path.isdir(hoard_folder(repo)):
        raise ValueError(f"no hoard folder in {repo}")
    if not os.path.isfile(os.path.join(hoard_folder(repo), CURRENT_UUID_FILENAME)):
        raise ValueError(f"no hoard guid in {repo}/.hoard/{CURRENT_UUID_FILENAME}")


def hoard_folder(repo):
    return os.path.join(repo, ".hoard")


def init_uuid(repo: str):
    with open(os.path.join(hoard_folder(repo), CURRENT_UUID_FILENAME), "w") as f:
        f.write(str(uuid.uuid4()))


def load_current_uuid(repo):
    with open(os.path.join(hoard_folder(repo), CURRENT_UUID_FILENAME), "r") as f:
        return f.readline()


def walk_repo(repo: str) -> Generator[Tuple[str, List[str], List[str]], None, None]:
    for dirpath, dirnames, filenames in os.walk(repo, topdown=True):
        if ".hoard" in dirnames:
            dirnames.remove(".hoard")

        yield dirpath, dirnames, filenames


def is_same_file(current: FileProps, hoard: HoardFileProps):
    if current.size != hoard.size:
        return False  # files differ by size

    if abs(current.mtime - hoard.mtime) > 1e-3:
        return False  # files differ by mtime

    return True  # files are the same TODO implement hashing


class HoardRemote:
    def __init__(self, uuid: str, doc: Dict[str, Any]):
        self.uuid = uuid
        self.doc = doc

    @property
    def name(self):
        return self.doc["name"] if "name" in self.doc else "INVALID"

    @property
    def mounted_at(self):
        return self.doc["mounted_at"] if "mounted_at" in self.doc else None

    def __setitem__(self, key: str, value: str):  # fixme make key an enum
        if key not in ["uuid", "name", "mounted_at"]:
            raise ValueError(f"Unrecognized param {key}!")
        self.doc[key] = value

    def mount_at(self, mount_at: str):
        self.doc["mounted_at"] = mount_at


class HoardRemotes:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc

    def declare(self, current_uuid: str, name: str):
        self.doc[current_uuid] = {"name": name}

    def names_map(self):
        return dict((props["name"], remote) for remote, props in self.doc.items() if "name" in props)

    def __getitem__(self, remote_uuid: str) -> Optional[HoardRemote]:
        return HoardRemote(remote_uuid, self.doc[remote_uuid] if remote_uuid in self.doc else None)

    def __len__(self):
        return len(self.doc)


class HoardConfig:
    @staticmethod
    def load(filename: str) -> "HoardConfig":
        if not os.path.isfile(filename):
            rtoml.dump({}, pathlib.Path(filename))
        with open(filename, "r", encoding="utf-8") as f:
            return HoardConfig(filename, rtoml.load(f))

    def __init__(self, filepath: str, contents_doc: Dict[str, Any]):
        self.filepath = filepath
        self.paths = contents_doc["paths"] if "paths" in contents_doc else {}
        self.remotes = HoardRemotes(contents_doc["remotes"] if "remotes" in contents_doc else {})

    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            rtoml.dump({
                "paths": self.paths,
                "remotes": self.remotes.doc
            }, f)


def path_in_hoard(current_file: str, remote: HoardRemote):
    curr_file_hoard_path = pathlib.Path(os.path.join(remote.mounted_at, current_file)).as_posix()
    return curr_file_hoard_path


class RepoCommand:
    def __init__(self, repo: str = ".", verbose: bool = False):
        self.repo = pathlib.Path(repo).absolute().as_posix()
        if verbose:
            logging.basicConfig(level=logging.INFO)

    def list_files(self, path: str):
        validate_repo(self.repo)
        for dirpath, dirnames, filenames in walk_repo(path):
            for filename in filenames:
                fullpath = str(os.path.join(dirpath, filename))
                print(fullpath)

    def init(self):
        if not os.path.isdir(self.repo):
            raise ValueError(f"folder {self.repo} does not exist")

        if not os.path.isdir(hoard_folder(self.repo)):
            os.mkdir(hoard_folder(self.repo))

        if not os.path.isfile(os.path.join(hoard_folder(self.repo), CURRENT_UUID_FILENAME)):
            init_uuid(self.repo)

        validate_repo(self.repo)

    def refresh(self):
        """ Refreshes the cache of the current hoard folder """
        validate_repo(self.repo)

        current_uuid = load_current_uuid(self.repo)
        logging.info(f"Refreshing uuid {current_uuid}")

        contents = Contents(os.path.join(hoard_folder(self.repo), f"{current_uuid}.contents"), contents_doc={})
        contents.config.touch_updated()

        logging.info("Counting files to add")
        nfiles, nfolders = 0, 0
        with alive_bar(0) as bar:
            for dirpath, dirnames, filenames in walk_repo(self.repo):
                nfiles += len(filenames)
                nfolders += len(dirnames)
                bar(len(filenames) + len(dirnames))

        logging.info(f"Reading all files in {self.repo}")
        with alive_bar(nfiles + nfolders) as bar:
            for dirpath, dirnames, filenames in walk_repo(self.repo):
                for filename in filenames:
                    fullpath = str(os.path.join(dirpath, filename))
                    relpath = pathlib.Path(fullpath).relative_to(self.repo).as_posix()

                    contents.fsobjects.add_file(
                        relpath, size=os.path.getsize(fullpath),
                        mtime=os.path.getmtime(fullpath))
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

    def _remotes_names(self) -> Dict[str, str]:
        logging.info(f"Reading config...")
        config = self._config()
        return config.remotes.names_map()

    def show(self, remote: str = "current"):
        remote_uuid = self._resolve_remote_uuid(remote)

        logging.info(f"Reading repo {self.repo}...")
        contents = Contents.load(self._contents_filename(remote_uuid))
        logging.info(f"Read repo!")

        config = self._config()

        print(f"Result for [{remote}]")
        print(f"UUID: {remote_uuid}.")
        print(
            f"name: {config.remotes[remote_uuid].name}")
        print(
            f"mount point: {config.remotes[remote_uuid].mounted_at}")
        print(f"Last updated on {contents.config.updated}.")
        print(f"  # files = {len(contents.fsobjects.files)}"
              f" of size {sum(f.size for f in contents.fsobjects.files.values())}")
        print(f"  # dirs  = {len(contents.fsobjects.dirs)}")

    def _resolve_remote_uuid(self, remote):
        if remote == "current":
            return load_current_uuid(self.repo)
        remotes = self._remotes_names()
        remote_uuid = remotes[remote] if remote in remotes else remote
        return remote_uuid

    def remotes(self):
        logging.info(f"Reading config in {self.repo}...")
        config = self._config()

        remotes_doc = config.remotes
        print(f"{len(remotes_doc)} total remotes.")
        for remote in remotes_doc:
            name_prefix = f"[{remote.name} " if remote.name != "INVALID" else ""

            print(f"  {name_prefix}{remote.uuid}")

    def _config(self) -> HoardConfig:
        config_file = os.path.join(hoard_folder(self.repo), CONFIG_FILE)
        config = HoardConfig.load(config_file)
        current_uuid = load_current_uuid(self.repo)

        if current_uuid not in config.paths:
            config.paths[current_uuid] = self.repo
            config.remotes.declare(current_uuid, name="local-repo")
            config.write()
        return config

    def config_remote(self, remote: str, param: str, value: str):
        remote_uuid = self._resolve_remote_uuid(remote)
        logging.info(f"Reading config in {self.repo}...")
        config = self._config()

        remote = config.remotes[remote_uuid]
        if remote is None:
            raise ValueError(f"remote_uuid {remote_uuid} does not exist")

        logging.info(f"Setting {param} to {value}")
        remote[param] = value

        logging.info(f"Writing config in {self.repo}...")
        config.write()
        logging.info(f"Config done!")

    def _hoard_contents_filename(self):
        return os.path.join(hoard_folder(self.repo), HOARD_CONTENTS_FILENAME)

    def commit_local(self):
        logging.info("Loading config")
        config = self._config()

        logging.info(f"Loading hoard TOML...")
        hoard = HoardContents.load(self._hoard_contents_filename())
        logging.info(f"Loaded hoard TOML!")

        current_uuid = load_current_uuid(self.repo)
        current_contents = Contents.load(self._contents_filename(current_uuid))

        remote = config.remotes[current_uuid]
        if remote is None or remote.mounted_at is None:
            raise ValueError(f"remote {current_uuid} is not mounted!")

        logging.info("Merging local changes...")
        for current_file, props in current_contents.fsobjects.files.items():
            curr_file_hoard_path = path_in_hoard(current_file, remote)

            if curr_file_hoard_path not in hoard.fsobjects.files.keys():
                logging.info(f"new file found: {curr_file_hoard_path}")
                hoard.fsobjects.add_available_file(curr_file_hoard_path, props, current_uuid)
            elif is_same_file(current_contents.fsobjects.files[current_file], hoard.fsobjects.files[curr_file_hoard_path]):
                logging.info(f"mark {current_file} as available here!")
                hoard.fsobjects.files[curr_file_hoard_path].ensure_available(current_uuid)
            else:
                logging.info(f"updating existing file {current_file}")

                hoard.fsobjects.update_file(curr_file_hoard_path, props)

        for current_dir, props in current_contents.fsobjects.dirs.items():
            curr_file_hoard_path = path_in_hoard(current_dir, remote)
            if curr_file_hoard_path not in hoard.fsobjects.dirs.keys():
                logging.info(f"new dir found: {current_dir}")
                hoard.fsobjects.add_dir(curr_file_hoard_path)
            else:
                pass  # dir is there already

        logging.info("Writing updated hoard contents...")
        hoard.write()
        logging.info("Local commit DONE!")

    def _contents_filename(self, remote_uuid):
        return os.path.join(hoard_folder(self.repo), f"{remote_uuid}.contents")

    def status_hoard(self):
        current_uuid = load_current_uuid(self.repo)

        logging.info(f"Reading current contents of {current_uuid} at {self.repo}...")
        current_contents = Contents.load(self._contents_filename(current_uuid))

        logging.info(f"Loading hoard TOML...")
        hoard = HoardContents.load(self._hoard_contents_filename())
        logging.info(f"Loaded hoard TOML!")
        logging.info(f"Computing status ...")

        print(f"Status of {current_uuid} at {self.repo}...")
        for curr_file, props in current_contents.fsobjects.files.items():
            if curr_file not in hoard.fsobjects.files.keys():
                print(f"A {curr_file}")
            elif is_same_file(current_contents.fsobjects.files[curr_file], hoard.fsobjects.files[curr_file]):
                pass  # logging.info(f"Skip adding {curr_file} as its contents are equal!")
            else:
                print(f"M {curr_file}")

        for curr_dir, props in current_contents.fsobjects.dirs.items():
            if curr_dir not in hoard.fsobjects.dirs.keys():
                print(f"AD {curr_dir}")
            else:
                pass  # dir is there already

        logging.info("Computing status done!")

    def mount_remote(self, remote: str, mount_point: str, force: bool = False):
        remote_uuid = self._resolve_remote_uuid(remote)
        logging.info(f"Reading config in {self.repo}...")
        config = self._config()

        remote = config.remotes[remote_uuid]
        if remote is None:
            raise ValueError(f"remote {remote_uuid} does not exist")

        if remote.mounted_at is not None and not force:
            print(
                f"Remote {remote_uuid} already mounted in {remote.mounted_at}, use --force to set.!")
            return

        mount_path = pathlib.Path(mount_point)

        if not mount_path.is_relative_to("/"):
            print(f"Mount point {mount_point} is absolute, must use relative!")
            return

        print(f"setting path to {mount_path.as_posix()}")

        remote.mount_at(mount_path.as_posix())
        config.write()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    fire.Fire(RepoCommand)
