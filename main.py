import os
import uuid
from datetime import datetime
from typing import Generator, List, Tuple, Dict, Any

import fire
import logging
import rtoml
from alive_progress import alive_bar

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


class Config:
    def __init__(self, config_doc: Dict[str, Any]):
        self.doc = config_doc

    def touch_updated(self):
        self.doc["updated"] = datetime.now().isoformat()

    @property
    def updated(self):
        return datetime.fromisoformat(self.doc["updated"])


class FileProps:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc

    @property
    def size(self):
        return self.doc["size"]

    @property
    def mtime(self):
        return self.doc["mtime"]


class DirProps:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc


class FSObjects:
    def __init__(self, fsobjects_doc: Dict[str, Any]):
        self.doc = fsobjects_doc
        self.files = dict((f, FileProps(data)) for f, data in self.doc.items() if not data['isdir'])
        self.dirs = dict((f, DirProps(data)) for f, data in self.doc.items() if data['isdir'])

    def add_file(self, fullpath: str, size: int, mtime: float):
        self.doc[fullpath] = {"size": size, "mtime": mtime, "isdir": False}
        self.files[fullpath] = FileProps(self.doc[fullpath])

    def add_dir(self, fullpath):
        self.doc[fullpath] = {"isdir": True}
        self.dirs[fullpath] = DirProps(self.doc[fullpath])


class Contents:
    @staticmethod
    def load(filepath: str):
        with open(filepath, "r", encoding="utf-8") as f:
            return Contents(filepath, rtoml.load(f))

    def __init__(self, filepath: str, contents_doc: Dict[str, Any]):
        self.filepath = filepath
        self.config = Config(contents_doc["config"] if "config" in contents_doc else {})
        self.fsobjects = FSObjects(contents_doc["fsobjects"] if "fsobjects" in contents_doc else {})

    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            rtoml.dump({
                "config": self.config.doc,
                "fsobjects": self.fsobjects.doc
            }, f)


class HoardFileProps:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc

    @property
    def size(self):
        return self.doc["size"]

    @property
    def mtime(self):
        return self.doc["mtime"]

    def update(self, props: FileProps):
        self.doc["size"] = props.size
        self.doc["mtime"] = props.mtime


class HoardFSObjects:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc
        self.files = dict((f, HoardFileProps(data)) for f, data in self.doc.items() if not data['isdir'])
        self.dirs = dict((f, DirProps(data)) for f, data in self.doc.items() if data['isdir'])

    def add_available_file(self, curr_file: str, props: FileProps, current_uuid: str):
        self.doc[curr_file] = {
            "isdir": False,
            "size": props.size,
            "mtime": props.mtime,
            "available": current_uuid
        }

        self.files[curr_file] = HoardFileProps(self.doc[curr_file])

    def add_dir(self, curr_dir: str):
        self.doc[curr_dir] = {"isdir": True}
        self.dirs[curr_dir] = DirProps(self.doc[curr_dir])

    def update_file(self, curr_file: str, props: FileProps):
        self.files[curr_file].update(props)


class HoardContents:
    @staticmethod
    def load(filename: str) -> "HoardContents":
        if not os.path.isfile(filename):
            with open(filename, "w", encoding="utf-8") as f:
                config = {"updated": datetime.now().isoformat()}
                rtoml.dump({
                    "config": config,
                    "fsobjects": {},
                }, f)
        with open(filename, "r", encoding="utf-8") as f:
            return HoardContents(filename, rtoml.load(f))

    def __init__(self, filepath: str, contents_doc: Dict[str, Any]):
        self.filepath = filepath
        self.config = Config(contents_doc["config"] if "config" in contents_doc else {})
        self.fsobjects = HoardFSObjects(contents_doc["fsobjects"] if "fsobjects" in contents_doc else {})

    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            rtoml.dump({
                "config": self.config.doc,
                "fsobjects": self.fsobjects.doc
            }, f)


def is_same_file(current: FileProps, hoard: HoardFileProps):
    if current.size != hoard.size:
        return False  # files differ by size

    if abs(current.mtime - hoard.mtime) > 1e-3:
        return False  # files differ by mtime

    return True  # files are the same TODO implement hashing


class RepoCommand:
    def __init__(self, repo: str = ".", verbose: bool = False):
        self.repo = os.path.abspath(repo)
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
                    contents.fsobjects.add_file(
                        fullpath, size=os.path.getsize(fullpath),
                        mtime=os.path.getmtime(fullpath))
                    bar()
                for dirname in dirnames:
                    fullpath = str(os.path.join(dirpath, dirname))
                    contents.fsobjects.add_dir(fullpath)
                    bar()

        logging.info(f"Files read!")

        logging.info(f"Writing cache...")
        contents.write()

        logging.info(f"Refresh done!")

    def _remotes_names(self) -> Dict[str, str]:
        logging.info(f"Reading config...")
        config = self._config()
        return dict((props["name"], remote) for remote, props in config["remotes"].items() if "name" in props)

    def show(self, remote: str = "current"):
        remote_uuid = self._resolve_remote_uuid(remote)

        logging.info(f"Reading repo {self.repo}...")
        contents = Contents.load(self._contents_filename(remote_uuid))
        logging.info(f"Read repo!")

        print(f"Result for [{remote}] with UUID {remote_uuid}.")
        print(f"Last updated on {contents.config.updated}.")
        print(f"  # files = {len(contents.fsobjects.files)}"
              f" of size {sum(f.size for f in contents.fsobjects.files.values())}")
        print(f"  # dirs  = {len(contents.fsobjects.dirs)}")

    def _resolve_remote_uuid(self, remote):
        remotes = self._remotes_names()
        remote_uuid = remotes[remote] if remote in remotes else remote
        return remote_uuid

    # def status(self):
    #     if not os.path.isfile(os.path.join(hoard_folder(self.repo), "current.contents")):
    #         print("Current content not refreshed!")
    #         return
    #
    #     logging.info(f"Reading current contents of {self.repo}...")
    #     current_contents_doc = read_contents_toml(self.repo, remote="current")

    def remotes(self):
        logging.info(f"Reading config in {self.repo}...")
        config_doc = self._config()

        remotes_doc = config_doc["remotes"]
        print(f"{len(remotes_doc)} total remotes.")
        for remote_uuid, props in remotes_doc.items():
            name_prefix = f"[{props['name']}] " if "name" in props else ""

            print(f"  {name_prefix}{remote_uuid}")

    def _config(self):
        config_file = os.path.join(hoard_folder(self.repo), CONFIG_FILE)

        if not os.path.isfile(config_file):
            current_uuid = load_current_uuid(self.repo)
            with open(config_file, "w", encoding="utf-8") as f:
                rtoml.dump({
                    "paths": {
                        current_uuid: self.repo
                    },
                    "remotes": {
                        current_uuid: {"name": "local-repo"}
                    }}, f)

        with open(config_file, "r", encoding="utf-8") as f:
            return rtoml.load(f)

    def config_remote(self, remote: str, param: str, value: str):
        remote_uuid = self._resolve_remote_uuid(remote)
        logging.info(f"Reading config in {self.repo}...")
        config_doc = self._config()

        if remote_uuid not in config_doc["remotes"]:
            raise ValueError(f"remote_uuid {remote_uuid} does not exist")

        logging.info(f"Setting {param} to {value}")
        config_doc["remotes"][remote_uuid][param] = value

        logging.info(f"Writing config in {self.repo}...")
        with open(os.path.join(hoard_folder(self.repo), CONFIG_FILE), "w", encoding="utf-8") as f:
            rtoml.dump(config_doc, f)
        logging.info(f"Config done!")

    def _hoard_contents_filename(self):
        return os.path.join(hoard_folder(self.repo), HOARD_CONTENTS_FILENAME)

    def commit_local(self):
        logging.info(f"Loading hoard TOML...")
        hoard = HoardContents.load(self._hoard_contents_filename())
        logging.info(f"Loaded hoard TOML!")

        current_uuid = load_current_uuid(self.repo)
        current_contents = Contents.load(self._contents_filename(current_uuid))

        logging.info("Merging local changes...")
        for curr_file, props in current_contents.fsobjects.files.items():
            if curr_file not in hoard.fsobjects.files.keys():
                logging.info(f"new file found: {curr_file}")
                hoard.fsobjects.add_available_file(curr_file, props, current_uuid)
            elif is_same_file(current_contents.fsobjects.files[curr_file], hoard.fsobjects.files[curr_file]):
                pass  # logging.info(f"Skip adding {curr_file} as its contents are equal!")
            else:
                logging.info(f"updating existing file {curr_file}")

                hoard.fsobjects.update_file(curr_file, props)

        for curr_dir, props in current_contents.fsobjects.dirs.items():
            if curr_dir not in hoard.fsobjects.dirs.keys():
                logging.info(f"new dir found: {curr_dir}")
                hoard.fsobjects.add_dir(curr_dir)
            else:
                pass  # dir is there already

        logging.info("Writing updated hoard contents...")
        hoard.write()
        logging.info("Local commit DONE!")

    def _contents_filename(self, remote_uuid):
        return os.path.join(hoard_folder(self.repo), f"{remote_uuid}.contents")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    fire.Fire(RepoCommand)
