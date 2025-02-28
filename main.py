import os
import shutil
import uuid
from datetime import datetime
from typing import Generator, List, Tuple, Dict

import fire
import logging
import rtoml
from alive_progress import alive_bar

from util import format_size

CONFIG_FILE = "hoard.config"
CURRENT_UUID_FILENAME = "current.uuid"
NONE_TOML = "MISSING"


def validate_repo(repo: str):
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


def load_uuid(repo):
    with open(os.path.join(hoard_folder(repo), CURRENT_UUID_FILENAME), "r") as f:
        return f.readline()


def walk_repo(repo: str) -> Generator[Tuple[str, List[str], List[str]], None, None]:
    for dirpath, dirnames, filenames in os.walk(repo, topdown=True):
        if ".hoard" in dirnames:
            dirnames.remove(".hoard")

        yield dirpath, dirnames, filenames


def read_contents_toml(repo: str, remote: str):
    with open(os.path.join(hoard_folder(repo), f"{remote}.contents"), "r", encoding="utf-8") as f:
        return rtoml.load(f)


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

        current_uuid = load_uuid(self.repo)
        logging.info(f"Refreshing uuid {current_uuid}")

        config = {"updated": datetime.now().isoformat()}

        logging.info("Counting files to add")
        nfiles, nfolders = 0, 0
        with alive_bar(0) as bar:
            for dirpath, dirnames, filenames in walk_repo(self.repo):
                nfiles += len(filenames)
                nfolders += len(dirnames)
                bar(len(filenames) + len(dirnames))

        logging.info(f"Reading all files in {self.repo}")
        fsobjects = {}
        with alive_bar(nfiles + nfolders) as bar:
            for dirpath, dirnames, filenames in walk_repo(self.repo):
                for filename in filenames:
                    fullpath = str(os.path.join(dirpath, filename))
                    file_props = {
                        "size": os.path.getsize(fullpath),
                        "mtime": os.path.getmtime(fullpath),
                        "isdir": False}
                    fsobjects[fullpath] = file_props
                    bar()
                for dirname in dirnames:
                    fullpath = str(os.path.join(dirpath, dirname))
                    dir_props = {
                        "isdir": True}
                    fsobjects[fullpath] = dir_props
                    bar()

        logging.info(f"Files read!")

        doc = {
            "config": config,
            "fsobjects": fsobjects}

        logging.info(f"Writing cache...")
        with open(os.path.join(hoard_folder(self.repo), "current.contents"), "w", encoding="utf-8") as f:
            rtoml.dump(doc, f)

        if not os.path.isfile(os.path.join(hoard_folder(self.repo), f"{current_uuid}.contents")):
            shutil.copy(
                os.path.join(hoard_folder(self.repo), "current.contents"),
                os.path.join(hoard_folder(self.repo), f"{current_uuid}.contents"))

        logging.info(f"Refresh done!")

    def _remotes_names(self) -> Dict[str, str]:
        logging.info(f"Reading config...")
        config = self._config()
        return dict((props["name"], remote) for remote, props in config["remotes"].items() if "name" in props)

    def show(self, remote: str = "current"):
        remote_uuid = self._resolve_remote_uuid(remote)

        logging.info(f"Reading repo {self.repo}...")
        doc = read_contents_toml(self.repo, remote=remote_uuid)
        logging.info(f"Read repo!")

        print(f"Result for [{remote}] with UUID {remote_uuid}.")
        print(f"Last updated on {datetime.fromisoformat(doc["config"]["updated"])}.")
        print(f"  # files = {len([f for f, data in doc['fsobjects'].items() if not data['isdir']])}"
              f" of size {format_size(sum(data['size'] for f, data in doc['fsobjects'].items() if not data['isdir']))}")
        print(f"  # dirs  = {len([f for f, data in doc['fsobjects'].items() if data['isdir']])}")

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
            current_uuid = load_uuid(self.repo)
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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    fire.Fire(RepoCommand)
