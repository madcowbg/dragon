import hashlib
import os
import uuid
from datetime import datetime

import fire
import logging
import tomlkit
from alive_progress import alive_bar

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


class RepoCommand:
    def __init__(self, repo: str = ".", verbose: bool = False):
        self.repo = os.path.abspath(repo)
        if verbose:
            logging.basicConfig(level=logging.INFO)

    def list_files(self, path: str):
        validate_repo(self.repo)
        for dirpath, dirnames, filenames in os.walk(path):
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

        config = tomlkit.table()
        config["updated"] = datetime.now().isoformat()

        logging.info("Counting files to add")
        nfiles, nfolders = 0, 0
        with alive_bar(0) as bar:
            for dirpath, dirnames, filenames in os.walk(self.repo):
                nfiles += len(filenames)
                nfolders += len(dirnames)
                bar(len(filenames) + len(dirnames))

        logging.info(f"Reading all files in {self.repo}")
        fsobjects = tomlkit.table()
        with alive_bar(nfiles + nfolders) as bar:
            for dirpath, dirnames, filenames in os.walk(self.repo):
                for filename in filenames:
                    fullpath = str(os.path.join(dirpath, filename))
                    file_props = tomlkit.inline_table()
                    file_props["size"] = os.path.getsize(fullpath)
                    file_props["mtime"] = os.path.getmtime(fullpath)
                    file_props["isdir"] = False
                    fsobjects[fullpath] = file_props
                    bar()
                for dirname in dirnames:
                    fullpath = str(os.path.join(dirpath, dirname))
                    dir_props = tomlkit.inline_table()
                    dir_props["isdir"] = True
                    fsobjects[fullpath] = dir_props
                    bar()

        logging.info(f"Files read!")

        doc = tomlkit.document()
        doc.add("config", config)
        doc.add("fsobjects", fsobjects)

        with open(os.path.join(hoard_folder(self.repo), "current.contents"), "w") as f:
            tomlkit.dump(doc, f)


def calc_file_md5(path: str) -> str:
    hasher = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 23), b''):
            hasher.update(chunk)
    return hasher.hexdigest()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    fire.Fire(RepoCommand)
