import hashlib
import os
import uuid

import fire

CURRENT_UUID_FILENAME = "current.uuid"


def validate_repo(repo: str):
    if not os.path.isdir(repo):
        raise ValueError(f"folder {repo} does not exist")
    if not os.path.isdir(hoard_folder(repo)):
        raise ValueError(f"no hoard folder in {repo}")
    if not os.path.isfile(os.path.join(hoard_folder(repo), CURRENT_UUID_FILENAME)):
        raise ValueError(f"no hoard guid in {repo}/.hoard/{CURRENT_UUID_FILENAME}")


def hoard_folder(repo):
    return os.path.join(repo, ".hoard")


class RepoCommand:
    def __init__(self, repo: str):
        self.repo = os.path.abspath(repo)

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
            with open(os.path.join(hoard_folder(self.repo), CURRENT_UUID_FILENAME), "w") as f:
                f.write(str(uuid.uuid4()))

        validate_repo(self.repo)


def calc_file_md5(path: str) -> str:
    hasher = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 23), b''):
            hasher.update(chunk)
    return hasher.hexdigest()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    fire.Fire(RepoCommand)
