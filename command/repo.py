import logging
import os
import uuid
from os.path import join

from contents.repo import RepoContents

CURRENT_UUID_FILENAME = "current.uuid"


def _current_uuid(path: str) -> str:
    with open(_uuid_filename(path), "r") as f:
        return f.readline()


def _config_folder(path: str) -> str:
    return os.path.join(path, ".hoard")


def _uuid_filename(path: str) -> str:
    return os.path.join(_config_folder(path), CURRENT_UUID_FILENAME)


def _has_uuid_filename(path: str) -> bool:
    return os.path.isfile(_uuid_filename(path))


class RepoOpeningFailed(Exception):
    pass


class MissingRepo(RepoOpeningFailed):
    pass


class WrongRepo(RepoOpeningFailed):
    pass


class RepoHasNoContents(RepoOpeningFailed):
    pass


class ProspectiveRepo:
    def __init__(self, path: str):
        self.path = path

    def _init_uuid(self):
        with open(os.path.join(self.config_folder, CURRENT_UUID_FILENAME), "w") as f:
            f.write(str(uuid.uuid4()))

    @property
    def config_folder(self):
        return _config_folder(self.path)

    def init(self):
        if not os.path.isdir(self.path):
            raise MissingRepo(f"folder {self.path} does not exist")

        if not os.path.isdir(self.config_folder):
            os.mkdir(self.config_folder)

        if not os.path.isfile(os.path.join(self.config_folder, CURRENT_UUID_FILENAME)):
            self._init_uuid()

    def _validate_repo(self):
        logging.info(f"Validating {self.path}")
        if not os.path.isdir(self.path):
            raise MissingRepo(f"folder {self.path} does not exist")
        if not os.path.isdir(self.config_folder):
            raise MissingRepo(f"no repo folder in {self.path}")
        if not os.path.isfile(os.path.join(self.config_folder, CURRENT_UUID_FILENAME)):
            raise MissingRepo(f"no repo guid in {self.path}/.hoard/{CURRENT_UUID_FILENAME}")

    @property
    def current_uuid(self) -> str:
        self._validate_repo()
        return _current_uuid(self.path)

    def open_repo(self):
        self._validate_repo()
        return OfflineRepo(self.path, _current_uuid(self.path))


class OfflineRepo(ProspectiveRepo):
    def __init__(self, path: str, repo_uuid: str):
        super().__init__(path)
        self.repo_uuid = repo_uuid
        self._validate_repo()

    def connect(self, require_contents: bool) -> "ConnectedRepo":
        return ConnectedRepo(self.path, self.current_uuid, require_contents)


class ConnectedRepo(OfflineRepo):
    def __init__(self, path: str, repo_uuid: str, require_contents: bool):
        super().__init__(path, repo_uuid)

        if repo_uuid != self.current_uuid:
            raise WrongRepo(
                f"Repo in {path} has uuid {self.current_uuid} "
                f"which differs from the requested uuid {repo_uuid}")

        if require_contents and not self.has_contents:
            logging.info(f"Contents file not found.")
            raise RepoHasNoContents()

    @property
    def has_contents(self):
        return os.path.isfile(join(_config_folder(self.path), f"{self.current_uuid}.contents"))

    def create_contents(self, create_for_uuid: str) -> RepoContents:
        assert not self.has_contents
        return RepoContents.create(
            os.path.join(self.config_folder, f"{self.current_uuid}.contents"), create_for_uuid)

    def open_contents(self) -> RepoContents:
        if not self.has_contents:
            raise RepoHasNoContents()
        return RepoContents.load_existing(os.path.join(self.config_folder, f"{self.current_uuid}.contents"))
