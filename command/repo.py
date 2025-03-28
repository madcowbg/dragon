import logging
import os
import uuid
from os.path import join
from typing import Optional

from contents.repo import RepoContents

CURRENT_UUID_FILENAME = "current.uuid"


class ConnectedRepo:
    def __init__(self, path: str, has_contents: bool):
        self.path = path
        self.has_contents = has_contents

    @property
    def current_uuid(self):
        return _current_uuid(self.path)

    @classmethod
    def connect_if_present(cls, remote_path, require_contents) -> Optional["ConnectedRepo"]:
        if not _has_uuid_filename(remote_path):
            logging.info(f"Repo UUID file not found: {_uuid_filename(remote_path)}")
            return None
        else:
            has_contents = os.path.isfile(join(_config_folder(remote_path), f"{_current_uuid(remote_path)}.contents"))
            if require_contents and not has_contents:
                logging.info(f"Contents file not found.")
                return None
            else:
                return ConnectedRepo(remote_path, has_contents=has_contents)

    def create_if_missing(self, create_for_uuid: str) -> RepoContents:
        if self.has_contents:
            return RepoContents.load_existing(os.path.join(self.config_folder(), f"{self.current_uuid}.contents"))
        else:
            return RepoContents.create(
                os.path.join(self.config_folder(), f"{self.current_uuid}.contents"), create_for_uuid)

    def open_contents(self) -> RepoContents:
        assert self.has_contents
        return RepoContents.load_existing(os.path.join(self.config_folder(), f"{self.current_uuid}.contents"))

    def config_folder(self):
        return _config_folder(self.path)


def _current_uuid(path: str) -> str:
    with open(_uuid_filename(path), "r") as f:
        return f.readline()


def _config_folder(path: str) -> str:
    return os.path.join(path, ".hoard")


def _uuid_filename(path: str) -> str:
    return os.path.join(_config_folder(path), CURRENT_UUID_FILENAME)


def _has_uuid_filename(path: str) -> bool:
    return os.path.isfile(_uuid_filename(path))


class OfflineRepo:
    def __init__(self, path: str):
        self.path = path

    @property
    def current_uuid(self) -> str:
        return ConnectedRepo.connect_if_present(self.path, False).current_uuid

    def _init_uuid(self):
        with open(os.path.join(self.config_folder, CURRENT_UUID_FILENAME), "w") as f:
            f.write(str(uuid.uuid4()))

    def _validate_repo(self):
        logging.info(f"Validating {self.path}")
        if not os.path.isdir(self.path):
            raise ValueError(f"folder {self.path} does not exist")
        if not os.path.isdir(self.config_folder):
            raise ValueError(f"no repo folder in {self.path}")
        if not os.path.isfile(os.path.join(self.config_folder, CURRENT_UUID_FILENAME)):
            raise ValueError(f"no repo guid in {self.path}/.hoard/{CURRENT_UUID_FILENAME}")

    def init(self):
        if not os.path.isdir(self.path):
            raise ValueError(f"folder {self.path} does not exist")

        if not os.path.isdir(self.config_folder):
            os.mkdir(self.config_folder)

        if not os.path.isfile(os.path.join(self.config_folder, CURRENT_UUID_FILENAME)):
            self._init_uuid()

        self._validate_repo()

    @property
    def config_folder(self):
        return _config_folder(self.path)

    def connect(self, require_contents: bool) -> ConnectedRepo:
        return ConnectedRepo.connect_if_present(self.path, require_contents)
