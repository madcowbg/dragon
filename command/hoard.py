import logging
import os
import pathlib
from typing import List

from command.repo import ConnectedRepo
from config import HoardConfig, HoardPaths
from contents.hoard import ReadonlyHoardContentsConn, HOARD_CONTENTS_LMDB_DIR
from exceptions import RepoOpeningFailed
from resolve_uuid import load_config, load_paths, CONFIG_FILE


class Hoard:
    def __init__(self, path: str):
        self.hoardpath = path

    def _contents_filename(self, remote_uuid):
        return os.path.join(self.hoardpath, f"{remote_uuid}.contents")

    def config(self, create: bool = False) -> HoardConfig:
        return load_config(self.hoardpath, create)

    def paths(self) -> HoardPaths:
        return load_paths(self.hoardpath)

    def connect_to_repo(self, remote_uuid: str, require_contents: bool) -> ConnectedRepo | None:
        remote_path = self.paths()[remote_uuid].find()
        logging.info(f"Using repo contents {remote_uuid} in {remote_path}...")
        return ConnectedRepo(remote_path, remote_uuid, require_contents)

    def can_connect_to_repo(self, remote_uuid: str) -> bool:
        try:
            self.connect_to_repo(remote_uuid, require_contents=True)
            return True
        except RepoOpeningFailed as of:
            logging.debug(of)
            return False

    def open_contents(self, create_missing: bool = False) -> ReadonlyHoardContentsConn:
        hoard_contents_lmdb_dir = os.path.join(self.hoardpath, HOARD_CONTENTS_LMDB_DIR)
        if not os.path.isfile(os.path.join(self.hoardpath, CONFIG_FILE)):
            raise ValueError(f"Hoard is not configured in {self.hoardpath}!")
        if not os.path.isdir(hoard_contents_lmdb_dir) and not create_missing:
            raise ValueError(
                f"Hoard contents dir {hoard_contents_lmdb_dir} is not available,"
                f" but --create-missing = False")
        return ReadonlyHoardContentsConn(pathlib.Path(self.hoardpath), self.config(create=False))

    def available_remotes(self) -> List[str]:
        return [
            remote.uuid for remote in self.config().remotes.all()
            if self.can_connect_to_repo(remote.uuid)]
