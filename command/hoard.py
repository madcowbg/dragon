import logging
import os

from command.repo_command import Repo
from config import HoardConfig, HoardPaths
from resolve_uuid import load_config, load_paths

HOARD_CONTENTS_FILENAME = "hoard.contents"


class Hoard:
    def __init__(self, path: str):
        self.hoardpath = path

    def _contents_filename(self, remote_uuid):
        return os.path.join(self.hoardpath, f"{remote_uuid}.contents")

    def config(self, create: bool = False) -> HoardConfig:
        return load_config(self.hoardpath, create)

    def paths(self) -> HoardPaths:
        return load_paths(self.hoardpath)

    def __getitem__(self, remote_uuid: str) -> Repo:
        remote_path = self.paths()[remote_uuid].find()
        logging.info(f"Using repo contents {remote_uuid} in {remote_path}...")
        return Repo(remote_path)

    def hoard_contents_filename(self):
        return os.path.join(self.hoardpath, HOARD_CONTENTS_FILENAME)
