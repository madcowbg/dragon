import logging
import os

from command.repo import ConnectedRepo
from config import HoardConfig, HoardPaths
from contents.hoard import HoardContents, HOARD_CONTENTS_FILENAME
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

    def open_contents(self, create_missing: bool = False, is_readonly: bool = True) -> HoardContents:
        hoard_contents_filename = os.path.join(self.hoardpath, HOARD_CONTENTS_FILENAME)
        if not os.path.isfile(os.path.join(self.hoardpath, CONFIG_FILE)):
            raise ValueError(f"Hoard is not configured in {self.hoardpath}!")
        if not os.path.isfile(hoard_contents_filename) and not create_missing:
            raise ValueError(
                f"Hoard contents file {hoard_contents_filename} is not available,"
                f" but --create-missing = False")
        return HoardContents.load(self.hoardpath, is_readonly)
