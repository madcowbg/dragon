import logging
import os

from command.repo_command import Repo
from config import HoardConfig, HoardPaths
from contents.repo import RepoContents
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

    def fetch_repo_contents(self, remote_uuid: str):
        remote_path = self.paths()[remote_uuid].find()
        logging.info(f"Using repo contents {remote_uuid} in {remote_path}...")
        repo = Repo(remote_path)
        current_contents = RepoContents.load(repo.contents_filename(remote_uuid))
        return current_contents

    def hoard_contents_filename(self):
        return os.path.join(self.hoardpath, HOARD_CONTENTS_FILENAME)
