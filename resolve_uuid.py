import logging
import os

from config import HoardConfig, HoardPaths

CONFIG_FILE = "hoard.config"
PATHS_FILE = "hoard.paths"


def load_config(hoardpath: str, create: bool) -> HoardConfig:
    config_file = os.path.join(hoardpath, CONFIG_FILE)
    return HoardConfig.load(config_file, create)


def resolve_remote_uuid(config: HoardConfig, remote) -> str:
    logging.info(f"Reading config...")
    remotes = config.remotes.names_map()
    remote_uuid = remotes[remote] if remote in remotes else remote
    return remote_uuid


def load_paths(hoardpath: str) -> HoardPaths:
    paths_file = os.path.join(hoardpath, PATHS_FILE)
    return HoardPaths.load(paths_file)
