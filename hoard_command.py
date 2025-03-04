import logging
import os
import pathlib
import shutil
from io import StringIO
from typing import Dict

from config import HoardRemote, HoardConfig
from contents import FileProps, HoardFileProps, Contents, HoardContents
from repo_command import RepoCommand

CONFIG_FILE = "hoard.config"
HOARD_CONTENTS_FILENAME = "hoard.contents"


def is_same_file(current: FileProps, hoard: HoardFileProps):
    if current.size != hoard.size:
        return False  # files differ by size

    if abs(current.mtime - hoard.mtime) > 1e-3:
        return False  # files differ by mtime

    return True  # files are the same TODO implement hashing


def path_in_hoard(current_file: str, remote: HoardRemote):
    curr_file_hoard_path = pathlib.Path(os.path.join(remote.mounted_at, current_file)).as_posix()
    return curr_file_hoard_path


class HoardCommand(object):
    def __init__(self, path: str):
        self.hoardpath = path

    def _contents_filename(self, remote_uuid):
        return os.path.join(self.hoardpath, f"{remote_uuid}.contents")

    def _remotes_names(self) -> Dict[str, str]:
        logging.info(f"Reading config...")
        config = self._config()
        return config.remotes.names_map()

    def _config(self) -> HoardConfig:
        config_file = os.path.join(self.hoardpath, CONFIG_FILE)
        return HoardConfig.load(config_file)

    def add_remote(self, remote_path: str, name: str):
        config = self._config()

        remote_abs_path = pathlib.Path(remote_path).absolute().as_posix()
        logging.info(f"Adding remote {remote_abs_path} to config...")

        logging.info("Loading remote from remote_path")
        repo_cmd = RepoCommand(remote_abs_path)

        logging.info(f"Getting remote uuid")
        remote_uuid = repo_cmd.current_uuid()

        resolved_uuid = self._resolve_remote_uuid(name)
        if resolved_uuid is not None and resolved_uuid != remote_uuid and resolved_uuid != name:  # fixme ugly AF
            raise ValueError(f"Remote uuid {name} already resolves to {resolved_uuid} and does not match {remote_uuid}")

        config.remotes.declare(remote_uuid, name)
        config.paths[remote_uuid] = remote_abs_path
        config.write()

        self.fetch(remote_uuid)

    def fetch(self, remote: str):
        remote_uuid = self._resolve_remote_uuid(remote)
        config = self._config()

        remote_path = config.paths[remote_uuid]

        logging.info(f"Fetching repo contents {remote_uuid} in {remote_path}...")
        repo_cmd = RepoCommand(remote_path)

        logging.debug(f"Copying {repo_cmd._contents_filename(remote_uuid)} to {self._contents_filename(remote_uuid)}")
        shutil.copy(repo_cmd._contents_filename(remote_uuid), self._contents_filename(remote_uuid))
        logging.info(f"Fetching done!")

    def show(self, remote: str):
        remote_uuid = self._resolve_remote_uuid(remote)

        logging.info(f"Reading repo {remote_uuid}...")
        contents = Contents.load(self._contents_filename(remote_uuid))
        logging.info(f"Read repo!")

        config = self._config()

        print(f"Result for [{remote}]")
        print(f"UUID: {remote_uuid}.")
        print(
            f"name: {config.remotes[remote_uuid].name}")
        print(
            f"mount point: {config.remotes[remote_uuid].mounted_at}")
        print(f"Last updated on {contents.config.updated}.")
        print(f"  # files = {len(contents.fsobjects.files)}"
              f" of size {sum(f.size for f in contents.fsobjects.files.values())}")
        print(f"  # dirs  = {len(contents.fsobjects.dirs)}")

    def config_remote(self, remote: str, param: str, value: str):
        remote_uuid = self._resolve_remote_uuid(remote)
        logging.info(f"Reading config in {self.hoardpath}...")
        config = self._config()

        remote = config.remotes[remote_uuid]
        if remote is None:
            raise ValueError(f"remote_uuid {remote_uuid} does not exist")

        logging.info(f"Setting {param} to {value}")
        remote[param] = value

        logging.info(f"Writing config in {self.hoardpath}...")
        config.write()
        logging.info(f"Config done!")

    def _hoard_contents_filename(self):
        return os.path.join(self.hoardpath, HOARD_CONTENTS_FILENAME)

    def status(self, remote: str):
        remote_uuid = self._resolve_remote_uuid(remote)

        logging.info(f"Reading current contents of {remote_uuid}...")
        current_contents = Contents.load(self._contents_filename(remote_uuid))

        logging.info(f"Loading hoard TOML...")
        hoard = HoardContents.load(self._hoard_contents_filename())
        logging.info(f"Loaded hoard TOML!")
        logging.info(f"Computing status ...")

        print(f"Status of {remote_uuid}:")
        for curr_file, props in current_contents.fsobjects.files.items():
            if curr_file not in hoard.fsobjects.files.keys():
                print(f"A {curr_file}")
            elif is_same_file(current_contents.fsobjects.files[curr_file], hoard.fsobjects.files[curr_file]):
                pass  # logging.info(f"Skip adding {curr_file} as its contents are equal!")
            else:
                print(f"M {curr_file}")

        for curr_dir, props in current_contents.fsobjects.dirs.items():
            if curr_dir not in hoard.fsobjects.dirs.keys():
                print(f"AD {curr_dir}")
            else:
                pass  # dir is there already

        logging.info("Computing status done!")

    def mount_remote(self, remote: str, mount_point: str, force: bool = False):
        remote_uuid = self._resolve_remote_uuid(remote)
        logging.info(f"Reading config in {self.hoardpath}...")
        config = self._config()

        remote = config.remotes[remote_uuid]
        if remote is None:
            raise ValueError(f"remote {remote_uuid} does not exist")

        if remote.mounted_at is not None and not force:
            print(
                f"Remote {remote_uuid} already mounted in {remote.mounted_at}, use --force to set.!")
            return

        mount_path = pathlib.Path(mount_point)

        if not mount_path.is_relative_to("/"):
            print(f"Mount point {mount_point} is absolute, must use relative!")
            return

        print(f"setting path to {mount_path.as_posix()}")

        remote.mount_at(mount_path.as_posix())
        config.write()

    def _resolve_remote_uuid(self, remote):
        remotes = self._remotes_names()
        remote_uuid = remotes[remote] if remote in remotes else remote
        return remote_uuid

    def remotes(self):
        logging.info(f"Reading config in {self.hoardpath}...")
        config = self._config()

        with StringIO() as out:
            out.write(f"{len(config.remotes)} total remotes.\n")
            for remote in config.remotes.all():
                name_prefix = f"[{remote.name} " if remote.name != "INVALID" else ""

                out.write(f"  {name_prefix}{remote.uuid}\n")
            return out.getvalue()

    def sync(self, remote: str):
        logging.info("Loading config")
        config = self._config()

        logging.info(f"Loading hoard TOML...")
        hoard = HoardContents.load(self._hoard_contents_filename())
        logging.info(f"Loaded hoard TOML!")

        remote_uuid = self._resolve_remote_uuid(remote)
        current_contents = Contents.load(self._contents_filename(remote_uuid))

        remote = config.remotes[remote_uuid]
        if remote is None or remote.mounted_at is None:
            raise ValueError(f"remote {remote_uuid} is not mounted!")

        logging.info("Merging local changes...")
        for current_file, props in current_contents.fsobjects.files.items():
            curr_file_hoard_path = path_in_hoard(current_file, remote)

            if curr_file_hoard_path not in hoard.fsobjects.files.keys():
                logging.info(f"new file found: {curr_file_hoard_path}")
                hoard.fsobjects.add_available_file(curr_file_hoard_path, props, remote_uuid)
            elif is_same_file(current_contents.fsobjects.files[current_file],
                              hoard.fsobjects.files[curr_file_hoard_path]):
                logging.info(f"mark {current_file} as available here!")
                hoard.fsobjects.files[curr_file_hoard_path].ensure_available(remote_uuid)
            else:
                logging.info(f"updating existing file {current_file}")

                hoard.fsobjects.update_file(curr_file_hoard_path, props)

        for current_dir, props in current_contents.fsobjects.dirs.items():
            curr_file_hoard_path = path_in_hoard(current_dir, remote)
            if curr_file_hoard_path not in hoard.fsobjects.dirs.keys():
                logging.info(f"new dir found: {current_dir}")
                hoard.fsobjects.add_dir(curr_file_hoard_path)
            else:
                pass  # dir is there already

        logging.info("Writing updated hoard contents...")
        hoard.write()
        logging.info("Local commit DONE!")
