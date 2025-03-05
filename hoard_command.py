import logging
import os
import pathlib
import shutil
from io import StringIO
from typing import Dict, Generator

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

    if current.fasthash != hoard.fasthash:
        return False  # fast hash is different

    return True  # files are the same


def path_in_hoard(current_file: str, mounted_at: str) -> str:
    curr_file_hoard_path = pathlib.Path(os.path.join(mounted_at, current_file)).as_posix()
    return curr_file_hoard_path


def path_in_local(hoard_file: str, mounted_at: str) -> str:
    return pathlib.Path(hoard_file).relative_to(mounted_at).as_posix()


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

        with StringIO() as out:
            out.write(f"Status of {remote_uuid}:\n")

            for diff in compare_local_to_hoard(current_contents, hoard, self._config()):
                if isinstance(diff, FileMissingInHoard):
                    out.write(f"A {diff.hoard_file}\n")
                elif isinstance(diff, FileContentsDiffer):
                    out.write(f"M {diff.hoard_file}\n")
                elif isinstance(diff, FileMissingInLocal):
                    out.write(f"D {diff.hoard_file}\n")
                elif isinstance(diff, DirMissingInHoard):
                    out.write(f"AF {diff.hoard_dir}\n")
                elif isinstance(diff, DirMissingInLocal):
                    out.write(f"DF {diff.hoard_dir}\n")
                else:
                    logging.info(f"Unused diff class: {type(diff)}")
            out.write("DONE")

            logging.info("Computing status done!")
            return out.getvalue()

    def mount_remote(self, remote: str, mount_point: str, force: bool = False):
        remote_uuid = self._resolve_remote_uuid(remote)
        logging.info(f"Reading config in {self.hoardpath}...")
        config = self._config()

        remote_doc = config.remotes[remote_uuid]
        if remote_doc is None:
            raise ValueError(f"remote {remote_uuid} does not exist")

        if remote_doc.mounted_at is not None and not force:
            return f"Remote {remote_uuid} already mounted in {remote_doc.mounted_at}, use --force to set.!"

        mount_path = pathlib.Path(mount_point)

        if not mount_path.is_relative_to("/"):
            return f"Mount point {mount_point} is absolute, must use relative!"

        logging.info(f"setting path to {mount_path.as_posix()}")

        remote_doc.mount_at(mount_path.as_posix())
        config.write()

        return f"set path of {remote} to {mount_path.as_posix()}\n"

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
                name_prefix = f"[{remote.name}] " if remote.name != "INVALID" else ""

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

        remote_doc = config.remotes[remote_uuid]
        if remote_doc is None or remote_doc.mounted_at is None:
            raise ValueError(f"remote_doc {remote_uuid} is not mounted!")

        logging.info("Merging local changes...")
        for diff in compare_local_to_hoard(current_contents, hoard, config):
            if isinstance(diff, FileMissingInHoard):
                hoard.fsobjects.add_available_file(diff.hoard_file, diff.local_props, remote_uuid)
            elif isinstance(diff, FileIsSame):
                logging.info(f"mark {diff.hoard_file} as available here!")
                hoard.fsobjects.files[diff.hoard_file].ensure_available(remote_uuid)
            elif isinstance(diff, FileContentsDiffer):
                logging.info(f"updating existing file {diff.local_file}")
                hoard.fsobjects.update_file(diff.hoard_file, diff.local_props)
            elif isinstance(diff, FileMissingInLocal):
                logging.info(f"deleting file {diff.hoard_file} as is no longer in local")
                hoard.fsobjects.delete_file(diff.hoard_file)
            elif isinstance(diff, DirMissingInHoard):
                logging.info(f"new dir found: {diff.local_dir}")
                hoard.fsobjects.add_dir(diff.hoard_dir)
            else:
                logging.info(f"skipping diff of type {type(diff)}")

        logging.info("Writing updated hoard contents...")
        hoard.write()
        logging.info("Local commit DONE!")

        return f"Sync'ed {remote} to hoard!"


class Diff:
    pass


class FileMissingInHoard(Diff):
    def __init__(self, current_file: str, curr_file_hoard_path: str, local_props: FileProps):
        self.local_file = current_file
        self.hoard_file = curr_file_hoard_path
        self.local_props = local_props


class FileIsSame(Diff):
    def __init__(self, current_file: str, curr_file_hoard_path: str):
        self.local_file = current_file
        self.hoard_file = curr_file_hoard_path


class FileContentsDiffer(Diff):
    def __init__(self, current_file: str, curr_file_hoard_path: str, local_props: FileProps,
                 hoard_props: HoardFileProps):
        self.local_file = current_file
        self.hoard_file = curr_file_hoard_path
        self.local_props = local_props
        self.hoard_props = hoard_props


class FileMissingInLocal(Diff):
    def __init__(self, current_file: str, curr_file_hoard_path: str):
        self.local_file = current_file
        self.hoard_file = curr_file_hoard_path


class DirMissingInHoard(Diff):
    def __init__(self, current_dir: str, curr_dir_hoard_path: str):
        self.local_dir = current_dir
        self.hoard_dir = curr_dir_hoard_path


class DirIsSame(Diff):
    def __init__(self, current_dir: str, curr_dir_hoard_path: str):
        self.local_dir = current_dir
        self.hoard_dir = curr_dir_hoard_path


class DirMissingInLocal(Diff):
    def __init__(self, current_dir: str, curr_dir_hoard_path: str):
        self.local_dir = current_dir
        self.hoard_dir = curr_dir_hoard_path


def compare_local_to_hoard(local: Contents, hoard: HoardContents, config: HoardConfig) -> Generator[Diff, None, None]:
    mounted_at = config.remotes[local.config.uuid].mounted_at

    for current_file, props in local.fsobjects.files.copy().items():
        curr_file_hoard_path = path_in_hoard(current_file, mounted_at)

        if curr_file_hoard_path not in hoard.fsobjects.files.keys():
            logging.info(f"local file not in hoard: {curr_file_hoard_path}")
            yield FileMissingInHoard(current_file, curr_file_hoard_path, props)
        elif is_same_file(
                local.fsobjects.files[current_file],
                hoard.fsobjects.files[curr_file_hoard_path]):
            logging.info(f"same in hoard {current_file}!")
            yield FileIsSame(current_file, curr_file_hoard_path)
        else:
            logging.info(f"file changes {current_file}")
            yield FileContentsDiffer(
                current_file, curr_file_hoard_path,
                props, hoard.fsobjects.files[curr_file_hoard_path])

    for hoard_file, props in hoard.fsobjects.files.copy().items():
        if not hoard_file.startswith(mounted_at):
            continue  # hoard file is not in the mounted location

        curr_file_path_in_local = path_in_local(hoard_file, mounted_at)
        if curr_file_path_in_local not in local.fsobjects.files.keys():
            yield FileMissingInLocal(curr_file_path_in_local, hoard_file)
        # else file is there, which is handled above

    for current_dir, props in local.fsobjects.dirs.copy().items():
        curr_dir_hoard_path = path_in_hoard(current_dir, mounted_at)
        if curr_dir_hoard_path not in hoard.fsobjects.dirs.keys():
            logging.info(f"new dir found: {current_dir}")
            yield DirMissingInHoard(current_dir, curr_dir_hoard_path)
        else:
            yield DirIsSame(current_dir, curr_dir_hoard_path)

    for hoard_dir, props in local.fsobjects.dirs.copy().items():
        if not hoard_dir.startswith(mounted_at):
            continue  # hoard dir is not in the mounted location

        curr_dir_path_in_local = path_in_local(hoard_dir, mounted_at)
        if curr_dir_path_in_local not in hoard.fsobjects.dirs.keys():
            logging.info(f"missing dir found in hoard: {hoard_dir}")
            yield DirMissingInLocal(curr_dir_path_in_local, hoard_dir)
        else:
            pass  # existing dirs are handled above
