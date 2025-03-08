import asyncio
import logging
import os
import pathlib
import shutil
from io import StringIO
from typing import Dict, Generator, List, Optional

from config import HoardRemote, HoardConfig, CavePath, HoardPaths
from contents import FileProps, HoardFileProps, Contents, HoardContents
from hashing import fast_hash_async, fast_hash
from repo_command import RepoCommand

CONFIG_FILE = "hoard.config"
PATHS_FILE = "hoard.paths"
HOARD_CONTENTS_FILENAME = "hoard.contents"


def is_same_file(current: FileProps, hoard: HoardFileProps):
    if current.size != hoard.size:
        return False  # files differ by size

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
        config = self.config()
        return config.remotes.names_map()

    def config(self) -> HoardConfig:
        config_file = os.path.join(self.hoardpath, CONFIG_FILE)
        return HoardConfig.load(config_file)

    def paths(self) -> HoardPaths:
        paths_file = os.path.join(self.hoardpath, PATHS_FILE)
        return HoardPaths.load(paths_file)

    def add_remote(self, remote_path: str, name: str):
        config = self.config()
        paths = self.paths()

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
        config.write()

        paths[remote_uuid] = CavePath.exact(remote_abs_path)
        paths.write()

        self.fetch(remote_uuid)

    def fetch(self, remote: str):
        remote_uuid = self._resolve_remote_uuid(remote)
        config = self.config()

        remote_path = self.paths()[remote_uuid].find()

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

        config = self.config()

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
        config = self.config()

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

            for diff in compare_local_to_hoard(current_contents, hoard, self.config()):
                if isinstance(diff, FileMissingInHoard):
                    out.write(f"A {diff.hoard_file}\n")
                elif isinstance(diff, FileContentsDiffer):
                    if diff.local_is_newer:
                        out.write(f"M {diff.hoard_file}\n")
                    else:
                        out.write(f"M- {diff.hoard_file}\n")
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
        config = self.config()

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
        config = self.config()

        with StringIO() as out:
            out.write(f"{len(config.remotes)} total remotes.\n")
            for remote in config.remotes.all():
                name_prefix = f"[{remote.name}] " if remote.name != "INVALID" else ""

                out.write(f"  {name_prefix}{remote.uuid}\n")
            return out.getvalue()

    def refresh(self, remote: str):
        logging.info("Loading config")
        config = self.config()

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
                if diff.local_is_newer:
                    logging.info(f"recording existing file as update: {diff.local_file}")
                    hoard.fsobjects.update_file(diff.hoard_file, diff.local_props)
                else:
                    logging.info(f"hoard file is newer, won't override: {diff.hoard_file}")
            elif isinstance(diff, FileMissingInLocal):
                # fixme pretty aggressive
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

    def health(self):
        logging.info("Loading config")
        config = self.config()

        logging.info(f"Loading hoard TOML...")
        hoard = HoardContents.load(self._hoard_contents_filename())
        logging.info(f"Loaded hoard TOML!")

        repo_health: Dict[str, Dict[int, int]] = dict()
        health_files: Dict[int, List[str]] = dict()
        for file, props in hoard.fsobjects.files.items():
            num_copies = len(props.available_at)
            if num_copies not in health_files:
                health_files[num_copies] = []
            health_files[num_copies].append(file)

            # count how many files are uniquely stored here
            for repo in props.available_at:
                if repo not in repo_health:
                    repo_health[repo] = dict()
                if num_copies not in repo_health[repo]:
                    repo_health[repo][num_copies] = 0
                repo_health[repo][num_copies] += 1

        with StringIO() as out:
            out.write("Health stats:\n")
            out.write(f"{len(config.remotes)} total remotes.\n")
            for remote in config.remotes.all():
                name_prefix = f"[{remote.name}] " if remote.name != "INVALID" else ""
                out.write(
                    f"  {name_prefix}{remote.uuid}: {repo_health.get(remote.uuid, {}).get(1, 0)} with no other copy\n")

            out.write("Hoard health stats:\n")
            for num, files in sorted(health_files.items()):
                out.write(f"  {num} copies: {len(files)} files\n")
            out.write("DONE")
            return out.getvalue()

    def clone(self, to_path: str, mount_at: str, name: str):
        if not os.path.isdir(to_path):
            return f"Cave dir {to_path} to create does not exist!"

        cave_cmd = RepoCommand(path=to_path)
        cave_cmd.init()
        cave_cmd.refresh()

        self.add_remote(to_path, name=name)
        self.mount_remote(name, mount_point=mount_at)
        return f"DONE"

    def sync_content(self, to_repo: str):
        logging.info(f"Loading hoard TOML...")
        hoard = HoardContents.load(self._hoard_contents_filename())

        logging.info(f"Loading current remote contents for {to_repo}...")
        to_repo_uuid = self._resolve_remote_uuid(to_repo)
        current_contents = Contents.load(self._contents_filename(to_repo_uuid))

        config = self.config()
        restore_cache = RestoreCache(self)

        stats = {"skipped": 0, "restored": 0, "errors": 0}

        def method_name(d: FileMissingInLocal | FileContentsDiffer):
            success, fullpath = _restore(
                d.hoard_file, d.local_file, to_repo_uuid, d.hoard_props, restore_cache)
            stats["restored" if success else "errors"] += 1
            if success:
                current_contents.fsobjects.add_file(
                    d.local_file,
                    size=os.path.getsize(fullpath),
                    mtime=os.path.getmtime(fullpath),
                    fasthash=fast_hash(fullpath))
            else:
                print(f"error while restoring {d.hoard_file}")

        logging.info("Iterating over hoard contents...")
        for diff in compare_local_to_hoard(current_contents, hoard, config):
            if isinstance(diff, FileMissingInHoard):
                logging.info(f"skipping file {diff.local_file} as it is only in local!")
                stats["skipped"] += 1
            elif isinstance(diff, FileIsSame):
                logging.info(f"skipping same file as {diff.hoard_file}.")
                stats["skipped"] += 1
            elif isinstance(diff, FileContentsDiffer):
                if diff.local_is_newer:
                    logging.info(f"skipping restore as local of {diff.hoard_file} is newer.")
                    stats["skipped"] += 1
                else:
                    print(f"restoring {diff.hoard_file} as hoard is newer...")
                    method_name(diff)
            elif isinstance(diff, FileMissingInLocal):
                print(f"restoring {diff.hoard_file} that is missing.")
                method_name(diff)
            elif isinstance(diff, DirMissingInHoard):
                logging.info(f"skipping dir {diff.local_dir} as it is only in local!")
            else:
                logging.info(f"skipping diff of type {type(diff)}")

        logging.info("Writing updated local contents...")
        current_contents.write()
        logging.info("Done writing.")

        with StringIO() as out:
            for s, v in sorted(stats.items()):
                out.write(f"{s}: {v}\n")
            out.write("DONE\n")
            return out.getvalue()


def _restore(
        hoard_file: str, local_file: str, local_uuid: str, hoard_props: HoardFileProps,
        restore_cache: "RestoreCache") -> (bool, str):
    fullpath_to_restore = os.path.join(restore_cache.remote_path(local_uuid), local_file)
    logging.info(f"Restoring hoard file {hoard_file} to {fullpath_to_restore}.")

    for remote_uuid in hoard_props.available_at:
        if restore_cache.config.remotes[remote_uuid] is None:
            logging.warning(f"remote {remote_uuid} is invalid, won't try to restore")
            continue

        path_in_source = path_in_local(hoard_file, mounted_at=restore_cache.mounted_at(remote_uuid))
        source_cave_path = restore_cache.remote_path(remote_uuid)

        file_fullpath = os.path.join(source_cave_path, path_in_source)
        if not os.path.isfile(file_fullpath):
            logging.error(f"File {file_fullpath} does not exist, but is needed for restore from {remote_uuid}!")
            continue

        remote_hash = fast_hash(file_fullpath)
        if hoard_props.fasthash != remote_hash:
            logging.error(
                f"File {file_fullpath} with fast hash {remote_hash}!={hoard_props.fasthash} that was expected.")
            continue

        logging.info(f"Copying {file_fullpath} to {fullpath_to_restore}")
        try:
            shutil.copyfile(file_fullpath, fullpath_to_restore)
            return True, fullpath_to_restore
        except shutil.SameFileError as e:
            logging.error(f"Are same file: {e}")

    return False, fullpath_to_restore


class RestoreCache:
    def __init__(self, cmd: HoardCommand):
        self.config = cmd.config()
        self.paths = cmd.paths()
        # self.remotes_contents = dict()
        #
        # for remote in self.config.remotes.all():
        #     remote_path = self.config.paths[remote.uuid]
        #     remote_cmd = RepoCommand(path=remote_path)
        #     try:
        #         remote_cmd._validate_repo()
        #
        #         self.remotes_contents[remote.uuid] = Contents.load(remote_path)
        #     except ValueError as e:
        #         logging.warning(f"Skipping invalid repo in {remote_path} due to {e}")

    def mounted_at(self, repo_uuid: str) -> str:
        return self.config.remotes[repo_uuid].mounted_at

    def remote_path(self, repo_uuid: str) -> str:
        return self.paths[repo_uuid].find()


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
    def __init__(
            self, current_file: str, curr_file_hoard_path: str,
            local_props: FileProps, hoard_props: HoardFileProps):
        self.local_file = current_file
        self.hoard_file = curr_file_hoard_path
        self.local_props = local_props
        self.hoard_props = hoard_props
        self.local_is_newer = local_props.mtime >= hoard_props.mtime


class FileMissingInLocal(Diff):
    def __init__(self, current_file: str, curr_file_hoard_path: str, hoard_props: HoardFileProps):
        self.local_file = current_file
        self.hoard_file = curr_file_hoard_path
        self.hoard_props = hoard_props


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
            yield FileMissingInLocal(curr_file_path_in_local, hoard_file, props)
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
