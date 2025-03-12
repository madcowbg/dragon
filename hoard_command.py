import logging
import os
import pathlib
import shutil
from abc import abstractmethod
from io import StringIO
from itertools import groupby
from typing import Dict, Generator, List, Optional

from alive_progress import alive_bar

from config import HoardRemote, HoardConfig, CavePath, HoardPaths, CaveType
from contents import FileProps, HoardFileProps, Contents, HoardContents, FileStatus
from contents_diff import Diff, FileMissingInHoard, FileIsSame, FileContentsDiffer, FileMissingInLocal, \
    DirMissingInHoard, DirIsSame, DirMissingInLocal
from hashing import fast_hash
from repo_command import RepoCommand
from util import format_size

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


class DiffHandler:
    def __init__(self, remote_uuid: str, hoard: HoardContents):
        self.remote_uuid = remote_uuid
        self.hoard = hoard

    @abstractmethod
    def handle_local_only(self, diff: "FileMissingInHoard", out: StringIO): pass

    @abstractmethod
    def handle_file_is_same(self, diff: "FileIsSame", out: StringIO): pass

    @abstractmethod
    def handle_file_contents_differ(self, diff: FileContentsDiffer, out: StringIO): pass

    @abstractmethod
    def handle_hoard_only(self, diff: FileMissingInLocal, out: StringIO): pass


class PartialDiffHandler(DiffHandler):
    def __init__(
            self, remote_uuid: str, hoard: HoardContents, repos_to_add_new_files: List[HoardRemote], fetch_new: bool):
        super().__init__(remote_uuid, hoard)
        self.repos_to_add_new_files = repos_to_add_new_files
        self.fetch_new = fetch_new

    def handle_local_only(self, diff: "FileMissingInHoard", out: StringIO):
        out.write(f"+{diff.hoard_file}\n")
        self.hoard.fsobjects.add_new_file(
            diff.hoard_file, diff.local_props,
            current_uuid=self.remote_uuid, repos_to_add_new_files=self.repos_to_add_new_files)

    def handle_file_is_same(self, diff: "FileIsSame", out: StringIO):
        goal_status = self.hoard.fsobjects.files[diff.hoard_file].status(self.remote_uuid)
        if goal_status == FileStatus.CLEANUP:
            logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
            out.write(f"?{diff.hoard_file}\n")
        elif goal_status == FileStatus.GET or goal_status == FileStatus.UNKNOWN:
            logging.info(f"mark {diff.hoard_file} as available here!")
            self.hoard.fsobjects.files[diff.hoard_file].mark_available(self.remote_uuid)
            out.write(f"={diff.hoard_file}\n")
        elif goal_status == FileStatus.AVAILABLE:
            pass
        else:
            raise ValueError(f"unrecognized hoard state for {diff.hoard_file}: {goal_status}")

    def handle_file_contents_differ(self, diff: FileContentsDiffer, out: StringIO):
        goal_status = self.hoard.fsobjects.files[diff.hoard_file].status(self.remote_uuid)
        if goal_status == FileStatus.CLEANUP:
            logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
            out.write(f"?{diff.hoard_file}\n")
        elif goal_status == FileStatus.GET:
            logging.info(f"current file is out of date and was marked for restore: {diff.hoard_file}")
            out.write(f"g{diff.hoard_file}\n")
        elif goal_status == FileStatus.AVAILABLE:  # file was changed in-place
            diff.hoard_props.replace_file(diff.local_props, self.remote_uuid)
            out.write(f"u{diff.hoard_file}\n")
            diff.hoard_props.mark_to_get(self.remote_uuid)

    def handle_hoard_only(self, diff: FileMissingInLocal, out: StringIO):
        goal_status = diff.hoard_props.status(self.remote_uuid)
        if goal_status == FileStatus.CLEANUP:
            logging.info(f"file had been deleted.")
            diff.hoard_props.remove_status(self.remote_uuid)
        elif goal_status == FileStatus.AVAILABLE:  # file was here, is no longer
            logging.info(f"deleting file {diff.hoard_file} as is no longer in local")
            diff.hoard_props.mark_to_delete()
            diff.hoard_props.remove_status(self.remote_uuid)
            out.write(f"-{diff.hoard_file}\n")
        elif goal_status == FileStatus.GET:
            logging.info(f"file fetch had been scheduled already.")
        elif goal_status == FileStatus.UNKNOWN:
            logging.info(f"file not related to repo, skipping!")
        else:
            raise NotImplementedError(f"Unrecognized goal status {goal_status}")


class IncomingDiffHandler(DiffHandler):
    def __init__(self, remote_uuid: str, hoard: HoardContents, repos_to_add_new_files: List[HoardRemote]):
        super().__init__(remote_uuid, hoard)
        self.repos_to_add_new_files = repos_to_add_new_files

    def handle_local_only(self, diff: FileMissingInHoard, out: StringIO):
        out.write(f"<+{diff.hoard_file}\n")
        hoard_file = self.hoard.fsobjects.add_new_file(
            diff.hoard_file, diff.local_props,
            current_uuid=self.remote_uuid, repos_to_add_new_files=self.repos_to_add_new_files)
        logging.info(f"marking {diff.hoard_file} for cleanup from {self.remote_uuid}")
        hoard_file.mark_for_cleanup(repo_uuid=self.remote_uuid)

    def handle_file_is_same(self, diff: FileIsSame, out: StringIO):
        logging.info(f"incoming file is already recorded in hoard.")
        logging.info(f"marking {diff.hoard_file} for cleanup from {self.remote_uuid}")
        out.write(f"-{diff.hoard_file}\n")
        diff.hoard_props.mark_for_cleanup(repo_uuid=self.remote_uuid)

    def handle_file_contents_differ(self, diff: FileContentsDiffer, out: StringIO):
        goal_status = self.hoard.fsobjects.files[diff.hoard_file].status(self.remote_uuid)
        if goal_status == FileStatus.CLEANUP:  # is already marked for deletion
            logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
            out.write(f"?{diff.hoard_file}\n")
        else:  # file was changed in-place
            diff.hoard_props.replace_file(diff.local_props, self.remote_uuid)
            out.write(f"u{diff.hoard_file}\n")
            diff.hoard_props.mark_for_cleanup(self.remote_uuid)

    def handle_hoard_only(self, diff: FileMissingInLocal, out: StringIO):
        logging.info(f"skipping file not in local.")
        if diff.hoard_props.status(self.remote_uuid) == FileStatus.CLEANUP:
            diff.hoard_props.remove_status(self.remote_uuid)


class BackupDiffHandler(DiffHandler):
    def handle_local_only(self, diff: FileMissingInHoard, out: StringIO):
        logging.info(f"skipping obsolete file from backup: {diff.hoard_file}")
        out.write(f"?{diff.hoard_file}\n")

    def handle_file_is_same(self, diff: FileIsSame, out: StringIO):
        logging.info(f"file already backed up ... skipping.")

        goal_status = diff.hoard_props.status(self.remote_uuid)
        if goal_status == FileStatus.GET or goal_status == FileStatus.UNKNOWN:
            diff.hoard_props.mark_available(self.remote_uuid)
            out.write(f"={diff.hoard_file}\n")

    def handle_file_contents_differ(self, diff: FileContentsDiffer, out: StringIO):
        goal_status = diff.hoard_props.status(self.remote_uuid)
        if goal_status == FileStatus.AVAILABLE:  # was backed-up here, get it again
            out.write(f"g{diff.hoard_file}\n")
            diff.hoard_props.mark_to_get(self.remote_uuid)

    def handle_hoard_only(self, diff: FileMissingInLocal, out: StringIO):
        goal_status = diff.hoard_props.status(self.remote_uuid)
        if goal_status == FileStatus.AVAILABLE:  # was backed-up here, get it again
            out.write(f"g{diff.hoard_file}\n")
            diff.hoard_props.mark_to_get(self.remote_uuid)
        elif goal_status == FileStatus.CLEANUP:  # file already deleted
            diff.hoard_props.remove_status(self.remote_uuid)
        elif goal_status == FileStatus.GET:
            pass
        elif goal_status == FileStatus.UNKNOWN:
            diff.hoard_props.mark_to_get(self.remote_uuid)  # fixme make into a backup set
        else:
            raise NotImplementedError(f"Unrecognized goal status {goal_status}")


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

    def add_remote(
            self, remote_path: str, name: str, mount_point: str,
            type: CaveType = CaveType.PARTIAL, fetch_new: bool = False):
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

        config.remotes.declare(remote_uuid, name, type, mount_point, fetch_new)
        config.write()

        paths[remote_uuid] = CavePath.exact(remote_abs_path)
        paths.write()

    def show(self, remote: str):
        remote_uuid = self._resolve_remote_uuid(remote)

        logging.info(f"Reading repo {remote_uuid}...")
        contents = self._fetch_repo_contents(remote_uuid)
        logging.info(f"Read repo!")

        config = self.config()

        print(f"Result for [{remote}]")
        print(f"UUID: {remote_uuid}.")
        print(f"name: {config.remotes[remote_uuid].name}")
        print(f"mount point: {config.remotes[remote_uuid].mounted_at}")
        print(f"type: {config.remotes[remote_uuid].type.value}")
        print(f"Last updated on {contents.config.updated}.")
        print(f"  # files = {len(contents.fsobjects.files)}"
              f" of size {format_size(sum(f.size for f in contents.fsobjects.files.values()))}")
        print(f"  # dirs  = {len(contents.fsobjects.dirs)}")

    def _hoard_contents_filename(self):
        return os.path.join(self.hoardpath, HOARD_CONTENTS_FILENAME)

    def status(self, remote: str):
        remote_uuid = self._resolve_remote_uuid(remote)

        logging.info(f"Reading current contents of {remote_uuid}...")
        current_contents = self._fetch_repo_contents(remote_uuid)

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

                out.write(f"  {name_prefix}{remote.uuid} ({remote.type.value})\n")
            out.write("Mounts:\n")

            mounts = dict((m, list(rs)) for m, rs in groupby(config.remotes.all(), lambda r: r.mounted_at))
            for mount, remotes in mounts.items():
                out.write(f"  {mount} -> {', '.join([remote.name for remote in remotes])}\n")
            out.write("DONE\n")
            return out.getvalue()

    def refresh(self, remote: str):
        logging.info("Loading config")
        config = self.config()

        logging.info(f"Loading hoard TOML...")
        hoard = HoardContents.load(self._hoard_contents_filename())
        logging.info(f"Loaded hoard TOML!")

        remote_uuid = self._resolve_remote_uuid(remote)
        remote_type = config.remotes[remote_uuid].type

        repos_to_add_new_files: List[HoardRemote] = [
            r for r in config.remotes.all() if
            (r.type == CaveType.PARTIAL and r.fetch_new) or r.type == CaveType.BACKUP]
        if remote_type == CaveType.PARTIAL:
            remote_op_handler: DiffHandler = PartialDiffHandler(
                remote_uuid, hoard,
                repos_to_add_new_files, config.remotes[remote_uuid].fetch_new)
        elif remote_type == CaveType.BACKUP:
            remote_op_handler: DiffHandler = BackupDiffHandler(remote_uuid, hoard)
        elif remote_type == CaveType.INCOMING:
            remote_op_handler: DiffHandler = IncomingDiffHandler(
                remote_uuid, hoard,
                repos_to_add_new_files)
        else:
            raise ValueError(f"FIXME unsupported remote type: {remote_type}")

        current_contents = self._fetch_repo_contents(remote_uuid)

        remote_doc = config.remotes[remote_uuid]
        if remote_doc is None or remote_doc.mounted_at is None:
            raise ValueError(f"remote_doc {remote_uuid} is not mounted!")

        logging.info("Merging local changes...")
        with StringIO() as out:
            for diff in compare_local_to_hoard(current_contents, hoard, config):
                if isinstance(diff, FileMissingInHoard):
                    remote_op_handler.handle_local_only(diff, out)
                elif isinstance(diff, FileIsSame):
                    remote_op_handler.handle_file_is_same(diff, out)
                elif isinstance(diff, FileContentsDiffer):
                    remote_op_handler.handle_file_contents_differ(diff, out)
                elif isinstance(diff, FileMissingInLocal):
                    remote_op_handler.handle_hoard_only(diff, out)
                elif isinstance(diff, DirMissingInHoard):
                    logging.info(f"new dir found: {diff.local_dir}")
                    hoard.fsobjects.add_dir(diff.hoard_dir)
                else:
                    logging.info(f"skipping diff of type {type(diff)}")

            logging.info("Writing updated hoard contents...")
            hoard.write()
            logging.info("Local commit DONE!")

            out.write(f"Sync'ed {remote} to hoard!")
            return out.getvalue()

    def _fetch_repo_contents(self, remote_uuid):
        remote_path = self.paths()[remote_uuid].find()
        logging.info(f"Using repo contents {remote_uuid} in {remote_path}...")
        repo_cmd = RepoCommand(remote_path)
        current_contents = Contents.load(repo_cmd._contents_filename(remote_uuid))
        return current_contents

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

    def clone(self, to_path: str, mount_at: str, name: str, fetch_new: bool = False):
        if not os.path.isdir(to_path):
            return f"Cave dir {to_path} to create does not exist!"

        cave_cmd = RepoCommand(path=to_path)
        cave_cmd.init()
        cave_cmd.refresh()

        self.add_remote(to_path, name=name, mount_point=mount_at, fetch_new=fetch_new)
        return f"DONE"

    def list_files(self):
        logging.info(f"Loading hoard TOML...")
        hoard = HoardContents.load(self._hoard_contents_filename())

        logging.info(f"Listing files...")
        with StringIO() as out:
            for file, props in sorted(hoard.fsobjects.files.items()):
                a = props.by_status(FileStatus.AVAILABLE)
                g = props.by_status(FileStatus.GET)
                c = props.by_status(FileStatus.CLEANUP)
                stats = (
                    f"{f'a:{len(a)} ' if len(a) > 0 else ''}"
                    f"{f'g:{len(g)} ' if len(g) > 0 else ''}"
                    f"{f'c:{len(c)}' if len(c) > 0 else ''}").strip()
                out.write(f"{file} = {stats}\n")
            out.write("DONE")
            return out.getvalue()

    def sync_contents(self, repo: Optional[str] = None):
        config = self.config()

        logging.info(f"Loading hoard TOML...")
        hoard = HoardContents.load(self._hoard_contents_filename())

        repo_uuids: List[str] = \
            [self._resolve_remote_uuid(repo)] if repo is not None else [r.uuid for r in config.remotes.all()]

        restore_cache = RestoreCache(self)

        with StringIO() as out:
            logging.info("try getting all requested files, per repo")
            for repo_uuid in repo_uuids:
                repo_mounted_at = config.remotes[repo_uuid].mounted_at
                logging.info(f"fetching for {config.remotes[repo_uuid].name} mounted at {repo_mounted_at}")
                out.write(f"{repo_uuid}:\n")
                for hoard_file, hoard_props in sorted(hoard.fsobjects.files.items()):
                    goal_status = hoard_props.status(repo_uuid)
                    if goal_status == FileStatus.GET:
                        local_file_to_restore = path_in_local(hoard_file, mounted_at=repo_mounted_at)
                        logging.debug(f"restoring {hoard_file} to {local_file_to_restore}...")

                        success, fullpath = _restore(
                            hoard_file, local_file_to_restore, repo_uuid, hoard_props, restore_cache)
                        if success:
                            out.write(f"+ {local_file_to_restore}\n")
                            hoard_props.mark_available(repo_uuid)
                        else:
                            out.write(f"E {local_file_to_restore}\n")
                            logging.error("error restoring file!")

            logging.info("Writing hoard file...")
            hoard.write()

            logging.info("try cleaning unneeded files, per repo")
            for repo_uuid in repo_uuids:
                repo_mounted_at = config.remotes[repo_uuid].mounted_at
                logging.info(f"cleaning repo {config.remotes[repo_uuid].name} at {repo_mounted_at}")
                out.write(f"{repo_uuid}:\n")

                for hoard_file, hoard_props in sorted(hoard.fsobjects.files.items()):
                    goal_status = hoard_props.status(repo_uuid)
                    if goal_status == FileStatus.CLEANUP:
                        to_be_got = hoard_props.by_status(FileStatus.GET)

                        if len(to_be_got) == 0:
                            logging.info("file doesn't need to be copied anymore, cleaning")
                            hoard_props.remove_status(repo_uuid)
                            local_file_to_restore = path_in_local(hoard_file, mounted_at=repo_mounted_at)
                            file_to_delete = os.path.join(restore_cache.remote_path(repo_uuid), local_file_to_restore)
                            logging.info(f"deleting {file_to_delete}...")

                            try:
                                os.remove(file_to_delete)
                                out.write(f"c {local_file_to_restore}\n")
                                logging.info("file deleted!")
                            except FileNotFoundError as e:
                                out.write(f"E {local_file_to_restore}\n")
                                logging.error(e)
                        else:
                            logging.info(f"file needs to be copied in {len(to_be_got)} places, retaining")
                            out.write(f"~ {local_file_to_restore}\n")

            logging.info("Writing hoard file...")
            hoard.write()

            out.write("DONE")
            return out.getvalue()


def _restore(
        hoard_file: str, local_file_to_restore: str, local_uuid: str, hoard_props: HoardFileProps,
        restore_cache: "RestoreCache") -> (bool, str):
    fullpath_to_restore = os.path.join(restore_cache.remote_path(local_uuid), local_file_to_restore)
    logging.info(f"Restoring hoard file {hoard_file} to {fullpath_to_restore}.")

    candidates = hoard_props.by_status(FileStatus.AVAILABLE) + hoard_props.by_status(FileStatus.CLEANUP)

    for remote_uuid in candidates:
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

    logging.error(f"Did not find any available for {hoard_file}!")
    return False, fullpath_to_restore


class RestoreCache:
    def __init__(self, cmd: HoardCommand):
        self.config = cmd.config()
        self.paths = cmd.paths()

    def mounted_at(self, repo_uuid: str) -> str:
        return self.config.remotes[repo_uuid].mounted_at

    def remote_path(self, repo_uuid: str) -> str:
        return self.paths[repo_uuid].find()


def compare_local_to_hoard(local: Contents, hoard: HoardContents, config: HoardConfig) -> Generator[Diff, None, None]:
    mounted_at = config.remotes[local.config.uuid].mounted_at

    print("Comparing current files to hoard:")
    with alive_bar(len(local.fsobjects.files)) as bar:
        for current_file, props in local.fsobjects.files.copy().items():
            bar()
            curr_file_hoard_path = path_in_hoard(current_file, mounted_at)

            if curr_file_hoard_path not in hoard.fsobjects.files.keys():
                logging.info(f"local file not in hoard: {curr_file_hoard_path}")
                yield FileMissingInHoard(current_file, curr_file_hoard_path, props)
            elif is_same_file(local.fsobjects.files[current_file], hoard.fsobjects.files[curr_file_hoard_path]):
                logging.info(f"same in hoard {current_file}!")
                yield FileIsSame(current_file, curr_file_hoard_path, props, hoard.fsobjects.files[curr_file_hoard_path])
            else:
                logging.info(f"file changes {current_file}")
                yield FileContentsDiffer(
                    current_file, curr_file_hoard_path, props, hoard.fsobjects.files[curr_file_hoard_path])

    print("Comparing hoard to current files")
    with alive_bar(len(local.fsobjects.files)) as bar:
        for hoard_file, props in hoard.fsobjects.files.copy().items():
            bar()
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
