import logging
import os
import pathlib
import shutil
import sys
from abc import abstractmethod
from io import StringIO
from itertools import groupby
from typing import Dict, Generator, List, Optional

from alive_progress import alive_bar

from config import HoardRemote, HoardConfig, CavePath, HoardPaths, CaveType
from contents import FileProps, HoardFileProps, Contents, HoardContents, FileStatus, HoardFile, HoardDir
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


class HoardPathing:
    def __init__(self, config: HoardConfig, paths: HoardPaths):
        self._config = config
        self._paths = paths

    class HoardPath:
        def __init__(self, path: str, pathing: "HoardPathing"):
            self._path = pathlib.Path(path)
            self._pathing = pathing

        def as_posix(self) -> str:
            return self._path.as_posix()

        def at_local(self, repo_uuid: str) -> Optional["HoardPathing.LocalPath"]:
            mounted_at = self._pathing._config.remotes[repo_uuid].mounted_at
            if not self._path.is_relative_to(mounted_at):
                return None
            else:
                return HoardPathing.LocalPath(self._path.relative_to(mounted_at).as_posix(), repo_uuid, self._pathing)

    class LocalPath:
        def __init__(self, path: str, repo_uuid: str, pathing: "HoardPathing"):
            self._path = pathlib.Path(path)
            self._repo_uuid = repo_uuid
            self._pathing = pathing

        def as_posix(self) -> str: return self._path.as_posix()

        def on_device_path(self) -> str:
            return pathlib.Path(self._pathing._paths[self._repo_uuid].find()).joinpath(self._path).as_posix()

        def at_hoard(self) -> "HoardPathing.HoardPath":
            joined_path = pathlib.Path(self._pathing._config.remotes[self._repo_uuid].mounted_at).joinpath(self._path)
            return HoardPathing.HoardPath(joined_path.as_posix(), self._pathing)

    def in_hoard(self, path: str) -> HoardPath:
        return self.HoardPath(path, self)

    def in_local(self, path: str, repo_uuid: str) -> LocalPath:
        return HoardPathing.LocalPath(path, repo_uuid, self)

    def repos_availability(self, folder: str) -> Dict[HoardRemote, str]:
        paths: Dict[HoardRemote, str] = {}
        for remote in self._config.remotes.all():
            relative_local_path = self.in_hoard(folder).at_local(remote.uuid)
            if relative_local_path is not None:
                paths[remote] = relative_local_path.as_posix()
        return paths


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


def filter_accessible(pathing: HoardPathing, repos: List[HoardRemote], hoard_file: str) -> List[HoardRemote]:
    return [r for r in repos if pathing.in_hoard(hoard_file).at_local(r.uuid) is not None]


class PartialDiffHandler(DiffHandler):
    def __init__(
            self, remote_uuid: str, hoard: HoardContents, repos_to_add_new_files: List[HoardRemote],
            fetch_new: bool, pathing: HoardPathing):
        super().__init__(remote_uuid, hoard)
        self.repos_to_add_new_files = repos_to_add_new_files
        self.fetch_new = fetch_new
        self.pathing = pathing

    def handle_local_only(self, diff: "FileMissingInHoard", out: StringIO):
        out.write(f"+{diff.hoard_file}\n")
        self.hoard.fsobjects.add_new_file(
            diff.hoard_file, diff.local_props,
            current_uuid=self.remote_uuid,
            repos_to_add_new_files=filter_accessible(self.pathing, self.repos_to_add_new_files, diff.hoard_file))

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
    def __init__(
            self, remote_uuid: str, hoard: HoardContents,
            repos_to_add_new_files: List[HoardRemote], pathing: HoardPathing):
        super().__init__(remote_uuid, hoard)
        self.repos_to_add_new_files = repos_to_add_new_files
        self.pathing = pathing

    def handle_local_only(self, diff: FileMissingInHoard, out: StringIO):
        out.write(f"<+{diff.hoard_file}\n")
        hoard_file = self.hoard.fsobjects.add_new_file(
            diff.hoard_file, diff.local_props,
            current_uuid=self.remote_uuid,
            repos_to_add_new_files=filter_accessible(self.pathing, self.repos_to_add_new_files, diff.hoard_file))
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


def _file_stats(props: HoardFileProps) -> str:
    a = props.by_status(FileStatus.AVAILABLE)
    g = props.by_status(FileStatus.GET)
    c = props.by_status(FileStatus.CLEANUP)
    res: List[str] = []
    if len(a) > 0:
        res.append(f'a:{len(a)}')
    if len(g) > 0:
        res.append(f'g:{len(g)}')
    if len(c) > 0:
        res.append(f'c:{len(c)}')
    return " ".join(res)


class HoardCommand(object):
    def __init__(self, path: str):
        self.hoardpath = path

    def _contents_filename(self, remote_uuid):
        return os.path.join(self.hoardpath, f"{remote_uuid}.contents")

    def _remotes_names(self) -> Dict[str, str]:
        logging.info(f"Reading config...")
        config = self.config()
        return config.remotes.names_map()

    def config(self, create: bool = False) -> HoardConfig:
        config_file = os.path.join(self.hoardpath, CONFIG_FILE)
        return HoardConfig.load(config_file, create)

    def paths(self) -> HoardPaths:
        paths_file = os.path.join(self.hoardpath, PATHS_FILE)
        return HoardPaths.load(paths_file)

    def init(self):
        logging.info(f"Reading or creating config...")
        self.config(True)
        return "DONE"

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

            for diff in compare_local_to_hoard(current_contents, hoard, self.config(), self.paths()):
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

    def remotes(self, show_paths: bool = False):
        logging.info(f"Reading config in {self.hoardpath}...")
        config = self.config()

        with StringIO() as out:
            out.write(f"{len(config.remotes)} total remotes.\n")
            for remote in config.remotes.all():
                name_prefix = f"[{remote.name}] " if remote.name != "INVALID" else ""
                exact_path = f" in {self.paths()[remote.uuid].find()}" if show_paths else ""

                out.write(f"  {name_prefix}{remote.uuid} ({remote.type.value}){exact_path}\n")
            out.write("Mounts:\n")

            mounts = dict((m, list(rs)) for m, rs in groupby(config.remotes.all(), lambda r: r.mounted_at))
            for mount, remotes in mounts.items():
                out.write(f"  {mount} -> {', '.join([remote.name for remote in remotes])}\n")
            out.write("DONE\n")
            return out.getvalue()

    def refresh(self, remote: str, ignore_epoch: bool = False):
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
                remote_uuid, hoard, repos_to_add_new_files,
                config.remotes[remote_uuid].fetch_new, pathing=HoardPathing(config, self.paths()))
        elif remote_type == CaveType.BACKUP:
            remote_op_handler: DiffHandler = BackupDiffHandler(remote_uuid, hoard)
        elif remote_type == CaveType.INCOMING:
            remote_op_handler: DiffHandler = IncomingDiffHandler(
                remote_uuid, hoard,
                repos_to_add_new_files, pathing=HoardPathing(config, self.paths()))
        else:
            raise ValueError(f"FIXME unsupported remote type: {remote_type}")

        current_contents = self._fetch_repo_contents(remote_uuid)

        if not ignore_epoch and hoard.epoch(remote_uuid) >= current_contents.config.epoch:
            return (
                f"Skipping update as past epoch {current_contents.config.epoch} "
                f"is not after hoard epoch {hoard.epoch(remote_uuid)}")

        remote_doc = config.remotes[remote_uuid]
        if remote_doc is None or remote_doc.mounted_at is None:
            raise ValueError(f"remote_doc {remote_uuid} is not mounted!")

        logging.info("Merging local changes...")
        with StringIO() as out:
            for diff in compare_local_to_hoard(current_contents, hoard, config, self.paths()):
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

            logging.info(f"Updating epoch of {remote_uuid} to {current_contents.config.epoch}")
            hoard.set_epoch(remote_uuid, current_contents.config.epoch)

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
        _ = self.config()  # validate hoard is available

        if not os.path.isdir(to_path):
            return f"Cave dir {to_path} to create does not exist!"

        cave_cmd = RepoCommand(path=to_path)
        cave_cmd.init()
        cave_cmd.refresh()

        self.add_remote(to_path, name=name, mount_point=mount_at, fetch_new=fetch_new)
        return f"DONE"

    def ls(
            self, selected_path: Optional[str] = None, depth: int = None,
            skip_folders: bool = False, show_remotes: int = False):
        logging.info(f"Loading hoard TOML...")
        hoard = HoardContents.load(self._hoard_contents_filename())

        if depth is None:
            depth = sys.maxsize if selected_path is None else 1

        if selected_path is None:
            selected_path = "/"

        pathing = HoardPathing(self.config(), self.paths())

        logging.info(f"Listing files...")
        with StringIO() as out:
            file: Optional[HoardFile]
            folder: Optional[HoardDir]
            for folder, file in hoard.fsobjects.tree.walk(selected_path, depth=depth):
                if file is not None:
                    stats = _file_stats(file.props)
                    out.write(f"{file.fullname} = {stats}\n")

                if not skip_folders and folder is not None:
                    if show_remotes:
                        repos_availability = sorted(
                            pathing.repos_availability(folder.fullname).items(),
                            key=lambda v: v[0].name)  # sort by repo name
                        remotes_stats = ", ".join([f"({repo.name}:{path})" for repo, path in repos_availability])

                        appendix = f' => {remotes_stats}' if remotes_stats != '' else ''
                        out.write(f"{folder.fullname}{appendix}\n")
                    else:
                        out.write(f"{folder.fullname}\n")

            out.write("DONE")
            return out.getvalue()

    def move(self, from_path: str, to_path: str, no_files: bool = True):
        assert no_files, "NOT IMPLEMENTED"
        config = self.config()
        pathing = HoardPathing(config, self.paths())

        from_path_in_hoard = pathing.in_hoard(from_path)
        to_path_in_hoard = pathing.in_hoard(to_path)

        repos_to_move: List[HoardRemote] = []
        for remote in config.remotes.all():
            if pathlib.Path(remote.mounted_at).is_relative_to(from_path):
                # mounted_at is a subfolder of from_path
                logging.info(f"{remote.name} will be moved as {remote.mounted_at} is subfolder of {from_path}")
                repos_to_move.append(remote)
                continue

            path_in_remote = from_path_in_hoard.at_local(remote.uuid)
            if path_in_remote is None:
                logging.info(f"Remote {remote.uuid} does not map path {from_path_in_hoard.as_posix()} ... skipping")
                continue

            assert path_in_remote.as_posix() != "."

            logging.warning(
                f"Remote {remote.uuid} contains path {from_path_in_hoard.as_posix()}"
                f" as inner {path_in_remote}, which requires moving files.")
            return f"Can't move {from_path} to {to_path}, requires moving files in {remote.name}:{path_in_remote.as_posix()}.\n"

        if len(repos_to_move) == 0:
            return f"No repos to move!"

        logging.info(f"Loading hoard TOML...")
        hoard = HoardContents.load(self._hoard_contents_filename())
        logging.info(f"Loaded hoard TOML.")

        with StringIO() as out:
            out.write("Moving files and folders:\n")
            for orig_file, props in hoard.fsobjects.files.copy().items():
                file_path = pathlib.Path(orig_file)
                if file_path.is_relative_to(from_path):
                    rel_path = file_path.relative_to(from_path)
                    logging.info(f"Relative file path to move: {rel_path}")
                    new_path = pathlib.Path(to_path).joinpath(rel_path).as_posix()

                    out.write(f"{orig_file}=>{new_path}\n")
                    hoard.fsobjects.move_file(orig_file, new_path, props)

            for orig_dir, props in hoard.fsobjects.dirs.copy().items():
                dir_path = pathlib.Path(orig_dir)
                if dir_path.is_relative_to(from_path):
                    rel_path = dir_path.relative_to(from_path)
                    logging.info(f"Relative dir path to move: {rel_path}")
                    new_path = pathlib.Path(to_path).joinpath(rel_path).as_posix()

                    out.write(f"{orig_dir}=>{new_path}\n")
                    hoard.fsobjects.move_dir(orig_dir, new_path, props)

            logging.info(f"Moving {', '.join(r.name for r in repos_to_move)}.")
            out.write(f"Moving {len(repos_to_move)} repos:\n")
            for remote in repos_to_move:
                relative_repo_mounted_at = pathlib.Path(remote.mounted_at).relative_to(from_path)
                logging.info(f"[{remote.name} is mounted {relative_repo_mounted_at.as_posix()} rel. to {from_path}]")
                final_mount_path = pathlib.Path(to_path_in_hoard.as_posix()).joinpath(relative_repo_mounted_at)
                logging.info(f"re-mounting it to {final_mount_path}")

                out.write(f"[{remote.name}] {remote.mounted_at} => {final_mount_path.as_posix()}\n")
                remote.mount_at(final_mount_path.as_posix())

            logging.info(f"Writing hoard...")
            hoard.write()

            logging.info("Writing config...")
            config.write()

            out.write("DONE")
            return out.getvalue()

    def sync_contents(self, repo: Optional[str] = None):
        config = self.config()

        logging.info(f"Loading hoard TOML...")
        hoard = HoardContents.load(self._hoard_contents_filename())

        repo_uuids: List[str] = \
            [self._resolve_remote_uuid(repo)] if repo is not None else [r.uuid for r in config.remotes.all()]

        pathing = HoardPathing(config, self.paths())

        with StringIO() as out:
            logging.info("try getting all requested files, per repo")
            for repo_uuid in repo_uuids:
                print(f"fetching for {config.remotes[repo_uuid].name}")
                out.write(f"{repo_uuid}:\n")
                with alive_bar(len(hoard.fsobjects.files)) as bar:  # fixme do it over only files to copy!
                    for hoard_file, hoard_props in sorted(hoard.fsobjects.files.items()):
                        bar()
                        goal_status = hoard_props.status(repo_uuid)
                        if goal_status == FileStatus.GET:
                            hoard_filepath = pathing.in_hoard(hoard_file)
                            local_filepath = hoard_filepath.at_local(repo_uuid)
                            logging.debug(f"restoring {hoard_file} to {local_filepath.as_posix()}...")

                            success, fullpath = _restore(hoard_filepath, repo_uuid, hoard_props, config)
                            if success:
                                out.write(f"+ {local_filepath.as_posix()}\n")
                                hoard_props.mark_available(repo_uuid)
                            else:
                                out.write(f"E {local_filepath.as_posix()}\n")
                                logging.error("error restoring file!")

            logging.info("Writing hoard file...")
            hoard.write()

            logging.info("try cleaning unneeded files, per repo")
            for repo_uuid in repo_uuids:
                print(f"cleaning repo {config.remotes[repo_uuid].name}")
                out.write(f"{repo_uuid}:\n")

                with alive_bar(len(hoard.fsobjects.files)) as bar:  # fixme do it over only files to cleanup!
                    for hoard_file, hoard_props in sorted(hoard.fsobjects.files.items()):
                        bar()
                        goal_status = hoard_props.status(repo_uuid)

                        if goal_status == FileStatus.CLEANUP:
                            to_be_got = hoard_props.by_status(FileStatus.GET)

                            local_path = pathing.in_hoard(hoard_file).at_local(repo_uuid)
                            local_file_to_delete = local_path.as_posix()

                            if len(to_be_got) == 0:
                                logging.info("file doesn't need to be copied anymore, cleaning")
                                hoard_props.remove_status(repo_uuid)

                                logging.info(f"deleting {local_path.on_device_path()}...")

                                try:
                                    os.remove(local_path.on_device_path())
                                    out.write(f"c {local_file_to_delete}\n")
                                    logging.info("file deleted!")
                                except FileNotFoundError as e:
                                    out.write(f"E {local_file_to_delete}\n")
                                    logging.error(e)
                            else:
                                logging.info(f"file needs to be copied in {len(to_be_got)} places, retaining")
                                out.write(f"~ {local_file_to_delete}\n")

            logging.info("Writing hoard file...")
            hoard.write()

            out.write("DONE")
            return out.getvalue()

    def enable_content(self, repo: str, path: str = ""):
        config = self.config()

        logging.info(f"Loading hoard TOML...")
        hoard = HoardContents.load(self._hoard_contents_filename())

        repo_uuid = self._resolve_remote_uuid(repo)
        repo_mounted_at = config.remotes[repo_uuid].mounted_at
        logging.info(f"repo {repo} mounted at {repo_mounted_at}")

        pathing = HoardPathing(config, self.paths())

        already_enabled = [FileStatus.AVAILABLE, FileStatus.GET]
        with StringIO() as out:
            for hoard_file, hoard_props in hoard.fsobjects.files.items():
                local_file = pathing.in_hoard(hoard_file).at_local(repo_uuid)
                if local_file is None:
                    continue
                if not pathlib.Path(local_file.as_posix()).is_relative_to(path):
                    logging.info(f"file not in {path}: {local_file.as_posix()}")
                    continue
                if hoard_props.status(repo_uuid) not in already_enabled:
                    logging.info(f"enabling file {hoard_file} on {repo_uuid}")
                    hoard_props.mark_to_get(repo_uuid)
                    out.write(f"+{hoard_file}\n")

            logging.info("Writing hoard file...")
            hoard.write()

            out.write("DONE")
            return out.getvalue()


def _restore(
        hoard_file: HoardPathing.HoardPath, local_uuid: str, hoard_props: HoardFileProps,
        config: HoardConfig) -> (bool, str):
    fullpath_to_restore = hoard_file.at_local(local_uuid).on_device_path()
    logging.info(f"Restoring hoard file {hoard_file} to {fullpath_to_restore}.")

    candidates = hoard_props.by_status(FileStatus.AVAILABLE) + hoard_props.by_status(FileStatus.CLEANUP)

    for remote_uuid in candidates:
        if config.remotes[remote_uuid] is None:
            logging.warning(f"remote {remote_uuid} is invalid, won't try to restore")
            continue

        file_fullpath = hoard_file.at_local(remote_uuid).on_device_path()

        if not os.path.isfile(file_fullpath):
            logging.error(f"File {file_fullpath} does not exist, but is needed for restore from {remote_uuid}!")
            continue

        remote_hash = fast_hash(file_fullpath)
        if hoard_props.fasthash != remote_hash:
            logging.error(
                f"File {file_fullpath} with fast hash {remote_hash}!={hoard_props.fasthash} that was expected.")
            continue

        dirpath, _ = os.path.split(fullpath_to_restore)
        logging.info(f"making necessary folders to restore: {dirpath}")
        os.makedirs(dirpath, exist_ok=True)

        logging.info(f"Copying {file_fullpath} to {fullpath_to_restore}")
        try:
            shutil.copy2(file_fullpath, fullpath_to_restore)
            return True, fullpath_to_restore
        except shutil.SameFileError as e:
            logging.error(f"Are same file: {e}")

    logging.error(f"Did not find any available for {hoard_file}!")
    return False, fullpath_to_restore


def compare_local_to_hoard(local: Contents, hoard: HoardContents, config: HoardConfig, paths: HoardPaths) -> Generator[
    Diff, None, None]:
    pathing = HoardPathing(config, paths)

    print("Comparing current files to hoard:")
    with alive_bar(len(local.fsobjects.files)) as bar:
        for current_file, props in local.fsobjects.files.copy().items():
            bar()

            curr_file_hoard_path = pathing.in_local(current_file, local.config.uuid).at_hoard()
            if curr_file_hoard_path.as_posix() not in hoard.fsobjects.files.keys():
                logging.info(f"local file not in hoard: {curr_file_hoard_path.as_posix()}")
                yield FileMissingInHoard(current_file, curr_file_hoard_path.as_posix(), props)
            elif is_same_file(
                    local.fsobjects.files[current_file],
                    hoard.fsobjects.files[curr_file_hoard_path.as_posix()]):
                logging.info(f"same in hoard {current_file}!")
                yield FileIsSame(current_file, curr_file_hoard_path.as_posix(), props, hoard.fsobjects.files[
                    curr_file_hoard_path.as_posix()])
            else:
                logging.info(f"file changes {current_file}")
                yield FileContentsDiffer(
                    current_file,
                    curr_file_hoard_path.as_posix(), props, hoard.fsobjects.files[curr_file_hoard_path.as_posix()])

    print("Comparing hoard to current files")
    with alive_bar(len(hoard.fsobjects.files)) as bar:
        for hoard_file, props in hoard.fsobjects.files.copy().items():
            bar()
            curr_file_path_in_local = pathing.in_hoard(hoard_file).at_local(local.config.uuid)
            if curr_file_path_in_local is None:
                continue  # hoard file is not in the mounted location

            if curr_file_path_in_local.as_posix() not in local.fsobjects.files.keys():
                yield FileMissingInLocal(curr_file_path_in_local.as_posix(), hoard_file, props)
            # else file is there, which is handled above

    for current_dir, props in local.fsobjects.dirs.copy().items():
        curr_dir_hoard_path = pathing.in_local(current_dir, local.config.uuid).at_hoard()
        if curr_dir_hoard_path.as_posix() not in hoard.fsobjects.dirs.keys():
            logging.info(f"new dir found: {current_dir}")
            yield DirMissingInHoard(current_dir, curr_dir_hoard_path.as_posix())
        else:
            yield DirIsSame(current_dir, curr_dir_hoard_path.as_posix())

    for hoard_dir, props in hoard.fsobjects.dirs.copy().items():
        curr_dir_path_in_local = pathing.in_hoard(hoard_dir).at_local(local.config.uuid)
        if curr_dir_path_in_local is None:
            continue  # hoard dir is not in the mounted location
        if curr_dir_path_in_local.as_posix() not in hoard.fsobjects.dirs.keys():
            logging.info(f"missing dir found in hoard: {hoard_dir}")
            yield DirMissingInLocal(curr_dir_path_in_local.as_posix(), hoard_dir)
        else:
            pass  # existing dirs are handled above
