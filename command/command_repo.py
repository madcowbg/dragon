import logging
import pathlib
from io import StringIO
from typing import List, Optional

from command.comparison_repo import FileDeleted, FileMoved, FileAdded, FileModified, FileIsSame, find_repo_changes, \
    FilesystemState, compute_changes_from_diffs
from command.fast_path import FastPosixPath
from command.hoard_ignore import HoardIgnore, DEFAULT_IGNORE_GLOBS
from command.repo import ProspectiveRepo
from contents.repo_props import RepoFileStatus
from daemon.daemon import run_daemon
from exceptions import MissingRepo, MissingRepoContents
from resolve_uuid import load_config, resolve_remote_uuid, load_paths
from task_logging import TaskLogger, PythonLoggingTaskLogger
from util import format_size, format_percent, safe_hex


class RepoCommand(object):
    def __init__(self, path: str = ".", name: Optional[str] = None):
        if name is not None:  # assume path is a hoard, and the name to use is the provided
            logging.info(f"Finding repo path by hoard assumed at {path}...")

            config = load_config(hoardpath=path, create=False)
            remote_uuid = resolve_remote_uuid(config, name)
            logging.info(f"resolved {name} to assumed uuid {remote_uuid}")

            paths = load_paths(hoardpath=path)
            cave_path = paths[remote_uuid]
            if cave_path is None:
                raise ValueError(f"No repo with uuid {remote_uuid}.")

            print(f"Resolved repo {name} to path {cave_path.find()}.")
            path = cave_path.find()

        self.repo = ProspectiveRepo(pathlib.Path(path).absolute().as_posix())

    def current_uuid(self) -> str:
        return self.repo.open_repo().current_uuid

    def init(self):
        logging.info(f"Creating repo in {self.repo.path}")
        self.repo.init()

        return f"Repo initialized at {self.repo.path}"

    async def refresh(self, show_details: bool = True, task_logger: TaskLogger = PythonLoggingTaskLogger()):
        """ Refreshes the cache of the current hoard folder """
        connected_repo = self.repo.open_repo().connect(False)
        if connected_repo is None:
            return f"No initialized repo in {self.repo.path}!"

        current_uuid = connected_repo.current_uuid
        try:
            contents = connected_repo.open_contents(is_readonly=False)
            first_refresh = False
        except MissingRepoContents as e:
            task_logger.warning("Repo contents missing, creating!")
            first_refresh = True
            contents = connected_repo.create_contents(current_uuid)

        task_logger.info(f"Refreshing uuid {current_uuid}{', is first refresh' if first_refresh else ''}")
        add_new_with_status = RepoFileStatus.PRESENT

        hoard_ignore = HoardIgnore(DEFAULT_IGNORE_GLOBS)

        with contents:
            task_logger.info("Start updating...")
            task_logger.info("Reading filesystem state...")
            state = FilesystemState(contents, task_logger)
            await state.read_state_from_filesystem(hoard_ignore, self.repo.path, task_logger)

            with StringIO() as out:
                if show_details:
                    async for change in compute_changes_from_diffs(state.diffs(), self.repo.path, add_new_with_status):
                        if isinstance(change, FileIsSame):
                            pass
                        elif isinstance(change, FileDeleted):
                            out.write(f"{change.details} {change.missing_relpath}\n")
                        elif isinstance(change, FileMoved):
                            out.write(f"MOVED {change.missing_relpath.as_posix()} TO {change.moved_to_relpath.as_posix()}\n")
                        elif isinstance(change, FileAdded):
                            out.write(f"{change.requested_status.value.upper()}_FILE {change.relpath.as_posix()}\n")
                        elif isinstance(change, FileModified):
                            out.write(f"MODIFIED_FILE {change.relpath.as_posix()}\n")
                        else:
                            raise TypeError(f"Unexpected change type {type(change)}")

                if len(out.getvalue()) == 0 and show_details:
                    out.write("NO CHANGES\n")

                out.write(f"old: {safe_hex(contents.fsobjects.root_id)}\n")
                # save modified as root
                contents.fsobjects.roots["REPO"].current = state.state_root_id
                out.write(f"current: {safe_hex(contents.fsobjects.root_id)}\n")

                contents.config.end_updating()

                out.write(f"Refresh done!")
                return out.getvalue()

    def status_index(self, show_files: bool = True, show_dates: bool = True, show_epoch = True):  # fixme remove show_epoch
        remote_uuid = self.current_uuid()

        logging.info(f"Reading repo {self.repo.path}...")
        with self.repo.open_repo().connect(False).open_contents(is_readonly=True) as contents:
            logging.info(f"Read repo!")

            with StringIO() as out:
                if show_files:
                    for file_or_dir, props in contents.fsobjects.all_status():
                        out.write(f"{file_or_dir.as_posix()}: {props.last_status.value}{'' if not show_epoch else f' @ -1'}\n")
                    out.write("--- SUMMARY ---\n")

                stats = contents.fsobjects.stats_existing
                out.writelines([
                    f"Result for local [{safe_hex(contents.fsobjects.root_id)}]:\n",
                    f"Max size: {format_size(contents.config.max_size)}\n"
                    f"UUID: {remote_uuid}\n",
                    f"Last updated on {contents.config.updated}\n" if show_dates else "",
                    f"  # files = {stats.num_files} of size {format_size(stats.total_size)}\n"])
                return out.getvalue()

    async def status(self):
        try:
            connected_repo = self.repo.open_repo().connect(False)
            current_uuid = connected_repo.current_uuid
        except MissingRepo:
            return f"Repo is not initialized at {self.repo.path}"

        try:
            contents = connected_repo.open_contents(is_readonly=True)
        except MissingRepoContents:
            return f"Repo {current_uuid} contents have not been refreshed yet!"

        files_same: List[FastPosixPath] = []
        files_new: List[FastPosixPath] = []
        files_mod: List[FastPosixPath] = []
        files_moved: List[FastPosixPath] = []
        files_del: List[FastPosixPath] = []

        hoard_ignore = HoardIgnore(DEFAULT_IGNORE_GLOBS)

        with contents:
            print("Calculating diffs between repo and filesystem...")
            async for change in find_repo_changes(self.repo.path, contents, hoard_ignore, RepoFileStatus.PRESENT):
                if isinstance(change, FileIsSame):
                    files_same.append(change.relpath)
                elif isinstance(change, FileDeleted):
                    files_del.append(change.missing_relpath)
                elif isinstance(change, FileMoved):
                    files_moved.append(change.moved_to_relpath)
                elif isinstance(change, FileAdded):
                    files_new.append(change.relpath)
                elif isinstance(change, FileModified):
                    files_mod.append(change.relpath)
                else:
                    raise TypeError(f"Unexpected change type {type(change)}")

            # assert len(files_same) + len(files_del) + len(files_mod) == contents.fsobjects.num_files
            # assert len(dir_new) + len(dir_same) == contents.fsobjects.num_dirs

            files_current = len(files_new) + len(files_same) + len(files_mod)
            with StringIO() as out:
                stats = contents.fsobjects.stats_existing
                out.write(
                    f"{current_uuid} [{safe_hex(contents.fsobjects.root_id)}]:\n"
                    f"files:\n"
                    f"    same: {len(files_same)} ({format_percent(len(files_same) / files_current)})\n"
                    f"     mod: {len(files_mod)} ({format_percent(len(files_mod) / files_current)})\n"
                    f"     new: {len(files_new)} ({format_percent(len(files_new) / files_current)})\n"
                    f"   moved: {len(files_moved)} ({format_percent(len(files_moved) / files_current)})\n"
                    f" current: {files_current}\n"
                    f" in repo: {stats.num_files}\n"
                    f" deleted: {len(files_del)} ({format_percent(len(files_del) / stats.num_files)})\n")

                return out.getvalue()

    async def watch(self, assume_current: bool = False):
        await run_daemon(self.repo.path, assume_current)
