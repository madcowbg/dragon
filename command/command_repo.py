import logging
import pathlib
from io import StringIO
from pathlib import PurePosixPath
from typing import List, Optional

from command.comparison_repo import FileDeleted, FileMoved, FileAdded, FileModified, DirAdded, DirRemoved, FileIsSame, \
    DirIsSame, find_repo_changes, RepoChange
from command.hoard_ignore import HoardIgnore, DEFAULT_IGNORE_GLOBS
from command.repo import ProspectiveRepo
from contents.repo import RepoContents
from contents.repo_props import RepoFileStatus
from exceptions import MissingRepo, MissingRepoContents
from resolve_uuid import load_config, resolve_remote_uuid, load_paths
from util import format_size, format_percent


def _apply_repo_change_to_contents(
        change: RepoChange, contents: RepoContents, show_details: bool, out: StringIO) -> None:
    print_maybe = (lambda line: out.write(line + "\n")) if show_details else (lambda line: None)

    if isinstance(change, FileIsSame):
        pass
    elif isinstance(change, FileDeleted):
        print_maybe(f"{change.details} {change.missing_relpath}")
        contents.fsobjects.mark_removed(PurePosixPath(change.missing_relpath))
    elif isinstance(change, FileMoved):
        contents.fsobjects.mark_moved(
            change.missing_relpath, change.moved_to_relpath,
            size=change.size, mtime=change.mtime, fasthash=change.moved_file_hash)

        print_maybe(f"MOVED {change.missing_relpath.as_posix()} TO {change.moved_to_relpath.as_posix()}")
    elif isinstance(change, FileAdded):
        contents.fsobjects.add_file(
            change.relpath, size=change.size, mtime=change.mtime, fasthash=change.fasthash,
            status=change.requested_status)

        print_maybe(f"{change.requested_status.value.upper()}_FILE {change.relpath.as_posix()}")
    elif isinstance(change, FileModified):
        contents.fsobjects.add_file(
            change.relpath, size=change.size, mtime=change.mtime, fasthash=change.fasthash,
            status=RepoFileStatus.MODIFIED)

        print_maybe(f"MODIFIED_FILE {change.relpath.as_posix()}")
    elif isinstance(change, DirIsSame):
        pass
    elif isinstance(change, DirAdded):
        contents.fsobjects.add_dir(change.relpath, status=RepoFileStatus.ADDED)
        print_maybe(f"ADDED_DIR {change.relpath}")
    elif isinstance(change, DirRemoved):
        print_maybe("REMOVED_DIR {diff.dirpath}")
        contents.fsobjects.mark_removed(PurePosixPath(change.dirpath))
    else:
        raise TypeError(f"Unexpected change type {type(change)}")


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

    def refresh(self, skip_integrity_checks: bool = False, show_details: bool = True):
        """ Refreshes the cache of the current hoard folder """
        connected_repo = self.repo.open_repo().connect(False)
        if connected_repo is None:
            return f"No initialized repo in {self.repo.path}!"

        current_uuid = connected_repo.current_uuid
        try:
            contents = connected_repo.open_contents(is_readonly=False)
            first_refresh = False
        except MissingRepoContents as e:
            logging.warning("Repo contents missing, creating!")
            first_refresh = True
            contents = connected_repo.create_contents(current_uuid)

        logging.info(f"Refreshing uuid {current_uuid}{', is first refresh' if first_refresh else ''}")
        add_new_with_status = RepoFileStatus.ADDED if not first_refresh else RepoFileStatus.PRESENT

        hoard_ignore = HoardIgnore(DEFAULT_IGNORE_GLOBS)

        with contents:
            logging.info("Start updating, setting is_dirty to TRUE")
            contents.config.start_updating()

            logging.info(f"Bumped epoch to {contents.config.epoch}")

            with StringIO() as out:
                for change in find_repo_changes(
                        self.repo.path, contents, hoard_ignore, add_new_with_status, skip_integrity_checks):
                    _apply_repo_change_to_contents(change, contents, show_details, out)

                logging.info("Ends updating, setting is_dirty to FALSE")
                contents.config.end_updating()
                assert not contents.config.is_dirty

                if len(out.getvalue()) == 0 and show_details:
                    out.write("NO CHANGES\n")

                out.write(f"Refresh done!")
                return out.getvalue()

    def status_index(self, show_files: bool = True, show_dates: bool = True):
        remote_uuid = self.current_uuid()

        logging.info(f"Reading repo {self.repo.path}...")
        with self.repo.open_repo().connect(False).open_contents(is_readonly=True) as contents:
            logging.info(f"Read repo!")

            with StringIO() as out:
                if show_files:
                    for file_or_dir, props in contents.fsobjects.all_status():
                        out.write(f"{file_or_dir.as_posix()}: {props.last_status.value} @ {props.last_update_epoch}\n")
                    out.write("--- SUMMARY ---\n")

                stats = contents.fsobjects.stats_existing
                out.writelines([
                    f"Result for local\n",
                    f"Max size: {format_size(contents.config.max_size)}\n"
                    f"UUID: {remote_uuid}\n",
                    f"Last updated on {contents.config.updated}\n" if show_dates else "",
                    f"  # files = {stats.num_files} of size {format_size(stats.total_size)}\n",
                    f"  # dirs  = {stats.num_dirs}\n", ])
                return out.getvalue()

    def status(self, skip_integrity_checks: bool = False):
        try:
            connected_repo = self.repo.open_repo().connect(False)
            current_uuid = connected_repo.current_uuid
        except MissingRepo:
            return f"Repo is not initialized at {self.repo.path}"

        try:
            contents = connected_repo.open_contents(is_readonly=True)
        except MissingRepoContents:
            return f"Repo {current_uuid} contents have not been refreshed yet!"

        files_same: List[PurePosixPath] = []
        files_new: List[PurePosixPath] = []
        files_mod: List[PurePosixPath] = []
        files_moved: List[PurePosixPath] = []
        files_del: List[PurePosixPath] = []

        dir_new = []
        dir_same = []
        dir_deleted = []

        hoard_ignore = HoardIgnore(DEFAULT_IGNORE_GLOBS)

        with contents:
            print("Calculating diffs between repo and filesystem...")
            for change in find_repo_changes(
                    self.repo.path, contents, hoard_ignore,
                    RepoFileStatus.ADDED, skip_integrity_checks):
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
                elif isinstance(change, DirIsSame):
                    dir_same.append(change.relpath)
                elif isinstance(change, DirAdded):
                    dir_new.append(change.relpath)
                elif isinstance(change, DirRemoved):
                    dir_deleted.append(change.dirpath)
                else:
                    raise TypeError(f"Unexpected change type {type(change)}")

            # assert len(files_same) + len(files_del) + len(files_mod) == contents.fsobjects.num_files
            # assert len(dir_new) + len(dir_same) == contents.fsobjects.num_dirs

            files_current = len(files_new) + len(files_same) + len(files_mod)
            dirs_current = len(dir_same) + len(dir_new)
            with StringIO() as out:
                stats = contents.fsobjects.stats_existing
                out.write(
                    f"{current_uuid}:\n"
                    f"files:\n"
                    f"    same: {len(files_same)} ({format_percent(len(files_same) / files_current)})\n"
                    f"     mod: {len(files_mod)} ({format_percent(len(files_mod) / files_current)})\n"
                    f"     new: {len(files_new)} ({format_percent(len(files_new) / files_current)})\n"
                    f"   moved: {len(files_moved)} ({format_percent(len(files_moved) / files_current)})\n"
                    f" current: {files_current}\n"
                    f" in repo: {stats.num_files}\n"
                    f" deleted: {len(files_del)} ({format_percent(len(files_del) / stats.num_files)})\n"
                    f"dirs:\n"
                    f"    same: {len(dir_same)}\n"
                    f"     new: {len(dir_new)} ({format_percent(len(dir_new) / dirs_current)})\n"
                    f" current: {dirs_current}\n"
                    f" in repo: {stats.num_dirs}\n"
                    f" deleted: {len(dir_deleted)} ({format_percent(len(dir_deleted) / max(1, stats.num_dirs))})\n")

                return out.getvalue()
