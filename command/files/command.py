import logging
import os
import shutil
import sys
import traceback
from io import StringIO
from command.fast_path import FastPosixPath
from typing import Optional, List, Dict

import aioshutil
from alive_progress import alive_bar

from command.contents.command import clean_dangling_files
from command.hoard import Hoard
from command.pathing import HoardPathing
from command.pending_file_ops import get_pending_operations, CopyFile, GetFile, CleanupFile, MoveFile
from config import HoardConfig, HoardPaths
from contents.hoard import HoardContents
from contents.hoard_props import HoardFileStatus, HoardFileProps
from hashing import fast_hash_async
from resolve_uuid import resolve_remote_uuid
from util import to_mb, run_async_in_parallel, format_size, run_in_separate_loop


class HoardCommandFiles:
    def __init__(self, hoard: Hoard):
        self.hoard = hoard

    async def pending(self, repo: Optional[str] = None):
        config = self.hoard.config()

        repo_uuids: List[str] = [resolve_remote_uuid(config, repo)] \
            if repo is not None else [r.uuid for r in config.remotes.all()]

        logging.info(f"Loading hoard contents...")
        async with self.hoard.open_contents(create_missing=False, is_readonly=True) as hoard:
            with StringIO() as out:
                for repo_uuid in repo_uuids:
                    logging.info(f"Iterating over pending ops in {repo_uuid}")
                    out.write(f"{config.remotes[repo_uuid].name}:\n")

                    repos_containing_what_this_one_needs: Dict[str, int] = dict()
                    for op in get_pending_operations(hoard, repo_uuid):
                        num_available = op.hoard_props.by_status(HoardFileStatus.AVAILABLE)
                        if isinstance(op, GetFile):
                            out.write(f"TO_GET (from {len(num_available)}) {op.hoard_file.as_posix()}\n")
                            for repo in num_available:
                                repos_containing_what_this_one_needs[repo] = \
                                    repos_containing_what_this_one_needs.get(repo, 0) + 1
                        elif isinstance(op, CopyFile):
                            out.write(f"TO_COPY (from {len(num_available)}+?) {op.hoard_file.as_posix()}\n")
                            for repo in num_available:
                                repos_containing_what_this_one_needs[repo] = \
                                    repos_containing_what_this_one_needs.get(repo, 0) + 1
                        elif isinstance(op, MoveFile):
                            out.write(f"TO_MOVE {op.hoard_file.as_posix()} from {op.old_hoard_file}\n")
                        elif isinstance(op, CleanupFile):
                            out.write(f"TO_CLEANUP (is in {len(num_available)}) {op.hoard_file.as_posix()}\n")
                        else:
                            raise ValueError(f"Unhandled op type: {type(op)}")
                    nc = sorted(map(
                        lambda uc: (config.remotes[uc[0]].name, uc[1]),  # uuid, count -> name, count
                        repos_containing_what_this_one_needs.items()))
                    for name, count in nc:
                        out.write(f" {name} has {count} files\n")
                out.write("DONE")
                return out.getvalue()

    async def push(self, repo: Optional[str] = None, all: bool = False):
        config = self.hoard.config()
        pathing = HoardPathing(config, self.hoard.paths())
        if all:
            if repo is not None:
                return f"Error: can't use --all and --repo={repo} at the same time."
            repo_uuids: List[str] = [r.uuid for r in config.remotes.all()]
        else:
            if repo is None:
                return f"Error: Need either --repo=REPO or --all."
            repo_uuids = [resolve_remote_uuid(config, repo)]

        logging.info(f"Loading hoard contents...")
        async with self.hoard.open_contents(False, is_readonly=False) as hoard:
            with StringIO() as out:
                logging.info("try getting all requested files, per repo")

                logging.info("Finding files that need copy, for easy lookup")
                files_to_copy = _find_files_to_copy(hoard)

                for repo_uuid in repo_uuids:
                    logging.info(f"fetching for {config.remotes[repo_uuid].name}")
                    out.write(f"{config.remotes[repo_uuid].name}:\n")

                    _fetch_files_in_repo(hoard, repo_uuid, pathing, files_to_copy, out)

                logging.info("Finding files that need copy - will not cleanup them!")
                files_to_copy = _find_files_to_copy(hoard)
                logging.info(f"Found {len(files_to_copy)} hashes to copy, won't cleanup them.")

                logging.info("try cleaning unneeded files, per repo")
                for repo_uuid in repo_uuids:
                    logging.info(f"cleaning repo {config.remotes[repo_uuid].name}")
                    out.write(f"{config.remotes[repo_uuid].name}:\n")

                    _cleanup_files_in_repo(hoard, repo_uuid, pathing, files_to_copy, out)

                clean_dangling_files(hoard, out)

                out.write("DONE")
                return out.getvalue()


def _fetch_files_in_repo(
        hoard: HoardContents, repo_uuid: str, pathing: HoardPathing,
        files_requiring_copy: Dict[str, List[str]], out: StringIO):
    files_to_fetch = sorted(hoard.fsobjects.to_fetch(repo_uuid))
    total_size = sum(f[1].size for f in files_to_fetch)

    with alive_bar(to_mb(total_size), unit="MB", title="Fetching files") as bar:
        def _execute_get(hoard_file: str, hoard_props: HoardFileProps):
            hoard_filepath = pathing.in_hoard(FastPosixPath(hoard_file))
            local_filepath = hoard_filepath.at_local(repo_uuid)
            logging.debug(f"restoring {hoard_file} to {local_filepath}...")
            success, fullpath = _restore_from_another_repo(
                hoard_filepath, repo_uuid, hoard_props, pathing._config, pathing._paths)
            if success:
                hoard_props.mark_available(repo_uuid)
                return f"+ {local_filepath}\n"
            else:
                logging.error("error restoring file!")
                return f"E {local_filepath}\n"

        class Copier:
            def __init__(self):
                self.current_size = 0
                self.current_size_mb = 0

            def copy_or_get_file(self, hoard_file: str, hoard_props: HoardFileProps) -> Optional[str]:
                if hoard_props.size > (5 * (1 << 30)):  # >5G
                    logging.warning(f"Copying large file {format_size(hoard_props.size)}: {hoard_file}")
                try:
                    assert isinstance(hoard_props, HoardFileProps)

                    goal_status = hoard_props.get_status(repo_uuid)
                    assert goal_status != HoardFileStatus.AVAILABLE
                    assert goal_status != HoardFileStatus.CLEANUP
                    assert goal_status != HoardFileStatus.UNKNOWN

                    if goal_status == HoardFileStatus.COPY:
                        candidates_to_copy = files_requiring_copy.get(hoard_props.fasthash, [])
                        logging.info(f"# of candidates to copy: {len(candidates_to_copy)}")

                        local_filepath = pathing.in_hoard(FastPosixPath(hoard_file)).at_local(repo_uuid)

                        success, fullpath = _restore_from_copy(
                            repo_uuid, local_filepath, hoard_props,
                            hoard, candidates_to_copy, pathing)
                        if success:
                            hoard_props.mark_available(repo_uuid)
                            return f"c+ {local_filepath}\n"
                        else:
                            logging.error("error restoring file from local copy!")
                            return f"E {local_filepath}\n"
                    elif goal_status == HoardFileStatus.MOVE:
                        to_be_moved_from = hoard_props.get_move_file(repo_uuid)

                        local_filepath_from = pathing.in_hoard(FastPosixPath(to_be_moved_from)).at_local(repo_uuid).on_device_path()
                        local_filepath = pathing.in_hoard(FastPosixPath(hoard_file)).at_local(repo_uuid).on_device_path()
                        try:
                            logging.info(f"Moving {local_filepath_from} to {local_filepath}")

                            dirpath, _ = os.path.split(local_filepath)
                            logging.info(f"making necessary folders to restore: {dirpath}")
                            os.makedirs(dirpath, exist_ok=True)

                            dest = shutil.move(local_filepath_from, local_filepath)
                            if dest is not None:
                                hoard_props.mark_available(repo_uuid)
                                return f"MOVED {to_be_moved_from} to {hoard_file}\n"
                            else:
                                return f"ERROR_MOVING {to_be_moved_from} to {hoard_file}\n"
                        except Exception as e:
                            traceback.print_exception(e)
                            logging.error(e)
                            return f"ERROR_MOVING [{e}] {to_be_moved_from} to {hoard_file}\n"
                    else:
                        assert goal_status == HoardFileStatus.GET, f"Unexpected status {goal_status.value}"

                        return _execute_get(hoard_file, hoard_props)
                finally:
                    self.current_size += hoard_props.size
                    bar(to_mb(self.current_size) - self.current_size_mb)
                    self.current_size_mb = to_mb(self.current_size)

        copier = Copier()
        outputs = [copier.copy_or_get_file(*fa) for fa in files_to_fetch]

        for line in outputs:
            if line is not None:
                out.write(line)


def _cleanup_files_in_repo(
        hoard: HoardContents, repo_uuid: str, pathing: HoardPathing, files_requiring_copy: Dict[str, any], out: StringIO):
    files_to_cleanup = sorted(hoard.fsobjects.to_cleanup(repo_uuid))
    with alive_bar(to_mb(sum(f[1].size for f in files_to_cleanup)), unit="MB", title="Cleaning files") as bar:
        for hoard_file, hoard_props in files_to_cleanup:
            assert isinstance(hoard_props, HoardFileProps)

            goal_status = hoard_props.get_status(repo_uuid)

            assert goal_status != HoardFileStatus.AVAILABLE
            assert goal_status != HoardFileStatus.GET
            assert goal_status != HoardFileStatus.UNKNOWN

            if goal_status == HoardFileStatus.CLEANUP:
                to_be_got = hoard_props.by_status(HoardFileStatus.GET)
                to_be_moved_to = hoard.fsobjects.where_to_move(repo_uuid, FastPosixPath(hoard_file))

                local_path = pathing.in_hoard(FastPosixPath(hoard_file)).at_local(repo_uuid)
                local_file_to_delete = local_path.as_pure_path.as_posix()

                if hoard_props.fasthash in files_requiring_copy:
                    logging.info(f"file with fasthash {hoard_props.fasthash} to be copied, retaining")
                    out.write(f"~h {local_file_to_delete}\n")
                elif len(to_be_got) > 0:
                    logging.info(f"file needs to be copied in {len(to_be_got)} places, retaining")
                    out.write(f"~ {local_file_to_delete}\n")
                elif len(to_be_moved_to) > 0:
                    logging.info(f"file needs to be moved to {to_be_moved_to}, retaining")
                    out.write(f"NEED_TO_BE_MOVED {local_file_to_delete}\n")
                else:
                    logging.info("file doesn't need to be copied anymore, cleaning")
                    hoard_props.remove_status(repo_uuid)

                    logging.info(f"deleting {local_path.on_device_path()}...")

                    try:
                        if os.path.exists(local_path.on_device_path()):
                            os.remove(local_path.on_device_path())

                        out.write(f"d {local_file_to_delete}\n")
                        logging.info("file deleted!")
                    except FileNotFoundError as e:
                        out.write(f"E {local_file_to_delete}\n")
                        logging.error(e)
            else:
                raise ValueError(f"Unexpected status {goal_status.value}")
            bar(to_mb(hoard_props.size))


def _find_files_to_copy(hoard: HoardContents) -> Dict[str, List[str]]:
    fasthashes_to_copy = [
        props.fasthash for filepath, props in hoard.fsobjects
        if isinstance(props, HoardFileProps) and len(props.by_status(HoardFileStatus.COPY)) > 0]

    files_to_copy: Dict[str, List[str]] = dict((h, []) for h in fasthashes_to_copy)
    for filepath, props in hoard.fsobjects:
        if isinstance(props, HoardFileProps) and props.fasthash in fasthashes_to_copy:
            files_to_copy[props.fasthash].append(filepath.as_posix())

    return files_to_copy


def _restore_from_another_repo(
        hoard_file: HoardPathing.HoardPath, uuid_to_restore_to: str, hoard_props: HoardFileProps,
        config: HoardConfig, paths: HoardPaths) -> (bool, str):
    fullpath_to_restore = hoard_file.at_local(uuid_to_restore_to).on_device_path()
    logging.info(f"Restoring hoard file {hoard_file} to {fullpath_to_restore}.")

    candidates = hoard_props.by_status(HoardFileStatus.AVAILABLE) + hoard_props.by_status(HoardFileStatus.CLEANUP)

    def sort_by_speed_then_latency(uuid: str) -> int:
        cave_path = paths[uuid]
        return cave_path.prioritize_speed_over_latency() if cave_path is not None else sys.maxsize

    for remote_uuid in sorted(candidates, key=sort_by_speed_then_latency):
        logging.info(
            f"Remote: {remote_uuid} "
            f"[{paths[remote_uuid].speed.value}: {paths[remote_uuid].latency.value}] for {hoard_file}")
        if config.remotes[remote_uuid] is None:
            logging.warning(f"remote {remote_uuid} is invalid, won't try to restore")
            continue

        file_fullpath = hoard_file.at_local(remote_uuid).on_device_path()

        success, restored_file = run_in_separate_loop(_restore_file(file_fullpath, fullpath_to_restore, hoard_props))
        if success:
            return True, restored_file

    logging.error(f"Did not find any available for {hoard_file}!")
    return False, fullpath_to_restore


async def _restore_file(fetch_fullpath: str, to_fullpath: str, hoard_props: HoardFileProps) -> (bool, str):
    if not os.path.isfile(fetch_fullpath):
        logging.error(f"File {fetch_fullpath} does not exist, but is needed for restore!")
        return False, None

    remote_hash = await fast_hash_async(fetch_fullpath)
    if hoard_props.fasthash != remote_hash:
        logging.error(
            f"File {fetch_fullpath} with fast hash {remote_hash}!={hoard_props.fasthash} that was expected.")
        return False, None

    dirpath, _ = os.path.split(to_fullpath)
    logging.info(f"making necessary folders to restore: {dirpath}")
    os.makedirs(dirpath, exist_ok=True)

    logging.info(f"Copying {fetch_fullpath} to {to_fullpath}")
    try:
        await aioshutil.copy2(fetch_fullpath, to_fullpath)
        return True, to_fullpath
    except aioshutil.SameFileError as e:
        logging.error(f"Are same file: {e}")


def _restore_from_copy(
        repo_uuid: str, local_filepath: HoardPathing.LocalPath, hoard_props: HoardFileProps,
        hoard: HoardContents, candidates_to_copy: List[str], pathing: HoardPathing) -> (bool, str):
    to_fullpath = local_filepath.on_device_path()
    print(f"Restoring to {to_fullpath}")
    for candidate_file in candidates_to_copy:
        other_props = hoard.fsobjects[FastPosixPath(candidate_file)]
        if other_props.get_status(repo_uuid) != HoardFileStatus.AVAILABLE:  # file is not available here
            logging.error("trying to restore from a file that is not available!")
            continue

        other_file_path = pathing.in_hoard(FastPosixPath(candidate_file)).at_local(repo_uuid).on_device_path()
        print(f"Restoring from {other_file_path} to {to_fullpath}.")

        success, restored_file = run_in_separate_loop(_restore_file(other_file_path, to_fullpath, hoard_props))
        if success:
            logging.info(f"Restore successful!")
            return True, restored_file
        else:
            logging.info(f"Restore FAILED.")
    return False, None
