import logging
import os
import shutil
import sys
import traceback
from io import StringIO
from os import PathLike
from typing import List, Optional

import aioshutil

from command.content_prefs import ContentPrefs
from command.fast_path import FastPosixPath
from command.pathing import HoardPathing
from command.tree_operations import add_to_current_tree, remove_from_current_tree
from config import HoardConfig, HoardPaths
from contents.hoard import HoardContents
from contents.hoard_props import HoardFileProps, HoardFileStatus
from hashing import fast_hash_async
from util import to_mb, format_size


async def _fetch_files_in_repo(
        content_prefs: ContentPrefs, hoard: HoardContents, repo_uuid: str, pathing: HoardPathing,
        out: StringIO, progress_bar):
    files_to_fetch = sorted(hoard.fsobjects.to_fetch(repo_uuid))
    total_size = sum(f[1].size for f in files_to_fetch)

    with progress_bar(to_mb(total_size), unit="MB", title="Fetching files") as bar:
        async def _execute_get(hoard_file: str, hoard_props: HoardFileProps):
            hoard_filepath = pathing.in_hoard(FastPosixPath(hoard_file))
            local_filepath = hoard_filepath.at_local(repo_uuid)
            logging.debug(f"restoring {hoard_file} to {local_filepath}...")
            success, fullpath = await _restore_from_another_repo(
                hoard_filepath, repo_uuid, hoard_props, pathing._config, pathing._paths)
            if success:
                add_to_current_tree(hoard, repo_uuid, hoard_file, hoard_props)

                return f"+ {local_filepath}\n"
            else:
                logging.error("error restoring file!")
                return f"E {local_filepath}\n"

        class Copier:
            def __init__(self):
                self.current_size = 0
                self.current_size_mb = 0

            async def copy_or_get_file(self, hoard_file: str, hoard_props: HoardFileProps) -> Optional[str]:
                if hoard_props.size > (5 * (1 << 30)):  # >5G
                    logging.warning(f"Copying large file {format_size(hoard_props.size)}: {hoard_file}")
                try:
                    assert isinstance(hoard_props, HoardFileProps)

                    goal_status = hoard_props.get_status(repo_uuid)
                    assert goal_status != HoardFileStatus.AVAILABLE
                    assert goal_status != HoardFileStatus.CLEANUP
                    assert goal_status != HoardFileStatus.UNKNOWN

                    if goal_status == HoardFileStatus.COPY:
                        candidates_to_copy = content_prefs.files_to_copy.get(hoard_props.fasthash, [])
                        logging.info(f"# of candidates to copy: {len(candidates_to_copy)}")

                        local_filepath = pathing.in_hoard(FastPosixPath(hoard_file)).at_local(repo_uuid)

                        success, fullpath = await _restore_from_copy(
                            repo_uuid, local_filepath, hoard_props,
                            hoard, candidates_to_copy, pathing)
                        if success:
                            add_to_current_tree(hoard, repo_uuid, hoard_file, hoard_props)
                            return f"c+ {local_filepath}\n"
                        else:
                            logging.error("error restoring file from local copy!")
                            return f"E {local_filepath}\n"
                    elif goal_status == HoardFileStatus.MOVE:
                        to_be_moved_from = hoard_props.get_move_file(repo_uuid)

                        local_filepath_from = pathing.in_hoard(FastPosixPath(to_be_moved_from)).at_local(
                            repo_uuid).on_device_path()
                        local_filepath = pathing.in_hoard(FastPosixPath(hoard_file)).at_local(
                            repo_uuid).on_device_path()
                        try:
                            logging.info(f"Moving {local_filepath_from} to {local_filepath}")

                            dirpath, _ = os.path.split(local_filepath)
                            logging.info(f"making necessary folders to restore: {dirpath}")
                            os.makedirs(dirpath, exist_ok=True)

                            dest = shutil.move(local_filepath_from, local_filepath)
                            if dest is not None:
                                add_to_current_tree(hoard, repo_uuid, hoard_file, hoard_props)
                                return f"MOVED {to_be_moved_from} to {hoard_file}\n"
                            else:
                                return f"ERROR_MOVING {to_be_moved_from} to {hoard_file}\n"
                        except Exception as e:
                            traceback.print_exception(e)
                            logging.error(e)
                            return f"ERROR_MOVING [{e}] {to_be_moved_from} to {hoard_file}\n"
                    else:
                        assert goal_status == HoardFileStatus.GET, f"Unexpected status {goal_status.value}"

                        return await _execute_get(hoard_file, hoard_props)
                finally:
                    self.current_size += hoard_props.size
                    bar(to_mb(self.current_size) - self.current_size_mb)
                    self.current_size_mb = to_mb(self.current_size)

        copier = Copier()
        outputs = [await copier.copy_or_get_file(*fa) for fa in files_to_fetch]

        for line in outputs:
            if line is not None:
                out.write(line)


async def _cleanup_files_in_repo(
        content_prefs: ContentPrefs, hoard: HoardContents, repo_uuid: str, pathing: HoardPathing,
        out: StringIO, progress_bar):
    files_to_cleanup = sorted(hoard.fsobjects.to_cleanup(repo_uuid))
    with progress_bar(to_mb(sum(f[1].size for f in files_to_cleanup)), unit="MB", title="Cleaning files") as bar:
        for hoard_file, hoard_props in files_to_cleanup:
            assert isinstance(hoard_props, HoardFileProps)

            assert hoard_props.get_status(repo_uuid) == HoardFileStatus.CLEANUP

            local_path = pathing.in_hoard(hoard_file).at_local(repo_uuid)
            if content_prefs.can_cleanup(hoard_file, hoard_props, repo_uuid, out):
                logging.info("file doesn't need to be copied anymore, cleaning")
                remove_from_current_tree(hoard, repo_uuid, hoard_file)

                logging.info(f"deleting {local_path.on_device_path()}...")

                try:
                    if os.path.exists(local_path.on_device_path()):
                        os.remove(local_path.on_device_path())

                    out.write(f"d {local_path.as_pure_path.as_posix()}\n")
                    logging.info("file deleted!")
                except FileNotFoundError as e:
                    out.write(f"E {local_path.as_pure_path.as_posix()}\n")
                    logging.error(e)
                except PermissionError as e:
                    out.write(f"PermissionError {local_path.as_pure_path.as_posix()}\n")
                    logging.error(e)

            bar(to_mb(hoard_props.size))


async def _restore_from_another_repo(
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

        success, restored_file = await _restore_file(file_fullpath, fullpath_to_restore, hoard_props)
        if success:
            return True, restored_file

    logging.error(f"Did not find any available for {hoard_file}!")
    return False, fullpath_to_restore


async def _restore_file(fetch_fullpath: PathLike, to_fullpath: PathLike, hoard_props: HoardFileProps) -> (bool, str):
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
    except PermissionError as e:
        logging.error(f"Permission error: {e}")
    return False, None


async def _restore_from_copy(
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

        success, restored_file = await _restore_file(other_file_path, to_fullpath, hoard_props)
        if success:
            logging.info(f"Restore successful!")
            return True, restored_file
        else:
            logging.info(f"Restore FAILED.")

    logging.info("Trying to fully restore instead of move as a last resort.")
    return _restore_from_another_repo(
        local_filepath.at_hoard(), repo_uuid, hoard_props, pathing._config, pathing._paths)
