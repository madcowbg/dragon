import logging
import os
import sys
from io import StringIO
from os import PathLike
from typing import Optional, Dict, Callable

import aioshutil

from command.fast_path import FastPosixPath
from command.pathing import HoardPathing
from config import HoardPaths
from contents.hoard import HoardContents, MovesAndCopies, HACK_create_from_hoard_props
from contents.hoard_props import HoardFileProps, HoardFileStatus
from hashing import fast_hash_async
from lmdb_storage.deferred_operations import add_to_current_tree_file_obj, remove_from_current_tree
from lmdb_storage.file_object import FileObject
from util import to_mb, format_size


async def _fetch_files_in_repo(
        moves_and_copies: MovesAndCopies, hoard: HoardContents, repo_uuid: str,
        pathing: HoardPathing, out: StringIO, progress_bar):
    files_to_fetch = sorted(hoard.fsobjects.to_fetch(repo_uuid))
    total_size = sum(f[1].size for f in files_to_fetch)

    with progress_bar(to_mb(total_size), unit="MB", title="Fetching files") as bar:
        class Copier:
            def __init__(self):
                self.current_size = 0
                self.current_size_mb = 0

            async def copy_or_get_file(self, hoard_file: str, file_obj: FileObject) -> Optional[str]:
                try:
                    return await copy_or_get(
                        hoard, pathing, moves_and_copies,
                        pathing.in_hoard(FastPosixPath(hoard_file)),
                        repo_uuid, file_obj)
                finally:
                    self.current_size += file_obj.size
                    bar(to_mb(self.current_size) - self.current_size_mb)
                    self.current_size_mb = to_mb(self.current_size)

        copier = Copier()
        outputs = [await copier.copy_or_get_file(hoard_path, file_obj) for hoard_path, file_obj in files_to_fetch]

        for line in outputs:
            if line is not None:
                out.write(line)


async def copy_or_get(
        hoard: HoardContents, pathing: HoardPathing, moves_and_copies: MovesAndCopies,
        hoard_path: HoardPathing.HoardPath, restore_to_uuid: str, file_obj: FileObject) -> str | None:
    local_path_to_restore = hoard_path.at_local(restore_to_uuid).as_pure_path
    fullpath_to_restore = hoard_path.at_local(restore_to_uuid).on_device_path()

    if file_obj.size > (5 * (1 << 30)):  # >5G
        logging.warning(f"Copying large file {format_size(file_obj.size)}: {hoard_path}")

    cleanup_local_paths = moves_and_copies.whereis_cleanup(restore_to_uuid, file_obj.file_id)
    logging.debug(f"Found %s paths in %s that can be moved from", len(cleanup_local_paths), restore_to_uuid)

    for move_path in cleanup_local_paths:
        candidate_move_path_on_device = pathing.in_hoard(move_path).at_local(restore_to_uuid).on_device_path()

        logging.debug(f"Trying copy from %s.", candidate_move_path_on_device)
        success, restored_file = await _restore_file(
            candidate_move_path_on_device, fullpath_to_restore, file_obj, move=True)
        if success:
            add_to_current_tree_file_obj(hoard, restore_to_uuid, hoard_path.as_pure_path.as_posix(), file_obj)
            # should be safe to make it out of date until the deferred ops are cleared, as we check data when copying
            remove_from_current_tree(hoard, restore_to_uuid, move_path.as_posix(), file_obj)
            return f"LOCAL_MOVE {local_path_to_restore}\n"

    # first candidate is the current device - copy if we can
    for expanded_path in moves_and_copies.get_existing_paths_in_uuid_expanded(restore_to_uuid, file_obj.file_id):
        candidate_copy_path_on_device = pathing.in_hoard(expanded_path).at_local(restore_to_uuid).on_device_path()
        logging.debug(f"Preparing to copy local %s to %s...", expanded_path, candidate_copy_path_on_device)

        success, restored_file = await _restore_file(
            candidate_copy_path_on_device, fullpath_to_restore, file_obj, move=False)
        if success:
            add_to_current_tree_file_obj(hoard, restore_to_uuid, hoard_path.as_pure_path.as_posix(), file_obj)
            return f"LOCAL_COPY {local_path_to_restore}\n"

    remote_candidates: Dict[str, list[FastPosixPath]] = dict(moves_and_copies.get_remote_copies_expanded(
        restore_to_uuid, file_obj.file_id))
    for candidate_uuid in sorted(remote_candidates.keys(), key=sort_by_speed_then_latency(pathing._paths)):
        for expanded_path in remote_candidates[candidate_uuid]:
            candidate_path_on_device = pathing.in_hoard(expanded_path).at_local(candidate_uuid).on_device_path()
            logging.debug(f"Preparing to copy remote %s to %s...", expanded_path, candidate_path_on_device)
            success, restored_file = await _restore_file(
                candidate_path_on_device, fullpath_to_restore, file_obj, move=False)
            if success:
                add_to_current_tree_file_obj(hoard, restore_to_uuid, hoard_path.as_pure_path.as_posix(), file_obj)
                return f"REMOTE_COPY [{hoard.remote_name(candidate_uuid)}] {local_path_to_restore}\n"

    logging.error(f"Did not succeed restoring {hoard_path}!")
    return f"ERROR_RESTORING {local_path_to_restore}\n"


def _cleanup_files_in_repo(
        moves_and_copies: MovesAndCopies, hoard: HoardContents, repo_uuid: str, pathing: HoardPathing,
        out: StringIO, progress_bar):
    files_to_cleanup = sorted(hoard.fsobjects.to_cleanup(repo_uuid))
    with progress_bar(to_mb(sum(f[1].size for f in files_to_cleanup)), unit="MB", title="Cleaning files") as bar:
        for hoard_file, file_obj in files_to_cleanup:
            assert isinstance(file_obj, FileObject)

            local_path = pathing.in_hoard(hoard_file).at_local(repo_uuid)

            where_is_needed = dict(moves_and_copies.whereis_needed(file_obj.id))
            logging.debug(f"Needed in {len(where_is_needed)} repos.")

            where_is_needed_but_not_in_repo = dict(
                (uuid, paths) for uuid, paths in where_is_needed.items()
                if len(moves_and_copies.get_existing_paths_in_uuid(uuid, file_obj.id)) == 0)
            logging.debug(f"Needed in {len(where_is_needed)} repos that would prevent this to be deleted.")

            if len(where_is_needed_but_not_in_repo) == 0:
                logging.info("file doesn't need to be copied anymore, cleaning")
                remove_from_current_tree(hoard, repo_uuid, hoard_file.as_posix(), file_obj)

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
            else:
                assert len(where_is_needed_but_not_in_repo) > 0
                repo_names = list(
                    hoard.hoard_config.remotes[uuid].name for uuid in where_is_needed_but_not_in_repo.keys())
                out.write(
                    f"NEEDS_MORE_COPIES ({len(where_is_needed_but_not_in_repo)}) {repo_names} {local_path.as_pure_path.as_posix()}\n")

                logging.info(
                    f"file {hoard_file} needs to be copied to {len(where_is_needed_but_not_in_repo)} repos, skipping")
            bar(to_mb(file_obj.size))


def sort_by_speed_then_latency(paths: HoardPaths) -> Callable[[str], int]:
    def comparator(uuid: str) -> int:
        cave_path = paths[uuid]
        return cave_path.prioritize_speed_over_latency() if cave_path is not None else sys.maxsize

    return comparator


async def _restore_file(
        fetch_fullpath: PathLike, to_fullpath: PathLike, file_obj: FileObject, move: bool) -> (bool, str):
    if not os.path.isfile(fetch_fullpath):
        logging.error(f"File {fetch_fullpath} does not exist, but is needed for restore!")
        return False, None

    remote_hash = await fast_hash_async(fetch_fullpath)
    if file_obj.fasthash != remote_hash:
        logging.error(
            f"File {fetch_fullpath} with fast hash {remote_hash}!={file_obj.fasthash} that was expected.")
        return False, None

    dirpath, _ = os.path.split(to_fullpath)
    logging.info(f"making necessary folders to restore: {dirpath}")
    os.makedirs(dirpath, exist_ok=True)

    if move:
        logging.info(f"Moving {fetch_fullpath} to {to_fullpath}")
        try:
            await aioshutil.move(fetch_fullpath, to_fullpath)
            return True, to_fullpath
        except aioshutil.SameFileError as e:
            logging.error(f"Are same file: {e}")
        except PermissionError as e:
            logging.error(f"Permission error: {e}")
    else:
        logging.info(f"Copying {fetch_fullpath} to {to_fullpath}")
        try:
            await aioshutil.copy2(fetch_fullpath, to_fullpath)
            return True, to_fullpath
        except aioshutil.SameFileError as e:
            logging.error(f"Are same file: {e}")
        except PermissionError as e:
            logging.error(f"Permission error: {e}")
    return False, None
