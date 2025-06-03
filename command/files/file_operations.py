import logging
import os
import sys
from io import StringIO
from os import PathLike
from typing import Optional, Dict, Callable

import aioshutil

from command.fast_path import FastPosixPath
from command.pathing import HoardPathing
from command.pending_file_ops import HACK_create_from_hoard_props
from command.tree_operations import remove_from_current_tree, add_to_current_tree_file_obj
from config import HoardPaths
from contents.hoard import HoardContents, MovesAndCopies
from contents.hoard_props import HoardFileProps, HoardFileStatus
from hashing import fast_hash_async
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

            async def copy_or_get_file(self, hoard_file: str, hoard_props: HoardFileProps) -> Optional[str]:
                try:
                    return await copy_or_get(
                        hoard, pathing, moves_and_copies,
                        pathing.in_hoard(FastPosixPath(hoard_file)),
                        repo_uuid, HACK_create_from_hoard_props(hoard_props))
                finally:
                    self.current_size += hoard_props.size
                    bar(to_mb(self.current_size) - self.current_size_mb)
                    self.current_size_mb = to_mb(self.current_size)

        copier = Copier()
        outputs = [await copier.copy_or_get_file(hoard_path, hoard_props) for hoard_path, hoard_props in files_to_fetch]

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

    # first candidate is the current device - copy if we can
    for expanded_path in moves_and_copies.get_existing_paths_in_uuid_expanded(restore_to_uuid, file_obj.file_id):
        candidate_path_on_device = pathing.in_hoard(expanded_path).at_local(restore_to_uuid).on_device_path()
        logging.debug(f"Preparing to copy local %s to %s...", expanded_path, candidate_path_on_device)

        success, restored_file = await _restore_file(candidate_path_on_device, fullpath_to_restore, file_obj)
        if success:
            add_to_current_tree_file_obj(hoard, restore_to_uuid, hoard_path.as_pure_path.as_posix(), file_obj)
            return f"+ {local_path_to_restore}\n"
            # return f"LOCAL_COPY {hoard_path.as_pure_path}\n" fixme

    remote_candidates: Dict[str, list[FastPosixPath]] = dict(moves_and_copies.get_remote_copies_expanded(
        restore_to_uuid, file_obj.file_id))
    for candidate_uuid in sorted(remote_candidates.keys(), key=sort_by_speed_then_latency(pathing._paths)):
        for expanded_path in remote_candidates[candidate_uuid]:
            candidate_path_on_device = pathing.in_hoard(expanded_path).at_local(candidate_uuid).on_device_path()
            logging.debug(f"Preparing to copy remote %s to %s...", expanded_path, candidate_path_on_device)
            success, restored_file = await _restore_file(candidate_path_on_device, fullpath_to_restore, file_obj)
            if success:
                add_to_current_tree_file_obj(hoard, restore_to_uuid, hoard_path.as_pure_path.as_posix(), file_obj)
                return f"+ {local_path_to_restore}\n"
                # return f"REMOTE_COPY {hoard_path.as_pure_path}\n" fixme

    logging.error(f"Did not succeed restoring {hoard_path}!")
    return f"E {local_path_to_restore}\n"


def _cleanup_files_in_repo(
        moves_and_copies: MovesAndCopies, hoard: HoardContents, repo_uuid: str, pathing: HoardPathing,
        out: StringIO, progress_bar):
    files_to_cleanup = sorted(hoard.fsobjects.to_cleanup(repo_uuid))
    with progress_bar(to_mb(sum(f[1].size for f in files_to_cleanup)), unit="MB", title="Cleaning files") as bar:
        for hoard_file, hoard_props in files_to_cleanup:
            assert isinstance(hoard_props, HoardFileProps)

            assert hoard_props.get_status(repo_uuid) == HoardFileStatus.CLEANUP

            local_path = pathing.in_hoard(hoard_file).at_local(repo_uuid)

            file_obj = FileObject.create(hoard_props.fasthash, hoard_props.size, None)

            where_is_needed = dict(moves_and_copies.whereis_needed(file_obj.id))
            logging.debug(f"Needed in {len(where_is_needed)} repos.")

            where_is_needed_but_not_in_repo = dict(
                (uuid, paths) for uuid, paths in where_is_needed.items()
                if len(moves_and_copies.get_existing_paths_in_uuid(uuid, file_obj.id)) == 0)
            logging.debug(f"Needed in {len(where_is_needed)} repos that would prevent this to be deleted.")

            if len(where_is_needed_but_not_in_repo) == 0:
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
            else:
                assert len(where_is_needed_but_not_in_repo) > 0
                repo_names = list(
                    hoard.hoard_config.remotes[uuid].name for uuid in where_is_needed_but_not_in_repo.keys())
                out.write(
                    f"NEEDS_MORE_COPIES ({len(where_is_needed_but_not_in_repo)}) {repo_names} {local_path.as_pure_path.as_posix()}\n")

                logging.info(
                    f"file {hoard_file} needs to be copied to {len(where_is_needed_but_not_in_repo)} repos, skipping")
            bar(to_mb(hoard_props.size))


def sort_by_speed_then_latency(paths: HoardPaths) -> Callable[[str], int]:
    def comparator(uuid: str) -> int:
        cave_path = paths[uuid]
        return cave_path.prioritize_speed_over_latency() if cave_path is not None else sys.maxsize

    return comparator


async def _restore_file(fetch_fullpath: PathLike, to_fullpath: PathLike, file_obj: FileObject) -> (bool, str):
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

    logging.info(f"Copying {fetch_fullpath} to {to_fullpath}")
    try:
        await aioshutil.copy2(fetch_fullpath, to_fullpath)
        return True, to_fullpath
    except aioshutil.SameFileError as e:
        logging.error(f"Are same file: {e}")
    except PermissionError as e:
        logging.error(f"Permission error: {e}")
    return False, None
