import logging
import os
import shutil
import sys
import traceback
from io import StringIO
from os import PathLike
from typing import List, Optional, Tuple, Dict, Iterable, Callable

import aioshutil

from command.content_prefs import ContentPrefs
from command.fast_path import FastPosixPath
from command.pathing import HoardPathing
from command.pending_file_ops import HACK_create_from_hoard_props
from command.tree_operations import add_to_current_tree, remove_from_current_tree, add_to_current_tree_file_obj
from config import HoardConfig, HoardPaths
from contents.hoard import HoardContents, MovesAndCopies
from contents.hoard_props import HoardFileProps, HoardFileStatus
from hashing import fast_hash_async
from lmdb_storage.file_object import FileObject
from util import to_mb, format_size


async def _fetch_files_in_repo(
        content_prefs: ContentPrefs, moves_and_copies: MovesAndCopies, hoard: HoardContents, repo_uuid: str,
        pathing: HoardPathing,
        out: StringIO, progress_bar):
    files_to_fetch = sorted(hoard.fsobjects.to_fetch(repo_uuid))
    total_size = sum(f[1].size for f in files_to_fetch)

    with progress_bar(to_mb(total_size), unit="MB", title="Fetching files") as bar:
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
                        raise NotImplementedError()
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
                        raise NotImplementedError()
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

                        return await self._execute_get(hoard_file, hoard_props)
                finally:
                    self.current_size += hoard_props.size
                    bar(to_mb(self.current_size) - self.current_size_mb)
                    self.current_size_mb = to_mb(self.current_size)

            async def _execute_get(self, hoard_file: str, hoard_props: HoardFileProps):
                hoard_filepath = pathing.in_hoard(FastPosixPath(hoard_file))
                local_filepath = hoard_filepath.at_local(repo_uuid)
                logging.debug(f"restoring {hoard_file} to {local_filepath}...")
                success, fullpath = await DEPRECATED_restore_from_another_repo(hoard_filepath, repo_uuid, hoard_props,
                                                                               pathing._paths)
                if success:
                    add_to_current_tree(hoard, repo_uuid, hoard_file, hoard_props)

                    return f"+ {local_filepath}\n"
                else:
                    logging.error("error restoring file!")
                    return f"E {local_filepath}\n"

        if True:
            outputs = [
                await copy_or_get(
                    hoard, pathing, moves_and_copies,
                    pathing.in_hoard(FastPosixPath(hoard_path)),
                    repo_uuid, HACK_create_from_hoard_props(hoard_props))
                for hoard_path, hoard_props in files_to_fetch]
        else:
            copier = Copier()
            outputs = [await copier.copy_or_get_file(hoard_path, hoard_props) for hoard_path, hoard_props in
                       files_to_fetch]

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


async def _cleanup_files_in_repo(
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


async def DEPRECATED_restore_from_another_repo(
        hoard_file: HoardPathing.HoardPath, uuid_to_restore_to: str,
        hoard_props: HoardFileProps, paths: HoardPaths) -> (bool, str):
    fullpath_to_restore = hoard_file.at_local(uuid_to_restore_to).on_device_path()
    logging.info(f"Restoring hoard file {hoard_file} to {fullpath_to_restore}.")

    candidate_repos = hoard_props.by_status(HoardFileStatus.AVAILABLE) + hoard_props.by_status(HoardFileStatus.CLEANUP)
    candidate_repos = sorted(candidate_repos, key=sort_by_speed_then_latency(paths))
    for remote_uuid in candidate_repos:
        logging.info(
            f"Remote: {remote_uuid} "
            f"[{paths[remote_uuid].speed.value}: {paths[remote_uuid].latency.value}] for {hoard_file}")

    fullpath_to_restore = hoard_file.at_local(uuid_to_restore_to).on_device_path()

    candidates_by_preference = list(sorted(
        (remote_uuid, hoard_file.at_local(remote_uuid).on_device_path()) for remote_uuid in candidate_repos))

    success, restored_path = await restore_file_from_candidates(
        HACK_create_from_hoard_props(hoard_props), fullpath_to_restore, [v for _, v in candidates_by_preference])
    if not success:
        logging.error(f"Did not succeed restoring {hoard_file}!")
    return success, restored_path


def sort_by_speed_then_latency(paths: HoardPaths) -> Callable[[str], int]:
    def comparator(uuid: str) -> int:
        cave_path = paths[uuid]
        return cave_path.prioritize_speed_over_latency() if cave_path is not None else sys.maxsize

    return comparator


async def restore_file_from_candidates(
        file_obj: FileObject,
        fullpath_to_restore: FastPosixPath, candidates: Iterable[FastPosixPath]):
    for file_fullpath in candidates:
        success, restored_file = await _restore_file(file_fullpath, fullpath_to_restore, file_obj)
        if success:
            return True, restored_file

    return False, fullpath_to_restore


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

        success, restored_file = await _restore_file(other_file_path, to_fullpath,
                                                     HACK_create_from_hoard_props(hoard_props))
        if success:
            logging.info(f"Restore successful!")
            return True, restored_file
        else:
            logging.info(f"Restore FAILED.")

    logging.info("Trying to fully restore instead of move as a last resort.")
    return DEPRECATED_restore_from_another_repo(local_filepath.at_hoard(), repo_uuid, hoard_props, pathing._paths)
