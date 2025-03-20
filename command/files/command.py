import logging
import os
from io import StringIO
from typing import Optional, List, Dict, Iterable

import aioshutil
from alive_progress import alive_bar

from command.contents.command import clean_dangling_files
from command.hoard import Hoard
from command.pathing import HoardPathing
from config import HoardConfig
from contents.hoard import HoardContents
from contents.props import HoardFileProps, FileStatus
from hashing import fast_hash_async
from resolve_uuid import resolve_remote_uuid
from util import to_mb, run_async_in_parallel

type FileOp = GetFile | CopyFile | CleanupFile


class GetFile:
    def __init__(self, hoard_file: str, hoard_props: HoardFileProps):
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props


class CopyFile:
    def __init__(self, hoard_file: str, hoard_props: HoardFileProps):
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props


class CleanupFile:
    def __init__(self, hoard_file: str, hoard_props: HoardFileProps):
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props


def _get_pending_operations(hoard: HoardContents, repo_uuid: str) -> Iterable[FileOp]:
    for hoard_file, hoard_props in hoard.fsobjects.with_pending(repo_uuid):
        goal_status = hoard_props.get_status(repo_uuid)
        if goal_status == FileStatus.GET:
            yield GetFile(hoard_file, hoard_props)
        elif goal_status == FileStatus.COPY:
            yield CopyFile(hoard_file, hoard_props)
        elif goal_status == FileStatus.CLEANUP:
            yield CleanupFile(hoard_file, hoard_props)
        else:
            raise ValueError(f"File {hoard_file} has no pending ops, yet was selected as one that has.")


class HoardCommandFiles:
    def __init__(self, hoard: Hoard):
        self.hoard = hoard

    def pending(self, repo: Optional[str] = None):
        config = self.hoard.config()

        repo_uuids: List[str] = [resolve_remote_uuid(config, repo)] \
            if repo is not None else [r.uuid for r in config.remotes.all()]

        logging.info(f"Loading hoard contents...")
        with HoardContents.load(self.hoard.hoard_contents_filename()) as hoard:
            with StringIO() as out:
                for repo_uuid in repo_uuids:
                    logging.info(f"Iterating over pending ops in {repo_uuid}")
                    out.write(f"{config.remotes[repo_uuid].name}:\n")
                    for op in _get_pending_operations(hoard, repo_uuid):
                        num_available = len(op.hoard_props.by_status(FileStatus.AVAILABLE))
                        if isinstance(op, GetFile):
                            out.write(f"TO_GET (from {num_available}) {op.hoard_file}\n")
                        elif isinstance(op, CopyFile):
                            out.write(f"TO_COPY (from {num_available}+?) {op.hoard_file}\n")
                        elif isinstance(op, CleanupFile):
                            out.write(f"TO_CLEANUP (is in {num_available}) {op.hoard_file}\n")
                        else:
                            raise ValueError(f"Unhandled op type: {type(op)}")

                out.write("DONE")
                return out.getvalue()

    def sync_contents(self, repo: Optional[str] = None):
        config = self.hoard.config()
        pathing = HoardPathing(config, self.hoard.paths())

        repo_uuids: List[str] = [resolve_remote_uuid(config, repo)] \
            if repo is not None else [r.uuid for r in config.remotes.all()]

        logging.info(f"Loading hoard contents...")
        with HoardContents.load(self.hoard.hoard_contents_filename()) as hoard:
            with StringIO() as out:
                logging.info("try getting all requested files, per repo")

                logging.info("Finding files that need copy, for easy lookup")
                files_to_copy = _find_files_to_copy(hoard)

                for repo_uuid in repo_uuids:
                    print(f"fetching for {config.remotes[repo_uuid].name}")
                    out.write(f"{repo_uuid}:\n")

                    _fetch_files_in_repo(hoard, repo_uuid, pathing, files_to_copy, out)

                logging.info("Writing hoard file...")
                hoard.write()

                logging.info("Finding files that need copy - will not cleanup them!")
                files_to_copy = _find_files_to_copy(hoard)
                logging.info(f"Found {len(files_to_copy)} hashes to copy, won't cleanup them.")

                logging.info("try cleaning unneeded files, per repo")
                for repo_uuid in repo_uuids:
                    print(f"cleaning repo {config.remotes[repo_uuid].name}")
                    out.write(f"{repo_uuid}:\n")

                    _cleanup_files_in_repo(hoard, repo_uuid, pathing, files_to_copy, out)

                clean_dangling_files(hoard, out)

                out.write("DONE")
                return out.getvalue()


def _fetch_files_in_repo(
        hoard: HoardContents, repo_uuid: str, pathing: HoardPathing,
        files_requiring_copy: Dict[str, List[str]], out: StringIO):
    files_to_fetch = sorted(hoard.fsobjects.to_fetch(repo_uuid))
    with alive_bar(to_mb(sum(f[1].size for f in files_to_fetch)), unit="MB") as bar:
        async def copy_or_get_file(hoard_file: str, hoard_props: HoardFileProps) -> Optional[str]:
            try:
                assert isinstance(hoard_props, HoardFileProps)

                goal_status = hoard_props.get_status(repo_uuid)
                assert goal_status != FileStatus.AVAILABLE
                assert goal_status != FileStatus.CLEANUP
                assert goal_status != FileStatus.UNKNOWN

                if goal_status == FileStatus.COPY:
                    candidates_to_copy = files_requiring_copy.get(hoard_props.fasthash, [])
                    logging.info(f"# of candidates to copy: {len(candidates_to_copy)}")

                    local_filepath = pathing.in_hoard(hoard_file).at_local(repo_uuid)

                    success, fullpath = await _restore_from_copy(
                        repo_uuid, local_filepath, hoard_props,
                        hoard, candidates_to_copy, pathing)
                    if success:
                        hoard_props.mark_available(repo_uuid)
                        return f"c+ {local_filepath.as_posix()}\n"
                    else:
                        logging.error("error restoring file from local copy!")
                        return f"E {local_filepath.as_posix()}\n"
                else:
                    assert goal_status == FileStatus.GET, f"Unexpected status {goal_status.value}"

                    hoard_filepath = pathing.in_hoard(hoard_file)
                    local_filepath = hoard_filepath.at_local(repo_uuid)
                    logging.debug(f"restoring {hoard_file} to {local_filepath.as_posix()}...")

                    success, fullpath = await _restore_from_another_repo(
                        hoard_filepath, repo_uuid, hoard_props, pathing._config)
                    if success:
                        hoard_props.mark_available(repo_uuid)
                        return f"+ {local_filepath.as_posix()}\n"
                    else:
                        logging.error("error restoring file!")
                        return f"E {local_filepath.as_posix()}\n"
            finally:
                bar(to_mb(hoard_props.size))

        outputs = run_async_in_parallel(files_to_fetch, copy_or_get_file)

        for line in outputs:
            if line is not None:
                out.write(line)


def _cleanup_files_in_repo(
        hoard: HoardContents, repo_uuid: str, pathing: HoardPathing, files_requiring_copy: List[str], out: StringIO):
    files_to_cleanup = sorted(hoard.fsobjects.to_cleanup(repo_uuid))
    with alive_bar(to_mb(sum(f[1].size for f in files_to_cleanup)), unit="MB") as bar:
        for hoard_file, hoard_props in files_to_cleanup:
            assert isinstance(hoard_props, HoardFileProps)

            goal_status = hoard_props.get_status(repo_uuid)

            assert goal_status != FileStatus.AVAILABLE
            assert goal_status != FileStatus.GET
            assert goal_status != FileStatus.UNKNOWN

            if goal_status == FileStatus.CLEANUP:
                to_be_got = hoard_props.by_status(FileStatus.GET)

                local_path = pathing.in_hoard(hoard_file).at_local(repo_uuid)
                local_file_to_delete = local_path.as_posix()

                if hoard_props.fasthash in files_requiring_copy:
                    logging.info(f"file with fasthash {hoard_props.fasthash} to be copied, retaining")
                    out.write(f"~h {local_file_to_delete}\n")
                elif len(to_be_got) == 0:
                    logging.info("file doesn't need to be copied anymore, cleaning")
                    hoard_props.remove_status(repo_uuid)

                    logging.info(f"deleting {local_path.on_device_path()}...")

                    try:
                        os.remove(local_path.on_device_path())
                        out.write(f"d {local_file_to_delete}\n")
                        logging.info("file deleted!")
                    except FileNotFoundError as e:
                        out.write(f"E {local_file_to_delete}\n")
                        logging.error(e)
                else:
                    logging.info(f"file needs to be copied in {len(to_be_got)} places, retaining")
                    out.write(f"~ {local_file_to_delete}\n")
            else:
                raise ValueError(f"Unexpected status {goal_status.value}")
            bar(to_mb(hoard_props.size))


def _find_files_to_copy(hoard: HoardContents) -> Dict[str, List[str]]:
    fasthashes_to_copy = [
        props.fasthash for filepath, props in hoard.fsobjects
        if isinstance(props, HoardFileProps) and len(props.by_status(FileStatus.COPY)) > 0]

    files_to_copy: Dict[str, List[str]] = dict((h, []) for h in fasthashes_to_copy)
    for filepath, props in hoard.fsobjects:
        if isinstance(props, HoardFileProps) and props.fasthash in fasthashes_to_copy:
            files_to_copy[props.fasthash].append(filepath)

    return files_to_copy


async def _restore_from_another_repo(
        hoard_file: HoardPathing.HoardPath, uuid_to_restore_to: str, hoard_props: HoardFileProps,
        config: HoardConfig) -> (bool, str):
    fullpath_to_restore = hoard_file.at_local(uuid_to_restore_to).on_device_path()
    logging.info(f"Restoring hoard file {hoard_file} to {fullpath_to_restore}.")

    candidates = hoard_props.by_status(FileStatus.AVAILABLE) + hoard_props.by_status(FileStatus.CLEANUP)

    for remote_uuid in candidates:
        if config.remotes[remote_uuid] is None:
            logging.warning(f"remote {remote_uuid} is invalid, won't try to restore")
            continue

        file_fullpath = hoard_file.at_local(remote_uuid).on_device_path()

        success, restored_file = await _restore_file(file_fullpath, fullpath_to_restore, hoard_props)
        if success:
            return True, restored_file

    logging.error(f"Did not find any available for {hoard_file.as_posix()}!")
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


async def _restore_from_copy(
        repo_uuid: str, local_filepath: HoardPathing.LocalPath, hoard_props: HoardFileProps,
        hoard: HoardContents, candidates_to_copy: List[str], pathing: HoardPathing) -> (bool, str):
    to_fullpath = local_filepath.on_device_path()
    print(f"Restoring to {to_fullpath}")
    for candidate_file in candidates_to_copy:
        other_props = hoard.fsobjects[candidate_file]
        if other_props.get_status(repo_uuid) != FileStatus.AVAILABLE:  # file is not available here
            continue

        other_file_path = pathing.in_hoard(candidate_file).at_local(repo_uuid).on_device_path()
        print(f"Restoring from {other_file_path} to {to_fullpath}.")

        success, restored_file = await _restore_file(other_file_path, to_fullpath, hoard_props)
        if success:
            logging.info(f"Restore successful!")
            return True, restored_file
        else:
            logging.info(f"Restore FAILED.")
    return False, None
