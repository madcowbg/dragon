import logging
from typing import Dict, AsyncGenerator

from alive_progress import alive_it

from command.fast_path import FastPosixPath
from command.pathing import HoardPathing
from contents.hoard import HoardContents
from contents.hoard_props import HoardFileProps
from contents.repo import RepoContents
from contents.repo_props import FileDesc, RepoFileStatus
from contents_diff import Diff, DiffType


def is_same_file(current: FileDesc, hoard: HoardFileProps):
    if current.size != hoard.size:
        return False  # files differ by size

    if current.fasthash != hoard.fasthash:
        return False  # fast hash is different

    return True  # files are the same


async def compare_local_to_hoard(
        local: RepoContents, hoard: HoardContents, pathing: HoardPathing,
        progress_tool=alive_it) \
        -> AsyncGenerator[Diff]:
    logging.info("Load current objects")
    all_local_with_any_status: Dict[FastPosixPath, FileDesc] = \
        dict(s for s in progress_tool(local.fsobjects.all_status(), title="Load current objects",
                                      total=local.fsobjects.len_existing()))

    logging.info("Load hoard objects in folder")
    all_hoard_in_folder: Dict[FastPosixPath, HoardFileProps] = dict([
        s async for s in hoard.fsobjects.in_folder(pathing.mounted_at(local.config.uuid))])
    logging.info("Loaded all objects.")

    for current_path, props in progress_tool(all_local_with_any_status.items(), title="Current files vs. Hoard"):
        assert isinstance(props, FileDesc)

        current_file = current_path
        curr_file_hoard_path = pathing.in_local(current_file, local.config.uuid).at_hoard()
        hoard_props = all_hoard_in_folder.get(curr_file_hoard_path.as_pure_path, None)
        if hoard_props is None:
            logging.info(f"local file not in hoard: {curr_file_hoard_path}")
            added = props.last_status == RepoFileStatus.ADDED
            yield Diff(DiffType.FileOnlyInLocal, current_file, curr_file_hoard_path.as_pure_path, props, None, added)
        elif is_same_file(props, hoard_props):
            logging.info(f"same in hoard {current_file}!")
            yield Diff(DiffType.FileIsSame, current_file, curr_file_hoard_path.as_pure_path, props, hoard_props, None)
        else:
            logging.info(f"file changes {current_file}")
            yield Diff(
                DiffType.FileContentsDiffer, current_file, curr_file_hoard_path.as_pure_path, props, hoard_props, None)

    hoard_file: FastPosixPath
    for hoard_file, props in progress_tool(
            all_hoard_in_folder.items(),
            title="Hoard vs. Current files"):
        curr_path_in_local = pathing.in_hoard(hoard_file).at_local(local.config.uuid)
        assert curr_path_in_local is not None  # hoard file is not in the mounted location
        local_props: FileDesc | None = all_local_with_any_status.get(curr_path_in_local.as_pure_path, None)

        assert isinstance(props, HoardFileProps)

        if local_props is None:
            yield Diff(
                DiffType.FileOnlyInHoardLocalUnknown, curr_path_in_local.as_pure_path, hoard_file, None, props, None)
        elif local_props.last_status == RepoFileStatus.DELETED:
            yield Diff(
                DiffType.FileOnlyInHoardLocalDeleted, curr_path_in_local.as_pure_path, hoard_file, local_props, props,
                None)
        elif local_props.last_status == RepoFileStatus.MOVED_FROM:
            yield Diff(
                DiffType.FileOnlyInHoardLocalMoved, curr_path_in_local.as_pure_path, hoard_file, local_props, props,
                None)
        elif local_props.last_status in (RepoFileStatus.ADDED, RepoFileStatus.PRESENT, RepoFileStatus.MODIFIED):
            pass  # file is there, which is handled above
        else:
            raise ValueError(f"Unrecognized state: {local_props.last_status}")
