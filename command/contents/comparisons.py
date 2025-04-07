import logging
from command.fast_path import FastPosixPath
from typing import Generator, Dict, AsyncGenerator

from alive_progress import alive_bar, alive_it

from command.pathing import HoardPathing
from contents.hoard import HoardContents
from contents.hoard_props import HoardFileProps, HoardDirProps
from contents.repo import RepoContents
from contents.repo_props import RepoFileProps, RepoFileStatus, RepoDirProps
from contents_diff import Diff, FileOnlyInLocal, FileIsSame, FileContentsDiffer, \
    FileOnlyInHoardLocalUnknown, FileOnlyInHoardLocalDeleted, FileOnlyInHoardLocalMoved


def is_same_file(current: RepoFileProps, hoard: HoardFileProps):
    if current.size != hoard.size:
        return False  # files differ by size

    if current.fasthash != hoard.fasthash:
        return False  # fast hash is different

    return True  # files are the same


async def compare_local_to_hoard(local: RepoContents, hoard: HoardContents, pathing: HoardPathing) \
        -> AsyncGenerator[Diff]:
    logging.info("Load current objects")
    all_local_with_any_status: Dict[FastPosixPath, RepoFileProps | RepoDirProps] = \
        dict(s for s in alive_it(local.fsobjects.all_status(), title="Load current objects"))

    logging.info("Load hoard objects in folder")
    all_hoard_in_folder: Dict[FastPosixPath, HoardFileProps | HoardDirProps] = dict([
        s async for s in hoard.fsobjects.in_folder(pathing.mounted_at(local.config.uuid))])
    logging.info("Loaded all objects.")

    with alive_bar(len(all_local_with_any_status), title="Current files vs. Hoard") as bar:
        for current_path, props in all_local_with_any_status.items():
            bar()
            if props.last_status == RepoFileStatus.DELETED or props.last_status == RepoFileStatus.MOVED_FROM:
                continue
            if isinstance(props, RepoFileProps):
                current_file = current_path
                curr_file_hoard_path = pathing.in_local(current_file, local.config.uuid).at_hoard()
                hoard_props = all_hoard_in_folder.get(curr_file_hoard_path.as_pure_path, None)
                if hoard_props is None:
                    logging.info(f"local file not in hoard: {curr_file_hoard_path}")
                    yield FileOnlyInLocal(
                        current_file, curr_file_hoard_path.as_pure_path, props,
                        props.last_status == RepoFileStatus.ADDED)
                elif is_same_file(props, hoard_props):
                    logging.info(f"same in hoard {current_file}!")
                    yield FileIsSame(current_file, curr_file_hoard_path.as_pure_path, props, hoard_props)
                else:
                    logging.info(f"file changes {current_file}")
                    yield FileContentsDiffer(
                        current_file,
                        curr_file_hoard_path.as_pure_path, props, hoard_props)

            elif isinstance(props, RepoDirProps):
                pass
            else:
                raise ValueError(f"unknown props type: {type(props)}")

    hoard_file: FastPosixPath
    for hoard_file, props in alive_it(
            all_hoard_in_folder.items(),
            title="Hoard vs. Current files"):
        curr_path_in_local = pathing.in_hoard(hoard_file).at_local(local.config.uuid)
        assert curr_path_in_local is not None  # hoard file is not in the mounted location
        local_props: RepoFileProps | None = all_local_with_any_status.get(curr_path_in_local.as_pure_path, None)

        if isinstance(props, HoardFileProps):
            if local_props is None:
                yield FileOnlyInHoardLocalUnknown(curr_path_in_local.as_pure_path, hoard_file, props)
            elif local_props.last_status == RepoFileStatus.DELETED:
                yield FileOnlyInHoardLocalDeleted(
                    curr_path_in_local.as_pure_path, hoard_file, props, local_props)
            elif local_props.last_status == RepoFileStatus.MOVED_FROM:
                yield FileOnlyInHoardLocalMoved(
                    curr_path_in_local.as_pure_path, hoard_file, props, local_props)
            elif local_props.last_status in (RepoFileStatus.ADDED, RepoFileStatus.PRESENT, RepoFileStatus.MODIFIED):
                pass  # file is there, which is handled above
            else:
                raise ValueError(f"Unrecognized state: {local_props.last_status}")
        elif isinstance(props, HoardDirProps):
            pass
        else:
            raise ValueError(f"unknown props type: {type(props)}")
