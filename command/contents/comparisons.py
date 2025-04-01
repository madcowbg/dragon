import logging
from pathlib import PurePosixPath
from typing import Generator

from alive_progress import alive_bar, alive_it

from command.pathing import HoardPathing
from contents.hoard import HoardContents
from contents.hoard_props import HoardFileProps, HoardDirProps
from contents.repo import RepoContents
from contents.repo_props import RepoFileProps, RepoFileStatus, RepoDirProps
from contents_diff import Diff, FileOnlyInLocal, FileIsSame, FileContentsDiffer, \
    DirMissingInHoard, DirIsSame, FileOnlyInHoardLocalUnknown, FileOnlyInHoardLocalDeleted, FileOnlyInHoardLocalMoved, \
    DirMissingInLocal


def is_same_file(current: RepoFileProps, hoard: HoardFileProps):
    if current.size != hoard.size:
        return False  # files differ by size

    if current.fasthash != hoard.fasthash:
        return False  # fast hash is different

    return True  # files are the same


def compare_local_to_hoard(local: RepoContents, hoard: HoardContents, pathing: HoardPathing) \
        -> Generator[Diff, None, None]:
    with alive_bar(local.fsobjects.len_existing(), title="Current files vs. Hoard") as bar:
        for current_path, props in local.fsobjects.existing():
            bar()
            if isinstance(props, RepoFileProps):
                current_file = current_path
                curr_file_hoard_path = pathing.in_local(current_file.as_posix(), local.config.uuid).at_hoard()
                if curr_file_hoard_path.as_pure_path not in hoard.fsobjects:
                    logging.info(f"local file not in hoard: {curr_file_hoard_path}")
                    assert props.last_status in (RepoFileStatus.PRESENT, RepoFileStatus.ADDED)
                    yield FileOnlyInLocal(
                        current_file, curr_file_hoard_path.as_pure_path, props,
                        props.last_status == RepoFileStatus.ADDED)
                elif is_same_file(
                        local.fsobjects.get_existing(current_file),
                        hoard.fsobjects[curr_file_hoard_path.as_pure_path]):
                    logging.info(f"same in hoard {current_file}!")
                    yield FileIsSame(current_file, curr_file_hoard_path.as_pure_path, props, hoard.fsobjects[
                        curr_file_hoard_path.as_pure_path])
                else:
                    logging.info(f"file changes {current_file}")
                    yield FileContentsDiffer(
                        current_file,
                        curr_file_hoard_path.as_pure_path, props, hoard.fsobjects[
                            curr_file_hoard_path.as_pure_path])

            elif isinstance(props, RepoDirProps):
                current_dir = current_path
                curr_dir_hoard_path = pathing.in_local(current_dir, local.config.uuid).at_hoard()
                if curr_dir_hoard_path.as_pure_path not in hoard.fsobjects:
                    logging.info(f"new dir found: {current_dir}")
                    yield DirMissingInHoard(current_dir, curr_dir_hoard_path.as_pure_path)
                else:
                    yield DirIsSame(current_dir, curr_dir_hoard_path.as_pure_path)
            else:
                raise ValueError(f"unknown props type: {type(props)}")

    hoard_file: PurePosixPath
    for hoard_file, props in alive_it(
            list(hoard.fsobjects.in_folder(pathing.mounted_at(local.config.uuid))),
            title="Hoard vs. Current files"):
        if isinstance(props, HoardFileProps):
            curr_file_path_in_local = pathing.in_hoard(hoard_file).at_local(local.config.uuid)
            assert curr_file_path_in_local is not None  # hoard file is not in the mounted location

            local_props: RepoFileProps | None = local.fsobjects.get_file_with_any_status(
                curr_file_path_in_local.as_pure_path)
            if local_props is None:
                yield FileOnlyInHoardLocalUnknown(curr_file_path_in_local.as_pure_path, hoard_file, props)
            elif local_props.last_status == RepoFileStatus.DELETED:
                yield FileOnlyInHoardLocalDeleted(
                    curr_file_path_in_local.as_pure_path, hoard_file, props, local_props)
            elif local_props.last_status == RepoFileStatus.MOVED_FROM:
                yield FileOnlyInHoardLocalMoved(
                    curr_file_path_in_local.as_pure_path, hoard_file, props, local_props)
            elif local_props.last_status in (RepoFileStatus.ADDED, RepoFileStatus.PRESENT, RepoFileStatus.MODIFIED):
                pass  # file is there, which is handled above
            else:
                raise ValueError(f"Unrecognized state: {local_props.last_status}")
        elif isinstance(props, HoardDirProps):
            hoard_dir = hoard_file
            curr_dir_path_in_local = pathing.in_hoard(hoard_dir).at_local(local.config.uuid)
            assert curr_dir_path_in_local is not None  # hoard dir is not in the mounted location

            if not local.fsobjects.in_existing(curr_dir_path_in_local.as_pure_path):
                logging.info(f"missing dir found in hoard: {hoard_dir}")
                yield DirMissingInLocal(curr_dir_path_in_local.as_pure_path, hoard_dir)
            else:
                pass  # existing dirs are handled above
        else:
            raise ValueError(f"unknown props type: {type(props)}")
