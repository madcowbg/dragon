import binascii
import logging
from typing import Dict, AsyncGenerator, Iterable, Tuple

from alive_progress import alive_it

from command.fast_path import FastPosixPath
from command.pathing import HoardPathing
from contents.hoard import HoardContents
from contents.hoard_props import HoardFileProps
from contents.repo import RepoContents
from contents.repo_props import FileDesc, RepoFileStatus
from contents_diff import Diff, DiffType
from lmdb_storage.file_object import FileObject
from lmdb_storage.tree_iteration import dfs
from lmdb_storage.tree_structure import Objects, ObjectType


def is_same_file(current: FileDesc, hoard: HoardFileProps):
    if current.size != hoard.size:
        return False  # files differ by size

    if current.fasthash != hoard.fasthash:
        return False  # fast hash is different

    return True  # files are the same


def read_files(objects: Objects[FileObject], root_id: bytes) -> Iterable[Tuple[FastPosixPath, FileDesc]]:
    with objects:
        for fullpath, obj_type, obj_id, obj, _ in dfs(objects, "", root_id):
            if obj_type == ObjectType.BLOB:
                yield (
                    FastPosixPath(fullpath).relative_to("/"),
                    FileDesc(obj.size, obj.fasthash, None))


async def compare_local_to_hoard(
        uuid: str, staging_root_id: bytes, hoard: HoardContents, pathing: HoardPathing,
        progress_tool=alive_it) \
        -> AsyncGenerator[Diff]:

    logging.info("Load current objects")
    all_local_with_any_status: Dict[FastPosixPath, FileDesc] = \
        dict(read_files(hoard.objects, staging_root_id))

    logging.info("Load hoard objects in folder")
    all_hoard_in_folder: Dict[FastPosixPath, HoardFileProps] = dict([
        s async for s in hoard.fsobjects.in_folder(pathing.mounted_at(uuid))])
    logging.info("Loaded all objects.")

    for current_path, props in progress_tool(all_local_with_any_status.items(), title="Current files vs. Hoard"):
        assert isinstance(props, FileDesc)

        current_file = current_path
        curr_file_hoard_path = pathing.in_local(current_file, uuid).at_hoard()
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
        curr_path_in_local = pathing.in_hoard(hoard_file).at_local(uuid)
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


def obtain_local_staging_to_hoard(hoard: HoardContents, local: RepoContents):
    logging.info("Copying objects from local to hoard")
    staging_root_id = local.fsobjects.root_id
    current_root = hoard.env.get_root_id(local.uuid)
    print(
        f"Current local contents "
        f"in hoard: {binascii.hexlify(current_root).decode() if current_root is not None else 'None'} "
        f"vs index: {binascii.hexlify(staging_root_id).decode()}")

    # ensures we have the same tree
    hoard.env.copy_trees_from(local.env, [staging_root_id])
    hoard.env.set_root_id(f"staging-{local.uuid}", staging_root_id)

    return staging_root_id, current_root
