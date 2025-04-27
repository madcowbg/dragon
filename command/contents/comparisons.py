import binascii
import logging
import uuid
from typing import Dict, AsyncGenerator, Iterable, Tuple

from alive_progress import alive_it

from command.fast_path import FastPosixPath
from command.pathing import HoardPathing
from contents.hoard import HoardContents, HoardFSObjects
from contents.hoard_props import HoardFileProps
from contents.repo import RepoContents
from contents.repo_props import FileDesc, RepoFileStatus
from contents_diff import Diff, DiffType
from lmdb_storage.file_object import FileObject
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.tree_iteration import dfs, zip_dfs
from lmdb_storage.tree_structure import Objects, ObjectType


def is_same_file(current: FileDesc, hoard: HoardFileProps):
    if current.size != hoard.size:
        return False  # files differ by size

    if current.fasthash != hoard.fasthash:
        return False  # fast hash is different

    return True  # files are the same


def read_files(objects: Objects[FileObject], root_id: bytes, current_root_id: bytes | None) -> Iterable[
    Tuple[FastPosixPath, Tuple[RepoFileStatus, FileDesc | None]]]:
    with objects:
        for fullpath, diff_type, left_id, right_id, should_skip in \
                zip_dfs(objects, path="", left_id=current_root_id, right_id=root_id, drilldown_same=True):
            # for fullpath, obj_type, obj_id, obj, _ in dfs(objects, "", root_id):
            right_obj = objects[right_id] if right_id is not None else None
            if isinstance(right_obj, FileObject):
                yield (
                    FastPosixPath(fullpath).relative_to("/"),
                    (RepoFileStatus.PRESENT, FileDesc(right_obj.size, right_obj.fasthash, None)))
            else:
                left_obj = objects[left_id] if left_id is not None else None
                if isinstance(left_obj, FileObject) and right_obj is None:
                    yield (
                        FastPosixPath(fullpath).relative_to("/"),
                        (RepoFileStatus.DELETED, FileDesc(left_obj.size, left_obj.fasthash, None)))


async def compare_local_to_hoard(
        uuid: str, hoard: HoardContents, pathing: HoardPathing,
        progress_tool=alive_it) \
        -> AsyncGenerator[Diff]:
    staging_root_id = hoard.env.roots(False)[uuid].staging
    current_root_id = hoard.env.roots(False)[uuid].current
    assert staging_root_id is not None

    logging.info("Load current objects")
    all_local_with_any_status: Dict[FastPosixPath, Tuple[RepoFileStatus, FileDesc]] = \
        dict(read_files(hoard.objects, staging_root_id, current_root_id))

    logging.info("Load hoard objects in folder")

    current_root_id = hoard.env.roots(False)["HEAD"].current

    mounted_at = pathing.mounted_at(uuid).relative_to("/")
    all_hoard_objs_in_folder: Dict[FastPosixPath, Tuple[RepoFileStatus, FileDesc]] = dict(
        (FastPosixPath("/" + p.as_posix()), f) for p, f in read_files(hoard.objects, current_root_id, None)
        if p.is_relative_to(mounted_at))

    logging.info("Loaded all objects.")

    for current_path, (status, props) in progress_tool(all_local_with_any_status.items(), title="Current files vs. Hoard"):
        if status == RepoFileStatus.DELETED or status == RepoFileStatus.MOVED_FROM:
            pass

        assert isinstance(props, FileDesc)

        current_file = current_path
        curr_file_hoard_path = pathing.in_local(current_file, uuid).at_hoard()
        hoard_props = hoard.fsobjects[curr_file_hoard_path.as_pure_path] \
            if curr_file_hoard_path.as_pure_path in hoard.fsobjects else None
        if hoard_props is None:
            logging.info(f"local file not in hoard: {curr_file_hoard_path}")
            added = False
            yield Diff(DiffType.FileOnlyInLocal, current_file, curr_file_hoard_path.as_pure_path, props, None, added)
        elif is_same_file(props, hoard_props):
            logging.info(f"same in hoard {current_file}!")
            yield Diff(DiffType.FileIsSame, current_file, curr_file_hoard_path.as_pure_path, props, hoard_props, None)
        else:
            logging.info(f"file changes {current_file}")
            yield Diff(
                DiffType.FileContentsDiffer, current_file, curr_file_hoard_path.as_pure_path, props, hoard_props, None)

    hoard_file: FastPosixPath
    for hoard_file, desc in progress_tool(
            all_hoard_objs_in_folder.items(),
            title="Hoard vs. Current files"):
        props = hoard.fsobjects[hoard_file]
        curr_path_in_local = pathing.in_hoard(hoard_file).at_local(uuid)
        assert curr_path_in_local is not None  # hoard file is not in the mounted location
        local_props: FileDesc | None
        status, local_props = all_local_with_any_status.get(curr_path_in_local.as_pure_path, (RepoFileStatus.DELETED, None))

        assert isinstance(props, HoardFileProps)

        if local_props is None:
            yield Diff(
                DiffType.FileOnlyInHoardLocalUnknown, curr_path_in_local.as_pure_path, hoard_file, None, props, None)
        elif status == RepoFileStatus.DELETED:
            yield Diff(
                DiffType.FileOnlyInHoardLocalDeleted, curr_path_in_local.as_pure_path, hoard_file, local_props, props,
                None)
        elif status == RepoFileStatus.MOVED_FROM:
            yield Diff(
                DiffType.FileOnlyInHoardLocalMoved, curr_path_in_local.as_pure_path, hoard_file, local_props, props,
                None)
        elif status in (RepoFileStatus.PRESENT, RepoFileStatus.MODIFIED):
            pass  # file is there, which is handled above
        else:
            raise ValueError(f"Unrecognized state: {status}")


async def sync_fsobject_to_object_storage(env: ObjectStorage, fsobjects: HoardFSObjects):
    old_root_id = env.roots(False)["HEAD"].current
    all_nondeleted = [
        (path.as_posix(), FileObject.create(hfo.fasthash, hfo.size))
        async for path, hfo in fsobjects.in_folder_non_deleted(FastPosixPath("/"))]

    with env.objects(write=True) as objects:
        current_root_id = objects.mktree_from_tuples(all_nondeleted)

    env.roots(write=True)["HEAD"].current = current_root_id
    print(
        f"Old HEAD: {binascii.hexlify(old_root_id) if old_root_id is not None else 'None'}"
        f" vs new head {binascii.hexlify(current_root_id)}.")
    return current_root_id


def copy_local_staging_to_hoard(hoard: HoardContents, local: RepoContents):
    logging.info("Copying objects from local to hoard")
    staging_root_id = local.fsobjects.root_id
    current_root = hoard.env.roots(False)[local.uuid].current
    print(
        f"Current local contents "
        f"in hoard: {binascii.hexlify(current_root).decode() if current_root is not None else 'None'} "
        f"vs index: {binascii.hexlify(staging_root_id).decode()}")

    # ensures we have the same tree
    hoard.env.copy_trees_from(local.env, [staging_root_id])
    hoard.env.roots(write=True)[local.uuid].staging = staging_root_id

    return staging_root_id, current_root
