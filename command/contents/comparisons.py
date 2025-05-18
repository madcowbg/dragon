import binascii
import logging
from typing import Dict

from command.fast_path import FastPosixPath
from config import HoardConfig
from contents.hoard import HoardContents, HoardFSObjects
from contents.hoard_props import HoardFileStatus
from contents.repo import RepoContents
from contents.repo_props import FileDesc
from lmdb_storage.file_object import FileObject
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.tree_iteration import zip_trees_dfs
from lmdb_storage.tree_structure import TreeObject, add_object, ObjectID


def sync_object_storage_to_recreate_fsobject_and_fspresence(
        env: ObjectStorage, fsobjects: HoardFSObjects, hoard_config: HoardConfig):
    fsobjects.parent.conn.execute("DELETE FROM fspresence")  # fixme DANGEROUS
    fsobjects.parent.conn.execute("DELETE FROM fsobject")  # fixme DANGEROUS

    remotes = list(hoard_config.remotes.all())
    root_ids = (
            [env.roots(write=False)[r.uuid].current for r in remotes] +
            [env.roots(write=False)[r.uuid].desired for r in remotes] +
            [env.roots(write=False)["HOARD"].desired])
    with (env.objects(write=True) as objects):
        for path, sub_ids, _ in zip_trees_dfs(objects, "", root_ids, drilldown_same=True):
            sub_ids = list(sub_ids)

            objs = [objects[sub_id] for sub_id in sub_ids if sub_id is not None]
            if any(isinstance(obj, TreeObject) for obj in objs):  # has tree
                assert all(not isinstance(obj, FileObject) for obj in objs)  # has no files
                continue

            existing_desired_ids = set(sub_id for sub_id in sub_ids[len(remotes):] if sub_id is not None)
            if len(existing_desired_ids) == 0:  # file is not in hoard
                # fixme that is required to patch when the file is no longer in hoard, but some current repos have it
                # fixme delete when we switch off fsobjects usage altogether
                only_current_file_id = [sub_id for sub_id in sub_ids[:len(remotes)] if sub_id is not None][0]
                file_object = objects[only_current_file_id]

                # create an obsolete file
                fsobjects.HACK_set_file_information(
                    FastPosixPath(path),
                    FileDesc(file_object.size, file_object.fasthash, None),
                    dict(
                        (remote.uuid, HoardFileStatus.CLEANUP)
                        for remote, sub_id_in_remote_current in zip(remotes, sub_ids[:len(remotes)])
                        if sub_id_in_remote_current is not None))

                continue

            assert len(existing_desired_ids) == 1, \
                f"should have only one file id, {sub_ids}"

            # create fsobject from desc
            only_desired_file_id = next(iter(existing_desired_ids))
            file_object = objects[only_desired_file_id]
            assert isinstance(file_object, FileObject)

            hoard_sub_id = sub_ids[-1]

            status: Dict[str, HoardFileStatus] = dict()
            for remote, sub_id_in_remote_current, sub_id_in_remote_desired \
                    in zip(remotes, sub_ids[:len(remotes)], sub_ids[len(remotes):-1]):  # skip the last, is the hoard

                assert remote is not None
                assert sub_id_in_remote_desired is None or sub_id_in_remote_desired == only_desired_file_id, \
                    f"bad - file is not the same?! {only_desired_file_id} != {sub_id_in_remote_desired}"

                remote_uuid = remote.uuid
                computed_status = compute_status(hoard_sub_id, sub_id_in_remote_current, sub_id_in_remote_desired)
                if computed_status is not None:
                    status[remote.uuid] = computed_status

            fsobjects.HACK_set_file_information(
                FastPosixPath(path),
                FileDesc(file_object.size, file_object.fasthash, None),  # fixme add md5
                status)


def compute_status(
        hoard_sub_id: ObjectID | None, sub_id_in_remote_current: ObjectID | None,
        sub_id_in_remote_desired: ObjectID | None) -> HoardFileStatus | None:
    if hoard_sub_id is None:  # is a deleted file
        return HoardFileStatus.CLEANUP
    elif sub_id_in_remote_current is not None:  # file is in current
        if sub_id_in_remote_desired is not None:
            if sub_id_in_remote_desired == sub_id_in_remote_current:
                return HoardFileStatus.AVAILABLE
            else:
                return HoardFileStatus.GET
        else:
            return HoardFileStatus.CLEANUP

    else:
        if sub_id_in_remote_desired is not None:
            return HoardFileStatus.GET
        else:
            return None  # file not desired and not current


def copy_local_staging_to_hoard(hoard: HoardContents, local: RepoContents, config: HoardConfig) -> None:
    logging.info("Copying objects from local to hoard")
    staging_root_id = local.fsobjects.root_id

    current_root = hoard.env.roots(False)[local.uuid].current
    print(
        f"Current local contents "
        f"in hoard: {binascii.hexlify(current_root).decode() if current_root is not None else 'None'} "
        f"vs index: {binascii.hexlify(staging_root_id).decode()}")

    # ensures we have the same tree
    hoard.env.copy_trees_from(local.env, [staging_root_id])

    with hoard.env.objects(write=True) as objects:
        abs_staging_root_id = add_object(
            objects, None,
            path=config.remotes[local.uuid].mounted_at._rem,
            obj_id=staging_root_id)
    hoard.env.roots(write=True)[local.uuid].staging = abs_staging_root_id
