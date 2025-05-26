import binascii
import logging

from config import HoardConfig
from contents.hoard import HoardContents
from contents.repo import RepoContents
from lmdb_storage.tree_object import MaybeObjectID
from lmdb_storage.tree_structure import add_object


def copy_local_staging_data_to_hoard(hoard: HoardContents, local: RepoContents, config: HoardConfig) -> MaybeObjectID:
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

    return abs_staging_root_id

def commit_local_staging(hoard: HoardContents, local: RepoContents, abs_staging_root_id: MaybeObjectID):
    hoard.env.roots(write=True)[local.uuid].staging = abs_staging_root_id
