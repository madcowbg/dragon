import binascii
import logging

from command.fast_path import FastPosixPath
from config import HoardConfig
from contents.hoard import HoardContents, HoardFSObjects
from contents.hoard_props import HoardFileStatus
from contents.repo import RepoContents, RepoFSObjects
from contents.repo_props import FileDesc
from lmdb_storage.file_object import FileObject
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.tree_iteration import zip_trees_dfs
from lmdb_storage.tree_structure import TreeObject, add_object
from util import safe_hex


async def sync_fsobject_to_object_storage(
        env: ObjectStorage, fsobjects: HoardFSObjects, repo_objects: RepoFSObjects, hoard_config: HoardConfig):
    # if env.roots(write=True)["HOARD"].desired is None:
    #     # fixme ugly hack to ensure unit tests do not change
    #     with env.objects(write=True) as objects:
    #         empty_dir = objects.mktree_from_tuples([])
    #     env.roots(write=True)["HOARD"].desired = empty_dir
    #
    # return
    raise_exception = False

    old_root_id = env.roots(False)["HOARD"].desired

    all_nondeleted = [
        (path.as_posix(), FileObject.create(hfo.fasthash, hfo.size))
        async for path, hfo in fsobjects.in_folder_non_deleted(FastPosixPath("/"))]

    with env.objects(write=True) as objects:
        current_root_id = objects.mktree_from_tuples(all_nondeleted)

    with env.objects(write=True) as objects:
        empty_dir = objects.mktree_from_tuples([])
    if raise_exception and old_root_id != current_root_id:
        if old_root_id is not None and current_root_id != empty_dir:
            raise ValueError(f"Hoard root changed: {old_root_id} != {current_root_id}")

    for remote in hoard_config.remotes.all():
        with env.objects(write=True) as objects:
            # remote_current_id = objects.mktree_from_tuples([
            #     (hoard_config.remotes[remote.uuid].mounted_at.joinpath(path).as_posix(),
            #      FileObject.create(hfo.fasthash, hfo.size))
            #     for path, hfo in repo_objects.existing()])
            remote_current_id = objects.mktree_from_tuples([
                (path.as_posix(), FileObject.create(hfo.fasthash, hfo.size))
                async for path, hfo in fsobjects.in_folder(FastPosixPath("/"))
                if hfo.get_status(remote.uuid) in (HoardFileStatus.AVAILABLE, HoardFileStatus.CLEANUP)])

            remote_desired_id = objects.mktree_from_tuples([
                (path.as_posix(), FileObject.create(hfo.fasthash, hfo.size))
                # fixme make path absolute
                async for path, hfo in fsobjects.in_folder(FastPosixPath("/"))
                if hfo.get_status(remote.uuid) in (
                    HoardFileStatus.AVAILABLE, HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.MOVE)])

        root = env.roots(True)[remote.uuid]
        if remote_current_id != root.current:
            if raise_exception and root.current is not None and remote_current_id != empty_dir:
                raise ValueError(
                    f"{remote.name}: {safe_hex(remote_current_id)} is not current, current={safe_hex(root.current)}")
            # assert root.current is None, f"{remote.name}: {safe_hex(remote_current_id)} is not current, current={safe_hex(root.current)}"
        if remote_desired_id != root.desired:
            if raise_exception and root.desired is not None and remote_desired_id != empty_dir:
                raise ValueError(
                    f"{remote.name}: {safe_hex(remote_desired_id)} is not desired, desired={safe_hex(root.desired)}")
            # assert root.desired is None, f"{remote.name}: {safe_hex(remote_desired_id)} is not desired, desired={safe_hex(root.desired)}"

        root.current = remote_current_id
        root.desired = remote_desired_id

    env.roots(write=True)["HOARD"].desired = current_root_id
    print(
        f"Old HEAD: {binascii.hexlify(old_root_id) if old_root_id is not None else 'None'}"
        f" vs new head {binascii.hexlify(current_root_id)}.")
    return current_root_id


def sync_object_storate_to_recreate_fsobject_and_fspresence(
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
                hoard_props = fsobjects.add_or_replace_file(
                    FastPosixPath(path),
                    FileDesc(file_object.size, file_object.fasthash, None))

                # mark to cleanup all current but not desired
                for remote, sub_id_in_remote_current in zip(remotes, sub_ids[:len(remotes)]):
                    if sub_id_in_remote_current is not None:
                        hoard_props.mark_for_cleanup([remote.uuid])

                continue

            assert len(existing_desired_ids) == 1, \
                f"should have only one file id, {sub_ids}"

            # create fsobject from desc
            only_desired_file_id = next(iter(existing_desired_ids))
            file_object = objects[only_desired_file_id]
            assert isinstance(file_object, FileObject)
            # fixme add md5
            hoard_props = fsobjects.add_or_replace_file(
                FastPosixPath(path),
                FileDesc(file_object.size, file_object.fasthash, None))

            hoard_sub_id = sub_ids[-1]
            should_exist = hoard_sub_id is not None

            for remote, sub_id_in_remote_current, sub_id_in_remote_desired \
                    in zip(remotes, sub_ids[:len(remotes)], sub_ids[len(remotes):-1]):  # skip the last, is the hoard

                assert remote is not None
                assert sub_id_in_remote_desired is None or sub_id_in_remote_desired == only_desired_file_id, \
                    f"bad - file is not the same?! {only_desired_file_id} != {sub_id_in_remote_desired}"

                if sub_id_in_remote_current is not None:  # file is in current
                    if sub_id_in_remote_desired is not None:
                        if sub_id_in_remote_desired == sub_id_in_remote_current:
                            hoard_props.mark_available(remote.uuid)
                        else:
                            hoard_props.mark_to_get([remote.uuid])
                    else:
                        hoard_props.mark_for_cleanup([remote.uuid])
                else:
                    if sub_id_in_remote_desired is not None:
                        hoard_props.mark_to_get([remote.uuid])
                    else:
                        pass  # file not desired and not current

                if not should_exist:
                    hoard_props.mark_to_delete_everywhere()


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
