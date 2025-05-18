import logging

from command.fast_path import FastPosixPath
from contents.hoard import HoardContents
from contents.hoard_props import HoardFileProps
from lmdb_storage.file_object import FileObject
from lmdb_storage.tree_operations import remove_child
from lmdb_storage.tree_structure import add_file_object


def add_to_current_tree(hoard: HoardContents, repo_uuid: str, hoard_file: str, hoard_props: HoardFileProps):
    roots = hoard.env.roots(write=True)
    repo_root = roots[repo_uuid]
    repo_current_root_id = repo_root.current

    with hoard.env.objects(write=True) as objects:
        new_repo_current_root_id = add_file_object(
            objects, repo_current_root_id, FastPosixPath(hoard_file)._rem,
            FileObject.create(hoard_props.fasthash, hoard_props.size))

    if new_repo_current_root_id == repo_current_root_id:
        logging.error(f"Adding {hoard_file} did not create a new root?!")

    repo_root.current = new_repo_current_root_id


# fixme merge with other add method
def add_to_desired_tree(hoard: HoardContents, repo_uuid: str, hoard_file: str, hoard_props: HoardFileProps):
    roots = hoard.env.roots(write=True)
    repo_root = roots[repo_uuid]
    repo_desired_root_id = repo_root.desired

    with hoard.env.objects(write=True) as objects:
        new_repo_desired_root_id = add_file_object(
            objects, repo_desired_root_id, FastPosixPath(hoard_file)._rem,
            FileObject.create(hoard_props.fasthash, hoard_props.size))

    if new_repo_desired_root_id == repo_desired_root_id:
        logging.error(f"Adding {hoard_file} to desired did not create a new root?!")

    repo_root.desired = new_repo_desired_root_id


def remove_from_current_tree(hoard: HoardContents, repo_uuid: str, hoard_file: FastPosixPath):
    roots = hoard.env.roots(write=True)
    repo_root = roots[repo_uuid]
    repo_current_root_id = repo_root.current

    with hoard.env.objects(write=True) as objects:
        new_repo_current_root_id = remove_child(objects, FastPosixPath(hoard_file)._rem, repo_current_root_id)

    if new_repo_current_root_id == repo_current_root_id:
        logging.error(f"Removing {hoard_file} from current did not create a new root?!")

    repo_root.current = new_repo_current_root_id


# fixme merge with other remove method
def remove_from_desired_tree(hoard: HoardContents, repo_uuid: str, hoard_file: FastPosixPath):
    roots = hoard.env.roots(write=True)
    repo_root = roots[repo_uuid]
    repo_desired_root_id = repo_root.desired

    with hoard.env.objects(write=True) as objects:
        new_repo_desired_root_id = remove_child(objects, FastPosixPath(hoard_file)._rem, repo_desired_root_id)

    if new_repo_desired_root_id == repo_desired_root_id:
        logging.error(f"Removing {hoard_file} from desired did not create a new root?!")

    repo_root.desired = new_repo_desired_root_id
