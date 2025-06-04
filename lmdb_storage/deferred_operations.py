import dataclasses
import enum
import logging
from typing import Iterable, Dict

from alive_progress import alive_it, alive_bar
from lmdb import Transaction
from msgspec import msgpack

from command.fast_path import FastPosixPath
from contents.hoard import HoardContents
from lmdb_storage.file_object import FileObject
from lmdb_storage.object_serialization import write_stored_object, read_stored_object
from lmdb_storage.tree_iteration import dfs
from lmdb_storage.tree_object import StoredObject, ObjectType
from lmdb_storage.tree_operations import remove_child
from lmdb_storage.tree_structure import add_file_object
from util import group_to_dict

BRANCH_CURRENT = "current"
BRANCH_DESIRED = "desired"


class DeferredOp(enum.Enum):
    ADD = "add"
    DEL = "del"


@dataclasses.dataclass()
class DeferredItem:
    uuid: str
    branch: str
    hoard_file: str
    stored_obj_id: bytes
    stored_obj_data: bytes
    op: DeferredOp


class HoardDeferredOperations:
    """ Logic for executing deferred tree operations, which are most useful for files pushing."""
    _txn: Transaction | None

    def __init__(self, parent: HoardContents):
        self._parent = parent
        self._txn = None

    def __enter__(self):
        assert self._txn is None, "Cannot start a second transaction!"
        self._txn = self._parent.env.begin(db_name="deferred_ops", write=True)
        self._txn.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self._txn is not None, "Cannot end a nonexistent transaction!"
        try:
            return self._txn.__exit__(exc_type, exc_val, exc_tb)
        finally:
            self._txn = None

    def set_queue_item(self, repo_uuid: str, branch: str, hoard_file: str, stored_obj: StoredObject, op: DeferredOp):
        assert self._txn is not None
        key_uuid_path = msgpack.encode([repo_uuid, branch, hoard_file])
        item = DeferredItem(repo_uuid, branch, hoard_file, stored_obj.id, write_stored_object(stored_obj), op)

        self._txn.put(key_uuid_path, msgpack.encode(item))

    def get_queue(self) -> Iterable[DeferredItem]:
        assert self._txn is not None, "Cannot get a nonexistent transaction!"
        for k, v in self._txn.cursor():
            yield msgpack.decode(v, type=DeferredItem)

    def clear_queue(self):
        for k, v in self._txn.cursor():
            self._txn.pop(k)

    def have_deferred_ops(self):
        with self:
            for item in self.get_queue():
                return True
        return False

    def apply_deferred_queue(self):
        with self:
            all_items = list(self.get_queue())

        for uuid, items_for_uuid in group_to_dict(all_items, key=lambda item: item.uuid).items():
            for branch, deferred_items_for_uuid_and_branch in group_to_dict(
                    items_for_uuid, key=lambda item: item.branch).items():
                repo_root = self._parent.env.roots(write=False)[uuid]

                if branch == BRANCH_CURRENT:
                    repo_root_id = repo_root.current
                    new_repo_root_id = repo_root_id
                elif branch == BRANCH_DESIRED:
                    repo_root_id = repo_root.desired
                    new_repo_root_id = repo_root_id
                else:
                    raise ValueError(f"Unknown branch '{branch}'")

                with self._parent.env.objects(write=False) as objects:
                    loaded_objs: Dict[str, FileObject] = {}
                    with alive_bar(title="Loading existing tree") as bar:
                        for path, obj_type, obj_id, obj, _ in dfs(objects, "", repo_root_id):
                            if obj_type == ObjectType.BLOB:
                                obj: FileObject
                                assert path not in loaded_objs
                                loaded_objs[path] = obj
                                bar()

                for item in alive_it(deferred_items_for_uuid_and_branch, title="Making changes to tree"):
                    file_obj: StoredObject = read_stored_object(item.stored_obj_id, item.stored_obj_data)
                    assert file_obj.object_type == ObjectType.BLOB
                    file_obj: FileObject

                    if item.op == DeferredOp.ADD:
                        loaded_objs[item.hoard_file] = file_obj
                    elif item.op == DeferredOp.DEL:
                        if item.hoard_file in loaded_objs:
                            del loaded_objs[item.hoard_file]
                        else:
                            logging.info("Trying to delete non-existent file %s", item.hoard_file)

                with self._parent.env.objects(write=True) as objects:
                    new_repo_root_id = objects.mktree_from_tuples(sorted(loaded_objs.items()), alive_it=alive_it) if len(
                        loaded_objs) > 0 else None

                if new_repo_root_id == repo_root_id:
                    logging.error(
                        f"Changing {len(deferred_items_for_uuid_and_branch)} files did not create a new root?!")

                repo_root = self._parent.env.roots(write=True)[uuid]
                if branch == BRANCH_CURRENT:
                    repo_root.current = new_repo_root_id

                else:
                    assert branch == BRANCH_DESIRED
                    repo_root.desired = new_repo_root_id

        logging.info(f"Cleaning deferred queue...")
        with self:
            self.clear_queue()  # we are in the same transaction


def add_to_current_tree_file_obj(
        hoard: HoardContents, repo_uuid: str, hoard_file: str, file_obj: FileObject):
    with HoardDeferredOperations(hoard) as deferred_ops:
        deferred_ops.set_queue_item(repo_uuid, BRANCH_CURRENT, hoard_file, file_obj, DeferredOp.ADD)


def add_to_desired_tree(
        hoard: HoardContents, repo_uuid: str, hoard_file: str, file_obj: FileObject):
    with HoardDeferredOperations(hoard) as deferred_ops:
        deferred_ops.set_queue_item(repo_uuid, BRANCH_DESIRED, hoard_file, file_obj, DeferredOp.ADD)


def remove_from_current_tree(
        hoard: HoardContents, repo_uuid: str, hoard_file: str, file_obj: FileObject):
    with HoardDeferredOperations(hoard) as deferred_ops:
        deferred_ops.set_queue_item(repo_uuid, BRANCH_CURRENT, hoard_file, file_obj, DeferredOp.DEL)


def remove_from_desired_tree(
        hoard: HoardContents, repo_uuid: str, hoard_file: str, file_obj: FileObject):
    with HoardDeferredOperations(hoard) as deferred_ops:
        deferred_ops.set_queue_item(repo_uuid, BRANCH_DESIRED, hoard_file, file_obj, DeferredOp.DEL)
