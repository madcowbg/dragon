import dataclasses
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Collection, Tuple, Dict

import lmdb
from alive_progress import alive_bar
from lmdb import Transaction, Environment, _Database

from lmdb_storage.object_serialization import read_stored_object, write_stored_object
from lmdb_storage.roots import Roots
from lmdb_storage.tree_iteration import dfs
from lmdb_storage.tree_structure import Objects, ObjectID, StoredObjects, TransactionCreator
from lmdb_storage.tree_object import ObjectType, StoredObject, TreeObject
from util import format_size


class InconsistentObjectStorage(BaseException):
    pass


@dataclasses.dataclass
class EnvParams:
    path: str
    map_size: int | None
    max_dbs: int


def maybe_migrate_storage(path):
    logging.warning(f"File not found: {path}")
    tmp_path = f"{path}-MIGRATION"
    if os.path.isdir(path) or os.path.isdir(tmp_path):
        if os.path.isdir(path):
            logging.error(f"Moving current path {path} to {tmp_path}")
            shutil.move(path, tmp_path)

        if os.path.isdir(tmp_path) and os.path.isfile(f"{tmp_path}/data.mdb"):
            logging.error(f"Migrating from folder-based temp storage: {tmp_path}")
            shutil.copy(f"{tmp_path}/data.mdb", path)

        if not os.path.isfile(path):
            logging.error("Migration was not successful! Exiting...")
            raise ValueError(f"Could not migrate folder {path} to file. Fix it manually!")


class ObjectEnvironmentCache:
    def __init__(self) -> None:
        self._cache: Dict[str, Tuple[EnvParams, Environment, Dict[str, _Database], int]] = dict()

    def obtain(self, path: str, map_size: int | None, max_dbs: int) -> Tuple[Environment, Dict[str, _Database]]:
        logging.debug(f"### LMDB OBTAIN {path}")

        env_params = EnvParams(path, max_dbs=max_dbs, map_size=map_size)

        if path not in self._cache:
            logging.info(f"### LMDB OPENING {path}")
            if not os.path.isfile(path):
                maybe_migrate_storage(path)

            env = lmdb.open(path, max_dbs=max_dbs, map_size=map_size, readonly=False, subdir=False)
            self._cache[path] = (
                env_params,
                env,
                {
                    "objects": env.open_db("objects".encode()),
                    "repos": env.open_db("repos".encode()),
                    "deferred_ops": env.open_db("deferred_ops".encode()),
                },
                0)

        cached_params, env, dbs, usage = self._cache[path]

        assert cached_params.path == path
        if env_params.map_size is not None and env_params.map_size != env.info()["map_size"]:
            raise ValueError(
                f"Trying to access a database with different size to be set: {env_params.map_size} but stored is with {env.info()["map_size"]}!")

        if env_params.max_dbs != cached_params.max_dbs:
            raise ValueError(
                f"Trying to access a database with different # of named dbs: {env_params.max_dbs} but stored is with {cached_params.max_dbs}")

        self._cache[path] = (cached_params, env, dbs, usage + 1)

        return env, dbs

    def release(self, path):
        if path not in self._cache:
            raise ValueError("Cannot release an uninitialized object!")

        cached_params, env, dbs, usage = self._cache[path]
        if usage == 0:
            raise ValueError("Cannot release an unused object!")

        logging.debug(f"### LMDB RELEASE {path}")
        usage -= 1
        if usage == 0:
            logging.info(f"### LMDB CLOSING CONNECTION {path}")
            env.close()
            self._cache.pop(path)
        else:
            self._cache[path] = (cached_params, env, dbs, usage)


OBJECT_ENVIRONMENT_CACHE = ObjectEnvironmentCache()
MAX_MAP_SIZE = 1 << 30


def used_size(env):
    # +--------------------+---------------------------------------+
    # | ``psize``          | Size of a database page in bytes.     |
    # +--------------------+---------------------------------------+
    # | ``depth``          | Height of the B-tree.                 |
    # +--------------------+---------------------------------------+
    # | ``branch_pages``   | Number of internal (non-leaf) pages.  |
    # +--------------------+---------------------------------------+
    # | ``leaf_pages``     | Number of leaf pages.                 |
    # +--------------------+---------------------------------------+
    # | ``overflow_pages`` | Number of overflow pages.             |
    # +--------------------+---------------------------------------+
    # | ``entries``        | Number of data items.                 |
    # +--------------------+---------------------------------------+
    stat = env.stat()
    used_size = stat["psize"] * (stat["leaf_pages"] + stat["branch_pages"] + stat["overflow_pages"])
    with env.begin(write=False) as txn:
        for db_name, _ in txn.cursor():
            dbi = env.open_db(db_name, txn=txn)
            stat = txn.stat(dbi)
            used_size += stat["psize"] * (stat["leaf_pages"] + stat["branch_pages"] + stat["overflow_pages"])
    return used_size


def used_ratio(env: Environment):
    return used_size(env) / env.info()["map_size"]


MAX_BACKUPS = 5


def store_backup_rotation(env: Environment):
    lmdb_path = env.path()
    backup_dir = Path(lmdb_path + "-BAK")
    backup_dir.mkdir(parents=True, exist_ok=True)
    backups = list(sorted(backup_dir.glob("backup_*.lmdb")))
    if len(backups) >= MAX_BACKUPS:
        for backup in backups[:-MAX_BACKUPS + 1]:
            backup.unlink(missing_ok=True)

    curr_bup_len = len(list(backup_dir.glob("backup_*.lmdb")))
    if curr_bup_len >= MAX_BACKUPS:
        logging.error("Too many backups, but will store anyway.")

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = backup_dir.joinpath(f"backup_{timestamp_str}.lmdb")
    logging.info(f"Storing backup as {backup_file}")
    backup_file.unlink(missing_ok=True)

    env.copy(backup_file.as_posix(), compact=True)


class ObjectStorage(TransactionCreator):
    def __init__(self, path: str, *, map_size: int | None = None, max_dbs=5):
        self._env_params = EnvParams(
            path, map_size=None if map_size is None else min(MAX_MAP_SIZE, map_size), max_dbs=max_dbs)

    def __enter__(self):
        self._env, self._dbs = OBJECT_ENVIRONMENT_CACHE.obtain(
            self._env_params.path, max_dbs=self._env_params.max_dbs, map_size=self._env_params.map_size)
        if self.used_ratio > 0.6:
            logging.warn(f"Start cleaning with used ratio of {self.used_ratio}.")
            self._env.set_mapsize(self._env_params.map_size * 2)
            self.gc()
            self._env.set_mapsize(self._env_params.map_size)
            logging.warn(f"Ended cleaning with used ratio of {self.used_ratio}.")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.debug(f"### LMDB CLOSE {self._env_params.path} {self._env.info()}")
        OBJECT_ENVIRONMENT_CACHE.release(self._env_params.path)
        self._env = None
        self._dbs = None

        return None

    @property
    def used_size(self) -> int:
        return used_size(self._env)

    @property
    def used_ratio(self):
        return used_ratio(self._env)

    def maybe_gc(self):
        if self.used_ratio > 0.6:
            self.gc()
            if self.used_ratio > 0.4:
                logging.error("Even after GC usage is more than 40%. Can fill up.")

    def gc(self, silent: bool = False):
        logging.warn(f"Used space = {format_size(self.used_size)}")
        logging.warn(f"Used pct = {100 * self.used_ratio}")

        root_ids = self.roots(write=False).all_live
        logging.info(f"found {len(root_ids)} live top-level refs.")

        with self.objects(write=False) as objects:
            self.validate_storage(objects, root_ids)

            live_ids = find_all_live(objects, root_ids)

        logging.info(f"retaining {len(live_ids)} live objects.")
        with self.objects(write=True) as objects:
            if not silent:
                ab = alive_bar(title="deleting objects")
                bar = ab.__enter__()
            try:
                for obj_id, _ in objects.txn.cursor():
                    if obj_id not in live_ids:
                        del objects[obj_id]
                        if not silent:
                            bar()
            finally:
                if not silent:
                    ab.__exit__(None, None, None)

            self.validate_storage(objects, root_ids)

        store_backup_rotation(self._env)

    def validate_storage(self, objects, root_ids):
        for root_id in root_ids:
            if objects[root_id] is None:
                raise InconsistentObjectStorage(f"Missing root ID {root_id}: not in stored objects!")

    def copy_trees_from(self, other: "ObjectStorage", root_ids: Collection[ObjectID]):
        assert isinstance(root_ids, Collection)
        with other.objects(write=False) as other_objects:
            with self.objects(write=True) as self_objects:
                for root_id in root_ids:
                    for _, _, live_id, _, should_skip in dfs(other_objects, "", root_id):
                        if live_id in self_objects:
                            should_skip()  # we already have it here, so do not drill down
                        else:
                            self_objects[live_id] = other_objects[live_id]

    def begin(self, db_name: str, write: bool) -> Transaction:
        return self._env.begin(db=self._dbs[db_name], write=write)

    def objects(self, write: bool) -> StoredObjects:
        return StoredObjects(
            self, db_name="objects", write=write, object_reader=read_stored_object, object_writer=write_stored_object)

    def roots(self, write: bool) -> Roots:
        return Roots(self, write)


def find_all_live(objects: Objects, root_ids: Collection[ObjectID]) -> Collection[ObjectID]:
    live_ids = set(root_ids)
    q = list(live_ids)
    while len(q) > 0:
        current_id = q.pop()

        live_obj: StoredObject = objects[current_id]
        if live_obj.object_type == ObjectType.TREE:
            live_obj: TreeObject
            # add all children to queue
            for _, child_id in live_obj.children:
                if child_id not in live_ids:
                    live_ids.add(child_id)
                    q.append(child_id)
        else:
            # do nothing on files, just verify that they exist
            assert live_obj.object_type == ObjectType.BLOB
    return live_ids


