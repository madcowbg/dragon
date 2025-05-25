import dataclasses
import logging
from typing import Collection, Tuple, Dict

import lmdb
from alive_progress import alive_bar
from lmdb import Transaction, Environment, _Database

from lmdb_storage.object_serialization import read_stored_object, write_stored_object
from lmdb_storage.roots import Roots
from lmdb_storage.tree_structure import TreeObject, Objects, ObjectID, StoredObjects, ObjectType, StoredObject


class InconsistentObjectStorage(BaseException):
    pass


@dataclasses.dataclass
class EnvParams:
    path: str
    map_size: int | None
    max_dbs: int


class ObjectEnvironmentCache:
    def __init__(self) -> None:
        self._cache: Dict[str, Tuple[EnvParams, Environment, Dict[str, _Database], int]] = dict()

    def obtain(self, path: str, map_size: int | None, max_dbs: int) -> Tuple[Environment, Dict[str, _Database]]:
        logging.info(f"### LMDB OBTAIN {path}\n")

        env_params = EnvParams(path, max_dbs=max_dbs, map_size=map_size)

        if path not in self._cache:
            logging.info(f"### LMDB OPENING {path}\n")
            env = lmdb.open(path, max_dbs=max_dbs, map_size=map_size, readonly=False)
            self._cache[path] = (
                env_params,
                env,
                {
                    "objects": env.open_db("objects".encode()),
                    "repos": env.open_db("repos".encode())},
                0)

        cached_params, env, dbs, usage = self._cache[path]

        assert cached_params.path == path
        if env_params.map_size is not None and env_params.map_size != env.info()["map_size"]:
            raise ValueError(f"Trying to access a database with different size to be set: {env_params.map_size} but stored is with {env.info()["map_size"]}!")

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

        logging.info(f"### LMDB RELEASE {path}\n")
        usage -= 1
        if usage == 0:
            logging.info(f"### LMDB CLOSING CONNECTION {path}\n")
            env.close()
            self._cache.pop(path)
        else:
            self._cache[path] = (cached_params, env, dbs, usage)

OBJECT_ENVIRONMENT_CACHE = ObjectEnvironmentCache()
MAX_MAP_SIZE = 1 << 30

class ObjectStorage:
    def __init__(self, path: str, *, map_size: int | None = None, max_dbs=5):
        self._env_params = EnvParams(path, map_size=None if map_size is None else min(MAX_MAP_SIZE, map_size), max_dbs=max_dbs)

    def __enter__(self):
        self._env, self._dbs = OBJECT_ENVIRONMENT_CACHE.obtain(self._env_params.path, max_dbs=self._env_params.max_dbs, map_size=self._env_params.map_size)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.info(f"### LMDB CLOSE {self._env_params.path} {self._env.info()}\n")
        OBJECT_ENVIRONMENT_CACHE.release(self._env_params.path)
        self._env = None
        self._dbs = None

        return None

    def gc(self):
        root_ids = self.roots(write=False).all_live
        logging.info(f"found {len(root_ids)} live top-level refs.")

        with self.objects(write=False) as objects:
            self.validate_storage(objects, root_ids)

            live_ids = find_all_live(objects, root_ids)

        logging.info(f"retaining {len(live_ids)} live objects.")
        with self.objects(write=True) as objects:
            with alive_bar(title="deleting objects") as bar:
                for obj_id, _ in objects.txn.cursor():
                    if obj_id not in live_ids:
                        del objects[obj_id]
                        bar()

            self.validate_storage(objects, root_ids)

    def validate_storage(self, objects, root_ids):
        for root_id in root_ids:
            if objects[root_id] is None:
                raise InconsistentObjectStorage(f"Missing root ID {root_id}: not in stored objects!")

    def copy_trees_from(self, other: "ObjectStorage", root_ids: Collection[ObjectID]):
        assert isinstance(root_ids, Collection)
        with other.objects(write=False) as other_objects:
            other_live_ids = find_all_live(other_objects, root_ids)

            with self.objects(write=True) as self_objects:
                for live_id in other_live_ids:
                    if live_id not in self_objects:
                        self_objects[live_id] = other_objects[live_id]

    def begin(self, db_name: str, write: bool) -> Transaction:
        return self._env.begin(db=self._dbs[db_name], write=write)

    def objects(self, write: bool) -> StoredObjects:
        return StoredObjects(self, write, read_stored_object, write_stored_object)

    def roots(self, write: bool) -> Roots:
        return Roots(self, write)


def find_all_live(objects: Objects, root_ids: Collection[ObjectID]) -> Collection[ObjectID]:
    live_ids = set(root_ids)
    q = list(live_ids)
    with alive_bar(title="iterating live objects") as bar:
        while len(q) > 0:
            current_id = q.pop()

            bar()
            live_obj: StoredObject = objects[current_id]
            if live_obj.object_type == ObjectType.TREE:
                live_obj: TreeObject
                # add all children to queue
                for child_id in live_obj.children.values():
                    if child_id not in live_ids:
                        live_ids.add(child_id)
                        q.append(child_id)
            else:
                # do nothing on files, just verify that they exist
                assert live_obj.object_type == ObjectType.BLOB
    return live_ids
