import logging
import random

import lmdb
from lmdb import Transaction

from lmdb_storage.object_serialization import read_stored_object, write_stored_object
from lmdb_storage.object_store import MAX_MAP_SIZE, used_ratio
from lmdb_storage.tree_structure import TransactionCreator, StoredObjects


class StatsCache(TransactionCreator):
    def __init__(self, path: str):
        self._env = lmdb.open(path, max_dbs=1, map_size=MAX_MAP_SIZE, readonly=False, subdir=False)
        if used_ratio(self._env) > 0.5:
            logging.warning("Stats cache is half full, deleting 50% of it!")
            with self.begin("cache", write=True) as txn:
                for k, _ in txn.cursor():
                    if random.randint(0, 1):
                        txn.delete(k)

    def begin(self, db_name: str, write: bool) -> Transaction:
        return self._env.begin(db=self._env.open_db(db_name), write=write)

    def cache(self, write: bool) -> StoredObjects:
        return StoredObjects(
            self, db_name="cache", write=write, object_reader=read_stored_object, object_writer=write_stored_object)
