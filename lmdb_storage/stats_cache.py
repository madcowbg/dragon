import logging
import random

import lmdb
from lmdb import Transaction

from lmdb_storage.object_store import MAX_MAP_SIZE, used_ratio


class StatsCache:
    def __init__(self, path: str):
        self._env = lmdb.open(path, max_dbs=1, map_size=MAX_MAP_SIZE, readonly=False, subdir=False)
        self._cache_db = self._env.open_db("cache".encode())
        self.maybe_gc()

    def begin(self, write: bool) -> Transaction:
        return self._env.begin(db=self._cache_db, write=write)

    def maybe_gc(self):
        if used_ratio(self._env) > 0.5:
            logging.warning("Stats cache is half full, deleting 50% of it!")
            with self.begin(write=True) as txn:
                for k, _ in txn.cursor():
                    if random.randint(0, 1):
                        txn.delete(k)
