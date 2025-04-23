from typing import Collection

from lmdb import Transaction

from lmdb_storage.tree_structure import ObjectID


class Roots:
    def __init__(self, storage: "ObjectStorage", write: bool):
        self.storage = storage
        self.write = write
        self.txn: Transaction | None = None

    def __enter__(self):
        assert self.txn is None

        self.txn = self.storage.begin(db_name="repos", write=self.write)
        self.txn.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self.txn is not None

        self.txn.__exit__(exc_type, exc_val, exc_tb)
        self.txn = None

    def get_root_id(self, name: str) -> ObjectID | None:
        with self:
            return self.txn.get(name.encode())

    def set_root_id(self, name: str, root_id: ObjectID | None):
        if root_id is not None:
            with self:
                self.txn.put(name.encode(), root_id)

    @property
    def all(self) -> Collection[ObjectID]:
        with self:
            root_ids = [root_id for k, root_id in self.txn.cursor()]
        return root_ids
