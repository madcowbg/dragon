import binascii
from typing import Collection, List

import msgspec
from lmdb import Transaction

from lmdb_storage.tree_structure import ObjectID


class RootData(msgspec.Struct):
    current: bytes | None
    staging: bytes | None

    @property
    def all(self) -> List[bytes]:
        return [self.current, self.staging]


class Root:
    def __init__(self, name: str, roots: "Roots"):
        self.name = name
        self.roots = roots

    def set_current(self, root_id: bytes):
        assert type(root_id) is bytes
        with self.roots as roots:
            root_data = self.load_from_storage
            root_data.current = root_id

            roots.txn.put(self.name.encode(), msgspec.msgpack.encode(root_data))

    @property
    def load_from_storage(self) -> RootData:
        data = self.roots.txn.get(self.name.encode())
        return msgspec.msgpack.decode(data, type=RootData) if data else RootData(None, None)

    def write_to_storage(self, root_data: RootData):
        self.roots.txn.put(self.name.encode(), msgspec.msgpack.encode(root_data))

    def get_current(self) -> bytes | None:
        with self.roots:
            return self.load_from_storage.current

    def set_staging(self, root_id: bytes):
        assert type(root_id) is bytes
        with self.roots:
            root_data = self.load_from_storage
            root_data.staging = root_id
            self.write_to_storage(root_data)

    def get_staging(self) -> bytes | None:
        with self.roots:
            return self.load_from_storage.staging


class Roots:
    def __init__(self, storage: "ObjectStorage", write: bool):
        self.storage = storage
        self.write = write
        self.txn: Transaction | None = None

    def __enter__(self):
        assert self.txn is None

        self.txn = self.storage.begin(db_name="repos", write=self.write)
        self.txn.__enter__()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self.txn is not None

        self.txn.__exit__(exc_type, exc_val, exc_tb)
        self.txn = None

    def __getitem__(self, name: str) -> Root:
        assert type(name) is str
        return Root(name, self)

    @property
    def all(self) -> List[Root]:
        with self:
            return [self[name.decode()] for name, _ in self.txn.cursor()]

    @property
    def all_live(self) -> Collection[ObjectID]:
        with self:
            roots = (self[id.decode()].load_from_storage for id, root_data in self.txn.cursor())
            root_ids = sum((root_data.all for root_data in roots), [])
            return sorted(
                list(root_id for root_id in root_ids if root_id is not None),
                key=lambda v: binascii.hexlify(v))
