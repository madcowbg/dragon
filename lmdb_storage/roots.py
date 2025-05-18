import binascii
from typing import Collection, List

import msgspec
from lmdb import Transaction

from lmdb_storage.tree_structure import ObjectID


class RootData(msgspec.Struct):
    current: bytes | None
    staging: bytes | None
    desired: bytes | None

    @property
    def all(self) -> List[bytes]:
        return [self.current, self.staging, self.desired]


class Root:
    def __init__(self, name: str, roots: "Roots"):
        self.name = name
        self.roots = roots

    def __eq__(self, other):
        return isinstance(other, Root) and other.name == self.name

    @property
    def current(self) -> bytes | None:
        with self.roots:
            return self.load_from_storage.current

    @current.setter
    def current(self, root_id: bytes):
        assert type(root_id) is bytes or root_id is None
        with self.roots.storage.objects(write=False) as objects:
            assert root_id is None or objects[root_id] is not None

        with self.roots as roots:
            root_data = self.load_from_storage
            root_data.current = root_id

            roots.txn.put(self.name.encode(), msgspec.msgpack.encode(root_data))

    @property
    def load_from_storage(self) -> RootData:
        data = self.roots.txn.get(self.name.encode())
        return msgspec.msgpack.decode(data, type=RootData) if data else RootData(None, None, None)

    def write_to_storage(self, root_data: RootData):
        self.roots.txn.put(self.name.encode(), msgspec.msgpack.encode(root_data))

    @property
    def desired(self) -> bytes | None:
        with self.roots:
            return self.load_from_storage.desired

    @desired.setter
    def desired(self, root_id: bytes | None):
        assert type(root_id) is bytes or root_id is None
        with self.roots.storage.objects(write=False) as objects:
            assert root_id is None or objects[root_id] is not None

        with self.roots:
            root_data = self.load_from_storage
            root_data.desired = root_id
            self.write_to_storage(root_data)

    @property
    def staging(self) -> bytes | None:
        with self.roots:
            return self.load_from_storage.staging

    @staging.setter
    def staging(self, root_id: bytes):
        assert type(root_id) is bytes or root_id is None
        with self.roots.storage.objects(write=False) as objects:
            assert root_id is None or objects[root_id] is not None

        with self.roots:
            root_data = self.load_from_storage
            root_data.staging = root_id
            self.write_to_storage(root_data)


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
    def all_roots(self) -> List[Root]:
        with self:
            return [self[name.decode()] for name, _ in self.txn.cursor()]

    @property
    def all_live(self) -> Collection[ObjectID]:
        all_roots = self.all_roots
        with self:
            root_ids = sum((root.load_from_storage.all for root in all_roots), [])
            return sorted(
                list(root_id for root_id in root_ids if root_id is not None),
                key=lambda v: binascii.hexlify(v))
