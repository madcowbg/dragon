import dataclasses
import enum
from typing import Dict

import msgpack
from lmdb import Transaction
from propcache import cached_property

## LMDB object format
#  object_id    blob
#
# blob is file:
#  Type.FILE, (fasthash, size)
#
# blob is tree:
#  Type.TREE, Dict[obj name to object_id]

type ObjectID = bytes


class ObjectType(enum.Enum):
    TREE = 1
    FILE = 2


@dataclasses.dataclass
class TreeObject:
    children: Dict[str, ObjectID]

    @cached_property
    def serialized(self) -> bytes:
        return msgpack.packb((ObjectType.TREE.value, self.children))

    @staticmethod
    def load(data: bytes) -> "TreeObject":
        object_type, children = msgpack.unpackb(data)
        assert object_type == ObjectType.TREE.value
        return TreeObject(children=children)


@dataclasses.dataclass
class FileObject:
    file_id: bytes
    fasthash: str
    size: int

    @cached_property
    def serialized(self) -> bytes:
        return msgpack.packb((ObjectType.FILE.value, self.fasthash, self.size))

    @staticmethod
    def load(file_id: bytes, data: bytes) -> "FileObject":
        object_type, fasthash, size = msgpack.unpackb(data)
        assert object_type == ObjectType.FILE.value
        return FileObject(file_id, fasthash, size)


def load_tree_or_file(obj_id: bytes, txn: Transaction) -> FileObject | TreeObject:
    obj_packed = txn.get(obj_id)  # todo use streaming op
    obj_data = msgpack.loads(obj_packed)  # fixme make this faster by extracting type away
    if obj_data[0] == ObjectType.FILE.value:
        return FileObject(obj_id, obj_data[1], obj_data[2])
    elif obj_data[0] == ObjectType.TREE.value:
        return TreeObject(obj_data[1])
    else:
        raise ValueError(f"Unrecognized type {obj_data[0]}")


class ExpandableTreeObject:
    def __init__(self, data: TreeObject, txn: Transaction):
        self.txn = txn
        self.children: Dict[str, ObjectID] = data.children

        self._files: Dict[str, FileObject] | None = None
        self._dirs: Dict[str, ExpandableTreeObject] | None = None

    @property
    def files(self) -> Dict[str, FileObject]:
        if self._files is None:
            self._load()
        return self._files

    @property
    def dirs(self) -> Dict[str, "ExpandableTreeObject"]:
        if self._dirs is None:
            self._load()
        return self._dirs

    def _load(self):
        self._files = dict()
        self._dirs = dict()

        for name, obj_id in self.children.items():
            obj = load_tree_or_file(obj_id, self.txn)
            if isinstance(obj, FileObject):
                self._files[name] = obj
            elif isinstance(obj, TreeObject):
                self._dirs[name] = ExpandableTreeObject(obj, self.txn)
            else:
                raise TypeError(f"Unexpected type {type(obj)}")

    @staticmethod
    def create(obj_id: bytes, txn: Transaction) -> "ExpandableTreeObject":
        return ExpandableTreeObject(TreeObject.load(txn.get(obj_id)), txn)
