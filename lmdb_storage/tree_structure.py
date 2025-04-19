import dataclasses
import enum
import hashlib
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
    def create(fasthash: str, size: int) -> "FileObject":
        file_packed = msgpack.packb((ObjectType.FILE.value, fasthash, size))
        file_id = hashlib.sha1(file_packed).digest()
        return FileObject(file_id=file_id, fasthash=fasthash, size=size)

    @staticmethod
    def load(file_id: bytes, data: bytes) -> "FileObject":
        object_type, fasthash, size = msgpack.unpackb(data)
        assert object_type == ObjectType.FILE.value
        return FileObject(file_id, fasthash, size)


class ExpandableTreeObject:
    def __init__(self, data: TreeObject, objects: "Objects"):
        self.objects = objects
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
            obj = self.objects[obj_id]
            if isinstance(obj, FileObject):
                self._files[name] = obj
            elif isinstance(obj, TreeObject):
                self._dirs[name] = ExpandableTreeObject(obj, self.objects)
            else:
                raise TypeError(f"Unexpected type {type(obj)}")

    @staticmethod
    def create(obj_id: bytes, objects: "Objects") -> "ExpandableTreeObject":
        return ExpandableTreeObject(objects[obj_id], objects)


class Objects:
    def __init__(self, storage: "ObjectStorage", write: bool):
        self.txn = storage.objects_txn(write=write)

    def __enter__(self):
        self.txn.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.txn.__exit__(exc_type, exc_val, exc_tb)
        return None

    def __getitem__(self, obj_id: bytes) -> FileObject | TreeObject:
        obj_packed = self.txn.get(obj_id)  # todo use streaming op
        obj_data = msgpack.loads(obj_packed)  # fixme make this faster by extracting type away
        if obj_data[0] == ObjectType.FILE.value:
            return FileObject(obj_id, obj_data[1], obj_data[2])
        elif obj_data[0] == ObjectType.TREE.value:
            return TreeObject(obj_data[1])
        else:
            raise ValueError(f"Unrecognized type {obj_data[0]}")
