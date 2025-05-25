import hashlib
from typing import Optional

import msgpack
from propcache import cached_property

from lmdb_storage.tree_structure import ObjectType, ObjectID


class FileObject:
    file_id: bytes
    fasthash: str
    md5: str
    size: int

    def __init__(self, file_id: bytes, data: any) -> None:
        self.file_id = file_id
        self.fasthash = data[0]
        self.size = data[1]
        self.md5 = data[2]
        self.object_type = ObjectType.BLOB

    @cached_property
    def serialized(self) -> bytes:
        return msgpack.packb((ObjectType.BLOB.value, (self.fasthash, self.size, self.md5)))

    @staticmethod
    def create(fasthash: str, size: int, md5: Optional[str] = None) -> "FileObject":
        file_packed = msgpack.packb((ObjectType.BLOB.value, (fasthash, size, md5)))
        file_id = hashlib.sha1(file_packed).digest()
        return FileObject(file_id=file_id, data=(fasthash, size, md5))

    @staticmethod
    def load(file_id: bytes, data: bytes) -> "FileObject":
        object_type, data = msgpack.unpackb(data)
        assert object_type == ObjectType.BLOB.value
        return FileObject(file_id, data)

    @property
    def id(self) -> ObjectID:
        return self.file_id

    def __eq__(self, other: "FileObject") -> bool:
        return isinstance(other, FileObject) and self.file_id == other.file_id
