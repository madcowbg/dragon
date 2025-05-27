import hashlib
from typing import Optional

import msgpack

from lmdb_storage.tree_object import ObjectType, StoredObject, ObjectID


class BlobObject(StoredObject):
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

    def __str__(self) -> str:
        return f"BlobObject[{self.file_id}, {self.fasthash}, {self.size}]"

    @staticmethod
    def create(fasthash: str, size: int, md5: Optional[str] = None) -> "BlobObject":
        file_packed = msgpack.packb((ObjectType.BLOB.value, (fasthash, size, md5)))
        file_id = hashlib.sha1(file_packed).digest()
        return BlobObject(file_id=file_id, data=(fasthash, size, md5))

    @property
    def id(self) -> ObjectID:
        return self.file_id

    def __eq__(self, other: "BlobObject") -> bool:
        return isinstance(other, BlobObject) and self.size == other.size and self.fasthash == other.fasthash and self.md5 == other.md5
