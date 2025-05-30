import hashlib
import logging
from typing import Optional, Tuple

import msgpack

from lmdb_storage.tree_object import ObjectType, StoredObject, ObjectID


class BlobObject(StoredObject):
    def __init__(self, id: bytes) -> None:
        self._file_id = id
        self.object_type = ObjectType.BLOB

    def __str__(self) -> str:
        return f"BlobObject[{self._file_id}]"

    @property
    def id(self) -> ObjectID:
        return self._file_id


class FileObject(BlobObject):
    file_id: bytes
    fasthash: str
    md5: str
    size: int

    def __init__(self, file_id: bytes, data: Tuple[str, int, str]) -> None:
        super().__init__(file_id)
        assert data[0] is not None and data[0] != 'null', f"INVALID FASTHASH FOR {file_id}!"

        self.file_id = file_id
        self.fasthash = data[0]
        self.size = data[1]
        self.md5 = data[2]

    def __str__(self) -> str:
        return f"FileObject[{self.file_id}, {self.fasthash}, {self.size}]"

    @staticmethod
    def create(fasthash: str, size: int, md5: Optional[str] = None) -> "FileObject":
        file_packed = msgpack.packb((ObjectType.BLOB.value, (fasthash, size, md5)))
        file_id = hashlib.sha1(file_packed).digest()
        return FileObject(file_id=file_id, data=(fasthash, size, md5))

    def __eq__(self, other: "FileObject") -> bool:
        return isinstance(other,
                          FileObject) and self.size == other.size and self.fasthash == other.fasthash and self.md5 == other.md5
