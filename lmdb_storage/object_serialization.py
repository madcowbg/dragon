import enum
import hashlib
from typing import Dict, Tuple, Iterable

import msgpack

from lmdb_storage.file_object import BlobObject, FileObject
from lmdb_storage.tree_object import ObjectType, StoredObject, TreeObject, TreeObjectBuilder, ObjectID


## LMDB object format
#  object_id    blob
#
# blob is file:
#  Type.FILE, (fasthash, size)
#
# blob is tree:
#  Type.TREE, Dict[obj name to object_id]

class BlobStorageFormat(enum.IntEnum):
    V0 = 0


def find_object_data_version(obj_packed):
    if obj_packed[0] == 146:  # for backward compatibility
        return BlobStorageFormat.V0
    else:
        assert obj_packed[0] == 0xff, f"Undetermined format version: first byte is {obj_packed[0]}!"
        v = obj_packed[1]
        assert v < 1
        return BlobStorageFormat(v)


def read_stored_object(obj_id: bytes, obj_packed: bytes) -> StoredObject:
    version = find_object_data_version(obj_packed)
    if version == BlobStorageFormat.V0:
        obj_data = msgpack.loads(obj_packed)  # fixme make this faster by extracting type away
        if obj_data[0] == ObjectType.BLOB.value:
            assert len(obj_data[1]) == 3, len(obj_data[1])
            return FileObject(obj_id, obj_data[1])
        elif obj_data[0] == ObjectType.TREE.value:
            return TreeObject(obj_id, dict(obj_data[1]))
        else:
            raise ValueError(f"Unrecognized type {obj_data[0]}")
    else:
        raise NotImplementedError(f"Not implemented version {version}")


def _serialize_tree_object(tree_obj_builder: Iterable[Tuple[str, ObjectID]]):
    return msgpack.packb((ObjectType.TREE.value, list(sorted(tree_obj_builder))))


def construct_tree_object(tree_obj_builder: TreeObjectBuilder) -> TreeObject:
    assert isinstance(tree_obj_builder, Dict)
    serialized = _serialize_tree_object(tree_obj_builder.items())
    return TreeObject(hashlib.sha1(serialized).digest(), tree_obj_builder)


def write_stored_object(obj: StoredObject) -> bytes:
    if obj.object_type == ObjectType.BLOB:
        assert isinstance(obj, FileObject)
        return msgpack.packb((ObjectType.BLOB.value, (obj.fasthash, obj.size, obj.md5)))
    elif obj.object_type == ObjectType.TREE:
        obj: TreeObject
        return _serialize_tree_object(obj.children)
    else:
        raise ValueError(f"Unrecognized type {obj.object_type}")
