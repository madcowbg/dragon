import hashlib
from typing import Dict

import msgpack

from lmdb_storage.file_object import BlobObject
from lmdb_storage.tree_object import ObjectType, StoredObject, TreeObject, TreeObjectBuilder


## LMDB object format
#  object_id    blob
#
# blob is file:
#  Type.FILE, (fasthash, size)
#
# blob is tree:
#  Type.TREE, Dict[obj name to object_id]


def read_stored_object(obj_id: bytes, obj_packed: bytes) -> StoredObject:
    obj_data = msgpack.loads(obj_packed)  # fixme make this faster by extracting type away
    if obj_data[0] == ObjectType.BLOB.value:
        return BlobObject(obj_id, obj_data[1])
    elif obj_data[0] == ObjectType.TREE.value:
        return TreeObject(obj_id, dict(obj_data[1]))
    else:
        raise ValueError(f"Unrecognized type {obj_data[0]}")



def construct_tree_object(tree_obj_builder: TreeObjectBuilder) -> TreeObject:
    assert isinstance(tree_obj_builder, Dict)
    serialized = msgpack.packb((ObjectType.TREE.value, list(sorted(tree_obj_builder.items()))))
    return TreeObject(hashlib.sha1(serialized).digest(), tree_obj_builder)


def write_stored_object(obj: StoredObject) -> bytes:
    if obj.object_type == ObjectType.BLOB:
        obj: BlobObject
        return msgpack.packb((ObjectType.BLOB.value, (obj.fasthash, obj.size, obj.md5)))
    elif obj.object_type == ObjectType.TREE:
        obj: TreeObject
        return msgpack.packb((ObjectType.TREE.value, list(sorted(obj.children))))
    else:
        raise ValueError(f"Unrecognized type {obj.object_type}")


