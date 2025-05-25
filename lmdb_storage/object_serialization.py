import msgpack

from lmdb_storage.file_object import BlobObject
from lmdb_storage.tree_structure import StoredObject, ObjectType, TreeObject


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
        return TreeObject(dict(obj_data[1]))
    else:
        raise ValueError(f"Unrecognized type {obj_data[0]}")


def write_stored_object(obj: StoredObject) -> bytes:
    if obj.object_type == ObjectType.BLOB:
        obj: BlobObject
        return msgpack.packb((ObjectType.BLOB.value, (obj.fasthash, obj.size, obj.md5)))
    elif obj.object_type == ObjectType.TREE:
        obj: TreeObject
        return msgpack.packb((ObjectType.TREE.value, list(sorted(obj.children.items()))))
    else:
        raise ValueError(f"Unrecognized type {obj.object_type}")
