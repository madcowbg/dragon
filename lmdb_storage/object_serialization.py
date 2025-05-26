import enum
import hashlib
import logging
import struct
from typing import Dict, Tuple, Iterable

import msgpack

from lmdb_storage.file_object import BlobObject
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
    V0 = 0  # legacy format with msgpack
    V1 = 1


def find_object_data_version(obj_packed):
    if obj_packed[0] == 146:  # for backward compatibility
        return BlobStorageFormat.V0
    else:
        assert obj_packed[0] == 0xff, f"Undetermined format version: first byte is {obj_packed[0]}!"
        v = int(obj_packed[1])
        assert v <= BlobStorageFormat.V1
        return BlobStorageFormat(v)


def read_stored_object(obj_id: bytes, obj_packed: bytes) -> StoredObject:
    version = find_object_data_version(obj_packed)
    if version == BlobStorageFormat.V0:
        obj_data = msgpack.loads(obj_packed)
        if obj_data[0] == ObjectType.BLOB.value:
            return BlobObject(obj_id, obj_data[1])
        elif obj_data[0] == ObjectType.TREE.value:
            return TreeObject(obj_id, dict(obj_data[1]))
        else:
            raise ValueError(f"Unrecognized type {obj_data[0]}")
    elif version == BlobStorageFormat.V1:
        assert obj_packed[0] == 0xff, "Missing 0xff in front"
        assert obj_packed[1] == 1, "Bad format!"
        obj_type = obj_packed[2]
        idx = 3
        if obj_type == ObjectType.TREE.value:
            children = dict()
            len_children = struct.unpack_from("I", buffer=obj_packed, offset=idx)[0]
            idx += 4
            for _ in range(len_children):
                name_len = struct.unpack_from("I", buffer=obj_packed, offset=idx)[0]
                idx += 4
                child_name_bytes = struct.unpack_from(f"{name_len}s", buffer=obj_packed, offset=idx)[0]
                idx += name_len
                obj_id_len = struct.unpack_from("I", buffer=obj_packed, offset=idx)[0]
                if obj_id_len != 32:
                    logging.warning(f"Wrong object ID length {obj_id_len}!")
                idx += 4
                obj_id = struct.unpack_from(f"{obj_id_len}s", buffer=obj_packed, offset=idx)[0]
                assert isinstance(obj_id, bytes)
                idx += obj_id_len

                children[child_name_bytes.decode("utf-8")] = obj_id
            assert idx == len(obj_packed), f"Did not read whole string, read {idx} from {len(obj_packed)}"

            return TreeObject(obj_id, children)
        elif obj_type == ObjectType.BLOB.value:
            raise NotImplementedError('V1 is not implemented yet')
        else:
            raise ValueError(f"Unrecognized type {obj_type}!")
    else:
        raise NotImplementedError(f"Not implemented version {version}")


def _serialize_tree_object(tree_obj_builder: Iterable[Tuple[str, ObjectID]]) -> bytes:
    children = sorted(tree_obj_builder)
    result = struct.pack("=BBBI", 0xff, BlobStorageFormat.V1, ObjectType.TREE.value, len(children))
    for child_name, child_id in children:
        child_name_bytearray = child_name.encode()
        result += struct.pack(
            f"=I{len(child_name_bytearray)}sI{len(child_id)}s",
            len(child_name_bytearray), child_name_bytearray,
            len(child_id), child_id)

    return result


def construct_tree_object(tree_obj_builder: TreeObjectBuilder) -> TreeObject:
    assert isinstance(tree_obj_builder, Dict)
    serialized = _serialize_tree_object(tree_obj_builder.items())
    return TreeObject(hashlib.sha1(serialized).digest(), tree_obj_builder)


def write_stored_object(obj: StoredObject) -> bytes:
    if obj.object_type == ObjectType.BLOB:
        obj: BlobObject
        return msgpack.packb((ObjectType.BLOB.value, (obj.fasthash, obj.size, obj.md5)))
    elif obj.object_type == ObjectType.TREE:
        obj: TreeObject
        return _serialize_tree_object(obj.children)
    else:
        raise ValueError(f"Unrecognized type {obj.object_type}")
