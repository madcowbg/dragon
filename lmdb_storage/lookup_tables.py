import sys
from typing import Iterable, Tuple, List, Callable, Dict

from command.fast_path import FastPosixPath
from lmdb_storage.file_object import FileObject
from lmdb_storage.tree_iteration import SkipFun, CANT_SKIP
from lmdb_storage.tree_object import ObjectType, ObjectID, StoredObject, TreeObject
from lmdb_storage.tree_structure import Objects
from util import format_size
from varint import encode, decode_buffer


def fast_dfs(
        objects: Objects, compressed_path: bytearray,
        obj_id: bytes) -> Iterable[Tuple[bytearray, ObjectType, ObjectID, StoredObject, SkipFun]]:
    if obj_id is None:
        return
    assert type(obj_id) is bytes

    obj = objects[obj_id]
    if obj is None:
        raise ValueError(f"{obj_id} is missing!")

    if not isinstance(obj, TreeObject):
        yield compressed_path, ObjectType.BLOB, obj_id, obj, CANT_SKIP
        return

    should_skip = False

    def skip_children() -> None:
        nonlocal should_skip
        should_skip = True

    yield compressed_path, ObjectType.TREE, obj_id, obj, skip_children
    if should_skip:
        return

    for child_idx, (child_name, child_id) in enumerate(obj.children):
        yield from fast_dfs(objects, compressed_path + encode(child_idx), child_id)


def decode_bytes_to_intpath(packed_lookup_data: bytes, idx: int) -> Tuple[int, List[int]]:
    cnt, idx = decode_buffer(packed_lookup_data, idx)
    last_idx = idx + cnt  # that's how much we have to decode
    path: List[int] = list()
    while idx < last_idx:
        path_part, idx = decode_buffer(packed_lookup_data, idx)
        path.append(path_part)
    return idx, path


class LookupTable[LookupData]:
    def __init__(self, packed_lookup_data: bytes, reader: Callable[[bytes, int], LookupData]):
        self.root_id = bytes(packed_lookup_data[:20])

        idx = 20
        lookup_table: Dict[bytes, List[int] | List[LookupData]] = dict()
        while idx < len(packed_lookup_data):
            prefix = bytes(packed_lookup_data[idx:idx + 20])
            assert type(prefix) is bytes
            idx += 20

            if prefix not in lookup_table:
                lookup_table[prefix] = [idx]
            else:
                lookup_table[prefix].append(idx)

            cnt, idx = decode_buffer(packed_lookup_data, idx)  # find size of path
            idx += cnt

        self._lookup_table = lookup_table
        self._packed_lookup_data = packed_lookup_data

        self._decoded_lookup_data = dict()
        self._reader = reader

    def __len__(self) -> int:
        return len(self._lookup_table)

    def __getitem__(self, obj_id: bytearray | bytes) -> Iterable[LookupData]:
        hash_prefix = bytes(obj_id) if isinstance(obj_id, bytearray) else obj_id
        idxs = self._lookup_table[hash_prefix]
        if isinstance(idxs[0], int):  # convert the list of ints to the list of unpacked paths
            idxs = [self._reader(self._packed_lookup_data, idx)[1] for idx in idxs]
            self._lookup_table[hash_prefix] = idxs
        return idxs

    def __contains__(self, obj_id: ObjectID) -> bool:
        return bytes(obj_id) in self._lookup_table

    def keys(self) -> Iterable[bytes]:
        return self._lookup_table.keys()

    def get_paths(self, obj_id: ObjectID, objects: Callable[[ObjectID], StoredObject]) -> Iterable[FastPosixPath]:
        if obj_id not in self:
            return

        for path in self[obj_id]:
            yield get_path_string(self.root_id, path, objects)


def get_path_string(root_id: ObjectID, path: List[int], objects: Callable[[ObjectID], StoredObject]) -> FastPosixPath:
    result = []
    current_id = root_id
    for pi in path:
        current_obj: StoredObject = objects(current_id)
        assert isinstance(current_obj, TreeObject)
        current_obj: TreeObject

        child_name, current_id = current_obj.children[pi]
        result.append(child_name)
    current_obj = objects(current_id)
    assert isinstance(current_obj, FileObject)

    return FastPosixPath(True, '', result)


def compute_lookup_table(objects: Objects, root_id: ObjectID) -> bytearray:
    files = 0
    packed_lookup_data = bytearray(root_id)
    tmp_path: bytearray
    for tmp_path, obj_type, obj_id, stored_obj, _ in fast_dfs(objects, bytearray(), root_id):
        if obj_type == ObjectType.BLOB:
            files += 1
            assert len(obj_id) == 20
            packed_lookup_data += obj_id + encode(len(tmp_path)) + tmp_path

    sys.stdout.write(f"decoded_files: {files}, {format_size(len(packed_lookup_data) // files)} per file\n")
    return packed_lookup_data
