import logging
import sys
from typing import Iterable, Tuple, List, Callable, Dict

from command.fast_path import FastPosixPath
from lmdb_storage.file_object import FileObject
from lmdb_storage.roots import Root
from lmdb_storage.tree_iteration import SkipFun, CANT_SKIP, ObjectIDs
from lmdb_storage.tree_object import ObjectType, ObjectID, StoredObject, TreeObject, MaybeObjectID
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


def fast_zip_left_dfs(
        objects: Objects, compressed_path: bytearray, left_id: MaybeObjectID, right_id: MaybeObjectID,
        drilldown_same: bool = True) -> Iterable[Tuple[bytearray, StoredObject | None, StoredObject | None, SkipFun]]:
    if left_id is None and right_id is None:
        return  # nothing more to yield

    left_obj = objects[left_id] if left_id else None
    right_obj = objects[right_id] if right_id else None

    if left_id == right_id and not drilldown_same:  # we got same value for all
        yield compressed_path, left_obj, right_obj, CANT_SKIP
        return

    if isinstance(left_obj, TreeObject):
        # has tree
        should_skip = False

        def skip_children() -> None:
            nonlocal should_skip
            should_skip = True

        yield compressed_path, left_obj, right_obj, skip_children

        if should_skip:
            return

        for child_idx, (child_name, _) in enumerate(left_obj.children):
            yield from fast_zip_left_dfs(
                objects, compressed_path + encode(child_idx),
                left_obj.get(child_name) if isinstance(left_obj, TreeObject) else None,
                right_obj.get(child_name) if isinstance(right_obj, TreeObject) else None,
                drilldown_same)
    else:
        # only one or more files
        yield compressed_path, left_obj, right_obj, CANT_SKIP


def decode_bytes_to_intpath(packed_lookup_data: bytes, idx: int) -> Tuple[int, List[int]]:
    cnt, idx = decode_buffer(packed_lookup_data, idx)
    last_idx = idx + cnt  # that's how much we have to decode
    path: List[int] = list()
    while idx < last_idx:
        path_part, idx = decode_buffer(packed_lookup_data, idx)
        path.append(path_part)
    return idx, path


def _read_packed[LookupData](packed_lookup_data) -> Tuple[Dict[bytes, List[int] | List[LookupData]], MaybeObjectID]:
    if len(packed_lookup_data) == 0:
        return dict(), None

    root_id = bytes(packed_lookup_data[:20])
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
    return lookup_table, root_id


class LookupTable[LookupData]:
    def __init__(self, packed_lookup_data: bytes, reader: Callable[[bytes, int], LookupData]):
        lookup_table, root_id = _read_packed(packed_lookup_data)

        self.root_id = root_id
        self._lookup_table = lookup_table
        self._packed_lookup_data = packed_lookup_data

        self._decoded_lookup_data = dict()
        self._reader = reader

    def __str__(self):
        return f"LookupTable[{len(self)}, root_id={self.root_id.hex() if self.root_id else None}]"

    def __len__(self) -> int:
        return len(self._lookup_table)

    def __getitem__(self, obj_id: bytearray | bytes) -> List[LookupData]:
        hash_prefix = bytes(obj_id) if isinstance(obj_id, bytearray) else obj_id
        if hash_prefix not in self._lookup_table:
            return []

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


def compute_lookup_table(objects: Objects, root_id: MaybeObjectID) -> bytearray:
    if root_id is None:
        return bytearray()

    files = 0
    packed_lookup_data = bytearray(root_id)
    tmp_path: bytearray
    for tmp_path, obj_type, obj_id, stored_obj, _ in fast_dfs(objects, bytearray(), root_id):
        if obj_type == ObjectType.BLOB:
            files += 1
            assert len(obj_id) == 20
            packed_lookup_data += obj_id + encode(len(tmp_path)) + tmp_path

    sys.stdout.write(
        f"decoded_files: {files}, {format_size(len(packed_lookup_data) // files) if files > 0 else 0} per file\n")
    return packed_lookup_data


def compute_difference_lookup_table(objects: Objects, existing_in: MaybeObjectID, missing_in: MaybeObjectID) -> bytearray:
    """Computes what files need to be deleted."""
    if existing_in is None:
        return bytearray()

    files = 0
    packed_lookup_data = bytearray(existing_in)  # all files to be deleted are from the current tree
    tmp_path: bytearray
    for tmp_path, existing_in_obj, missing_in_obj, _ \
            in fast_zip_left_dfs(objects, bytearray(), existing_in, missing_in, drilldown_same=False):
        if existing_in_obj is None:
            continue  # skip missing in current tree or not deleted from desired

        if existing_in_obj == missing_in_obj:
            continue

        if missing_in_obj is not None:
            logging.debug("Missing %s is actually just different", missing_in_obj)

        if existing_in_obj.object_type == ObjectType.TREE:
            continue  # skip trees

        current_obj: FileObject

        files += 1
        assert len(existing_in_obj.id) == 20
        packed_lookup_data += existing_in_obj.id + encode(len(tmp_path)) + tmp_path

    sys.stdout.write(
        f"diff tree - decoded_files: {files},"
        f" {format_size(len(packed_lookup_data) // files) if files > 0 else 0} per file\n")
    return packed_lookup_data
