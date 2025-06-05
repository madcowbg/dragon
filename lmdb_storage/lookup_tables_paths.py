import hashlib
import logging
import sys
from typing import Iterable, Tuple, Callable

from command.fast_path import FastPosixPath
from lmdb_storage.file_object import FileObject
from lmdb_storage.lookup_tables import LookupTableObjToPaths, CompressedPath

from lmdb_storage.tree_iteration import SkipFun, CANT_SKIP
from lmdb_storage.tree_object import ObjectType, ObjectID, StoredObject, TreeObject, MaybeObjectID
from lmdb_storage.tree_structure import Objects
from util import format_size
from varint import encode, decode_buffer


def fast_compressed_path_dfs(
        objects: Objects, compressed_path: bytearray,
        obj_id: bytes) -> Iterable[Tuple[bytearray, ObjectType, ObjectID, StoredObject, SkipFun]]:
    return fast_path_dfs(objects, compressed_path, obj_id, lambda p, i, c: p + encode(i))


def fast_path_dfs[P](
        objects: Objects, start_path: P, start_id: MaybeObjectID, state_extender: Callable[[P, int, str], P],
) -> Iterable[Tuple[P, ObjectType, ObjectID, StoredObject, SkipFun]]:
    if start_id is None:
        return

    def _fast_path(path: P, obj_id: ObjectID) -> Iterable[Tuple[P, ObjectType, ObjectID, StoredObject, SkipFun]]:
        assert type(obj_id) is bytes

        obj = objects[obj_id]
        if obj is None:
            raise ValueError(f"{obj_id} is missing!")

        if not isinstance(obj, TreeObject):
            yield path, ObjectType.BLOB, obj_id, obj, CANT_SKIP
            return

        should_skip = False

        def skip_children() -> None:
            nonlocal should_skip
            should_skip = True

        yield path, ObjectType.TREE, obj_id, obj, skip_children
        if should_skip:
            return

        for child_idx, (child_name, child_id) in enumerate(obj.children):
            yield from _fast_path(state_extender(path, child_idx, child_name), child_id)

    yield from _fast_path(start_path, start_id)


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


def lookup_paths(
        self: LookupTableObjToPaths[CompressedPath], obj_id: ObjectID,
        objects: Callable[[ObjectID], StoredObject]) -> Iterable[FastPosixPath]:
    if obj_id not in self:
        return

    for path in self[obj_id]:
        yield get_path_string(self.root_id, path, objects)


def get_path_string(
        root_id: ObjectID, path: CompressedPath,
        objects: Callable[[ObjectID], StoredObject]) -> FastPosixPath:
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


def compute_obj_id_to_path_lookup_table(objects: Objects, root_id: MaybeObjectID) -> bytearray:
    files = 0
    packed_lookup_data = bytearray()
    tmp_path: bytearray
    for tmp_path, obj_type, obj_id, stored_obj, _ in fast_compressed_path_dfs(objects, bytearray(), root_id):
        if obj_type == ObjectType.BLOB:
            files += 1
            assert len(obj_id) == 20
            packed_lookup_data += obj_id + encode(len(tmp_path)) + tmp_path

    sys.stdout.write(
        f"decoded_files: {files}, {format_size(len(packed_lookup_data) // files) if files > 0 else 0} per file\n")
    return packed_lookup_data


def compute_obj_id_to_path_difference_lookup_table(
        objects: Objects, existing_in: MaybeObjectID, missing_in: MaybeObjectID) -> bytearray:
    """Computes what files need to be deleted."""
    files = 0
    packed_lookup_data = bytearray()
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


def decode_bytes_to_intpath(packed_lookup_data: bytes, idx: int) -> Tuple[int, CompressedPath]:
    cnt, idx = decode_buffer(packed_lookup_data, idx)
    last_idx = idx + cnt  # that's how much we have to decode
    path: CompressedPath = list()
    while idx < last_idx:
        path_part, idx = decode_buffer(packed_lookup_data, idx)
        path.append(path_part)
    return idx, path


def decode_bytes_to_object_id(packed_lookup_data: bytes, idx: int) -> Tuple[int, bytes]:
    cnt, idx = decode_buffer(packed_lookup_data, idx)
    return idx + cnt, packed_lookup_data[idx:idx + cnt]


def compute_path_lookup_table(objects: Objects, root_id: MaybeObjectID) -> bytearray:
    files = 0
    packed_lookup_data = bytearray()
    for tmp_path, obj_type, obj_id, stored_obj, _ in fast_path_dfs(objects, "", root_id, lambda p, i, c: p + c):
        if obj_type == ObjectType.BLOB:
            files += 1
            digested_path = hashlib.sha1(tmp_path.encode()).digest()
            assert len(digested_path) == 20

            packed_lookup_data += digested_path + encode(len(obj_id)) + obj_id

    sys.stdout.write(
        f"decoded_paths: {files}, {format_size(len(packed_lookup_data) // files) if files > 0 else 0} per file\n")
    return packed_lookup_data
