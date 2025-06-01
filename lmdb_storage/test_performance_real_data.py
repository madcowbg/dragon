import os
import sys
import unittest
from datetime import datetime
from timeit import default_timer
from typing import Tuple, Iterable, List, Dict, Callable
from unittest.async_case import IsolatedAsyncioTestCase

import varint

from command.fast_path import FastPosixPath
from command.hoard import Hoard
from contents.hoard import HoardFilesIterator, HoardContents
from dragon import TotalCommand
from lmdb_storage.file_object import FileObject
from lmdb_storage.tree_iteration import SkipFun, CANT_SKIP
from lmdb_storage.tree_object import ObjectType, ObjectID, StoredObject, TreeObject
from lmdb_storage.tree_structure import Objects
from util import format_size


def force_iterating_over(hoard_contents: HoardContents) -> Tuple[int, int]:
    decoded_file, decoded_folder = 0, 0
    with hoard_contents.env.objects(write=False) as objects:
        for k, v in objects.txn.cursor():
            value = objects[k]
            if value.object_type == ObjectType.BLOB:
                decoded_file += 1
            elif value.object_type == ObjectType.TREE:
                decoded_folder += 1
    return decoded_file, decoded_folder


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

    for child_idx, child_name in enumerate(obj.sorted_children_names):
        child_id = obj.get(child_name)
        yield from fast_dfs(objects, compressed_path + varint.encode(child_idx), child_id)


def decode_buffer(buffer: bytes, offset: int) -> Tuple[int, int]:
    """Read a varint from `stream`"""
    shift = 0
    result = 0
    while True:
        i = buffer[offset]
        offset += 1
        result |= (i & 0x7f) << shift
        shift += 7
        if not (i & 0x80):
            break

    return result, offset


def _decode_path(packed_lookup_data, idx):
    cnt, idx = decode_buffer(packed_lookup_data, idx)
    last_idx = idx + cnt  # that's how much we have to decode
    path: List[int] = list()
    while idx < last_idx:
        path_part, idx = decode_buffer(packed_lookup_data, idx)
        path.append(path_part)
    return idx, path


class LookupTable:
    def __init__(self, packed_lookup_data: bytes):
        idx = 0
        lookup_table: Dict[int, List[int] | List[List[int]]] = dict()
        while idx < len(packed_lookup_data):
            prefix = int.from_bytes(packed_lookup_data[idx:idx + 4], signed=False)
            assert type(prefix) is int
            idx += 4

            if prefix not in lookup_table:
                lookup_table[prefix] = [idx]
            else:
                lookup_table[prefix].append(idx)

            cnt, idx = decode_buffer(packed_lookup_data, idx) # find size of path
            idx += cnt

        self._lookup_table = lookup_table
        self._packed_lookup_data = packed_lookup_data

        self._decoded_lookup_data = dict()

    def __getitem__(self, obj_id: ObjectID) -> Iterable[List[int]]:
        return self.with_prefix(int.from_bytes(obj_id[:4], signed=False))

    def with_prefix(self, hash_prefix: int) -> Iterable[List[int]]:
        idxs = self._lookup_table[hash_prefix]
        if isinstance(idxs[0], int):  # convert the list of ints to the list of unpacked paths
            idxs = [_decode_path(self._packed_lookup_data, idx)[1] for idx in idxs]
            self._lookup_table[hash_prefix] = idxs
        return idxs

    def __contains__(self, obj_id: ObjectID) -> bool:
        return int.from_bytes(obj_id[:4], signed=False) in self._lookup_table

    def keys(self) -> Iterable[int]:
        return self._lookup_table.keys()


def follow_path(objects: Callable[[ObjectID], StoredObject], root_id: ObjectID, path: List[int]) -> FileObject:
    current_id = root_id
    for pi in path:
        current_obj: StoredObject = objects(current_id)
        assert isinstance(current_obj, TreeObject)
        current_obj: TreeObject

        child_name = current_obj.sorted_children_names[pi]
        current_id = current_obj.get(child_name)

    current_obj = objects(current_id)
    assert isinstance(current_obj, FileObject)
    return current_obj


def find_paths(read_with_cache, lookup_table, root_id, obj_id) -> Iterable[FileObject]:
    if obj_id not in lookup_table:
        return

    candidates = lookup_table[obj_id]
    for c_path in candidates:
        c_obj = follow_path(read_with_cache, root_id, c_path)

        if c_obj.file_id == obj_id:
            yield c_path


@unittest.skipUnless(os.getenv('RUN_LENGTHY_TESTS'), reason="Lengthy test")
class TestPerformance(IsolatedAsyncioTestCase):
    async def test_load_all_and_decode(self):
        path = r"C:\Users\Bono\hoard"
        hoard = Hoard(path)
        async with hoard.open_contents(create_missing=False) as hoard_contents:
            decoded_file, decoded_folder = force_iterating_over(hoard_contents)  # just force loading

            start = default_timer()
            sys.stdout.write(f"\nstart: {datetime.now()}\n")
            decoded_file, decoded_folder = force_iterating_over(hoard_contents)

            sys.stdout.write(
                f"\nend:   {datetime.now()} | time: {default_timer() - start}s decoded_files: {decoded_file}, decoded_folders: {decoded_folder}\n")

    async def test_fast_approximate_mapping(self):
        path = r"C:\Users\Bono\hoard"
        hoard = Hoard(path)
        async with hoard.open_contents(create_missing=False) as hoard_contents:
            decoded_file, decoded_folder = force_iterating_over(hoard_contents)  # just force loading

            root_id = hoard_contents.env.roots(write=False)["HOARD"].desired

            with hoard_contents.env.objects(write=False) as objects:

                start = default_timer()

                files = 0
                packed_lookup_data = bytearray()
                tmp_path: bytearray
                for tmp_path, obj_type, obj_id, stored_obj, _ in fast_dfs(objects, bytearray(), root_id):
                    if obj_type == ObjectType.BLOB:
                        files += 1
                        packed_lookup_data += obj_id[:4] + varint.encode(len(tmp_path)) + tmp_path

                sys.stdout.write(f"\ncreating packed data time: {default_timer() - start}s\n")
                sys.stdout.write(f"packed_data: {format_size(len(packed_lookup_data))}\n")
                sys.stdout.write(f"decoded_files: {files}, {format_size(len(packed_lookup_data) // files)} per file\n")

                start = default_timer()
                lookup_table = LookupTable(packed_lookup_data)
                sys.stdout.write(f"\nread lookup table time: {default_timer() - start}s\n")
                sys.stdout.write(
                    f"decoded_entries: {files}, size {format_size(len(packed_lookup_data) // files)} per file.\n")

                hash_prefix = list(sorted(lookup_table.keys()))[99]
                one_path = list(lookup_table.with_prefix(hash_prefix))[0]

                current_obj = follow_path(objects.__getitem__, root_id, one_path)

                assert int.from_bytes(current_obj.file_id[:4], signed=False) == hash_prefix

                start = default_timer()
                files = 0
                collisions = 0

                cache = dict()
                def read_with_cache(obj_id: ObjectID) -> StoredObject:
                    if obj_id not in cache:
                        new_val = objects[obj_id]
                        cache[obj_id] = new_val
                        return new_val
                    else:
                        return cache[obj_id]

                for tmp_path, obj_type, obj_id, stored_obj, _ in fast_dfs(objects, bytearray(), root_id):
                    if obj_type == ObjectType.BLOB:
                        files += 1

                        assert obj_id in lookup_table, "All hoard files should be here!"

                        candidates = lookup_table[obj_id]
                        found = 0
                        for c_path in candidates:
                            c_obj = follow_path(read_with_cache, root_id, c_path)

                            if c_obj.file_id == obj_id:
                                found += 1
                            else:
                                collisions += 1
                        assert found > 0

                time = default_timer() - start
                sys.stdout.write(f"\nlooked up all hashes: {time}s, {1000 * (time / files)}ms per file\n")
                sys.stdout.write(f" {files} files, with {collisions} collisions!\n")

                start = default_timer()
                paths_in_hoard = 0
                files_in_hoard = 0
                files_not_in_hoard = 0
                for k, v in objects.txn.cursor():
                    value = objects[k]
                    if value.object_type == ObjectType.BLOB:

                        paths = list(find_paths(read_with_cache, lookup_table, root_id, k))
                        paths_in_hoard += len(paths)

                        if len(paths) == 0:
                            files_not_in_hoard += 1
                        else:
                            files_in_hoard += 1

                time = default_timer() - start
                sys.stdout.write(
                    f"\nlooked up all hashes in db: {time}s,"
                    f" {1000 * (time / (files_in_hoard + files_not_in_hoard))}ms per file\n")
                sys.stdout.write(
                    f" {files_in_hoard} files, {paths_in_hoard} paths and {files_not_in_hoard} not in hoard.\n")

    async def test_iterate_over(self):
        path = r"C:\Users\Bono\hoard"
        # env = ObjectStorage(path + "\hoard.contents.lmdb")
        hoard = Hoard(path)

        decoded_file, decoded_folder, repo_files = 0, 0, 0
        sys.stdout.write(f"\nto open: {datetime.now()}\n")
        async with hoard.open_contents(create_missing=False) as hoard_contents:
            sys.stdout.write(f"\nstart: {datetime.now()}\n")
            for file, prop in HoardFilesIterator.all(hoard_contents):
                decoded_file += 1
                repo_files += len(prop.presence.items())

            sys.stdout.write(f"\nend:   {datetime.now()} | decoded_files: {decoded_file}, repo_files: {repo_files}\n")

        sys.stdout.write(f"\nclosed:   {datetime.now()} | decoded_files: {decoded_file}\n")

    async def test_query(self):
        path = r"C:\Users\Bono\hoard"
        # env = ObjectStorage(path + "\hoard.contents.lmdb")
        hoard = Hoard(path)

        sys.stdout.write(f"\nto open: {datetime.now()}\n")
        async with hoard.open_contents(create_missing=False) as hoard_contents:
            sys.stdout.write(f"\nstart: {datetime.now()}\n")
            query = hoard_contents.fsobjects.query
            # query.is_deleted(FastPosixPath("."))
            sys.stdout.write(f"\nstats start: {datetime.now()}\n")
            count_nondeleted = query.count_non_deleted(FastPosixPath("/"))
            sys.stdout.write(f"\ncount_nondeleted:  {count_nondeleted} at {datetime.now()}\n")
            count_without_src = query.num_without_source(FastPosixPath("/"))
            sys.stdout.write(f"\ncount_without_src: {count_without_src} at {datetime.now()}\n")

            sys.stdout.write(f"\nend:   {datetime.now()}\n")

        sys.stdout.write(f"\nclosed:   {datetime.now()}\n")

    async def test_load_all_and_get_size(self):
        path = r"C:\Users\Bono\hoard"

        hoard_cmd = TotalCommand(path=path).hoard

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: 5da1fcab7ec9b5595e1da8e51d0036f8ea78d120\n'
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|GoPro@NAS                |      1583|      1583|          |          |\n'
            '|Insta360@NAS             |      3820|      3820|          |          |\n'
            '|Misc@NAS                 |    171622|    171591|        31|          |\n'
            '|Photos@NAS               |    235936|    235936|          |          |\n'
            '|Videos@NAS               |      5928|      5928|          |          |\n'
            '|cloud-drive@laptop       |    156999|    156999|          |          |\n'
            '|euclid-external-hdd      |    136715|        14|          |    136701|\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|GoPro@NAS                |     1.3TB|     1.3TB|          |          |\n'
            '|Insta360@NAS             |     8.5TB|     8.5TB|          |          |\n'
            '|Misc@NAS                 |     1.0TB|     1.0TB|    46.1MB|          |\n'
            '|Photos@NAS               |     1.7TB|     1.7TB|          |          |\n'
            '|Videos@NAS               |     1.7TB|     1.7TB|          |          |\n'
            '|cloud-drive@laptop       |    60.8GB|    60.8GB|          |          |\n'
            '|euclid-external-hdd      |     2.1TB|    29.0GB|          |     2.1TB|\n'), res)
