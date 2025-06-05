import os
import sys
import unittest
from datetime import datetime
from timeit import default_timer
from typing import Tuple, Iterable, List, Callable
from unittest.async_case import IsolatedAsyncioTestCase

import varint

from command.fast_path import FastPosixPath
from command.hoard import Hoard
from contents.hoard import HoardFilesIterator, HoardContents
from dragon import TotalCommand
from lmdb_storage.file_object import FileObject
from lmdb_storage.lookup_tables import LookupTable
from lmdb_storage.lookup_tables_paths import fast_dfs, lookup_paths, get_path_string, compute_obj_id_to_path_lookup_table, \
    decode_bytes_to_intpath
from lmdb_storage.tree_object import ObjectType, ObjectID, StoredObject, TreeObject
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


def _resolve(self: LookupTable[List[int]], obj_id: ObjectID, objects: Callable[[ObjectID], StoredObject]) -> Iterable[
    Tuple[List[int], FileObject]]:
    if obj_id not in self:
        return

    paths = self[obj_id]

    for path in paths:
        yield path, _follow_path(self, path, objects)


def _follow_path(self: LookupTable[List[int]], path: List[int],
                 objects: Callable[[ObjectID], StoredObject]) -> FileObject:
    current_id = self.root_id
    for pi in path:
        current_obj: StoredObject = objects(current_id)
        assert isinstance(current_obj, TreeObject)
        current_obj: TreeObject

        child_name, current_id = current_obj.children[pi]
    current_obj = objects(current_id)
    assert isinstance(current_obj, FileObject)
    return current_obj


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

                packed_lookup_data = compute_obj_id_to_path_lookup_table(objects, root_id)

                sys.stdout.write(f"\ncreating packed data time: {default_timer() - start}s\n")
                sys.stdout.write(f"packed_data: {format_size(len(packed_lookup_data))}\n")

                start = default_timer()
                lookup_table = LookupTable[List[int]](packed_lookup_data, decode_bytes_to_intpath)

                sys.stdout.write(f"\nread lookup table time: {default_timer() - start}s\n")
                sys.stdout.write(
                    f"decoded_entries: {len(lookup_table)}, size {format_size(len(packed_lookup_data) // len(lookup_table))} per file.\n")

                hash_prefix = list(sorted(lookup_table.keys()))[99]
                one_path = list(lookup_table[hash_prefix])[0]

                current_obj = _follow_path(lookup_table, one_path, objects.__getitem__)

                assert get_path_string(lookup_table.root_id, one_path, objects.__getitem__) \
                       == FastPosixPath(
                    True, '', [
                        'Misc', 'cloud-drive', 'Projects', 'git-annex', 'doc', 'design', 'assistant', 'blog',
                        'day_249__quiet_day.mdwn'])

                assert current_obj.file_id == hash_prefix

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

                # start = default_timer()
                # files = 0
                # for tmp_path, obj_type, obj_id, stored_obj, _ in fast_dfs(objects, bytearray(), root_id):
                #     if obj_type == ObjectType.BLOB:
                #         files += 1
                #         candidates = lookup_table[obj_id]
                #
                #         for c_path in candidates:
                #             c_obj = follow_path(read_with_cache, root_id, c_path)
                #
                # time = default_timer() - start
                # sys.stdout.write(f"\nheating cache: {time}s, {1000 * (time / files)}ms per file\n")

                start = default_timer()
                files = 0
                all_files = list()
                for tmp_path, obj_type, obj_id, stored_obj, _ in fast_dfs(objects, bytearray(), root_id):
                    if obj_type == ObjectType.BLOB:
                        all_files.append(obj_id)
                #
                # random.shuffle(all_files)
                #
                # for file_id in sorted(all_files):
                for file_id in all_files:
                    obj_type = objects[file_id].object_type
                    if obj_type == ObjectType.BLOB:
                        files += 1

                        assert obj_id in lookup_table, "All hoard files should be here!"

                        candidates = list(lookup_table[obj_id])
                        assert len(candidates) > 0, "All hoard files should be found!"

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
                        paths_and_files = list(_resolve(lookup_table, k, read_with_cache))
                        paths_in_hoard += len(paths_and_files)

                        paths_as_string = list(lookup_paths(lookup_table, k, read_with_cache))
                        assert len(paths_as_string) == len(paths_and_files)

                        if len(paths_and_files) == 0:
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
