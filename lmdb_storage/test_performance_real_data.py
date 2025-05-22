import os
import sys
import unittest
from datetime import datetime
from unittest.async_case import IsolatedAsyncioTestCase

from command.fast_path import FastPosixPath
from command.hoard import Hoard
from contents.hoard import HoardFilesIterator
from dragon import TotalCommand
from lmdb_storage.file_object import FileObject
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.tree_structure import TreeObject


@unittest.skipUnless(os.getenv('RUN_LENGTHY_TESTS'), reason="Lengthy test")
class TestPerformance(IsolatedAsyncioTestCase):
    async def test_load_all_and_decode(self):
        path = r"C:\Users\Bono\hoard\hoard.contents.lmdb"
        env = ObjectStorage(path)

        sys.stdout.write(f"\nstart: {datetime.now()}\n")
        decoded_file, decoded_folder = 0, 0
        with env.objects(write=False) as objects:
            for k, v in objects.txn.cursor():
                value = objects[k]
                if isinstance(value, FileObject):
                    decoded_file += 1
                elif isinstance(value, TreeObject):
                    decoded_folder += 1

        sys.stdout.write(
            f"\nend:   {datetime.now()} | decoded_files: {decoded_file}, decoded_folders: {decoded_folder}\n")

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
            count_nondeleted = query.count_non_deleted(FastPosixPath("."))
            sys.stdout.write(f"\ncount_nondeleted:  {count_nondeleted} at {datetime.now()}\n")
            count_without_src = query.num_without_source(FastPosixPath("."))
            sys.stdout.write(f"\ncount_without_src: {count_without_src} at {datetime.now()}\n")

            sys.stdout.write(f"\nend:   {datetime.now()}\n")

        sys.stdout.write(f"\nclosed:   {datetime.now()}\n")

    async def test_load_all_and_get_size(self):
        path = r"C:\Users\Bono\hoard"

        hoard_cmd = TotalCommand(path=path).hoard

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: e10e064c040fbe9395a889b089a84abc9d51027c\n'
            '|Num Files                |total     |available |\n'
            '|GoPro@NAS                |      1579|      1579|\n'
            '|Insta360@NAS             |      3820|      3820|\n'
            '|Videos@NAS               |      5515|      5515|\n'
            '|cloud-drive@laptop       |    156010|    156010|\n'
            '\n'
            '|Size                     |total     |available |\n'
            '|GoPro@NAS                |     1.3TB|     1.3TB|\n'
            '|Insta360@NAS             |     8.5TB|     8.5TB|\n'
            '|Videos@NAS               |     1.7TB|     1.7TB|\n'
            '|cloud-drive@laptop       |    64.9GB|    64.9GB|\n'), res)
