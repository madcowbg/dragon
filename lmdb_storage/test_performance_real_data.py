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
from lmdb_storage.tree_structure import TreeObject, ObjectType


@unittest.skipUnless(os.getenv('RUN_LENGTHY_TESTS'), reason="Lengthy test")
class TestPerformance(IsolatedAsyncioTestCase):
    async def test_load_all_and_decode(self):
        path = r"C:\Users\Bono\hoard\hoard.contents.lmdb"
        with ObjectStorage(path) as env:

            sys.stdout.write(f"\nstart: {datetime.now()}\n")
            decoded_file, decoded_folder = 0, 0
            with env.objects(write=False) as objects:
                for k, v in objects.txn.cursor():
                    value = objects[k]
                    if value.object_type == ObjectType.BLOB:
                        decoded_file += 1
                    elif value.object_type == ObjectType.TREE:
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
            'Root: 9a4629ceade3186f608684211231b6dffd7992d0\n'
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|GoPro@NAS                |      1579|      1579|          |          |\n'
            '|Insta360@NAS             |      3820|      3820|          |          |\n'
            '|Misc@NAS                 |     14479|     14479|          |          |\n'
            '|Videos@NAS               |      5625|      5625|          |          |\n'
            '|backups-hdd-01           |    150350|      2892|          |    147458|\n'
            '|backups-hdd-02           |     61837|       574|          |     61263|\n'
            '|backups-vol-01           |      1920|      1920|          |          |\n'
            '|backups-vol-02           |      2465|      2365|          |       100|\n'
            '|backups-vol-03           |       671|       447|          |       224|\n'
            '|backups-vol-04           |      2541|      2262|          |       279|\n'
            '|backups-vol-05           |    215560|       323|          |    215237|\n'
            '|cloud-drive@laptop       |    156010|    156010|          |          |\n'
            '|euclid-external-hdd      |     63967|     13722|       771|     49474|\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|GoPro@NAS                |     1.3TB|     1.3TB|          |          |\n'
            '|Insta360@NAS             |     8.5TB|     8.5TB|          |          |\n'
            '|Misc@NAS                 |   740.1GB|   740.1GB|          |          |\n'
            '|Videos@NAS               |     1.7TB|     1.7TB|          |          |\n'
            '|backups-hdd-01           |     3.6TB|     2.7TB|          |   968.9GB|\n'
            '|backups-hdd-02           |     1.9TB|     1.2TB|          |   731.6GB|\n'
            '|backups-vol-01           |     1.8TB|     1.8TB|          |          |\n'
            '|backups-vol-02           |     1.8TB|     1.4TB|          |   413.3GB|\n'
            '|backups-vol-03           |     1.8TB|     1.8TB|          |     1.5MB|\n'
            '|backups-vol-04           |     1.4TB|     1.1TB|          |   213.5GB|\n'
            '|backups-vol-05           |     2.0TB|   434.0GB|          |     1.6TB|\n'
            '|cloud-drive@laptop       |    64.9GB|    64.9GB|          |          |\n'
            '|euclid-external-hdd      |     1.3TB|   231.4GB|   537.8GB|   523.8GB|\n'), res)
