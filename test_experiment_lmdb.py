import logging
import os
import pathlib
import unittest
from typing import List, Iterable
from unittest.async_case import IsolatedAsyncioTestCase

import msgpack
from alive_progress import alive_it

from command.test_command_file_changing_flows import populate
from command.test_hoard_command import populate_repotypes, init_complex_hoard
from contents.hoard_props import HoardFileStatus
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.tree_diff import zip_trees
from lmdb_storage.tree_structure import FileObject, ExpandableTreeObject
from sql_util import sqlite3_standard
from util import FIRST_VALUE


def _list_uuids(conn) -> List[str]:
    curr = conn.cursor()
    curr.row_factory = FIRST_VALUE
    all_repos = list(curr.execute("SELECT uuid FROM fspresence GROUP BY uuid ORDER BY uuid"))
    return all_repos


@unittest.skipUnless(os.getenv('LMDB_DEVELOPMENT_TEST'), reason="Uses total hoard")
class MyTestCase(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = "./tests"
        self.obj_storage_path = f"{self.tmpdir}/test/example.lmdb"
        pathlib.Path(self.obj_storage_path).parent.mkdir(parents=True, exist_ok=True)

        populate(self.tmpdir)
        populate_repotypes(self.tmpdir)

    # @unittest.skip("Made to run only locally to benchmark")
    async def test_create_lmdb(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir)

        await hoard_cmd.contents.pull(all=True)

        env = ObjectStorage(self.obj_storage_path)
        path = rf"{hoard_cmd.hoard.hoardpath}\hoard.contents"
        is_readonly = True

        with sqlite3_standard(f"file:{path}{'?mode=ro' if is_readonly else ''}", uri=True) as conn:
            def create_file_tuple(cursor, row):
                fullpath, fasthash, size = row
                return fullpath, FileObject.create(fasthash, size)

            curr = conn.cursor()
            curr.row_factory = create_file_tuple

            all_data = list(alive_it(
                curr.execute("SELECT fullpath, fasthash, size FROM fsobject ORDER BY fullpath"),
                title="loading from sqlite"))

            with env.objects(write=True) as objects:
                root_id = objects.mktree_from_tuples(all_data)

            with env.repos_txn(write=True) as txn:
                txn.put("HEAD".encode(), root_id)

            all_repos = _list_uuids(conn)
            logging.info("# repos: {}".format(len(all_repos)))

            for uuid in all_repos:
                per_uuid_data = curr.execute(
                    "SELECT fullpath, fasthash, size FROM fsobject "
                    "WHERE EXISTS ("
                    "  SELECT 1 FROM fspresence "
                    "  WHERE fsobject.fsobject_id == fspresence.fsobject_id AND uuid = ? AND status = ?)"
                    "ORDER BY fullpath",
                    (uuid, HoardFileStatus.AVAILABLE.value))

                uuid_data = list(alive_it(per_uuid_data, title=f"Loading for uuid {uuid}"))

                with env.objects(write=True) as objects:
                    uuid_root_id = objects.mktree_from_tuples(uuid_data)

                with env.repos_txn(write=True) as txn:
                    txn.put(uuid.encode(), uuid_root_id)

    # @unittest.skip("Made to run only locally to benchmark")
    def test_fully_load_lmdb(self):
        env = ObjectStorage(self.obj_storage_path)  # , map_size=(1 << 30) // 4)

        with env.repos_txn(write=False) as txn:
            root_id = txn.get("HEAD".encode())

        with env.objects(write=False) as objects:
            root = ExpandableTreeObject.create(root_id, objects)

            def all_files(tree: ExpandableTreeObject) -> Iterable[FileObject]:
                yield from tree.files.values()
                for subtree in tree.dirs.values():
                    yield from all_files(subtree)

            all_files = list(alive_it(all_files(root), title="loading from lmdb..."))
            logging.warning(f"# all_files: {len(all_files)}")

    # @unittest.skip("Made to run only locally to benchmark")
    def test_dump_lmdb(self):
        env = ObjectStorage(self.obj_storage_path)  # , map_size=(1 << 30) // 4)
        with env.objects_txn(write=False) as txn:
            with txn.cursor() as curr:
                with open("test/dbdump.msgpack", "wb") as f:
                    # msgpack.dump(((k, v) for k, v in alive_it(curr, title="loading from lmdb...")), f)
                    msgpack.dump(list(((k, v) for k, v in curr)), f)

    def test_tree_compare(self):
        env = ObjectStorage(self.obj_storage_path)
        # uuid = "f8f42230-2dc7-48f4-b1b7-5298a309e3fd"
        # uuid = "726613d5-2b92-451e-b863-833a579456f5"
        uuid = "766c936d-fbe9-4cf0-b2df-e47b40888581"

        with env.repos_txn(write=False) as txn:
            hoard_id = txn.get("HEAD".encode())
            repo_id = txn.get(uuid.encode())

        with env.objects(write=False) as objects:
            diffs = [
                (path, diff_type.value)
                for path, diff_type, left_id, right_id, should_skip
                in alive_it(zip_trees(objects, "root", hoard_id, repo_id))]
            self.assertEqual([
                ('root', 'different'),
                ('root/test.me.1', 'same'),
                ('root/test.me.4', 'same'),
                ('root/test.me.5', 'right_missing'),
                ('root/wat', 'different'),
                ('root/wat/test.me.2', 'same'),
                ('root/wat/test.me.3', 'same'),
                ('root/wat/test.me.6', 'right_missing')], diffs)

            diffs = []
            for path, diff_type, left_id, right_id, should_skip in zip_trees(objects, "root", hoard_id, repo_id):
                if path == 'root/wat':
                    should_skip()
                diffs.append((path, diff_type.value))
            self.assertEqual([
                ('root', 'different'),
                ('root/test.me.1', 'same'),
                ('root/test.me.4', 'same'),
                ('root/test.me.5', 'right_missing'),
                ('root/wat', 'different')], diffs)


def test_gc(self):
    objs = ObjectStorage(self.obj_storage_path)
    objs.gc()


if __name__ == '__main__':
    unittest.main()
