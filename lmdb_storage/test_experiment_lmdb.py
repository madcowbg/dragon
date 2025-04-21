import binascii
import logging
import pathlib
import shutil
import unittest
from typing import List, Iterable
from unittest.async_case import IsolatedAsyncioTestCase

import msgpack
from alive_progress import alive_it

from command.test_command_file_changing_flows import populate
from command.test_hoard_command import populate_repotypes, init_complex_hoard
from contents.hoard_props import HoardFileStatus
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.tree_iteration import dfs, zip_dfs
from lmdb_storage.tree_structure import ExpandableTreeObject, add_file_object, Objects, remove_file_object
from lmdb_storage.file_object import FileObject
from sql_util import sqlite3_standard
from util import FIRST_VALUE


def _list_uuids(conn) -> List[str]:
    curr = conn.cursor()
    curr.row_factory = FIRST_VALUE
    all_repos = list(curr.execute("SELECT uuid FROM fspresence GROUP BY uuid ORDER BY uuid"))
    return all_repos


def dump_tree(objects: Objects[FileObject], root_id):
    return list((path, obj_type.value) for path, obj_type, _, _, _ in dfs(objects, "$ROOT", root_id))


class VariousLMDBFunctions(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = "./tests"
        self.obj_storage_path = f"{self.tmpdir}/test/example.lmdb"

        pathlib.Path(self.obj_storage_path).parent.mkdir(parents=True, exist_ok=True)

        populate(self.tmpdir)
        populate_repotypes(self.tmpdir)

    async def test_create_lmdb(self):
        shutil.rmtree(self.obj_storage_path, ignore_errors=True)

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

    def test_dump_lmdb(self):
        env = ObjectStorage(self.obj_storage_path)  # , map_size=(1 << 30) // 4)
        with env.objects_txn(write=False) as txn:
            with txn.cursor() as curr:
                with open(self.tmpdir + "/test/dbdump.msgpack", "wb") as f:
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
                in alive_it(zip_dfs(objects, "root", hoard_id, repo_id))]
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
            for path, diff_type, left_id, right_id, should_skip in zip_dfs(objects, "root", hoard_id, repo_id):
                if path == 'root/wat':
                    should_skip()
                diffs.append((path, diff_type.value))
            self.assertEqual([
                ('root', 'different'),
                ('root/test.me.1', 'same'),
                ('root/test.me.4', 'same'),
                ('root/test.me.5', 'right_missing'),
                ('root/wat', 'different')], diffs)

    def test_tree_compare_with_missing_trees(self):
        env = ObjectStorage(self.obj_storage_path)
        left_uuid = "766c936d-fbe9-4cf0-b2df-e47b40888581"
        right_uuid = "a9d34fae-af49-4a26-82c9-e74488470b09"

        with env.repos_txn(write=False) as txn:
            left_id = txn.get(left_uuid.encode())
            right_id = txn.get(right_uuid.encode())

        with env.objects(write=True) as objects:
            left_id = add_file_object(objects, left_id, "newdir/new.file".split("/"), FileObject.create("dasda", 1))
            old_left_id = left_id
            left_id = add_file_object(objects, left_id, "wat/lat/new.file".split("/"), FileObject.create("dasda", 2))

            right_id = add_file_object(
                objects, right_id, "wat/zat/new.file".split("/"), FileObject.create("dadassda", 3))

        with env.objects(write=False) as objects:
            diffs = [
                (path, diff_type.value)
                for path, diff_type, left_id, right_id, should_skip
                in alive_it(zip_dfs(objects, "", left_id, None))]
            self.assertEqual([
                ('', 'right_missing'),
                ('/newdir', 'right_missing'),
                ('/newdir/new.file', 'right_missing'),
                ('/test.me.1', 'right_missing'),
                ('/test.me.4', 'right_missing'),
                ('/wat', 'right_missing'),
                ('/wat/lat', 'right_missing'),
                ('/wat/lat/new.file', 'right_missing'),
                ('/wat/test.me.2', 'right_missing'),
                ('/wat/test.me.3', 'right_missing')], diffs)

            diffs = [
                (path, diff_type.value)
                for path, diff_type, left_id, right_id, should_skip
                in alive_it(zip_dfs(objects, "", left_id, old_left_id, False))]
            self.assertEqual([
                ('', 'different'),
                ('/newdir', 'same'),
                ('/test.me.1', 'same'),
                ('/test.me.4', 'same'),
                ('/wat', 'different'),
                ('/wat/lat', 'right_missing'),
                ('/wat/lat/new.file', 'right_missing'),
                ('/wat/test.me.2', 'same'),
                ('/wat/test.me.3', 'same')], diffs)

            diffs = [
                (path, diff_type.value)
                for path, diff_type, left_id, right_id, should_skip
                in alive_it(zip_dfs(objects, "", left_id, old_left_id, True))]
            self.assertEqual([
                ('', 'different'),
                ('/newdir', 'same'),
                ('/newdir/new.file', 'same'),
                ('/test.me.1', 'same'),
                ('/test.me.4', 'same'),
                ('/wat', 'different'),
                ('/wat/lat', 'right_missing'),
                ('/wat/lat/new.file', 'right_missing'),
                ('/wat/test.me.2', 'same'),
                ('/wat/test.me.3', 'same')], diffs)

            diffs = [
                (path, diff_type.value)
                for path, diff_type, left_id, right_id, should_skip
                in alive_it(zip_dfs(objects, "", left_id, right_id))]
            self.assertEqual([
                ('', 'different'),
                ('/newdir', 'right_missing'),
                ('/newdir/new.file', 'right_missing'),
                ('/test.me.1', 'same'),
                ('/test.me.4', 'right_missing'),
                ('/wat', 'different'),
                ('/wat/lat', 'right_missing'),
                ('/wat/lat/new.file', 'right_missing'),
                ('/wat/test.me.2', 'same'),
                ('/wat/test.me.3', 'right_missing'),
                ('/wat/zat', 'left_missing'),
                ('/wat/zat/new.file', 'left_missing')], diffs)

            diffs = [
                (path, diff_type.value)
                for path, diff_type, left_id, right_id, should_skip
                in alive_it(zip_dfs(objects, "", right_id, left_id))]
            self.assertEqual([
                ('', 'different'),
                ('/test.me.1', 'same'),
                ('/wat', 'different'),
                ('/wat/test.me.2', 'same'),
                ('/wat/zat', 'right_missing'),
                ('/wat/zat/new.file', 'right_missing'),
                ('/wat/lat', 'left_missing'),
                ('/wat/lat/new.file', 'left_missing'),
                ('/wat/test.me.3', 'left_missing'),
                ('/newdir', 'left_missing'),
                ('/newdir/new.file', 'left_missing'),
                ('/test.me.4', 'left_missing')], diffs)

    def test_dfs(self):
        env = ObjectStorage(self.obj_storage_path)
        with env.repos_txn(write=False) as txn:
            hoard_id = txn.get("HEAD".encode())

        with env.objects(write=False) as objects:
            all_nodes = dump_tree(objects, hoard_id)
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2),
                ('$ROOT/test.me.4', 2),
                ('$ROOT/test.me.5', 2),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2),
                ('$ROOT/wat/test.me.3', 2),
                ('$ROOT/wat/test.me.6', 2)], all_nodes)

            nodes = list()
            for path, obj_type, obj_id, obj, skip_children in dfs(objects, "$ROOT", hoard_id):
                if path == '$ROOT/wat':
                    skip_children()
                nodes.append((path, obj_type.value))
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2),
                ('$ROOT/test.me.4', 2),
                ('$ROOT/test.me.5', 2),
                ('$ROOT/wat', 1)], nodes)

    def test_gc(self):
        objs = ObjectStorage(self.obj_storage_path)
        objs.gc()

    def test_create_manual_tree(self):
        objs = ObjectStorage(self.obj_storage_path)
        # with objs.repos_txn(write=True) as txn:
        #     txn.put("MANUAL_HEAD",
        #
        with objs.objects(write=True) as objects:
            tree_id = add_file_object(
                objects, None, "wat/da/faque.isit".split("/"), FileObject.create("dasda", 100))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/da', 1),
                ('$ROOT/wat/da/faque.isit', 2)], dump_tree(objects, tree_id))

            tree_id = add_file_object(
                objects, tree_id, "wat/is/dis.isit".split("/"), FileObject.create("dasda", 101))
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/da', 1),
                ('$ROOT/wat/da/faque.isit', 2),
                ('$ROOT/wat/is', 1),
                ('$ROOT/wat/is/dis.isit', 2)], dump_tree(objects, tree_id))

            tree_id = add_file_object(
                objects, tree_id, "wat/da/another.isit".split("/"), FileObject.create("dasda", 100))
            tree_id = remove_file_object(objects, tree_id, "wat/da/faque.isit".split("/"))
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/da', 1),
                ('$ROOT/wat/da/another.isit', 2),
                ('$ROOT/wat/is', 1),
                ('$ROOT/wat/is/dis.isit', 2)], dump_tree(objects, tree_id))

            tree_id = remove_file_object(objects, tree_id, "wat/is/dis.isit".split("/"))
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/da', 1),
                ('$ROOT/wat/da/another.isit', 2),
                ('$ROOT/wat/is', 1)], dump_tree(objects, tree_id))

        objs.gc()

    def test_copy_trees(self):
        env = ObjectStorage(self.obj_storage_path)

        root_ids = sorted(env.all_roots)
        self.assertEqual([
            b'89527b0fa576e127d04089d9cb5aab0e5619696d',
            b'9fbdcfe094f258f954ba6f65c4a3641d25b32e06',
            b'a80f91bc48850a1fb3459bb76b9f6308d4d35710',
            b'd995800c80add686a027bac8628ca610418c64b6',
            b'f6a74030fa0a826b18e424d44f8aca9be8c657f3'], [binascii.hexlify(r) for r in root_ids])

        with env.objects(write=False) as objects:
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.3', 2)],
                dump_tree(objects, binascii.unhexlify(b'9fbdcfe094f258f954ba6f65c4a3641d25b32e06')))

        another_storage_path = f"{self.tmpdir}/test/other.lmdb"
        shutil.rmtree(another_storage_path, ignore_errors=True)
        pathlib.Path(another_storage_path).parent.mkdir(parents=True, exist_ok=True)

        other_env = ObjectStorage(another_storage_path)

        roots_to_copy = [binascii.unhexlify(hex_id) for hex_id in (
            b'89527b0fa576e127d04089d9cb5aab0e5619696d',
            b'9fbdcfe094f258f954ba6f65c4a3641d25b32e06',
            b'a80f91bc48850a1fb3459bb76b9f6308d4d35710',)]

        other_env.copy_trees_from(env, roots_to_copy)

        with other_env.objects(write=False) as objects:
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.3', 2)],
                dump_tree(objects, binascii.unhexlify(b'9fbdcfe094f258f954ba6f65c4a3641d25b32e06')))

        other_env.gc()  # note - we have not saved the roots, so this will clean it up
        with other_env.objects(write=False) as objects:
            try:
                dump_tree(objects, binascii.unhexlify(b'9fbdcfe094f258f954ba6f65c4a3641d25b32e06'))
                raise "should have thrown exception"
            except ValueError:
                pass


if __name__ == '__main__':
    unittest.main()
