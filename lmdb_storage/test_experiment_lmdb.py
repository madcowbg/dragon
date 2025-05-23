import binascii
import logging
import pathlib
import shutil
import unittest
from tempfile import TemporaryDirectory
from typing import List, Iterable
from unittest.async_case import IsolatedAsyncioTestCase

import msgpack
from alive_progress import alive_it

from command.test_command_file_changing_flows import populate
from command.test_hoard_command import populate_repotypes, init_complex_hoard
from lmdb_storage.file_object import FileObject
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.operations.fast_association import FastAssociation
from lmdb_storage.operations.types import Procedure
from lmdb_storage.operations.generator import TreeGenerator
from lmdb_storage.operations.util import ByRoot, ObjectsByRoot
from lmdb_storage.tree_iteration import dfs, zip_dfs
from lmdb_storage.tree_structure import ExpandableTreeObject, add_file_object, Objects, remove_file_object, ObjectType, \
    ObjectID, TreeObject, MaybeObjectID


def dump_tree(objects: Objects[FileObject], root_id, show_fasthash: bool = False):
    return list(
        (path, obj_type.value) if not show_fasthash or obj_type == ObjectType.TREE
        else (path, obj_type.value, obj.fasthash)
        for path, obj_type, _, obj, _ in dfs(objects, "$ROOT", root_id))


def dump_diffs(objects: Objects[FileObject], left_id: ObjectID, right_id: ObjectID):
    return [
        (path, diff_type.value)
        for path, diff_type, left_id, right_id, should_skip
        in zip_dfs(objects, "", left_id, right_id)]


class VariousLMDBFunctions(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir_obj = TemporaryDirectory(delete=True)
        self.tmpdir = self.tmpdir_obj.name

        self.obj_storage_path = f"{self.tmpdir}/hoard/hoard.contents.lmdb"

        pathlib.Path(self.obj_storage_path).parent.mkdir(parents=True, exist_ok=True)

        populate(self.tmpdir)
        populate_repotypes(self.tmpdir)

    async def test_fully_load_lmdb(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir)
        await hoard_cmd.contents.pull(all=True)

        with ObjectStorage(self.obj_storage_path) as env:  # , map_size=(1 << 30) // 4)

            with env.objects(write=False) as objects:
                root_id = env.roots(write=True)["HOARD"].desired

                root = ExpandableTreeObject.create(root_id, objects)

                def all_files(tree: ExpandableTreeObject) -> Iterable[FileObject]:
                    yield from tree.files.values()
                    for subtree in tree.dirs.values():
                        yield from all_files(subtree)

                all_files = list(alive_it(all_files(root), title="loading from lmdb..."))
                logging.warning(f"# all_files: {len(all_files)}")

    def test_dump_lmdb(self):
        with ObjectStorage(self.obj_storage_path) as env:  # , map_size=(1 << 30) // 4)
            with env.begin(db_name="objects", write=False) as txn:
                with txn.cursor() as curr:
                    with open(self.tmpdir + "/dbdump.msgpack", "wb") as f:
                        # msgpack.dump(((k, v) for k, v in alive_it(curr, title="loading from lmdb...")), f)
                        msgpack.dump(list(((k, v) for k, v in curr)), f)

    async def test_tree_compare(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir)
        await hoard_cmd.contents.pull(all=True)

        with ObjectStorage(self.obj_storage_path) as env:  # , map_size=(1 << 30) // 4)
            uuid = full_cave_cmd.current_uuid()

            with env.objects(write=False) as objects:
                hoard_id = env.roots(write=False)["HOARD"].desired
                repo_id = env.roots(write=False)[uuid].current

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

    async def test_tree_compare_with_missing_trees(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir)
        await hoard_cmd.contents.pull(all=True)

        with ObjectStorage(self.obj_storage_path) as env:
            left_uuid = full_cave_cmd.current_uuid()
            right_uuid = partial_cave_cmd.current_uuid()

            left_id = env.roots(write=False)[left_uuid].staging
            right_id = env.roots(write=False)[right_uuid].staging

            with env.objects(write=True) as objects:
                left_id = add_file_object(objects, left_id, "newdir/new.file".split("/"), FileObject.create("dasda", 1))
                old_left_id = left_id
                left_id = add_file_object(objects, left_id, "wat/lat/new.file".split("/"),
                                          FileObject.create("dasda", 2))

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

                diffs = dump_diffs(objects, left_id, right_id)
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
                    ('/newdir', 'left_missing'),
                    ('/newdir/new.file', 'left_missing'),
                    ('/test.me.1', 'same'),
                    ('/test.me.4', 'left_missing'),
                    ('/wat', 'different'),
                    ('/wat/lat', 'left_missing'),
                    ('/wat/lat/new.file', 'left_missing'),
                    ('/wat/test.me.2', 'same'),
                    ('/wat/test.me.3', 'left_missing'),
                    ('/wat/zat', 'right_missing'),
                    ('/wat/zat/new.file', 'right_missing')], diffs)

    async def test_dfs(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir)
        await hoard_cmd.contents.pull(all=True)

        with ObjectStorage(self.obj_storage_path) as env:

            with env.objects(write=False) as objects:
                all_nodes = dump_tree(objects, env.roots(write=False)["HOARD"].desired)
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
                for path, obj_type, obj_id, obj, skip_children in dfs(
                        objects, "$ROOT", env.roots(write=False)["HOARD"].desired):
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
        with ObjectStorage(self.obj_storage_path) as env:
            env.gc()

    def test_create_manual_tree(self):
        with ObjectStorage(self.obj_storage_path) as objs:
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

                self.assertEqual(
                    'Root\n'
                    '┖wat\n'
                    '┃┖is\n'
                    '┃┃┖dis.isit\n'
                    '┃┖da\n'
                    '┃┃┖another.isit', PrettyPrintProcedure.as_str(objects, tree_id))

                tree_id = remove_file_object(objects, tree_id, "wat/is/dis.isit".split("/"))
                self.assertEqual([
                    ('$ROOT', 1),
                    ('$ROOT/wat', 1),
                    ('$ROOT/wat/da', 1),
                    ('$ROOT/wat/da/another.isit', 2),
                    ('$ROOT/wat/is', 1)], dump_tree(objects, tree_id))

                self.assertEqual(
                    'Root\n'
                    '┖wat\n'
                    '┃┖is\n'
                    '┃┖da\n'
                    '┃┃┖another.isit', PrettyPrintProcedure.as_str(objects, tree_id))

                self.assertEqual(
                    'Root\n'
                    '┖wat\n'
                    '┃┖is\n'
                    '┃┖da\n'
                    '┃┃┖another.isit', PrettyPrintGenerator.as_str(objects, tree_id))

            objs.gc()

    async def test_copy_trees(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir)
        await hoard_cmd.contents.pull(all=True)

        with ObjectStorage(self.obj_storage_path) as env:

            root_ids = env.roots(write=False).all_live
            self.assertEqual([
                b'1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad',
                b'1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad',
                b'3a0889e00c0c4ace24843be76d59b3baefb16d77',
                b'3a0889e00c0c4ace24843be76d59b3baefb16d77',
                b'3d1726bd296f20d36cb9df60a0da4d4feae29248',
                b'3d1726bd296f20d36cb9df60a0da4d4feae29248',
                b'8da76083b9eab9f49945d8f2487df38ab909b7df',
                b'8da76083b9eab9f49945d8f2487df38ab909b7df',
                b'8da76083b9eab9f49945d8f2487df38ab909b7df',
                b'f9bfc2be6cc201aa81b733b9d83c1030cc88bffe',
                b'f9bfc2be6cc201aa81b733b9d83c1030cc88bffe',
                b'f9bfc2be6cc201aa81b733b9d83c1030cc88bffe'], [binascii.hexlify(r) for r in root_ids])

            with env.objects(write=False) as objects:
                self.assertEqual([
                    ('$ROOT', 1),
                    ('$ROOT/test.me.1', 2),
                    ('$ROOT/wat', 1),
                    ('$ROOT/wat/test.me.3', 2)],
                    dump_tree(objects, binascii.unhexlify(b'3a0889e00c0c4ace24843be76d59b3baefb16d77')))

            another_storage_path = f"{self.tmpdir}/test/other.lmdb"
            shutil.rmtree(another_storage_path, ignore_errors=True)
            pathlib.Path(another_storage_path).parent.mkdir(parents=True, exist_ok=True)

            with ObjectStorage(another_storage_path) as other_env:
                roots_to_copy = [binascii.unhexlify(hex_id) for hex_id in (
                    b'1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad',
                    b'3a0889e00c0c4ace24843be76d59b3baefb16d77',
                    b'8da76083b9eab9f49945d8f2487df38ab909b7df',)]

                other_env.copy_trees_from(env, roots_to_copy)

                with other_env.objects(write=False) as objects:
                    self.assertEqual([
                        ('$ROOT', 1),
                        ('$ROOT/test.me.1', 2),
                        ('$ROOT/wat', 1),
                        ('$ROOT/wat/test.me.3', 2)],
                        dump_tree(objects, binascii.unhexlify(b'3a0889e00c0c4ace24843be76d59b3baefb16d77')))

                other_env.gc()  # note - we have not saved the roots, so this will clean it up
                with other_env.objects(write=False) as objects:
                    try:
                        dump_tree(objects, binascii.unhexlify(b'3a0889e00c0c4ace24843be76d59b3baefb16d77'))
                        raise "should have thrown exception"
                    except ValueError:
                        pass


class PrettyPrintProcedure(Procedure[FileObject]):
    def __init__(self, objects: Objects[FileObject]):
        self.objects = objects
        self.out: List[str] = []

    def run_on_level(self, state: List[str], original: ByRoot[TreeObject | FileObject]):
        if len(state) == 0:
            self.out.append("Root")
        else:
            self.out.append("┃" * (len(state) - 1) + "┖" + state[-1])

    def should_drill_down(self, state: List[str], trees: ByRoot[TreeObject], files: ByRoot[FileObject]) -> bool:
        return True

    @staticmethod
    def as_str(objects: Objects[FileObject], root_id: MaybeObjectID) -> str:
        pp = PrettyPrintProcedure(objects)
        pp.execute(ObjectsByRoot.singleton("root", root_id))
        return "\n".join(reversed(pp.out))


class PrettyPrintGenerator(TreeGenerator[FileObject, str]):
    def compute_on_level(self, state: List[str], original: FastAssociation[TreeObject | FileObject]) -> Iterable[str]:
        if len(state) == 0:
            yield "Root"
        else:
            yield "┃" * (len(state) - 1) + "┖" + state[-1]

    def should_drill_down(self, state: List[str], trees: FastAssociation[TreeObject],
                          files: FastAssociation[FileObject]) -> bool:
        return True

    def __init__(self, objects: Objects[FileObject]):
        self.objects = objects
        self.out: List[str] = []

    @staticmethod
    def as_str(objects: Objects[FileObject], root_id: MaybeObjectID) -> str:
        return "\n".join(
            reversed(list(PrettyPrintGenerator(objects).execute(ObjectsByRoot.singleton("root", root_id)))))


if __name__ == '__main__':
    unittest.main()
