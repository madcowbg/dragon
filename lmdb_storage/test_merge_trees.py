import binascii
import hashlib
import pathlib
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase

from command.test_command_file_changing_flows import populate
from command.test_hoard_command import populate_repotypes
from lmdb_storage.file_object import FileObject
from lmdb_storage.merge_trees import TakeOneFile, merge_trees
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.test_experiment_lmdb import dump_tree, dump_diffs
from lmdb_storage.three_way_merge import ThreewayMerge
from lmdb_storage.tree_iteration import zip_dfs
from lmdb_storage.tree_structure import ObjectID


class TestingMergingOfTrees(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = "./tests"
        self.obj_storage_path = f"{self.tmpdir}/test/example.lmdb"

        pathlib.Path(self.obj_storage_path).parent.mkdir(parents=True, exist_ok=True)

        populate(self.tmpdir)
        populate_repotypes(self.tmpdir)

    def test_merge_combining(self):
        env = ObjectStorage(self.obj_storage_path)

        root_ids = sorted(env.roots(write=False).all_live)
        self.assertEqual([
            b'89527b0fa576e127d04089d9cb5aab0e5619696d',
            b'9fbdcfe094f258f954ba6f65c4a3641d25b32e06',
            b'a80f91bc48850a1fb3459bb76b9f6308d4d35710',
            b'd995800c80add686a027bac8628ca610418c64b6',
            b'f6a74030fa0a826b18e424d44f8aca9be8c657f3'], [binascii.hexlify(r) for r in root_ids])

        root_left_id = binascii.unhexlify(b'f6a74030fa0a826b18e424d44f8aca9be8c657f3')
        root_right_id = binascii.unhexlify(b'9fbdcfe094f258f954ba6f65c4a3641d25b32e06')
        root_third_id = binascii.unhexlify(b'89527b0fa576e127d04089d9cb5aab0e5619696d')

        with env.objects(write=True) as objects:
            diffs = [
                (path, diff_type.value)
                for path, diff_type, left_id, right_id, should_skip
                in zip_dfs(objects, "", root_left_id, root_right_id)]

            self.assertEqual([
                ('', 'different'),
                ('/test.me.1', 'same'),
                ('/wat', 'different'),
                ('/wat/test.me.2', 'right_missing'),
                ('/wat/test.me.3', 'left_missing')], diffs)

            merged = merge_trees(
                dict((binascii.hexlify(it).decode(), it) for it in [root_left_id, root_right_id]),
                TakeOneFile(objects))
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, '1881f6f9784fb08bf6690e9763b76ac3'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, 'd6dcdb1bc4677aab619798004537c4e3'),
                ('$ROOT/wat/test.me.3', 2, '7c589c09e2754a164ba2e8f06feac897')],
                dump_tree(objects, merged["MERGED"], show_fasthash=True))

            merged = merge_trees(dict((binascii.hexlify(it).decode(), it) for it in root_ids), TakeOneFile(objects))
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, '1881f6f9784fb08bf6690e9763b76ac3'),
                ('$ROOT/test.me.4', 2, '6228a39ea262e9797f8efef82cd0eeba'),
                ('$ROOT/test.me.5', 2, 'ac8419ee7f30e5ba4da89914da71b299'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, 'd6dcdb1bc4677aab619798004537c4e3'),
                ('$ROOT/wat/test.me.3', 2, '7c589c09e2754a164ba2e8f06feac897'),
                ('$ROOT/wat/test.me.6', 2, 'c907b68b6a1f18c6135c112be53c978b')],
                dump_tree(objects, merged["MERGED"], show_fasthash=True))

    def test_merge_raw(self):
        tmpdir = TemporaryDirectory(delete=True)
        env, partial_id, full_id, backup_id, incoming_id = populate_trees(tmpdir.name + "/test-objects.lmdb")

        with env.objects(write=True) as objects:
            merged_ids = merge_trees({'one': backup_id, 'another': incoming_id}, TakeOneFile(objects))
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.3', 2, '2cbc8608c915e94723752d4f0c54302f'),
                ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                dump_tree(objects, merged_ids["MERGED"], show_fasthash=True))

    def test_merge_threeway(self):
        tmpdir = TemporaryDirectory(delete=True)
        env, partial_id, full_id, backup_id, incoming_id = populate_trees(tmpdir.name + "/test-objects.lmdb")

        with env.objects(write=True) as objects:
            hoard_id = objects.mktree_from_tuples([])
            self.assertEqual(b'a80f91bc48850a1fb3459bb76b9f6308d4d35710', binascii.hexlify(hoard_id))

        with env.objects(write=True) as objects:
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.3', 2, '2cbc8608c915e94723752d4f0c54302f')],
                dump_tree(objects, full_id, show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.3', 2, '2cbc8608c915e94723752d4f0c54302f')],
                dump_tree(objects, backup_id, show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                dump_tree(objects, incoming_id, show_fasthash=True))

            merged_ids = merge_trees({
                'current': backup_id, 'staging': incoming_id,
                'full': full_id, 'partial': partial_id, 'hoard': hoard_id},
                ThreewayMerge(
                    objects, current='current', staging='staging', others=['full', 'partial', 'hoard'],
                    fetch_new={'full', 'hoard'}))

            self.assertEqual(['current', 'staging', 'full', 'partial', 'hoard'], list(merged_ids.keys()))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                dump_tree(objects, merged_ids['full'], show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                dump_tree(objects, merged_ids['partial'], show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                dump_tree(objects, merged_ids['current'], show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                dump_tree(objects, merged_ids['staging'], show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                dump_tree(objects, merged_ids['hoard'], show_fasthash=True))

            self.assertEqual([
                ('', 'different'),
                ('/test.me.4', 'same'),
                ('/test.me.5', 'same'),
                ('/wat', 'different'),
                ('/wat/test.me.2', 'right_missing'),
                ('/wat/test.me.6', 'same')],
                dump_diffs(objects, merged_ids['full'], merged_ids['hoard']))

            self.assertEqual([
                ('', 'different'),
                ('/wat', 'different'),
                ('/wat/test.me.2', 'right_missing'),
                ('/wat/test.me.7', 'right_missing'),
                ('/wat/test.me.6', 'left_missing'),
                ('/test.me.4', 'left_missing'),
                ('/test.me.5', 'left_missing')],
                dump_diffs(objects, merged_ids['partial'], merged_ids['hoard']))

    def test_merge_threeway_incrementally(self):
        tmpdir = TemporaryDirectory(delete=True)
        env, partial_id, full_id, backup_id, incoming_id = populate_trees(tmpdir.name + "/test-objects.lmdb")

        with env.objects(write=True) as objects:
            hoard_id = objects.mktree_from_tuples([])
            self.assertEqual(b'a80f91bc48850a1fb3459bb76b9f6308d4d35710', binascii.hexlify(hoard_id))

            merged_ids = merge_trees(
                {'empty': objects.mktree_from_tuples([]), "staging": partial_id, 'hoard': hoard_id},
                ThreewayMerge(objects, current='empty', staging='staging', others=['hoard'], fetch_new={'hoard'}))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                dump_tree(objects, merged_ids['hoard'], show_fasthash=True))
            hoard_id = merged_ids['hoard']

            merged_ids = merge_trees(
                {'empty': objects.mktree_from_tuples([]), "staging": full_id, 'hoard': hoard_id},
                ThreewayMerge(objects, current='empty', staging='staging', others=['hoard'], fetch_new={'hoard'}))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.3', 2, '2cbc8608c915e94723752d4f0c54302f'),
                ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                dump_tree(objects, merged_ids['hoard'], show_fasthash=True))
            hoard_id = merged_ids['hoard']

            merged_ids = merge_trees(
                {'empty': objects.mktree_from_tuples([]), "staging": backup_id, 'hoard': hoard_id},
                ThreewayMerge(objects, current='empty', staging='staging', others=['hoard'], fetch_new={'hoard'}))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.3', 2, '2cbc8608c915e94723752d4f0c54302f'),
                ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                dump_tree(objects, merged_ids['hoard'], show_fasthash=True))
            hoard_id = merged_ids['hoard']

        with env.objects(write=True) as objects:
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.3', 2, '2cbc8608c915e94723752d4f0c54302f')],
                dump_tree(objects, full_id, show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.3', 2, '2cbc8608c915e94723752d4f0c54302f')],
                dump_tree(objects, backup_id, show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                dump_tree(objects, incoming_id, show_fasthash=True))

            merged_ids = merge_trees({
                'current': backup_id, 'staging': incoming_id,
                'full': full_id, 'partial': partial_id, 'hoard': hoard_id},
                ThreewayMerge(
                    objects, current='current', staging='staging', others=['full', 'partial', 'hoard'],
                    fetch_new={'full', 'hoard'}))

            self.assertEqual(['current', 'staging', 'full', 'partial', 'hoard'], list(merged_ids.keys()))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                dump_tree(objects, merged_ids['full'], show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                dump_tree(objects, merged_ids['partial'], show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                dump_tree(objects, merged_ids['current'], show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                dump_tree(objects, merged_ids['staging'], show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5'),
                ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                dump_tree(objects, merged_ids['hoard'], show_fasthash=True))

            self.assertEqual([
                ('', 'different'),
                ('/test.me.4', 'same'),
                ('/test.me.5', 'same'),
                ('/wat', 'different'),
                ('/wat/test.me.2', 'same'),
                ('/wat/test.me.6', 'same'),
                ('/wat/test.me.7', 'left_missing')],
                dump_diffs(objects, merged_ids['full'], merged_ids['hoard']))

            self.assertEqual([
                ('', 'different'),
                ('/wat', 'different'),
                ('/wat/test.me.2', 'same'),
                ('/wat/test.me.7', 'same'),
                ('/wat/test.me.6', 'left_missing'),
                ('/test.me.4', 'left_missing'),
                ('/test.me.5', 'left_missing')],
                dump_diffs(objects, merged_ids['partial'], merged_ids['hoard']))


def make_file(data: str) -> FileObject:
    return FileObject.create(hashlib.md5(data.encode()).hexdigest(), len(data))


def populate_trees(filepath: str) -> (ObjectStorage, ObjectID, ObjectID, ObjectID, ObjectID):
    env = ObjectStorage(filepath)
    with env.objects(write=True) as objects:
        partial_id = objects.mktree_from_tuples([
            ('/test.me.1', make_file("gsadfs")),
            ('/wat/test.me.2', make_file("gsadf3dq")),
            ('/wat/test.me.7', make_file("gsadfs3dq"))])

        full_id = objects.mktree_from_tuples([
            ('/test.me.1', make_file("gsadfs")),
            ('/test.me.4', make_file("fwadeaewdsa")),
            ('/wat/test.me.2', make_file("gsadf3dq")),
            ('/wat/test.me.3', make_file("afaswewfas"))])

        backup_id = objects.mktree_from_tuples([
            ('/test.me.1', make_file("gsadfs")),
            ('/wat/test.me.3', make_file("afaswewfas"))])

        incoming_id = objects.mktree_from_tuples([
            ('/wat/test.me.3', make_file("asdgvarfa")),
            ('/test.me.4', make_file("fwadeaewdsa")),
            ('/test.me.5', make_file("adsfg")),
            ('/wat/test.me.6', make_file("f2fwsdf"))])

        return env, partial_id, full_id, backup_id, incoming_id
