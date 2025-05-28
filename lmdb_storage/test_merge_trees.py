import binascii
import hashlib
import pathlib
from tempfile import TemporaryDirectory
from typing import Iterable, Tuple, Union, Dict, Collection, List
from unittest import IsolatedAsyncioTestCase

from lmdb import Transaction

from command.test_command_file_changing_flows import populate
from command.test_hoard_command import populate_repotypes, init_complex_hoard
from lmdb_storage.file_object import BlobObject
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.operations.fast_association import FastAssociation
from lmdb_storage.operations.naive_ops import TakeOneFile
from lmdb_storage.operations.three_way_merge import ThreewayMerge, MergePreferences, TransformedRoots
from lmdb_storage.operations.util import ObjectsByRoot, ByRoot
from lmdb_storage.test_experiment_lmdb import dump_tree, dump_diffs
from lmdb_storage.tree_iteration import zip_dfs
from lmdb_storage.tree_object import StoredObject, TreeObject
from lmdb_storage.tree_structure import ObjectID, Objects, do_nothing


class InMemoryObjectsExtension(Objects):
    def __init__(self, env: ObjectStorage) -> None:
        self.stored_objects = env.objects(write=False)
        self.in_mem: Dict[ObjectID, BlobObject | TreeObject] = dict()

    def __enter__(self) -> Objects:
        self.stored_objects.__enter__()
        return self

    @property
    def txn(self) -> Transaction:
        return self.stored_objects.txn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stored_objects.__exit__(exc_type, exc_val, exc_tb)
        return None

    def __contains__(self, obj_id: bytes) -> bool:
        return obj_id in self.in_mem or obj_id in self.stored_objects

    def __getitem__(self, obj_id: bytes) -> Union[BlobObject, TreeObject, None]:
        in_mem = self.in_mem.get(obj_id, None)
        return in_mem if in_mem else self.stored_objects[obj_id]

    def __setitem__(self, obj_id: bytes, obj: Union[BlobObject, TreeObject]):
        stored = self.stored_objects[obj_id]
        if stored is None:
            self.in_mem[obj_id] = obj
        elif obj != stored:
            raise ValueError("Cannot change a stored object!")

    def __delitem__(self, obj_id: bytes) -> None:
        raise ValueError("Cannot delete InMemory objects")

    def mktree_from_tuples(self, all_data: Iterable[Tuple[str, BlobObject]], alive_it=do_nothing) -> bytes:
        pass


class TestingMergingOfTrees(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir_obj = TemporaryDirectory(delete=True)
        self.tmpdir = self.tmpdir_obj.name

        self.obj_storage_path = f"{self.tmpdir}/hoard/hoard.contents.lmdb"

        pathlib.Path(self.obj_storage_path).parent.mkdir(parents=True, exist_ok=True)

        populate(self.tmpdir)
        populate_repotypes(self.tmpdir)

    async def test_merge_combining(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir)
        await hoard_cmd.contents.pull(all=True)

        with ObjectStorage(self.obj_storage_path) as env:
            root_ids = sorted(env.roots(write=False).all_live)
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

            root_left_id = binascii.unhexlify(b'f9bfc2be6cc201aa81b733b9d83c1030cc88bffe')
            root_right_id = binascii.unhexlify(b'3a0889e00c0c4ace24843be76d59b3baefb16d77')
            root_third_id = binascii.unhexlify(b'1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad')

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

                objects_by_root = ObjectsByRoot.from_map(dict([
                    (binascii.hexlify(root_left_id).decode(), root_left_id),
                    (binascii.hexlify(root_right_id).decode(), root_right_id)]))

                merged_id = TakeOneFile(objects).execute(objects_by_root)
                self.assertEqual([
                    ('$ROOT', 1),
                    ('$ROOT/test.me.1', 2, '1881f6f9784fb08bf6690e9763b76ac3'),
                    ('$ROOT/wat', 1),
                    ('$ROOT/wat/test.me.2', 2, 'd6dcdb1bc4677aab619798004537c4e3'),
                    ('$ROOT/wat/test.me.3', 2, '7c589c09e2754a164ba2e8f06feac897')],
                    dump_tree(objects, merged_id, show_fasthash=True))

                objects_by_root = ObjectsByRoot.from_map(dict((binascii.hexlify(it).decode(), it) for it in root_ids))
                merged_id = TakeOneFile(objects).execute(objects_by_root)
                self.assertEqual([
                    ('$ROOT', 1),
                    ('$ROOT/test.me.1', 2, '1881f6f9784fb08bf6690e9763b76ac3'),
                    ('$ROOT/test.me.4', 2, '6228a39ea262e9797f8efef82cd0eeba'),
                    ('$ROOT/test.me.5', 2, 'ac8419ee7f30e5ba4da89914da71b299'),
                    ('$ROOT/wat', 1),
                    ('$ROOT/wat/test.me.2', 2, 'd6dcdb1bc4677aab619798004537c4e3'),
                    ('$ROOT/wat/test.me.3', 2, '7c589c09e2754a164ba2e8f06feac897'),
                    ('$ROOT/wat/test.me.6', 2, 'c907b68b6a1f18c6135c112be53c978b')],
                    dump_tree(objects, merged_id, show_fasthash=True))

    def test_merge_raw(self):
        tmpdir = TemporaryDirectory(delete=True)
        env, partial_id, full_id, backup_id, incoming_id = populate_trees(tmpdir.name + "/test-objects.lmdb")
        with env as env:
            with env.objects(write=True) as objects:
                merged_id = TakeOneFile(objects).execute(
                    ObjectsByRoot.from_map({'one': backup_id, 'another': incoming_id}))
                self.assertEqual([
                    ('$ROOT', 1),
                    ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                    ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                    ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                    ('$ROOT/wat', 1),
                    ('$ROOT/wat/test.me.3', 2, '2cbc8608c915e94723752d4f0c54302f'),
                    ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                    dump_tree(objects, merged_id, show_fasthash=True))

    def test_merge_threeway(self):
        tmpdir = TemporaryDirectory(delete=True)
        env, partial_id, full_id, backup_id, incoming_id = populate_trees(tmpdir.name + "/test-objects.lmdb")

        with env as env:
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
                    ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d'),
                    ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                    dump_tree(objects, incoming_id, show_fasthash=True))

                merged_ids = ThreewayMerge(
                    objects, current_id=backup_id, staging_id=incoming_id, repo_name='staging',
                    merge_prefs=NaiveMergePreferences(['full', 'hoard'],
                                                      allowed_roots=('current', 'staging', 'full', 'partial',
                                                                     'hoard'))).execute(
                    ObjectsByRoot.from_map({
                        'current': backup_id, 'staging': incoming_id,
                        'full': full_id, 'partial': partial_id, 'hoard': hoard_id}), )

                self.assertEqual({'current', 'staging', 'full', 'partial', 'hoard'}, set(merged_ids.assigned_keys()))

                self.assertEqual([
                    ('$ROOT', 1),
                    ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                    ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                    ('$ROOT/wat', 1),
                    ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                    ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d'),
                    ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                    dump_tree(objects, merged_ids.get_if_present('full'), show_fasthash=True))

                self.assertEqual([
                    ('$ROOT', 1),
                    ('$ROOT/wat', 1),
                    ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                    ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                    dump_tree(objects, merged_ids.get_if_present('partial'), show_fasthash=True))

                self.assertEqual([
                    ('$ROOT', 1),
                    ('$ROOT/wat', 1),
                    ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d')],
                    dump_tree(objects, merged_ids.get_if_present('current'), show_fasthash=True))

                self.assertEqual([
                    ('$ROOT', 1),
                    ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                    ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                    ('$ROOT/wat', 1),
                    ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d'),
                    ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                    dump_tree(objects, merged_ids.get_if_present('staging'), show_fasthash=True))

                self.assertEqual([
                    ('$ROOT', 1),
                    ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                    ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                    ('$ROOT/wat', 1),
                    ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d'),
                    ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                    dump_tree(objects, merged_ids.get_if_present('hoard'), show_fasthash=True))

                self.assertEqual([
                    ('', 'different'),
                    ('/test.me.4', 'same'),
                    ('/test.me.5', 'same'),
                    ('/wat', 'different'),
                    ('/wat/test.me.2', 'right_missing'),
                    ('/wat/test.me.3', 'same'),
                    ('/wat/test.me.6', 'same')],
                    dump_diffs(objects, merged_ids.get_if_present('full'), merged_ids.get_if_present('hoard')))

                self.assertEqual([
                    ('', 'different'),
                    ('/test.me.4', 'left_missing'),
                    ('/test.me.5', 'left_missing'),
                    ('/wat', 'different'),
                    ('/wat/test.me.2', 'right_missing'),
                    ('/wat/test.me.3', 'left_missing'),
                    ('/wat/test.me.6', 'left_missing'),
                    ('/wat/test.me.7', 'right_missing')],
                    dump_diffs(objects, merged_ids.get_if_present('partial'), merged_ids.get_if_present('hoard')))

    def test_merge_threeway_incrementally(self):
        tmpdir = TemporaryDirectory(delete=True)
        env, partial_id, full_id, backup_id, incoming_id = populate_trees(tmpdir.name + "/test-objects.lmdb")
        with env as env:
            with env.objects(write=True) as objects:
                hoard_id = objects.mktree_from_tuples([])
                self.assertEqual(b'a80f91bc48850a1fb3459bb76b9f6308d4d35710', binascii.hexlify(hoard_id))

                merged_ids = ThreewayMerge(
                    objects, current_id=objects.mktree_from_tuples([]), staging_id=partial_id, repo_name='staging',
                    merge_prefs=NaiveMergePreferences(['hoard'], allowed_roots=('current', 'staging', 'full', 'partial',
                                                                                'hoard'))).execute(
                    ObjectsByRoot.from_map(
                        {'empty': objects.mktree_from_tuples([]), "staging": partial_id, 'hoard': hoard_id}))

                self.assertEqual([
                    ('$ROOT', 1),
                    ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                    ('$ROOT/wat', 1),
                    ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                    ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                    dump_tree(objects, merged_ids.get_if_present('hoard'), show_fasthash=True))
                hoard_id = merged_ids.get_if_present('hoard')

                merged_ids = ThreewayMerge(
                    objects, current_id=objects.mktree_from_tuples([]), staging_id=full_id, repo_name='staging',
                    merge_prefs=NaiveMergePreferences(['hoard'], allowed_roots=('current', 'staging', 'full', 'partial',
                                                                                'hoard'))).execute(
                    ObjectsByRoot.from_map(
                        {'empty': objects.mktree_from_tuples([]), "staging": full_id, 'hoard': hoard_id}))

                self.assertEqual([
                    ('$ROOT', 1),
                    ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                    ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                    ('$ROOT/wat', 1),
                    ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                    ('$ROOT/wat/test.me.3', 2, '2cbc8608c915e94723752d4f0c54302f'),
                    ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                    dump_tree(objects, merged_ids.get_if_present('hoard'), show_fasthash=True))
                hoard_id = merged_ids.get_if_present('hoard')

                merged_ids = ThreewayMerge(
                    objects, current_id=objects.mktree_from_tuples([]), staging_id=backup_id, repo_name='staging',
                    merge_prefs=NaiveMergePreferences(['hoard'], allowed_roots=('current', 'staging', 'full', 'partial',
                                                                                'hoard'))).execute(
                    ObjectsByRoot.from_map(
                        {'empty': objects.mktree_from_tuples([]), "staging": backup_id, 'hoard': hoard_id}))

                self.assertEqual([
                    ('$ROOT', 1),
                    ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                    ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                    ('$ROOT/wat', 1),
                    ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                    ('$ROOT/wat/test.me.3', 2, '2cbc8608c915e94723752d4f0c54302f'),
                    ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                    dump_tree(objects, merged_ids.get_if_present('hoard'), show_fasthash=True))
                hoard_id = merged_ids.get_if_present('hoard')

            with InMemoryObjectsExtension(env) as objects:
                self._run_threeway_merge_after_hoard_created(objects, partial_id, full_id, backup_id, incoming_id,
                                                             hoard_id)

            with env.objects(write=True) as objects:
                self._run_threeway_merge_after_hoard_created(objects, partial_id, full_id, backup_id, incoming_id,
                                                             hoard_id)

    def _run_threeway_merge_after_hoard_created(
            self, objects: Objects,
            partial_id: ObjectID, full_id: ObjectID, backup_id: ObjectID, incoming_id: ObjectID, hoard_id: ObjectID):
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
            ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d'),
            ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
            dump_tree(objects, incoming_id, show_fasthash=True))

        merged_ids = ThreewayMerge(
            objects, current_id=backup_id, staging_id=incoming_id, repo_name='staging',
            merge_prefs=NaiveMergePreferences(['full', 'hoard'], allowed_roots=('current', 'staging', 'full', 'partial',
                                                                                'hoard'))).execute(
            ObjectsByRoot.from_map({
                'current': backup_id, 'staging': incoming_id,
                'full': full_id, 'partial': partial_id, 'hoard': hoard_id}))

        self.assertEqual({'current', 'staging', 'full', 'partial', 'hoard'}, set(merged_ids.assigned_keys()))

        self.assertEqual([
            ('$ROOT', 1),
            ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
            ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
            ('$ROOT/wat', 1),
            ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
            ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d'),
            ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
            dump_tree(objects, merged_ids.get_if_present('full'), show_fasthash=True))

        self.assertEqual([
            ('$ROOT', 1),
            ('$ROOT/wat', 1),
            ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
            ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
            dump_tree(objects, merged_ids.get_if_present('partial'), show_fasthash=True))

        self.assertEqual([
            ('$ROOT', 1),
            ('$ROOT/wat', 1),
            ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d')],
            dump_tree(objects, merged_ids.get_if_present('current'), show_fasthash=True))

        self.assertEqual([
            ('$ROOT', 1),
            ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
            ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
            ('$ROOT/wat', 1),
            ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d'),
            ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
            dump_tree(objects, merged_ids.get_if_present('staging'), show_fasthash=True))

        self.assertEqual([
            ('$ROOT', 1),
            ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
            ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
            ('$ROOT/wat', 1),
            ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
            ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d'),
            ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5'),
            ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
            dump_tree(objects, merged_ids.get_if_present('hoard'), show_fasthash=True))

        self.assertEqual([
            ('', 'different'),
            ('/test.me.4', 'same'),
            ('/test.me.5', 'same'),
            ('/wat', 'different'),
            ('/wat/test.me.2', 'same'),
            ('/wat/test.me.3', 'same'),
            ('/wat/test.me.6', 'same'),
            ('/wat/test.me.7', 'left_missing')],
            dump_diffs(objects, merged_ids.get_if_present('full'), merged_ids.get_if_present('hoard')))

        self.assertEqual([
            ('', 'different'),
            ('/test.me.4', 'left_missing'),
            ('/test.me.5', 'left_missing'),
            ('/wat', 'different'),
            ('/wat/test.me.2', 'same'),
            ('/wat/test.me.3', 'left_missing'),
            ('/wat/test.me.6', 'left_missing'),
            ('/wat/test.me.7', 'same')],
            dump_diffs(objects, merged_ids.get_if_present('partial'), merged_ids.get_if_present('hoard')))


def make_file(data: str) -> BlobObject:
    return BlobObject.create(hashlib.md5(data.encode()).hexdigest(), len(data))


def populate_trees(filepath: str) -> (ObjectStorage, ObjectID, ObjectID, ObjectID, ObjectID):
    with ObjectStorage(filepath) as env:
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


class NaiveMergePreferences(MergePreferences):
    def __init__(self, to_modify: Collection[str], allowed_roots: Collection[str]):
        self.to_modify = list(to_modify)
        self.allowed_roots = list(allowed_roots)
        self.empty_association = FastAssociation(self.allowed_roots, (None,) * len(self.allowed_roots))

    def where_to_apply_diffs(self, original_roots: List[str]) -> List[str]:
        return list(set(original_roots + self.to_modify))

    def where_to_apply_adds(self, original_roots: List[str]) -> List[str]:
        return list(set(original_roots + self.to_modify))

    def combine_both_existing(
            self, path: List[str], original_roots: ByRoot[StoredObject],
            staging_original: BlobObject, base_original: BlobObject) -> TransformedRoots:
        result: TransformedRoots = TransformedRoots.wrap(self.empty_association.new())
        for merge_name in (self.where_to_apply_diffs(list(original_roots.assigned_keys()))):
            result.HACK_maybe_set_by_key(merge_name, staging_original.file_id)
        return result

    def combine_base_only(
            self, path: List[str], repo_name: str, original_roots: ByRoot[StoredObject],
            base_original: BlobObject) -> FastAssociation[ObjectID]:

        return self.empty_association

    def combine_staging_only(
            self, path: List[str], repo_name: str, original_roots: ByRoot[StoredObject],
            staging_original: BlobObject) -> FastAssociation[ObjectID]:
        assert type(staging_original) is BlobObject

        result: TransformedRoots = TransformedRoots.wrap(self.empty_association.new())
        for merge_name in (self.where_to_apply_adds(list(original_roots.assigned_keys()))):
            result.HACK_maybe_set_by_key(merge_name, staging_original.file_id)
        return result

    def merge_missing(self, path: List[str], original_roots: ByRoot[StoredObject]) -> FastAssociation[ObjectID]:
        return TransformedRoots.HACK_create(original_roots.map(lambda obj: obj.id))
