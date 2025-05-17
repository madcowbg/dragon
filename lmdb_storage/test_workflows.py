import binascii
import unittest
from tempfile import TemporaryDirectory

from lmdb_storage.pull_contents import pull_contents
from lmdb_storage.test_experiment_lmdb import dump_tree
from lmdb_storage.test_merge_trees import populate_trees
from lmdb_storage.three_way_merge import NaiveMergePreferences
from lmdb_storage.tree_structure import remove_file_object
from util import safe_hex


class TestWorkflows(unittest.TestCase):
    def test_check_empty_tree_id(self):
        tmpdir = TemporaryDirectory(delete=True)
        env, partial_id, full_id, backup_id, incoming_id = populate_trees(tmpdir.name + "/test-objects.lmdb")

        with env.objects(write=True) as objects:
            empty_tree_id = objects.mktree_from_tuples([])
            self.assertEqual(b'a80f91bc48850a1fb3459bb76b9f6308d4d35710', binascii.hexlify(empty_tree_id))

    def test_pull_contents(self):
        tmpdir = TemporaryDirectory(delete=True)
        env, partial_id, full_id, backup_id, incoming_id = populate_trees(tmpdir.name + "/test-objects.lmdb")

        # init it as empty staging
        roots = env.roots(write=True)
        roots["HOARD"].desired = None
        roots["partial-uuid"].desired = None
        roots["partial-uuid"].current = None
        roots["full-uuid"].desired = None
        roots["full-uuid"].current = None
        roots["backup-uuid"].desired = None
        roots["backup-uuid"].current = None
        roots["incoming-uuid"].desired = None
        roots["incoming-uuid"].current = None

        pull_contents(env, repo_uuid="partial-uuid", staging_id=partial_id,
                      merge_prefs=NaiveMergePreferences({"full-uuid"}))

        roots = env.roots(write=False)
        current_full_id = roots["full-uuid"].desired
        current_hoard_id = roots["HOARD"].desired

        with env.objects(write=True) as objects:
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                dump_tree(objects, current_full_id, show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                dump_tree(objects, current_hoard_id, show_fasthash=True))

        pull_contents(
            env, repo_uuid="incoming-uuid", staging_id=incoming_id, merge_prefs=NaiveMergePreferences({"full-uuid"}))

        current_full_id = roots["full-uuid"].desired
        current_hoard_id = roots["HOARD"].desired
        current_incoming_id = roots["incoming-uuid"].desired

        with env.objects(write=False) as objects:
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d'),
                ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5'),
                ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                dump_tree(objects, current_full_id, show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d'),
                ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5')],
                dump_tree(objects, current_incoming_id, show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d'),
                ('$ROOT/wat/test.me.6', 2, 'd6a296dae0ca6991df926b8d18f43cc5'),
                ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                dump_tree(objects, current_hoard_id, show_fasthash=True))

        self.assertEqual([
            ('HOARD', '0038b3'),
            ('backup-uuid', 'a80f91'),
            ('full-uuid', '0038b3'),
            ('incoming-uuid', '5435fb'),
            ('partial-uuid', '0c4b36')],
            sorted((root.name, safe_hex(root.desired)[:6]) for root in roots.all))

        # remove a file from staging
        with env.objects(write=True) as objects:
            staging_incoming_id = remove_file_object(objects, current_incoming_id, "wat/test.me.6".split("/"))
            self.assertNotEqual(incoming_id, staging_incoming_id)

        pull_contents(
            env, repo_uuid="incoming-uuid", staging_id=staging_incoming_id,
            merge_prefs=NaiveMergePreferences({"full-uuid"}))

        self.assertEqual([
            ('HOARD', 'e1d05b'),
            ('backup-uuid', 'a80f91'),
            ('full-uuid', 'e1d05b'),
            ('incoming-uuid', '639323'),
            ('partial-uuid', '0c4b36')],
            sorted((root.name, safe_hex(root.desired)[:6]) for root in roots.all))

        current_full_id = roots["full-uuid"].desired
        current_hoard_id = roots["HOARD"].desired
        current_incoming_id = roots["incoming-uuid"].desired
        current_backup_id = roots["backup-uuid"].desired

        self.assertEqual(current_incoming_id, staging_incoming_id)

        with env.objects(write=False) as objects:
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d'),
                ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                dump_tree(objects, current_full_id, show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d')],
                dump_tree(objects, current_incoming_id, show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.3', 2, 'ad78b7d31e769862e45f8efc7d39618d'),
                ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                dump_tree(objects, current_hoard_id, show_fasthash=True))

            self.assertEqual([('$ROOT', 1)], dump_tree(objects, current_backup_id, show_fasthash=True))

        # adding the backup too
        pull_contents(
            env, repo_uuid="backup-uuid", staging_id=backup_id, merge_prefs=NaiveMergePreferences({"full-uuid"}))

        current_full_id = roots["full-uuid"].desired
        current_hoard_id = roots["HOARD"].desired
        current_incoming_id = roots["incoming-uuid"].desired
        current_backup_id = roots["backup-uuid"].desired

        with env.objects(write=False) as objects:
            self.assertEqual(current_backup_id, backup_id)

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.3', 2, '2cbc8608c915e94723752d4f0c54302f')],  # fixme that is the bad place
                dump_tree(objects, current_incoming_id, show_fasthash=True))

            # fixme that should not have happened when pulling backups, should implement the logic
            self.assertNotEqual(current_incoming_id, staging_incoming_id)  # one file was updated, using the backups

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.3', 2, '2cbc8608c915e94723752d4f0c54302f'),  # fixme that is the bad place
                ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                dump_tree(objects, current_full_id, show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.3', 2, '2cbc8608c915e94723752d4f0c54302f')],  # fixme that is the bad place
                dump_tree(objects, current_incoming_id, show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/test.me.4', 2, '5c8ffab0d25ab7692378bf41d495e046'),
                ('$ROOT/test.me.5', 2, '79e651dd08483b1483fb6e992c928e21'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, '663c6e6ae648bb1a1a893b5134dbdd7b'),
                ('$ROOT/wat/test.me.3', 2, '2cbc8608c915e94723752d4f0c54302f'),  # fixme that is the bad place
                ('$ROOT/wat/test.me.7', 2, '46e7da788d1c605a2293d580eeceeefd')],
                dump_tree(objects, current_hoard_id, show_fasthash=True))

            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, 'e10d2982020fc21760e4e5245b57f664'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.3', 2, '2cbc8608c915e94723752d4f0c54302f')],  # fixme should not have changed!
                dump_tree(objects, current_backup_id, show_fasthash=True))
