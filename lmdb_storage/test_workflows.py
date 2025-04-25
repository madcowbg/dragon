import binascii
import unittest
from tempfile import TemporaryDirectory
from typing import Set

from lmdb_storage.merge_trees import ObjectsByRoot
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.test_experiment_lmdb import dump_tree
from lmdb_storage.test_merge_trees import populate_trees
from lmdb_storage.three_way_merge import ThreewayMerge
from lmdb_storage.tree_structure import ObjectID, remove_file_object


def pull_contents(env: ObjectStorage, repo_uuid: str, staging_id: ObjectID, fetch_new: Set[str]):
    assert "HOARD" not in fetch_new
    fetch_new = fetch_new.copy()
    fetch_new.add("HOARD")

    # assign roots
    roots = env.roots(write=False)
    repo_current_id = roots[repo_uuid].current
    hoard_head_id = roots["HOARD"].desired

    repo_staging_id = staging_id

    other_roots = [r for r in roots.all if r.name != "HOARD"]
    other_root_names = [r.name for r in other_roots]
    current_ids = ObjectsByRoot(
        list(set([r.name for r in roots.all] + ["current", "staging", "HOARD"])),
        [(r.name, r.desired) for r in other_roots] + [
            ("current", repo_current_id),
            ("staging", repo_staging_id),
            ("HOARD", hoard_head_id)])

    assert all(v is not None for v in current_ids.assigned_values())

    # execute merge
    with env.objects(write=True) as objects:
        merged_ids = ThreewayMerge(
            objects, current="current", staging='staging', others=other_root_names,
            fetch_new=set([repo_uuid] + list(fetch_new))).merge_trees(current_ids) # fixme hack to fetch_new maybe not proper?

        assert len(set(current_ids.assigned().keys()) - set(merged_ids.assigned().keys())) == 0, \
            f"{set(merged_ids.assigned().keys())} not contains all of {set(current_ids.assigned().keys())}"

    # accept the changed IDs todo implement dry-run
    roots = env.roots(write=True)
    for other_name in other_root_names:
        roots[other_name].desired = merged_ids.get_if_present(other_name)

    roots["HOARD"].desired = merged_ids.get_if_present("HOARD")
    roots[repo_uuid].current = merged_ids.get_if_present("current")


class TestWorkflows(unittest.TestCase):
    def test_pull_contents(self):
        tmpdir = TemporaryDirectory(delete=True)
        env, partial_id, full_id, backup_id, incoming_id = populate_trees(tmpdir.name + "/test-objects.lmdb")

        with env.objects(write=True) as objects:
            empty_tree_id = objects.mktree_from_tuples([])
            self.assertEqual(b'a80f91bc48850a1fb3459bb76b9f6308d4d35710', binascii.hexlify(empty_tree_id))

        # init it as empty staging
        roots = env.roots(write=True)
        roots["HOARD"].current = empty_tree_id
        roots["partial-uuid"].desired = empty_tree_id
        roots["partial-uuid"].current = empty_tree_id
        roots["full-uuid"].desired = empty_tree_id
        roots["full-uuid"].current = empty_tree_id
        roots["backup-uuid"].desired = empty_tree_id
        roots["backup-uuid"].current = empty_tree_id
        roots["incoming-uuid"].desired = empty_tree_id
        roots["incoming-uuid"].current = empty_tree_id

        pull_contents(env, repo_uuid="partial-uuid", staging_id=partial_id, fetch_new={"full-uuid"})

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

        pull_contents(env, repo_uuid="incoming-uuid", staging_id=incoming_id, fetch_new={"full-uuid"})

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

        # remove a file from staging
        with env.objects(write=True) as objects:
            staging_incoming_id = remove_file_object(objects, current_incoming_id, "wat/test.me.6".split("/"))
            self.assertNotEqual(incoming_id, staging_incoming_id)

        pull_contents(env, repo_uuid="incoming-uuid", staging_id=staging_incoming_id, fetch_new={"full-uuid"})

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

            self.assertEqual([
                ('$ROOT', 1), ],
                dump_tree(objects, current_backup_id, show_fasthash=True))

        # adding the backup too
        pull_contents(env, repo_uuid="backup-uuid", staging_id=backup_id, fetch_new={"full-uuid"})

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
