import binascii
import unittest
from tempfile import TemporaryDirectory
from typing import Set

from lmdb_storage.merge_trees import merge_trees
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.test_merge_trees import populate_trees
from lmdb_storage.three_way_merge import ThreewayMerge
from lmdb_storage.tree_structure import ObjectID


def pull_contents(env: ObjectStorage, repo_uuid: str, staging_id: ObjectID, fetch_new: Set[str]):
    assert "HOARD" not in fetch_new
    fetch_new = fetch_new.copy()
    fetch_new.add("HOARD")

    # assign roots
    roots = env.roots(write=False)
    repo_current_id = roots[repo_uuid].get_current()
    hoard_head_id = roots["HOARD"].get_current()

    repo_staging_id = staging_id

    other_roots = [r for r in roots.all if r.name != repo_current_id and r.name != "HOARD"]
    other_root_names = [r.name for r in other_roots]
    current_ids = dict((r.name, r.get_current()) for r in other_roots)
    current_ids["current"] = repo_current_id
    current_ids["staging"] = repo_staging_id
    current_ids["HOARD"] = hoard_head_id

    assert all(v is not None for v in current_ids.values())

    # execute merge
    with env.objects(write=True) as objects:
        merged_ids = merge_trees(
            current_ids,
            ThreewayMerge(
                objects, current="current", staging='staging', others=other_root_names,
                fetch_new=fetch_new))

        assert set(merged_ids.keys()) == set(current_ids.keys()), \
            f"{set(merged_ids.keys())} != {set(current_ids.keys())}"

    # accept the changed IDs todo implement dry-run
    roots = env.roots(write=True)
    for other_name in other_root_names:
        roots[other_name].set_current(merged_ids[other_name])

    roots["HOARD"].set_current(merged_ids["HOARD"])
    roots[repo_uuid].set_current(merged_ids["current"])


class TestWorkflows(unittest.TestCase):
    def test_pull_contents(self):
        tmpdir = TemporaryDirectory(delete=True)
        env, partial_id, full_id, backup_id, incoming_id = populate_trees(tmpdir.name + "/test-objects.lmdb")

        with env.objects(write=True) as objects:
            empty_tree_id = objects.mktree_from_tuples([])
            self.assertEqual(b'a80f91bc48850a1fb3459bb76b9f6308d4d35710', binascii.hexlify(empty_tree_id))

        # init it as empty staging
        roots = env.roots(write=True)
        roots["HOARD"].set_current(empty_tree_id)
        roots["partial-uuid"].set_current(empty_tree_id)
        roots["full-uuid"].set_current(empty_tree_id)
        roots["backup-uuid"].set_current(empty_tree_id)
        roots["incoming-uuid"].set_current(empty_tree_id)

        pull_contents(env, repo_uuid="partial-uuid", staging_id=partial_id, fetch_new={"full-uuid"})
