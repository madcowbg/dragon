from typing import Set, List

from lmdb_storage.merge_trees import ObjectsByRoot
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.roots import Root
from lmdb_storage.three_way_merge import ThreewayMerge
from lmdb_storage.tree_structure import ObjectID


def pull_contents(env: ObjectStorage, repo_uuid: str, staging_id: ObjectID, fetch_new: Set[str]):
    env.roots(write=True)[repo_uuid].staging = staging_id

    roots = env.roots(write=False)
    repo_roots = [r for r in roots.all if r.name != "HOARD"]
    repo_root_names = [r.name for r in repo_roots]

    merged_ids = merge_contents(env, roots[repo_uuid], repo_roots, fetch_new)

    commit_merged(env, repo_uuid, repo_root_names, merged_ids)

    return merged_ids


def merge_contents(env: ObjectStorage, repo_root: Root, other_repo_roots: List[Root], fetch_new: Set[str]) -> ObjectsByRoot:
    assert "HOARD" not in fetch_new
    fetch_new = fetch_new.copy()
    fetch_new.add("HOARD")

    assert repo_root in other_repo_roots, f"{repo_root.name} is missing from other_roots={other_repo_roots}"

    # assign roots
    repo_current_id = repo_root.current
    repo_staging_id = repo_root.staging

    hoard_head_id = env.roots(False)["HOARD"].desired

    other_root_names = [r.name for r in other_repo_roots]
    current_ids = ObjectsByRoot(
        list(other_root_names + ["current", "staging", "HOARD"]),
        [(r.name, r.desired) for r in other_repo_roots] + [
            ("current", repo_current_id),
            ("staging", repo_staging_id),
            ("HOARD", hoard_head_id)])

    assert all(v is not None for v in current_ids.assigned_values())

    # execute merge
    with env.objects(write=True) as objects:
        merged_ids = ThreewayMerge(
            objects, current="current", staging='staging', others=other_root_names,
            fetch_new=set([repo_root.name] + list(fetch_new))).merge_trees(
            current_ids)  # fixme hack to fetch_new maybe not proper?

        assert len(set(current_ids.assigned().keys()) - set(merged_ids.assigned().keys())) == 0, \
            f"{set(merged_ids.assigned().keys())} not contains all of {set(current_ids.assigned().keys())}"

    return merged_ids


def commit_merged(env: ObjectStorage, repo_uuid: str, other_root_names: List[str], merged_ids):
    roots = env.roots(write=True)

    # set current for the repo being merged
    roots[repo_uuid].current = merged_ids.get_if_present("current")

    # accept the changed IDs as desired
    roots["HOARD"].desired = merged_ids.get_if_present("HOARD")
    for other_name in other_root_names:
        roots[other_name].desired = merged_ids.get_if_present(other_name)
