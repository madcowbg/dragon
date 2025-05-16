from io import StringIO
from typing import Set, List

from lmdb_storage.merge_trees import ObjectsByRoot, ByRoot
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.roots import Root
from lmdb_storage.three_way_merge import ThreewayMerge, MergePreferences, NaiveMergePreferences
from lmdb_storage.tree_structure import ObjectID


def pull_contents(env: ObjectStorage, repo_uuid: str, staging_id: ObjectID, merge_prefs: NaiveMergePreferences):
    assert "HOARD" not in merge_prefs.to_modify
    merge_prefs = NaiveMergePreferences(merge_prefs.to_modify + [repo_uuid, "HOARD"])

    env.roots(write=True)[repo_uuid].staging = staging_id

    roots = env.roots(write=False)
    repo_roots = [r for r in roots.all if r.name != "HOARD"]
    repo_root_names = [r.name for r in repo_roots]

    merged_ids = merge_contents(env, roots[repo_uuid], repo_roots, merge_prefs)

    roots = env.roots(write=True)
    commit_merged(roots['HOARD'], roots[repo_uuid], [roots[rn] for rn in repo_root_names], merged_ids)

    return merged_ids


def merge_contents(
        env: ObjectStorage, repo_root: Root, all_repo_roots: List[Root], merge_prefs: MergePreferences) \
        -> ByRoot[ObjectID]:
    assert repo_root in all_repo_roots, f"{repo_root.name} is missing from other_roots={all_repo_roots}"

    # assign roots
    repo_current_id = repo_root.current
    repo_staging_id = repo_root.staging

    hoard_head_id = env.roots(False)["HOARD"].desired

    all_root_names = [r.name for r in all_repo_roots] + ["HOARD"]
    current_ids = ByRoot[ObjectID](
        list(set(list(all_root_names) + ["current", "staging"])),
        [(r.name, r.desired) for r in all_repo_roots] + [
            ("current", repo_current_id),
            ("staging", repo_staging_id),
            ("HOARD", hoard_head_id)])

    assert all(v is not None for v in current_ids.values())

    # execute merge
    with env.objects(write=True) as objects:
        merged_ids = ThreewayMerge(
            objects, current="current", staging='staging', others=all_root_names, merge_prefs=merge_prefs) \
            .merge_trees(current_ids)

    return merged_ids


def commit_merged(hoard: Root, repo: Root, all_roots: List[Root], merged_ids: ByRoot[ObjectID]):
    # set current for the repo being merged
    repo.current = merged_ids.get_if_present("current")
    assert repo.name in merged_ids, f"{repo.name} is missing from merged_ids={merged_ids}"
    assert repo in all_roots, f"{repo} is missing from all_roots={all_roots}"

    # accept the changed IDs as desired
    hoard.desired = merged_ids.get_if_present("HOARD")
    for other_root in all_roots:
        other_root.desired = merged_ids.get_if_present(other_root.name)
