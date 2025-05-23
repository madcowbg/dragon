from typing import List, Optional

from command.content_prefs import ContentPrefs
from command.contents.pull_preferences import PullMergePreferences, PullPreferences
from lmdb_storage.operations.util import ByRoot
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.roots import Root
from lmdb_storage.operations.three_way_merge import ThreewayMerge, MergePreferences, TransformedRoots
from lmdb_storage.tree_structure import ObjectID


def merge_contents(
        env: ObjectStorage, repo_root: Root, all_repo_roots: List[Root],
        *, preferences: PullPreferences = None,
        content_prefs: ContentPrefs = None,
        merge_prefs: MergePreferences = None,
        merge_only: Optional[List[str]] = None) \
        -> TransformedRoots:
    assert repo_root in all_repo_roots, f"{repo_root.name} is missing from other_roots={all_repo_roots}"

    # assign roots
    repo_current_id = repo_root.current
    repo_staging_id = repo_root.staging

    all_root_names = [r.name for r in all_repo_roots] + ["HOARD"]

    if merge_prefs is not None:
        assert preferences is None
        assert content_prefs is None
    else:
        assert preferences is not None, "Missing preferences"
        assert content_prefs is not None, "Missing content_prefs"
        merge_prefs = PullMergePreferences(
            preferences, content_prefs, preferences.local_uuid, preferences.remote_type,
            uuid_roots=[r.name for r in all_repo_roots] if merge_only is None else merge_only,
            roots_to_merge=all_root_names)

    current_ids = ByRoot[ObjectID](
        list(set(all_root_names + ["current", "staging"])),
        [(r.name, r.desired) for r in all_repo_roots] + [
            ("current", repo_current_id),
            ("staging", repo_staging_id),
            ("HOARD", env.roots(False)["HOARD"].desired)])

    assert all(v is not None for v in current_ids.values())

    # execute merge
    with env.objects(write=True) as objects:
        merged_ids = ThreewayMerge(
            objects, current_id=repo_current_id, staging_id=repo_staging_id, repo_name=repo_root.name,
            merge_prefs=merge_prefs) \
            .execute(current_ids)

    return merged_ids


def commit_merged(hoard: Root, repo: Root, all_roots: List[Root], merged_ids: TransformedRoots):
    # set current for the repo being merged
    repo.current = repo.staging

    assert repo in all_roots, f"{repo} is missing from all_roots={all_roots}"

    # accept the changed IDs as desired
    hoard.desired = merged_ids.get_if_present("HOARD")
    for other_root in all_roots:
        other_root.desired = merged_ids.get_if_present(other_root.name)
