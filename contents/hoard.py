import logging
import os
import pathlib
import sys
from datetime import datetime
from functools import cached_property
from types import NoneType
from typing import Dict, Any, Optional, Tuple, Generator, Iterable, List

import rtoml
from alive_progress import alive_bar

from command.fast_path import FastPosixPath
from config import HoardConfig
from contents.hoard_props import HoardFileProps, GET_BY_MOVE, GET_BY_COPY, RESERVED
from contents.hoard_tree_walking import walk, composite_from_roots, hoard_tree_root
from contents.recursive_stats_calc import UsedSizeCalculator, NodeID, QueryStatsCalculator, drilldown, FolderStats, SizeCountPresenceStatsCalculator, SizeCountPresenceStats, FileStats, QueryStats, UsedSize, \
    CachedReader
from contents.repo import RepoContentsConfig
from lmdb_storage.cached_calcs import AppCachedCalculator
from lmdb_storage.file_object import FileObject
from lmdb_storage.lookup_tables import LookupTableObjToPaths, CompressedPath
from lmdb_storage.lookup_tables_paths import lookup_paths, get_path_string, compute_obj_id_to_path_lookup_table, \
    compute_obj_id_to_path_difference_lookup_table, decode_bytes_to_intpath
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.tree_iteration import zip_trees_dfs, dfs, DiffType, zip_dfs
from lmdb_storage.tree_object import ObjectType, StoredObject, TreeObject, ObjectID
from lmdb_storage.tree_structure import Objects
from util import custom_isabs

HOARD_CONTENTS_LMDB_DIR = "hoard.contents.lmdb"
HOARD_CONTENTS_TOML = "hoard.contents.toml"


class HoardContentsConfig:
    def __init__(self, file: pathlib.Path, is_readonly: bool):
        self.file = file
        self.is_readonly = is_readonly

        with open(file, "r") as f:
            self.doc = rtoml.load(f)

    def write(self):
        if self.is_readonly:
            raise ValueError("Cannot write to read-only contents!")

        with open(self.file, "w") as f:
            rtoml.dump(self.doc, f)

    def touch_updated(self) -> None:
        self.doc["updated"] = datetime.now().isoformat()
        self.write()

    def remote_uuids(self) -> List[str]:
        return list(self.doc.get("remotes", dict()).keys())

    def _remote_config(self, remote_uuid: str) -> Dict[str, Any]:
        if "remotes" not in self.doc:
            self.doc["remotes"] = {}

        if remote_uuid not in self.doc["remotes"]:
            self.doc["remotes"][remote_uuid] = {}
            self.write()

        return self.doc["remotes"][remote_uuid]

    def mark_up_to_date(self, remote_uuid: str, updated: datetime):
        self._remote_config(remote_uuid)["updated"] = updated.isoformat()
        self.write()

    def updated(self, remote_uuid: str) -> Optional[datetime]:
        remote = self._remote_config(remote_uuid)
        return datetime.fromisoformat(remote["updated"]) if "updated" in remote else None

    def save_remote_config(self, config: RepoContentsConfig):
        self._remote_config(config.uuid)["config"] = config.doc
        self.write()

    def restore_remote_config(self, config: RepoContentsConfig):
        config.doc["max_size"] = self.max_size(config.uuid)
        config.write()

    def max_size(self, uuid: str):
        return self._remote_config(uuid).get("config", {}).get("max_size", 0)

    def set_max_size_fallback(self, uuid: str, max_size: int):
        # fixme remove when all unit tests that rely on size are fixed
        remote = self._remote_config(uuid)
        if 'config' not in remote:
            remote["config"] = {}
        if 'max_size' not in remote["config"]:
            remote["config"]["max_size"] = max_size
            self.write()


class MovesAndCopies:
    def __init__(self, parent: "HoardContents") -> None:
        self.parent = parent

    @cached_property
    def _lookup_current(self) -> Dict[str, LookupTableObjToPaths[CompressedPath]]:
        roots = self.parent.env.roots(write=False)
        with self.parent.env.objects(write=False) as objects:
            return dict(
                (remote.uuid, LookupTableObjToPaths[CompressedPath](
                    compute_obj_id_to_path_lookup_table(objects, roots[remote.uuid].current),
                    decode_bytes_to_intpath, roots[remote.uuid].current))
                for remote in self.parent.hoard_config.remotes.all())

    @cached_property
    def _lookup_desired_but_not_current(self) -> Dict[str, LookupTableObjToPaths[CompressedPath]]:
        roots = self.parent.env.roots(write=False)
        with self.parent.env.objects(write=False) as objects:
            return dict(
                (remote.uuid, LookupTableObjToPaths[CompressedPath](
                    compute_obj_id_to_path_difference_lookup_table(
                        objects, roots[remote.uuid].desired, roots[remote.uuid].current),
                    decode_bytes_to_intpath, roots[remote.uuid].desired))
                for remote in self.parent.hoard_config.remotes.all())

    @cached_property
    def _lookup_current_but_not_desired(self) -> Dict[str, LookupTableObjToPaths[CompressedPath]]:
        roots = self.parent.env.roots(write=False)
        with self.parent.env.objects(write=False) as objects:
            return dict(
                (remote.uuid, LookupTableObjToPaths[CompressedPath](
                    compute_obj_id_to_path_difference_lookup_table(
                        objects, roots[remote.uuid].current, roots[remote.uuid].desired),
                    decode_bytes_to_intpath, roots[remote.uuid].current))
                for remote in self.parent.hoard_config.remotes.all())

    @cached_property
    def _lookup_hoard_desired(self) -> LookupTableObjToPaths[CompressedPath]:
        roots = self.parent.env.roots(write=False)
        with self.parent.env.objects(write=False) as objects:
            return LookupTableObjToPaths[CompressedPath](
                compute_obj_id_to_path_difference_lookup_table(
                    objects, roots["HOARD"].desired, roots["HOARD"].current),
                decode_bytes_to_intpath, roots["HOARD"].desired)

    def get_existing_paths_in_uuid(self, in_uuid: str, desired_id: ObjectID) -> List[CompressedPath]:
        return self._lookup_current[in_uuid][desired_id]

    def get_existing_paths_in_uuid_expanded(self, in_uuid: str, desired_id: ObjectID) -> Iterable[FastPosixPath]:
        with self.parent.env.objects(write=False) as objects:
            return list(lookup_paths(self._lookup_current[in_uuid], desired_id, objects.__getitem__))

    def get_paths_in_hoard_expanded(self, desired_id: ObjectID) -> Iterable[FastPosixPath]:
        with self.parent.env.objects(write=False) as objects:
            return list(lookup_paths(self._lookup_hoard_desired, desired_id, objects.__getitem__))

    def get_paths_in_hoard(self, desired_id: ObjectID) -> Iterable[CompressedPath]:
        return list(self._lookup_hoard_desired[desired_id])

    def resolve_on_hoard(self, compressed_path: CompressedPath, objects: Objects) -> FastPosixPath:
        return get_path_string(self._lookup_hoard_desired.root_id, compressed_path, objects.__getitem__)

    def get_remote_copies(self, skip_uuid: str, desired_id: ObjectID) -> Iterable[Tuple[str, List[CompressedPath]]]:
        for uuid in self._lookup_current.keys():
            if uuid == skip_uuid:
                continue

            existing_paths = list(self._lookup_current[uuid][desired_id])
            if len(existing_paths) > 0:
                yield uuid, existing_paths

    def get_remote_copies_expanded(
            self, skip_uuid: str, desired_id: ObjectID) -> Iterable[Tuple[str, List[FastPosixPath]]]:
        with self.parent.env.objects(write=False) as objects:
            for uuid in self._lookup_current.keys():
                if uuid == skip_uuid:
                    continue

                existing_paths = list(lookup_paths(self._lookup_current[uuid], desired_id, objects.__getitem__))
                if len(existing_paths) > 0:
                    yield uuid, existing_paths

    def whereis_needed(self, current_id: ObjectID) -> Iterable[Tuple[str, List[CompressedPath]]]:
        for uuid, lookup_table in self._lookup_desired_but_not_current.items():
            paths_needing_to_get = list(lookup_table[current_id])
            if len(paths_needing_to_get) > 0:
                yield uuid, paths_needing_to_get

    def whereis_cleanup(self, uuid: str, current_id: ObjectID):
        with self.parent.env.objects(write=False) as objects:
            return list(lookup_paths(self._lookup_current_but_not_desired[uuid], current_id, objects.__getitem__))


class ReadonlyHoardFSObjects:
    def __init__(self, parent: "HoardContents"):
        self.parent = parent

        self._size_and_count_agg = AppCachedCalculator(
            SizeCountPresenceStatsCalculator(self.parent),
            SizeCountPresenceStats)

    def hoard_files(self) -> Iterable[Tuple[FastPosixPath, FileObject]]:
        hoard_root_id = self.parent.env.roots(write=False)["HOARD"].desired
        with self.parent.env.objects(write=False) as objects:
            for path, obj_type, obj_id, obj, _ in dfs(objects, "", hoard_root_id):
                if obj_type == ObjectType.TREE:
                    continue
                obj: FileObject
                yield FastPosixPath(path), obj

    def in_folder(self, folder: FastPosixPath) -> Iterable[Tuple[FastPosixPath, FileObject]]:
        assert custom_isabs(folder.as_posix())  # from 3.13 behavior change...
        folder = folder.as_posix()
        folder_with_trailing = folder if folder.endswith("/") else folder + "/"
        assert folder_with_trailing.endswith('/')

        for folder, file in walk(hoard_tree_root(self.parent), CachedReader(self.parent), folder, sys.maxsize):
            if file is not None:
                yield FastPosixPath(file.fullname), file.file_obj

    def status_by_uuid(
            self, folder_path: FastPosixPath | None, extended: bool = False) -> Dict[str, Dict[str, Dict[str, Any]]]:
        stats: Dict[str, Dict[str, Dict[str, Any]]] = dict()

        node_id = composite_from_roots(self.parent)
        path_node_id = drilldown(self.parent, node_id, folder_path._rem if folder_path is not None else [])
        if path_node_id is None:
            logging.error(f"Requesting info for missing folder path {folder_path}?!")
            node_stats = SizeCountPresenceStats(0)
        else:
            node_stats: SizeCountPresenceStats = self._size_and_count_agg[path_node_id]

        for remote in self.parent.hoard_config.remotes.all():
            stats_for_remote = node_stats.for_remote(remote.uuid)
            if stats_for_remote.total.nfiles == 0:
                continue  # fixme ugly hack

            stats[remote.uuid] = {"total": {
                "nfiles": stats_for_remote.total.nfiles,
                "size": stats_for_remote.total.size}}

            for status, remote_stats in stats_for_remote.presence.items():
                stats[remote.uuid][status.value] = {
                    "nfiles": remote_stats.nfiles,
                    "size": remote_stats.size}

        if extended:
            moves_and_copies = MovesAndCopies(self.parent)

            with self.parent.env.objects(write=False) as objects:
                for remote in self.parent.hoard_config.remotes.all():
                    if remote.uuid not in stats:
                        continue  # won't extend missing  fixme is ugly hack

                    can_be_moved_cnt, can_be_moved_size = 0, 0
                    can_be_copied_cnt, can_be_copied_size = 0, 0
                    needed_to_get_cnt, needed_to_get_size = 0, 0

                    remote_root = self.parent.env.roots(write=False)[remote.uuid]
                    with alive_bar(title="Checking for extended stats") as bar:
                        for path, diff_type, current_id, desired_id, _ in zip_dfs(
                                objects, '', remote_root.current, remote_root.desired):
                            bar()
                            if diff_type == DiffType.LEFT_MISSING or diff_type == DiffType.DIFFERENT:
                                # file is missing from current, how can we get it?

                                assert desired_id is not None
                                desired_obj: StoredObject = objects[desired_id]

                                if desired_obj.object_type == ObjectType.TREE:
                                    continue  # skip trees

                                assert desired_obj.object_type == ObjectType.BLOB
                                desired_obj: FileObject

                                paths_to_move_inside_repo = moves_and_copies.get_existing_paths_in_uuid(
                                    remote.uuid, desired_id)
                                if len(paths_to_move_inside_repo) > 0:
                                    # can be moved/copied in repo
                                    can_be_moved_cnt += 1
                                    can_be_moved_size += desired_obj.size

                                for uuid, requested_paths in moves_and_copies.get_remote_copies(
                                        remote.uuid, desired_id):
                                    assert len(requested_paths) > 0

                                    can_be_copied_cnt += 1
                                    can_be_copied_size += desired_obj.size

                                    break  # we know that it is desired

                            elif diff_type == DiffType.RIGHT_MISSING:
                                # file is missing from desired, so supposed to be cleaned up
                                assert current_id is not None and desired_id is None
                                current_obj: StoredObject = objects[current_id]

                                if current_obj.object_type == ObjectType.TREE:
                                    continue  # skip trees
                                current_obj: FileObject

                                needed_in_places = 0
                                for uuid, paths_needing_to_get in moves_and_copies.whereis_needed(current_id):
                                    assert len(paths_needing_to_get) > 0
                                    needed_in_places += len(paths_needing_to_get)

                                if needed_in_places > 0:
                                    needed_to_get_cnt += 1
                                    needed_to_get_size += current_obj.size

                    if can_be_moved_cnt > 0:
                        stats[remote.uuid][GET_BY_MOVE] = {
                            "nfiles": can_be_moved_cnt,
                            "size": can_be_moved_size}

                    if can_be_copied_cnt > 0:
                        stats[remote.uuid][GET_BY_COPY] = {
                            "nfiles": can_be_copied_cnt,
                            "size": can_be_copied_size}

                    if needed_to_get_cnt > 0:
                        stats[remote.uuid][RESERVED] = {
                            "nfiles": needed_to_get_cnt,
                            "size": needed_to_get_size}

        return stats

    def to_fetch(self, repo_uuid: str) -> Generator[Tuple[str, FileObject], None, None]:
        remote_root = self.parent.env.roots(write=False)[repo_uuid]
        with self.parent.env.objects(write=False) as objects:
            for path, diff_type, current_id, desired_id, _ in zip_dfs(
                    objects, '', remote_root.current, remote_root.desired):
                if diff_type == DiffType.LEFT_MISSING or diff_type == DiffType.DIFFERENT:
                    assert desired_id is not None
                    desired_obj: StoredObject = objects[desired_id]
                    if desired_obj.object_type == ObjectType.TREE:
                        continue
                    desired_obj: FileObject
                    yield path, desired_obj

    def to_cleanup(self, repo_uuid: str) -> Generator[Tuple[FastPosixPath, FileObject], None, None]:
        remote_root = self.parent.env.roots(write=False)[repo_uuid]
        with self.parent.env.objects(write=False) as objects:
            for path, diff_type, current_id, desired_id, _ in zip_dfs(
                    objects, '', remote_root.current, remote_root.desired):
                if diff_type == DiffType.RIGHT_MISSING:
                    assert current_id is not None
                    current_obj: StoredObject = objects[current_id]
                    if current_obj.object_type == ObjectType.TREE:
                        continue
                    current_obj: FileObject
                    yield FastPosixPath(path), current_obj

    def __contains__(self, file_path: FastPosixPath) -> bool:
        assert file_path.is_absolute()
        raise NotImplementedError()

    def used_size(self, repo_uuid: str) -> int:
        return self.query.used_size(repo_uuid)

    def stats_in_folder(self, folder_path: FastPosixPath) -> Tuple[int, int]:
        assert folder_path.is_absolute()

        return self.query.stats_in_folder(folder_path)

    @cached_property
    def query(self) -> "Query":
        return Query(self.parent)

    def desired_hoard(self) -> Iterable[Tuple[FastPosixPath, FileObject]]:
        current_root_id = self.parent.env.roots(write=False)["HOARD"].desired
        return self._iterate_all_files(current_root_id)

    def _iterate_all_files(self, current_root_id):
        with self.parent.env.objects(write=False) as objects:
            for path, object_type, obj_id, obj, _ in dfs(objects, "", current_root_id):
                if object_type == ObjectType.BLOB:
                    assert isinstance(obj, FileObject)
                    yield FastPosixPath(path), obj

    def desired_in_repo(self, remote_uuid: str):
        current_repo_id = self.parent.env.roots(write=False)[remote_uuid].desired
        return self._iterate_all_files(current_repo_id)


class Query:
    def __init__(self, parent: "HoardContents"):
        self.parent = parent

        self._repo_stats_agg = AppCachedCalculator(UsedSizeCalculator(parent), UsedSize)
        self._file_and_folder_stats = AppCachedCalculator(QueryStatsCalculator(parent), QueryStats)

    def count_non_deleted(self, folder_name: FastPosixPath) -> int:
        stats = self._get_folder_stats(folder_name)
        return stats.count_non_deleted

    def _get_folder_stats(self, folder_name: FastPosixPath) -> FolderStats:
        stats = self._get_stats(folder_name)

        if not stats.folder:
            raise ValueError(f"Received info for file at {folder_name}?!")

        return stats.folder

    def _get_file_stats(self, file_name: FastPosixPath) -> FileStats:
        stats = self._get_stats(file_name)

        if not stats.file:
            raise ValueError(f"Received info for folder at {file_name}?!")

        return stats.file

    def _get_stats(self, folder_name: FastPosixPath) -> QueryStats:
        assert folder_name.is_absolute()
        node_id = composite_from_roots(self.parent)
        path_node_id = drilldown(self.parent, node_id, folder_name._rem)

        assert path_node_id is not None
        stats = self._file_and_folder_stats[path_node_id]
        return stats

    def num_without_source(self, folder_name: FastPosixPath) -> int:
        assert folder_name.is_absolute()
        stats = self._get_folder_stats(folder_name)
        return stats.num_without_sources

    def is_deleted(self, file_name: FastPosixPath) -> bool:
        assert file_name.is_absolute()
        return self._get_file_stats(file_name).is_deleted

    def num_sources(self, file_name: FastPosixPath) -> int:
        assert file_name.is_absolute()
        return self._get_file_stats(file_name).num_sources

    def stats_in_folder(self, folder_path: FastPosixPath):
        assert folder_path.is_absolute()
        stats = self._get_folder_stats(folder_path)
        return stats.count, stats.used_size

    def used_size(self, repo_uuid: str) -> int:
        repo_root = self.parent.env.roots(write=False)[repo_uuid]
        repo_root_node: NodeID = NodeID(repo_root.desired, repo_root.current)
        return self._repo_stats_agg[repo_root_node].used_size


class HoardContents:
    config: HoardContentsConfig
    fsobjects: ReadonlyHoardFSObjects

    def __init__(self, folder: pathlib.Path, is_readonly: bool, hoard_config: HoardConfig):
        self.hoard_config: HoardConfig = hoard_config
        self.config = HoardContentsConfig(folder.joinpath(HOARD_CONTENTS_TOML), is_readonly)
        self.fsobjects = ReadonlyHoardFSObjects(self)

        self.env = ObjectStorage(os.path.join(folder, HOARD_CONTENTS_LMDB_DIR), map_size=1 << 30)  # 1GB
        self.env.__enter__()

    def close(self, writeable: bool):
        self.env.gc()
        if writeable:
            self.validate_desired()

        self.env.__exit__(None, None, None)
        self.env = None

        if writeable:
            self.config.write()

        self.config = None
        self.fsobjects = None

    def validate_desired(self):
        """Validate that all desired trees have only one file version for each file."""
        all_roots = self.env.roots(write=False).all_roots
        try:
            hoard_root_idx = [r.name for r in all_roots].index("HOARD")
        except ValueError:
            logging.warning("HOARD root is not defined?!")
            hoard_root_idx = None

        with self.env.objects(write=False) as objects:
            for path, desired_roots, _ in zip_trees_dfs(
                    objects, "", [r.desired for r in all_roots], drilldown_same=False):
                desired_objs = list(map((lambda o_id: objects[o_id] if o_id is not None else None), desired_roots))
                desired_types = map(type, desired_objs)
                non_none_types = set(desired_types) - {NoneType}
                if len(non_none_types) > 1:
                    raise ValueError(f"object at path {path} is of many types in different trees: %s", non_none_types)
                non_none_type = next(iter(non_none_types))
                if non_none_type is TreeObject:
                    pass
                else:
                    assert non_none_type is FileObject
                    file_ids = {o.id for o in desired_objs if o is not None}
                    if len(file_ids) > 1:
                        raise ValueError(f"Object at path {path} has multiple desired file versions: {file_ids}")

                    if hoard_root_idx is not None and desired_objs[hoard_root_idx] is None:
                        raise ValueError(f"File at path {path} is not in hoard root!")

    def remote_name(self, candidate_uuid) -> str:
        return self.hoard_config.remotes[candidate_uuid].name


def HACK_create_from_hoard_props(hoard_props: HoardFileProps) -> FileObject:
    return FileObject.create(hoard_props.fasthash, hoard_props.size, None)
