import logging
import logging
import os
import pathlib
import sys
from datetime import datetime
from functools import cached_property
from types import NoneType
from typing import Dict, Any, Optional, Tuple, Generator, Iterable, List, AsyncGenerator

import rtoml

from command.fast_path import FastPosixPath
from config import HoardConfig
from contents.hoard_props import HoardFileStatus, HoardFileProps
from contents.recursive_stats_calc import UsedSizeCalculator, NodeID, QueryStatsCalculator, composite_from_roots, \
    drilldown, FolderStats, SizeCountPresenceStatsCalculator, SizeCountPresenceStats
from contents.repo import RepoContentsConfig
from lmdb_storage.file_object import BlobObject, FileObject
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.operations.fast_association import FastAssociation
from lmdb_storage.operations.generator import TreeGenerator
from lmdb_storage.operations.util import ByRoot
from lmdb_storage.tree_calculation import CachedCalculator, ValueCalculator
from lmdb_storage.tree_iteration import zip_trees_dfs
from lmdb_storage.tree_object import ObjectType, StoredObject, TreeObject, MaybeObjectID
from lmdb_storage.tree_operations import get_child
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


class HoardTree:
    def __init__(self, objects: "ReadonlyHoardFSObjects"):
        self.root = HoardDir(None, "", self)
        self.objects = objects

    def walk(self, from_path: str = "/", depth: int = sys.maxsize) -> \
            Generator[Tuple[Optional["HoardDir"], Optional["HoardFile"]], None, None]:
        assert custom_isabs(from_path)
        current = self.root
        for folder in pathlib.Path(from_path).parts[1:]:
            current = current.get_dir(folder)
            if current is None:
                return

        yield from current.walk(depth)


class HoardFile:
    def __init__(self, parent: "HoardDir", name: str, fullname: str, fsobjects: "ReadonlyHoardFSObjects"):
        self.parent = parent
        self.name = name

        self.fullname = fullname
        self.fsobjects = fsobjects

        self._props = None

    @property
    def props(self) -> HoardFileProps:
        if self._props is None:
            self._props = self.fsobjects[FastPosixPath(self.fullname)]
        return self._props

    def reload_props(self):
        self._props = self.fsobjects[FastPosixPath(self.fullname)]


class HoardDir:
    @cached_property
    def fullname(self) -> str:  # fixme replace with FullPosixPath
        parent_path = pathlib.Path(self.parent.fullname) if self.parent is not None else pathlib.Path("/")
        return parent_path.joinpath(self.name).as_posix()

    def __init__(self, parent: Optional["HoardDir"], name: str, tree: HoardTree):
        self.tree = tree
        self.name = name
        self.parent = parent

    @property
    def dirs(self) -> Dict[str, "HoardDir"]:
        return dict(
            (_filename(subname), HoardDir(self, _filename(subname), self.tree))
            for subname in self.tree.objects.get_sub_dirs(self._as_parent))

    @property
    def _as_parent(self):
        return self.fullname if self.fullname != "/" else ""

    @property
    def files(self) -> Dict[str, HoardFile]:
        return dict(
            (_filename(subname), HoardFile(self, _filename(subname), subname, self.tree.objects))
            for subname in self.tree.objects.get_sub_files(self._as_parent))

    def get_dir(self, subname: str) -> Optional["HoardDir"]:  # FIXME remove, slow and obsolete
        return self.dirs.get(subname, None)

    def walk(self, depth: int) -> Generator[Tuple[Optional["HoardDir"], Optional["HoardFile"]], None, None]:
        yield self, None
        if depth <= 0:
            return
        for hoard_file in self.files.values():
            yield None, hoard_file
        for hoard_dir in self.dirs.values():
            yield from hoard_dir.walk(depth - 1)


def _filename(filepath: str) -> str:
    _, name = os.path.split(filepath)
    return name


STATUSES_TO_FETCH = [HoardFileStatus.COPY, HoardFileStatus.GET, HoardFileStatus.MOVE]


class HoardFilesIterator(TreeGenerator[BlobObject, Tuple[str, HoardFileProps]]):
    def __init__(self, objects: Objects, parent: "HoardContents"):
        self.parent = parent
        self.objects = objects

    def compute_on_level(
            self, path: List[str], original: FastAssociation[StoredObject]
    ) -> Iterable[Tuple[FastPosixPath, HoardFileProps]]:
        path = FastPosixPath("/" + "/".join(path))
        file_obj: BlobObject | None = original.get_if_present("HOARD")

        if file_obj is None:
            # fixme this is the legacy case where we iterate over current but not desired files. remove!
            file_obj: BlobObject | None = next(
                (f for root_name, f in original.available_items() if f.object_type == ObjectType.BLOB), None)

        if not file_obj or file_obj.object_type != ObjectType.BLOB:
            logging.debug("Skipping path %s as it is not a BlobObject", path)
            return

        assert isinstance(file_obj, FileObject)
        yield path, HoardFileProps(
            self.parent, path, file_obj.size, file_obj.fasthash, by_root=original, file_id=file_obj.id)

    def should_drill_down(self, path: List[str], trees: ByRoot[TreeObject], files: ByRoot[BlobObject]) -> bool:
        return True

    @staticmethod
    def all(parent: "HoardContents") -> Iterable[Tuple[FastPosixPath, HoardFileProps]]:
        hoard_root, root_ids = find_roots(parent)

        obj_ids = ByRoot(
            [name for name, _ in root_ids] + ["HOARD"],
            root_ids + [("HOARD", hoard_root)])

        with parent.env.objects(write=False) as objects:
            yield from HoardFilesIterator(objects, parent).execute(obj_ids=obj_ids)


def find_roots(parent: "HoardContents") -> (MaybeObjectID, List[Tuple[str, MaybeObjectID]]):
    roots = parent.env.roots(write=False)
    hoard_root = roots["HOARD"].desired
    all_roots = roots.all_roots
    with roots:
        root_data = [(r.name, r.load_from_storage) for r in all_roots]
    root_ids = sum(
        [[("current@" + name, data.current), ("desired@" + name, data.desired)] for name, data in root_data],
        # fixme should only iterate over desired files
        [])
    return hoard_root, root_ids


def hoard_file_props_from_tree(parent, file_path: FastPosixPath) -> HoardFileProps:
    hoard_root, root_ids = find_roots(parent)
    with parent.env.objects(write=False) as objects:
        hoard_child_id = get_child(objects, file_path._rem, hoard_root)
        file_obj: StoredObject | None = objects[hoard_child_id] if hoard_child_id is not None else None
        if file_obj is not None:
            assert isinstance(file_obj, FileObject)
            file_obj: FileObject
            return HoardFileProps(parent, file_path, file_obj.size, file_obj.fasthash)

        # fixme this is the legacy case where we iterate over current but not desired files. remove!
        for _, root_id in root_ids:
            root_child_id = get_child(objects, file_path._rem, root_id)
            file_obj = objects[root_child_id] if root_child_id is not None else None
            if file_obj and file_obj.object_type == ObjectType.BLOB:
                assert isinstance(file_obj, FileObject)
                return HoardFileProps(parent, file_path, file_obj.size, file_obj.fasthash)

        raise ValueError("Should not have tried getting a nonexistent file!")


class ReadonlyHoardFSObjects:
    def __init__(self, parent: "HoardContents"):
        self.parent = parent

        self._size_and_count_agg = CachedCalculator(SizeCountPresenceStatsCalculator(self.parent))

    @cached_property
    async def tree(self) -> HoardTree:
        return HoardTree(self)

    def __getitem__(self, file_path: FastPosixPath) -> HoardFileProps:
        assert file_path.is_absolute()
        return hoard_file_props_from_tree(self.parent, file_path)

    def by_fasthash(self, fasthash: str) -> Iterable[Tuple[FastPosixPath, HoardFileProps]]:
        logging.error("Should not use this method, is too slow!")
        for path, props in HoardFilesIterator.all(self.parent):
            if props.fasthash == fasthash:
                yield path, props

    def __iter__(self) -> Iterable[Tuple[FastPosixPath, HoardFileProps]]:
        yield from HoardFilesIterator.all(self.parent)

    def with_pending(self, repo_uuid: str) -> Iterable[Tuple[FastPosixPath, HoardFileProps]]:
        pending_statuses = {HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.MOVE, HoardFileStatus.CLEANUP}
        for path, props in HoardFilesIterator.all(self.parent):
            if props.get_status(repo_uuid) in pending_statuses:
                yield path, props

    def available_in_repo(self, repo_uuid: str) -> Iterable[Tuple[FastPosixPath, HoardFileProps]]:
        for path, props in HoardFilesIterator.all(self.parent):
            if props.get_status(repo_uuid) == HoardFileStatus.AVAILABLE:
                yield path, props

    def to_get_in_repo(self, repo_uuid: str) -> Iterable[Tuple[FastPosixPath, HoardFileProps]]:
        for path, props in HoardFilesIterator.all(self.parent):
            if props.get_status(repo_uuid) == HoardFileStatus.GET:
                yield path, props

    async def in_folder(self, folder: FastPosixPath) -> AsyncGenerator[
        Tuple[FastPosixPath, HoardFileProps]]:
        assert custom_isabs(folder.as_posix())  # from 3.13 behavior change...
        folder = folder.as_posix()
        folder_with_trailing = folder if folder.endswith("/") else folder + "/"
        assert folder_with_trailing.endswith('/')

        # fixme this could be done faster by directly drilling down to the folder
        for path, props in HoardFilesIterator.all(self.parent):
            if path.simple.startswith(folder_with_trailing):
                yield path, props

    async def in_folder_non_deleted(self, folder: FastPosixPath) -> AsyncGenerator[
        Tuple[FastPosixPath, HoardFileProps]]:
        assert custom_isabs(folder.as_posix())  # from 3.13 behavior change...

        folder = folder.as_posix()
        folder_with_trailing = folder if folder.endswith("/") else folder + "/"
        assert folder_with_trailing.endswith('/')

        # fixme this could be done faster by directly drilling down to the folder
        for path, props in HoardFilesIterator.all(self.parent):
            if path.simple.startswith(folder_with_trailing):
                if any(status != HoardFileStatus.CLEANUP for uuid, status in props.presence.items()):
                    yield path, props

    def status_by_uuid(self, folder_path: FastPosixPath | None) -> Dict[str, Dict[str, Dict[str, Any]]]:
        stats: Dict[str, Dict[str, Dict[str, Any]]] = dict()

        node_id = composite_from_roots(self.parent)
        path_node_id = drilldown(self.parent, node_id, folder_path._rem if folder_path is not None else [])
        if path_node_id is None:
            logging.error(f"Requesting info for missing folder path {folder_path}?!")
            node_stats = SizeCountPresenceStats()
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

        return stats

    def to_fetch(self, repo_uuid: str) -> Generator[Tuple[str, HoardFileProps], None, None]:
        for path, props in HoardFilesIterator.all(self.parent):
            if props.get_status(repo_uuid) in STATUSES_TO_FETCH:
                yield path.as_posix(), props

    def to_cleanup(self, repo_uuid: str) -> Generator[Tuple[FastPosixPath, HoardFileProps], None, None]:
        for path, props in HoardFilesIterator.all(self.parent):
            if props.get_status(repo_uuid) == HoardFileStatus.CLEANUP:
                yield path, props

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

    def get_sub_dirs(self, fullpath: str) -> Iterable[str]:
        # fixme drilldown can be done via actual tree, no need for this hack
        hoard_root = self.parent.env.roots(write=False)["HOARD"].desired
        with self.parent.env.objects(write=False) as objects:
            path_id = get_child(objects, FastPosixPath(fullpath)._rem, hoard_root)
            path_obj = objects[path_id] if path_id else None
            if isinstance(path_obj, TreeObject):
                children = [(child_name, objects[child_id]) for child_name, child_id in path_obj.children]
                yield from [fullpath + "/" + child_name for child_name, child_obj in children if
                            isinstance(child_obj, TreeObject)]

    def get_sub_files(self, fullpath: str) -> Iterable[str]:
        # fixme drilldown can be done via actual tree, no need for this hack
        hoard_root = self.parent.env.roots(write=False)["HOARD"].desired
        with self.parent.env.objects(write=False) as objects:
            path_id = get_child(objects, FastPosixPath(fullpath)._rem, hoard_root)
            path_obj: StoredObject | None = objects[path_id] if path_id else None
            if path_obj and path_obj.object_type == ObjectType.TREE:
                path_obj: TreeObject
                children = [(child_name, objects[child_id]) for child_name, child_id in path_obj.children]
                yield from [
                    fullpath + "/" + child_name for child_name, child_obj in children
                    if child_obj.object_type == ObjectType.BLOB]


class Query:
    def __init__(self, parent: "HoardContents"):
        self.parent = parent

        # fixme replace with live aggregator
        self.file_stats = dict()
        for path, props in HoardFilesIterator.all(self.parent):
            presence = props.presence
            self.file_stats[path.simple] = {
                "is_deleted": len(
                    [uuid for uuid, status in presence.items() if status != HoardFileStatus.CLEANUP]) == 0,
                "num_sources": len([uuid for uuid, status in presence.items() if
                                    status in (HoardFileStatus.AVAILABLE, HoardFileStatus.MOVE)]),
                "used_size": props.size
            }
        self._repo_stats_agg = CachedCalculator(UsedSizeCalculator(parent))

    def count_non_deleted(self, folder_name: FastPosixPath) -> int:
        count = 0
        for path, stats in self.file_stats.items():
            if path.startswith(folder_name.simple + "/") and not stats["is_deleted"]:
                count += 1
        return count

    def num_without_source(self, folder_name: FastPosixPath) -> int:
        count = 0
        for path, stats in self.file_stats.items():
            if path.startswith(folder_name.simple + "/") and stats["num_sources"] == 0:
                count += 1
        return count

    def is_deleted(self, file_name: FastPosixPath) -> bool:
        return self.file_stats[file_name.simple]["is_deleted"]

    def stats_in_folder(self, folder_path: FastPosixPath):
        count, used_size = 0, 0
        # fixme replace with size aggregator
        for path, stats in self.file_stats.items():
            if path.startswith(folder_path.simple + "/"):
                used_size += stats["used_size"]
                count += 1
        return count, used_size

    def used_size(self, repo_uuid: str) -> int:
        repo_root = self.parent.env.roots(write=False)[repo_uuid]
        repo_root_node: NodeID = (repo_root.desired, repo_root.current)
        return self._repo_stats_agg[repo_root_node].value


STATUSES_THAT_USE_SIZE = [HoardFileStatus.AVAILABLE, HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.CLEANUP]


class ReadonlyHoardContentsConn:
    def __init__(self, folder: pathlib.Path, config: HoardConfig):
        self.folder = folder
        self.config = config

    async def __aenter__(self) -> "HoardContents":
        self.contents = HoardContents(self.folder, True, self.config)
        return self.contents

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.contents.close(False)
        return None

    def writeable(self):
        return HoardContentsConn(self.folder, self.config)


class HoardContentsConn:
    def __init__(self, folder: pathlib.Path, config: HoardConfig):
        self.config = config

        toml_filename = os.path.join(folder, HOARD_CONTENTS_TOML)
        if not os.path.isfile(toml_filename):
            with open(toml_filename, "w") as f:
                rtoml.dump({
                    "updated": datetime.now().isoformat()
                }, f)

        self.folder = folder

    async def __aenter__(self) -> "HoardContents":
        self.contents = HoardContents(self.folder, False, self.config)
        return self.contents

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.contents.close(True)
        return None


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
