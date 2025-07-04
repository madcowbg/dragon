import os
import shutil
from datetime import datetime
from typing import Tuple, Iterable

import rtoml

from command.fast_path import FastPosixPath
from contents.repo_props import FileDesc
from exceptions import MissingRepoContents
from lmdb_storage.file_object import FileObject
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.roots import Roots
from lmdb_storage.tree_calculation import RecursiveSumCalculator, TreeReader
from lmdb_storage.cached_calcs import CachedCalculator
from lmdb_storage.tree_iteration import dfs
from lmdb_storage.tree_object import ObjectType, StoredObject
from lmdb_storage.tree_structure import Objects, ObjectID, add_file_object, remove_file_object, StoredObjects


class RepoFSObjects:
    class Stats:
        def __init__(self, objects: Objects, roots: Roots):
            self.objects = objects
            self.root_id = roots["REPO"].current

            self.count_aggregator = CachedCalculator[ObjectID, int](
                RecursiveSumCalculator[ObjectID, StoredObject](lambda file_obj: 1, TreeReader(objects)))
            self.size_aggregator = CachedCalculator[ObjectID, int](
                RecursiveSumCalculator[ObjectID, StoredObject](lambda file_obj: file_obj.size, TreeReader(objects)))

        @property
        def num_files(self) -> int:
            return self.count_aggregator[self.root_id]

        @property
        def total_size(self) -> int:
            return self.size_aggregator[self.root_id]

    def __init__(self, objects: Objects, roots: Roots, config: "RepoContentsConfig"):
        self.objects = objects
        self.roots = roots
        self.config = config

    @property
    def root_id(self) -> ObjectID | None:
        return self.roots["REPO"].current

    @property
    def stats_existing(self):
        return RepoFSObjects.Stats(self.objects, self.roots)

    def all_status(self) -> Iterable[Tuple[FastPosixPath, FileDesc]]:
        yield from self.existing()

    def existing(self) -> Iterable[Tuple[FastPosixPath, FileDesc]]:
        root_id = self.root_id
        assert root_id is None or len(root_id) == 20
        with self.objects as objects:
            for fullpath, obj_type, obj_id, obj, _ in dfs(objects, "", root_id):
                if obj_type == ObjectType.BLOB:
                    assert isinstance(obj, FileObject)
                    yield (
                        FastPosixPath(fullpath).relative_to("/"),
                        FileDesc(obj.size, obj.fasthash, None))

    def add_file(self, filepath: FastPosixPath, file_obj: FileObject) -> None:
        root_id = self.root_id
        with self.objects as objects:
            root_id = add_file_object(
                objects, root_id, filepath.as_posix().split("/"), file_obj)

        self.roots["REPO"].current = root_id

    def mark_moved(self, from_file: FastPosixPath, to_file: FastPosixPath, file_obj: FileObject):
        assert not from_file.is_absolute()
        assert not to_file.is_absolute()

        self.mark_removed(from_file)

        # add the new file
        self.add_file(to_file, file_obj)

    def mark_removed(self, path: FastPosixPath):
        assert not path.is_absolute()

        root_id = self.roots["REPO"].current
        with self.objects as objects:
            new_root_id = remove_file_object(
                objects, root_id, path.as_posix().split("/"))
        self.roots["REPO"].current = new_root_id


class RepoContentsConfig:
    def __init__(self, config_path: str):
        self.config_path = config_path
        with open(self.config_path, "r") as f:
            self.doc = rtoml.load(f)

    def write(self):
        with open(self.config_path, "w") as f:
            rtoml.dump(self.doc, f)

    def touch_updated(self) -> None:
        self.write()

    @property
    def updated(self) -> datetime:
        return datetime.fromisoformat(self.doc["last_updated"])

    def end_updating(self):
        self.doc["last_updated"] = datetime.now().isoformat()
        self.write()

    @property
    def uuid(self) -> str: return self.doc["uuid"]

    @property
    def max_size(self) -> int:
        return self.doc["max_size"]

    @max_size.setter
    def max_size(self, value: int) -> None:
        self.doc["max_size"] = value
        self.write()


class RepoContents:
    @staticmethod
    def create(folder: str, uuid: str):
        contents_filepath = os.path.join(folder, f"{uuid}.contents")
        config_filepath = os.path.join(folder, f"{uuid}.toml")

        assert not os.path.isdir(f"{contents_filepath}.lmdb") and not os.path.isdir(config_filepath)

        with open(config_filepath, "w") as f:
            rtoml.dump({
                "uuid": uuid,
                "updated": datetime.now().isoformat(),
                "max_size": shutil.disk_usage(folder).total
            }, f)

        with ObjectStorage(f"{contents_filepath}.lmdb", map_size=1 << 30):  # 1GB
            pass

        return RepoContents.load_existing(folder, uuid, is_readonly=False)

    @staticmethod
    def load_existing(folder: str, uuid: str, is_readonly: bool):
        return RepoContents(folder, uuid, is_readonly)

    env: ObjectStorage
    objects: Objects

    def __init__(self, folder: str, uuid: str, is_readonly: bool):
        self.folder = folder
        self.uuid = uuid
        self.is_readonly = is_readonly

        if not os.path.exists(f"{self.filepath}.lmdb"):
            raise MissingRepoContents(f"File {self.filepath}.lmdb does not exist.")

    @property
    def filepath(self):
        return os.path.join(self.folder, f"{self.uuid}.contents")

    @property
    def config_filepath(self):
        return os.path.join(self.folder, f"{self.uuid}.toml")

    def __enter__(self) -> "RepoContents":
        self.config = RepoContentsConfig(self.config_filepath)

        self.env = ObjectStorage(f"{self.filepath}.lmdb", map_size=1 << 30)  # 1GB
        self.env.__enter__()

        self.objects = self.env.objects(write=True)

        self.fsobjects = RepoFSObjects(self.objects, self.env.roots(write=True), self.config)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        assert self.objects is not None
        self.objects = None

        self.env.gc()
        self.env.__exit__(exc_type, exc_val, exc_tb)
        self.env = None

        self.config.write()

        return False
