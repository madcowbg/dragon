import os
import shutil
from datetime import datetime
from typing import Tuple, Iterable

import rtoml

from command.fast_path import FastPosixPath
from contents.repo_props import FileDesc, RepoFileStatus
from exceptions import MissingRepoContents
from lmdb_storage.file_object import FileObject
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.tree_iteration import dfs
from lmdb_storage.tree_structure import Objects, ObjectID, ObjectType, add_file_object, remove_file_object
from util import FIRST_VALUE


class RepoFSObjects:
    class Stats:
        def __init__(self, objects: Objects[FileObject], root_id: ObjectID):
            self.objects = objects
            self.root_id = root_id

        @property
        def num_files(self) -> int:
            with self.objects as objects:
                return sum(
                    1 for _, obj_type, _, _, _ in dfs(objects, "", self.root_id)
                    if obj_type == ObjectType.BLOB)

        @property
        def total_size(self) -> int:
            with self.objects as objects:
                return sum(
                    obj.size for _, obj_type, _, obj, _ in dfs(objects, "", self.root_id)
                    if obj_type == ObjectType.BLOB)

    def __init__(self, objects: Objects[FileObject], root_id: ObjectID, config: "RepoContentsConfig"):
        self.objects = objects
        self.root_id = root_id
        self.config = config

        assert self.root_id is None or len(self.root_id) == 20

    @property
    def stats_existing(self):
        return RepoFSObjects.Stats(self.objects, self.root_id)

    def _first_value_cursor(self):
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr

    def len_existing(self) -> int:
        return self.stats_existing.num_files

    def all_status(self) -> Iterable[Tuple[FastPosixPath, FileDesc]]:
        yield from self.existing()

    def existing(self) -> Iterable[Tuple[FastPosixPath, FileDesc]]:
        assert self.root_id is None or len(self.root_id) == 20
        with self.objects as objects:
            for fullpath, obj_type, obj_id, obj, _ in dfs(objects, "", self.root_id):
                if obj_type == ObjectType.BLOB:
                    yield (
                        FastPosixPath(fullpath).relative_to("/"),
                        FileDesc(obj.size, obj.fasthash, None))

    def add_file(self, filepath: FastPosixPath, size: int, fasthash: str) -> None:
        with self.objects as objects:
            self.root_id = add_file_object(
                objects, self.root_id, filepath.as_posix().split("/"), FileObject.create(fasthash, size))

    def mark_moved(self, from_file: FastPosixPath, to_file: FastPosixPath, size: int, mtime: float, fasthash: str):
        assert not from_file.is_absolute()
        assert not to_file.is_absolute()

        self.mark_removed(from_file)

        # add the new file
        self.add_file(to_file, size, fasthash)

    def mark_removed(self, path: FastPosixPath):
        assert not path.is_absolute()

        with self.objects as objects:
            self.root_id = remove_file_object(
                objects, self.root_id, path.as_posix().split("/"))

            self.root_id = remove_file_object(
                objects, self.root_id, path.as_posix().split("/"))


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

    @property
    def is_dirty(self) -> bool: return self.doc["is_updating"]

    def start_updating(self):
        self.doc["is_updating"] = True
        self.doc["epoch"] = self.doc.get("epoch", 0) + 1
        self.write()

    def end_updating(self):
        self.doc["is_updating"] = False
        self.doc["last_updated"] = datetime.now().isoformat()
        self.write()

    @property
    def uuid(self) -> str: return self.doc["uuid"]

    @property
    def epoch(self) -> int: return self.doc["epoch"]

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

        ObjectStorage(f"{contents_filepath}.lmdb")

        return RepoContents.load_existing(folder, uuid, is_readonly=False)

    @staticmethod
    def load_existing(folder: str, uuid: str, is_readonly: bool):
        return RepoContents(folder, uuid, is_readonly)

    env: ObjectStorage
    objects: Objects[FileObject]

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

        root_id = self.env.roots(write=False)["ROOT"].get_current()

        self.objects = self.env.objects(write=True)

        self.fsobjects = RepoFSObjects(self.objects, root_id, self.config)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        assert self.objects is not None
        self.write()
        self.env.gc()

        self.objects = None
        self.env = None

        self.config.write()

        return False

    def write(self):
        self.env.roots(write=True)["ROOT"].set_current(self.fsobjects.root_id)
