import logging
import os
import shutil
from datetime import datetime
from sqlite3 import Connection, Cursor, Row
from typing import Tuple, Iterable

import rtoml

from command.fast_path import FastPosixPath
from contents.repo_props import RepoFileProps, RepoFileStatus
from exceptions import MissingRepoContents
from sql_util import sqlite3_standard
from util import FIRST_VALUE


class RepoFSObjects:
    class Stats:
        def __init__(self, parent: "RepoFSObjects"):
            self.parent = parent

        @property
        def num_files(self) -> int:
            return self.parent._first_value_cursor().execute(
                "SELECT count(1) FROM fsobject WHERE isdir=FALSE AND last_status NOT IN (?, ?)",
                (RepoFileStatus.DELETED.value, RepoFileStatus.MOVED_FROM.value)).fetchone()


        @property
        def total_size(self) -> int:
            return self.parent._first_value_cursor().execute(
                "SELECT sum(size) FROM fsobject WHERE isdir=FALSE AND last_status NOT IN (?, ?)",
                (RepoFileStatus.DELETED.value, RepoFileStatus.MOVED_FROM.value)).fetchone()

    def __init__(self, parent: "RepoContents"):
        self.parent = parent

    @property
    def stats_existing(self):
        return RepoFSObjects.Stats(self)

    def _first_value_cursor(self):
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr

    def len_existing(self) -> int:
        return self._first_value_cursor().execute(
            "SELECT count(1) FROM fsobject WHERE last_status NOT IN (?, ?) and isdir = FALSE",
            (RepoFileStatus.DELETED.value, RepoFileStatus.MOVED_FROM.value)).fetchone()

    @staticmethod
    def _create_pair_path_props(cursor: Cursor, row: Row) -> Tuple[FastPosixPath, RepoFileProps]:
        fullpath, isdir, size, mtime, fasthash, md5, last_status, last_update_epoch, last_related_fullpath = row
        assert isdir == False
        return FastPosixPath(fullpath), RepoFileProps(
            size, mtime, fasthash, md5, RepoFileStatus(last_status), last_update_epoch, last_related_fullpath)

    def get_existing(self, file_path: FastPosixPath) -> RepoFileProps:
        assert not file_path.is_absolute()
        curr = self.parent.conn.cursor()
        curr.row_factory = RepoFSObjects._create_pair_path_props

        _, props = curr.execute(
            "SELECT fullpath, isdir, size, mtime, fasthash, md5, last_status, last_update_epoch, last_related_fullpath "
            "FROM fsobject "
            "WHERE fsobject.fullpath = ? AND last_status NOT IN (?, ?) and isdir = FALSE",
            (file_path.as_posix(), RepoFileStatus.DELETED.value, RepoFileStatus.MOVED_FROM.value)).fetchone()
        return props

    def get_file_with_any_status(self, file_path: FastPosixPath) -> RepoFileProps | None:
        assert not file_path.is_absolute()
        curr = self.parent.conn.cursor()
        curr.row_factory = RepoFSObjects._create_pair_path_props

        all_pairs = curr.execute(
            "SELECT fullpath, isdir, size, mtime, fasthash, md5, last_status, last_update_epoch, last_related_fullpath "
            "FROM fsobject "
            "WHERE fsobject.fullpath = ? and isdir = FALSE ",
            (file_path.as_posix(),)).fetchall()
        if len(all_pairs) == 0:
            return None
        else:
            assert len(all_pairs) == 1
            _, props = all_pairs[0]
            return props

    def all_status(self) -> Iterable[Tuple[FastPosixPath, RepoFileProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = RepoFSObjects._create_pair_path_props

        yield from curr.execute(
            "SELECT fullpath, isdir, size, mtime, fasthash, md5, last_status, last_update_epoch, last_related_fullpath "
            "FROM fsobject WHERE isdir = FALSE ORDER BY fullpath")

    def existing(self) -> Iterable[Tuple[FastPosixPath, RepoFileProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = RepoFSObjects._create_pair_path_props

        yield from curr.execute(
            "SELECT fullpath, isdir, size, mtime, fasthash, md5, last_status, last_update_epoch, last_related_fullpath "
            "FROM fsobject WHERE last_status NOT IN (?, ?) and isdir = FALSE",
            (RepoFileStatus.DELETED.value, RepoFileStatus.MOVED_FROM.value))

    def in_existing(self, file_path: FastPosixPath) -> bool:
        assert not file_path.is_absolute()
        return self._first_value_cursor().execute(
            "SELECT count(1) FROM fsobject WHERE fsobject.fullpath = ? AND last_status NOT IN (?, ?) and isdir = FALSE",
            (file_path.as_posix(), RepoFileStatus.DELETED.value, RepoFileStatus.MOVED_FROM.value)).fetchone() > 0

    def add_file(self, filepath: FastPosixPath, size: int, mtime: float, fasthash: str, status: RepoFileStatus) -> None:
        assert not filepath.is_absolute()
        self.parent.conn.execute(
            "INSERT OR REPLACE INTO fsobject(fullpath, isdir, size, mtime, fasthash, md5, last_status, last_update_epoch, last_related_fullpath) "
            "VALUES (?, FALSE, ?, ?, ?, NULL, ?, ?, NULL)",
            (filepath.as_posix(), size, mtime, fasthash, status.value, self.parent.config.epoch))

    def mark_moved(self, from_file: FastPosixPath, to_file: FastPosixPath, size: int, mtime: float, fasthash: str):
        assert not from_file.is_absolute()
        assert not to_file.is_absolute()
        # mark old file to refer to the new file
        self.parent.conn.execute(
            "UPDATE fsobject SET last_status = ?, last_update_epoch = ?, last_related_fullpath = ? "
            "WHERE fsobject.fullpath = ?",
            (RepoFileStatus.MOVED_FROM.value, self.parent.config.epoch, to_file.as_posix(), from_file.as_posix()))

        # add the new file
        self.add_file(to_file, size, mtime, fasthash, RepoFileStatus.ADDED)

    def mark_removed(self, path: FastPosixPath):
        assert not path.is_absolute()
        self.parent.conn.execute(
            "UPDATE fsobject SET last_status = ?, last_update_epoch = ?, last_related_fullpath = NULL "
            "WHERE fsobject.fullpath = ?",
            (RepoFileStatus.DELETED.value, self.parent.config.epoch, path.as_posix()))


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

        assert not os.path.isfile(contents_filepath) and not os.path.isdir(config_filepath)

        with open(config_filepath, "w") as f:
            rtoml.dump({
                "uuid": uuid,
                "updated": datetime.now().isoformat(),
                "max_size": shutil.disk_usage(folder).total
            }, f)

        conn = sqlite3_standard(contents_filepath)
        curr = conn.cursor()

        curr.execute(
            "CREATE TABLE fsobject("
            " fsobject_id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " fullpath TEXT NOT NULL UNIQUE,"
            " isdir BOOL NOT NULL,"
            " size INTEGER,"
            " mtime REAL,"
            " fasthash TEXT,"
            " md5 TEXT,"
            " last_status TEXT NOT NULL,"
            " last_update_epoch INTEGER NOT NULL,"
            " last_related_fullpath TEXT)")

        conn.commit()
        conn.close()

        return RepoContents.load_existing(folder, uuid, is_readonly=False)

    @staticmethod
    def load_existing(folder: str, uuid: str, is_readonly: bool):
        return RepoContents(folder, uuid, is_readonly)

    conn: Connection | None

    def __init__(self, folder: str, uuid: str, is_readonly: bool):
        self.folder = folder
        self.uuid = uuid
        self.is_readonly = is_readonly

        if not os.path.isfile(self.filepath):
            raise MissingRepoContents(f"File {self.filepath} does not exist.")

        self.conn = None

    @property
    def filepath(self): return os.path.join(self.folder, f"{self.uuid}.contents")

    @property
    def config_filepath(self): return os.path.join(self.folder, f"{self.uuid}.toml")

    def __enter__(self) -> "RepoContents":
        assert self.conn is None
        self.conn = sqlite3_standard(f"file:{self.filepath}{'?mode=ro' if self.is_readonly else ''}", uri=True)
        self.fsobjects = RepoFSObjects(self)
        self.config = RepoContentsConfig(self.config_filepath)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        assert self.conn is not None
        self.conn.commit()
        self.conn.close()
        self.config.write()

        return False
