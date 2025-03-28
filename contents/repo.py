import os
import shutil
import sqlite3
from datetime import datetime
from sqlite3 import Connection, Cursor, Row
from typing import Generator, Tuple, Optional

import rtoml

from exceptions import MissingRepoContents
from contents.repo_props import RepoFileProps, RepoDirProps, RepoFileStatus
from util import FIRST_VALUE


class FSObjects:
    class Stats:
        def __init__(self, parent: "FSObjects"):
            self.parent = parent

        @property
        def num_files(self) -> int:
            return self.parent._first_value_cursor().execute(
                "SELECT count(1) FROM fsobject WHERE isdir=FALSE AND last_status != ?",
                (RepoFileStatus.DELETED.value,)).fetchone()

        @property
        def num_dirs(self) -> int:
            return self.parent._first_value_cursor().execute(
                "SELECT count(1) FROM fsobject WHERE isdir=TRUE AND last_status != ?",
                (RepoFileStatus.DELETED.value,)).fetchone()

        @property
        def total_size(self) -> int:
            return self.parent._first_value_cursor().execute(
                "SELECT sum(size) FROM fsobject WHERE isdir=FALSE AND last_status != ?",
                (RepoFileStatus.DELETED.value,)).fetchone()

    def __init__(self, parent: "RepoContents"):
        self.parent = parent

    @property
    def stats_existing(self):
        return FSObjects.Stats(self)

    def _first_value_cursor(self):
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr

    def len_existing(self) -> int:
        return self._first_value_cursor().execute(
            "SELECT count(1) FROM fsobject WHERE last_status != ?",
            (RepoFileStatus.DELETED.value,)).fetchone()

    @staticmethod
    def _create_fsobjectprops(cursor: Cursor, row: Row) -> Tuple[str, RepoFileProps | RepoDirProps]:
        fullpath, isdir, size, mtime, fasthash, md5, last_status, last_update_epoch = row

        if isdir:
            return fullpath, RepoDirProps(RepoFileStatus(last_status), last_update_epoch)
        else:
            return fullpath, RepoFileProps(size, mtime, fasthash, md5, RepoFileStatus(last_status), last_update_epoch)

    def get_existing(self, file_path: str) -> RepoFileProps | RepoDirProps:
        curr = self.parent.conn.cursor()
        curr.row_factory = FSObjects._create_fsobjectprops

        _, props = curr.execute(
            "SELECT fullpath, isdir, size, mtime, fasthash, md5, last_status, last_update_epoch FROM fsobject "
            "WHERE fsobject.fullpath = ? AND last_status != ?",
            (file_path, RepoFileStatus.DELETED.value)).fetchone()
        return props

    def all_status(self) -> Generator[Tuple[str, RepoFileProps | RepoDirProps], None, None]:
        curr = self.parent.conn.cursor()
        curr.row_factory = FSObjects._create_fsobjectprops

        yield from curr.execute(
            "SELECT fullpath, isdir, size, mtime, fasthash, md5, last_status, last_update_epoch "
            "FROM fsobject ORDER BY fullpath")

    def existing(self) -> Generator[Tuple[str, RepoFileProps | RepoDirProps], None, None]:
        curr = self.parent.conn.cursor()
        curr.row_factory = FSObjects._create_fsobjectprops

        yield from curr.execute(
            "SELECT fullpath, isdir, size, mtime, fasthash, md5, last_status, last_update_epoch "
            "FROM fsobject WHERE last_status != ?",
            (RepoFileStatus.DELETED.value,))  # fixme should not be filtering maybe?

    def in_existing(self, file_path: str) -> bool:
        return self._first_value_cursor().execute(
            "SELECT count(1) FROM fsobject WHERE fsobject.fullpath = ? AND last_status != ?",
            (file_path, RepoFileStatus.DELETED.value)).fetchone() > 0  # fixme should not be filtering maybe?

    def add_file(self, filepath: str, size: int, mtime: float, fasthash: str, status: RepoFileStatus) -> None:
        self.parent.conn.execute(
            "INSERT OR REPLACE INTO fsobject(fullpath, isdir, size, mtime, fasthash, md5, last_status, last_update_epoch) "
            "VALUES (?, FALSE, ?, ?, ?, NULL, ?, ?)",
            (filepath, size, mtime, fasthash, status.value, self.parent.config.epoch))

    def add_dir(self, dirpath: str, status: RepoFileStatus):
        self.parent.conn.execute(
            "INSERT OR REPLACE INTO fsobject(fullpath, isdir, md5, last_status, last_update_epoch) "
            "VALUES (?, TRUE, NULL, ?, ?)",
            (dirpath, status.value, self.parent.config.epoch))

    def mark_removed(self, path: str):
        self.parent.conn.execute(
            "UPDATE fsobject SET last_status = ?, last_update_epoch = ? WHERE fsobject.fullpath = ?",
            (RepoFileStatus.DELETED.value, self.parent.config.epoch, path))


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

        conn = sqlite3.connect(contents_filepath)
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
            " last_update_epoch INTEGER NOT NULL)")

        conn.commit()
        conn.close()

        return RepoContents.load_existing(folder, uuid)

    @staticmethod
    def load_existing(folder: str, uuid: str):
        return RepoContents(folder, uuid)

    conn: Connection | None

    def __init__(self, folder: str, uuid: str):
        self.folder = folder
        self.uuid = uuid

        if not os.path.isfile(self.filepath):
            raise MissingRepoContents(f"File {self.filepath} does not exist.")

        self.conn = None

    @property
    def filepath(self): return os.path.join(self.folder, f"{self.uuid}.contents")

    @property
    def config_filepath(self): return os.path.join(self.folder, f"{self.uuid}.toml")

    def __enter__(self) -> "RepoContents":
        assert self.conn is None
        self.conn = sqlite3.connect(self.filepath)
        self.fsobjects = FSObjects(self)
        self.config = RepoContentsConfig(self.config_filepath)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        assert self.conn is not None
        self.conn.commit()
        self.conn.close()
        self.config.write()

        return False
