import os
import sqlite3
from datetime import datetime
from sqlite3 import Connection, Cursor, Row
from typing import Generator, Tuple, Optional

from contents.props import RepoFileProps, RepoDirProps, RepoFileStatus
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
    def __init__(self, parent: "RepoContents"):
        self.parent = parent

    def touch_updated(self) -> None:
        self.parent.conn.execute("UPDATE config SET updated = ?", (datetime.now().isoformat(),))

    @property
    def updated(self) -> datetime:
        return datetime.fromisoformat(self._first_value_cursor().execute("SELECT updated FROM config").fetchone())

    @property
    def is_dirty(self) -> bool:
        return self._first_value_cursor().execute("SELECT is_dirty FROM config").fetchone()

    def start_updating(self):
        self.parent.conn.execute("UPDATE config SET is_dirty = TRUE")

    def end_updating(self):
        self.parent.conn.execute("UPDATE config SET is_dirty = FALSE")

    @property
    def uuid(self) -> str:
        return self._first_value_cursor().execute("SELECT uuid FROM config").fetchone()

    @property
    def epoch(self) -> int:
        return self._first_value_cursor().execute("SELECT epoch FROM config").fetchone()

    def bump_epoch(self):
        self.parent.conn.execute("UPDATE config SET epoch = (SELECT MAX(epoch) FROM config) + 1")
        self.parent.conn.commit()

    def _first_value_cursor(self) -> Cursor:
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr


class RepoContents:
    @staticmethod
    def create(filepath: str, create_for_uuid: Optional[str] = None):
        assert not os.path.isfile(filepath)
        conn = sqlite3.connect(filepath)
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

        curr.execute(
            "CREATE TABLE config("
            "uuid text PRIMARY KEY NOT NULL, "
            "epoch INTEGER DEFAULT 0,"
            "updated TEXT NOT NULL,"
            "is_dirty BOOLEAN NOT NULL)")
        curr.execute(
            "INSERT INTO config(uuid, updated, is_dirty) VALUES (?, ?, TRUE)",
            (create_for_uuid, datetime.now().isoformat()))

        conn.commit()
        conn.close()

        return RepoContents.load_existing(filepath)

    @staticmethod
    def load_existing(filepath: str):
        if not os.path.isfile(filepath):
            raise ValueError(f"File {filepath} does not exist, need to pass create=True to create.")
        return RepoContents(filepath)

    conn: Connection

    def __init__(self, filepath: str):
        self.filepath = filepath

        self.conn = None

    def __enter__(self) -> "RepoContents":
        assert self.conn is None
        self.conn = sqlite3.connect(self.filepath)
        self.fsobjects = FSObjects(self)
        self.config = RepoContentsConfig(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        assert self.conn is not None
        self.conn.commit()
        self.conn.close()

        return False
