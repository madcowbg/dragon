import os
import sqlite3
from datetime import datetime
from sqlite3 import Connection, Cursor, Row
from typing import Generator, Tuple, Optional

from contents.props import DirProps, RepoFileProps, FSObjectProps
from util import FIRST_VALUE


class FSObjects:
    def __init__(self, parent: "RepoContents"):
        self.parent = parent

    @property
    def num_files(self) -> int:
        return self._first_value_cursor().execute("SELECT count(1) FROM fsobject WHERE isdir=FALSE").fetchone()

    def _first_value_cursor(self):
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr

    @property
    def num_dirs(self) -> int:
        return self._first_value_cursor().execute("SELECT count(1) FROM fsobject WHERE isdir=TRUE").fetchone()

    @property
    def total_size(self) -> int:
        return self._first_value_cursor().execute("SELECT sum(size) FROM fsobject WHERE isdir=FALSE").fetchone()

    def __len__(self) -> int:
        return self._first_value_cursor().execute("SELECT count(1) FROM fsobject").fetchone()

    @staticmethod
    def _create_fsobjectprops(cursor: Cursor, row: Row) -> Tuple[str, FSObjectProps]:
        fullpath, isdir, size, mtime, fasthash = row

        if isdir:
            return fullpath, DirProps({})
        else:
            return fullpath, RepoFileProps({"size": size, "mtime": mtime, "fasthash": fasthash})

    def __getitem__(self, file_path: str) -> FSObjectProps:
        curr = self.parent.conn.cursor()
        curr.row_factory = FSObjects._create_fsobjectprops

        _, props = curr.execute(
            "SELECT fullpath, isdir, size, mtime, fasthash FROM fsobject WHERE fsobject.fullpath = ?",
            (file_path,)).fetchone()
        return props

    def __iter__(self) -> Generator[Tuple[str, FSObjectProps], None, None]:
        curr = self.parent.conn.cursor()
        curr.row_factory = FSObjects._create_fsobjectprops

        yield from curr.execute("SELECT fullpath, isdir, size, mtime, fasthash FROM fsobject")

    def __contains__(self, file_path: str) -> bool:
        return self._first_value_cursor().execute(
            "SELECT count(1) FROM fsobject WHERE fsobject.fullpath = ?",
            (file_path,)).fetchone() > 0

    def add_file(self, filepath: str, size: int, mtime: float, fasthash: str) -> None:
        self.parent.conn.execute(
            "INSERT OR REPLACE INTO fsobject(fullpath, isdir, size, mtime, fasthash) VALUES (?, FALSE, ?, ?, ?)",
            (filepath, size, mtime, fasthash))

    def add_dir(self, dirpath: str):
        self.parent.conn.execute("INSERT OR REPLACE INTO fsobject(fullpath, isdir) VALUES (?, TRUE)", (dirpath,))

    def remove(self, path: str):
        self.parent.conn.execute("DELETE FROM fsobject WHERE fsobject.fullpath = ?", (path,))


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
    def load(filepath: str, create_for_uuid: Optional[str] = None):
        if not os.path.isfile(filepath):
            if create_for_uuid is not None:
                conn = sqlite3.connect(filepath)
                curr = conn.cursor()

                curr.execute(
                    "CREATE TABLE fsobject("
                    " fsobject_id INTEGER PRIMARY KEY AUTOINCREMENT,"
                    " fullpath TEXT NOT NULL UNIQUE,"
                    " isdir BOOL NOT NULL,"
                    " size INTEGER,"
                    " mtime REAL,"
                    " fasthash TEXT)")

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
            else:
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
