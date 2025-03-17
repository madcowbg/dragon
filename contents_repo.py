import abc
import os
import sqlite3
from datetime import datetime
from sqlite3 import Connection, Cursor
from typing import Dict, Any, Generator, Tuple, Optional

import rtoml

from util import FIRST_VALUE
from contents_props import RepoFileProps, DirProps, RepoFileProps, FSObjectProps


class RepoContentsConfig(abc.ABC):
    uuid: str
    epoch: int

    updated: datetime

    @abc.abstractmethod
    def touch_updated(self) -> None: pass

    @abc.abstractmethod
    def bump_epoch(self): pass


class TOMLContentsConfig(RepoContentsConfig):
    def __init__(self, config_doc: Dict[str, Any]):
        self.doc = config_doc

    def touch_updated(self) -> None:
        self.doc["updated"] = datetime.now().isoformat()

    @property
    def updated(self) -> datetime:
        return datetime.fromisoformat(self.doc["updated"])

    @property
    def uuid(self) -> str:
        return self.doc["uuid"]

    @property
    def epoch(self) -> int:
        return int(self.doc["epoch"]) if "epoch" in self.doc else 0

    def bump_epoch(self):
        self.doc["epoch"] = self.epoch + 1


class FSObjects(abc.ABC):
    @abc.abstractmethod
    def __len__(self) -> int: pass

    @abc.abstractmethod
    def __getitem__(self, key: str) -> FSObjectProps: pass

    @abc.abstractmethod
    def __iter__(self) -> Generator[Tuple[str, FSObjectProps], None, None]: pass

    @abc.abstractmethod
    def __contains__(self, item: str) -> bool: pass

    num_files: int
    num_dirs: int
    total_size: int

    @abc.abstractmethod
    def add_file(self, filepath: str, size: int, mtime: float, fasthash: str) -> None: pass

    @abc.abstractmethod
    def add_dir(self, dirpath): pass

    @abc.abstractmethod
    def remove(self, path: str): pass


class TOMLFSObjects(FSObjects):
    def __init__(self, fsobjects_doc: Dict[str, Any]):
        self._doc = fsobjects_doc
        self._objects = dict(
            (f, RepoFileProps(data) if not data['isdir'] else DirProps(data))
            for f, data in self._doc.items())

    def __len__(self) -> int: return len(self._objects)

    def __getitem__(self, key: str) -> FSObjectProps: return self._objects[key]

    def __iter__(self) -> Generator[Tuple[str, FSObjectProps], None, None]:
        yield from self._objects.copy().items()

    def __contains__(self, item: str) -> bool:
        return item in self._objects

    @property
    def num_files(self):
        return len([f for f, p in self if isinstance(p, RepoFileProps)])

    @property
    def num_dirs(self):
        return len([f for f, p in self if isinstance(p, DirProps)])

    @property
    def total_size(self) -> int:
        return sum(p.size for _, p in self if isinstance(p, RepoFileProps))

    def add_file(self, filepath: str, size: int, mtime: float, fasthash: str) -> None:
        self._doc[filepath] = {"size": size, "mtime": mtime, "isdir": False, "fasthash": fasthash}
        self._objects[filepath] = RepoFileProps(self._doc[filepath])

    def add_dir(self, dirpath):
        self._doc[dirpath] = {"isdir": True}
        self._objects[dirpath] = DirProps(self._doc[dirpath])

    def remove(self, path: str):
        self._doc.pop(path)
        self._objects.pop(path)


class RepoContents(abc.ABC):
    fsobjects: FSObjects
    config: RepoContentsConfig

    @abc.abstractmethod
    def __enter__(self) -> "RepoContents": pass

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool: pass

    @staticmethod
    def load(filepath: str, create_for_uuid: Optional[str] = None, write_on_exit: bool = True):
        # return TOMLRepoContents.load(filepath, create_for_uuid, write_on_exit)
        return SQLRepoContents.load(filepath, create_for_uuid)


class TOMLRepoContents(RepoContents):
    @staticmethod
    def load(filepath: str, create_for_uuid: Optional[str] = None, write_on_exit: bool = True):
        if not os.path.isfile(filepath):
            if create_for_uuid is not None:
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(rtoml.dumps({"config": {"uuid": create_for_uuid}, "epoch": 0}))
            else:
                raise ValueError(f"File {filepath} does not exist, need to pass create=True to create.")

        return TOMLRepoContents(filepath, write_on_exit)

    def __init__(self, filepath: str, write_on_exit: bool):
        self.filepath = filepath
        self.write_on_exit = write_on_exit

        self.config = None
        self.fsobjects = None

    def __enter__(self):
        with open(self.filepath, "r", encoding="utf-8") as f:
            contents_doc = rtoml.load(f)

        self.config = TOMLContentsConfig(contents_doc["config"] if "config" in contents_doc else {})
        self.fsobjects = TOMLFSObjects(contents_doc["fsobjects"] if "fsobjects" in contents_doc else {})

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.write_on_exit:
            self.write()
        self.config = None
        self.fsobjects = None

        return False

    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            rtoml.dump({
                "config": self.config.doc,
                "fsobjects": self.fsobjects._doc
            }, f)


class SQLFSObjects(FSObjects):
    def __init__(self, parent: "SQLRepoContents"):
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

    def __getitem__(self, file_path: str) -> FSObjectProps:
        fullpath, isdir, size, mtime, fasthash = self.parent.conn.execute(
            "SELECT fullpath, isdir, size, mtime, fasthash "
            "FROM fsobject "
            "WHERE fsobject.fullpath = ? ",
            (file_path,)).fetchone()
        if isdir:
            return DirProps({})
        else:
            return RepoFileProps({"size": size, "mtime": mtime, "fasthash": fasthash})

    def __iter__(self) -> Generator[Tuple[str, FSObjectProps], None, None]:
        for fullpath, isdir, size, mtime, fasthash in self.parent.conn.execute(
                "SELECT fullpath, isdir, size, mtime, fasthash FROM fsobject "):

            if isdir:
                yield fullpath, DirProps({})
            else:
                yield fullpath, RepoFileProps({"size": size, "mtime": mtime, "fasthash": fasthash})

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


class SQLRepoContentsConfig(RepoContentsConfig):
    def __init__(self, parent: "SQLRepoContents"):
        self.parent = parent

    def touch_updated(self) -> None:
        self.parent.conn.execute("UPDATE config SET updated = ?", (datetime.now().isoformat(),))

    @property
    def updated(self) -> datetime:
        return datetime.fromisoformat(self._first_value_cursor().execute("SELECT updated FROM config").fetchone())

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


class SQLRepoContents(RepoContents):
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
                    "updated TEXT NOT NULL)")
                curr.execute(
                    "INSERT INTO config(uuid, updated) VALUES (?, ?)",
                    (create_for_uuid, datetime.now().isoformat()))

                conn.commit()
                conn.close()
            else:
                raise ValueError(f"File {filepath} does not exist, need to pass create=True to create.")

        return SQLRepoContents(filepath)

    conn: Connection

    def __init__(self, filepath: str):
        self.filepath = filepath

        self.conn = None

    def __enter__(self) -> "RepoContents":
        assert self.conn is None
        self.conn = sqlite3.connect(self.filepath)
        self.fsobjects = SQLFSObjects(self)
        self.config = SQLRepoContentsConfig(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        assert self.conn is not None
        self.write()
        self.conn.close()

        return False

    def write(self):
        self.conn.commit()
