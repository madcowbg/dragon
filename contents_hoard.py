import abc
import logging
import os
import pathlib
import sqlite3
import sys
from datetime import datetime
from sqlite3 import Connection
from typing import Dict, Any, List, Optional, Tuple, Generator, Iterator

import rtoml

from contents_props import RepoFileProps, DirProps, FileStatus, HoardFileProps, FSObjectProps, TOMLHoardFileProps, \
    SQLHoardFileProps
from contents_repo import get_singular_value


class HoardContentsConfig:
    @abc.abstractmethod
    def touch_updated(self) -> None: pass

    @property
    @abc.abstractmethod
    def updated(self) -> datetime: pass


class SQLHoardContentsConfig(HoardContentsConfig):
    def __init__(self, parent: "SQLHoardContents"):
        self.parent = parent

    def touch_updated(self) -> None:
        self.parent.conn.execute("UPDATE config SET updated = ?", (datetime.now().isoformat(),))

    @property
    def updated(self) -> datetime:
        return datetime.fromisoformat(self.parent.conn.execute("SELECT updated FROM config").fetchone()[0])


class TOMLHoardContentsConfig(HoardContentsConfig):
    def __init__(self, config_doc: Dict[str, Any]):
        self.doc = config_doc

    def touch_updated(self) -> None:
        self.doc["updated"] = datetime.now().isoformat()

    @property
    def updated(self) -> datetime:
        return datetime.fromisoformat(self.doc["updated"])


class HoardTree:
    def __init__(self, objects: Iterator[Tuple[str, FSObjectProps]]):
        self.root = HoardDir(None, "", self)

        for path, props in objects:
            if isinstance(props, HoardFileProps):
                filepath = path
                assert os.path.isabs(filepath)
                current = self.root
                parts = pathlib.Path(filepath).parts
                for folder in parts[1:-1]:
                    current = current.get_or_create_dir(folder)
                current.create_file(parts[-1], props)
            elif isinstance(props, DirProps):
                dirpath = path
                assert os.path.isabs(dirpath)
                current = self.root
                for folder in pathlib.Path(dirpath).parts[1:]:
                    current = current.get_or_create_dir(folder)
            else:
                raise ValueError(f"Invalid props type: {type(props)}")

    def walk(self, from_path: str = "/", depth: int = sys.maxsize) -> \
            Generator[Tuple[Optional["HoardDir"], Optional["HoardFile"]], None, None]:
        assert os.path.isabs(from_path)
        current = self.root
        for folder in pathlib.Path(from_path).parts[1:]:
            current = current.get_dir(folder)
            if current is None:
                return

        yield from current.walk(depth)


class HoardFile:
    def __init__(self, parent: "HoardDir", name: str, props: HoardFileProps):
        self.parent = parent
        self.name = name
        self.props = props

        self._fullname: Optional[pathlib.Path] = None

    @property
    def fullname(self):
        if self._fullname is None:
            self._fullname = pathlib.Path(self.parent.fullname).joinpath(self.name)
        return self._fullname.as_posix()


class HoardDir:
    @property
    def fullname(self):
        if self._fullname is None:
            parent_path = pathlib.Path(self.parent.fullname) if self.parent is not None else pathlib.Path("/")
            self._fullname = parent_path.joinpath(self.name)
        return self._fullname.as_posix()

    def __init__(self, parent: Optional["HoardDir"], name: str, tree: HoardTree):
        self.tree = tree
        self.name = name
        self.parent = parent

        self.dirs: Dict[str, HoardDir] = {}
        self.files: Dict[str, HoardFile] = {}

        self._fullname: Optional[str] = None

    def get_or_create_dir(self, subname: str) -> "HoardDir":
        if subname not in self.dirs:
            self.dirs[subname] = HoardDir(self, subname, self.tree)
        return self.dirs[subname]

    def get_dir(self, subname: str) -> Optional["HoardDir"]:
        return self.dirs.get(subname, None)

    def create_file(self, filename: str, props: HoardFileProps):
        assert filename not in self.files
        self.files[filename] = HoardFile(self, filename, props)

    def walk(self, depth: int) -> Generator[Tuple[Optional["HoardDir"], Optional["HoardFile"]], None, None]:
        yield self, None
        if depth <= 0:
            return
        for hoard_file in self.files.values():
            yield None, hoard_file
        for hoard_dir in self.dirs.values():
            yield from hoard_dir.walk(depth - 1)


class HoardFSObjects:
    @abc.abstractmethod
    def __len__(self): pass

    @abc.abstractmethod
    def __getitem__(self, key: str) -> FSObjectProps: pass

    @abc.abstractmethod
    def __iter__(self) -> Generator[Tuple[str, FSObjectProps], None, None]: pass

    @abc.abstractmethod
    def __contains__(self, item: str) -> bool: pass

    @abc.abstractmethod
    def add_new_file(
            self, filepath: str, props: RepoFileProps,
            current_uuid: str, repos_to_add_new_files: List[str]) -> HoardFileProps: pass

    @abc.abstractmethod
    def add_dir(self, curr_dir: str): pass

    @abc.abstractmethod
    def delete(self, curr_path: str): pass

    @abc.abstractmethod
    def move(self, orig_path: str, new_path: str, props: DirProps | HoardFileProps): pass

    @abc.abstractmethod
    def copy(self, from_fullpath: str, to_fullpath: str): pass

    @property
    @abc.abstractmethod
    def num_files(self): pass

    @property
    @abc.abstractmethod
    def num_dirs(self): pass

    @property
    @abc.abstractmethod
    def total_size(self) -> int: pass

    @abc.abstractmethod
    def to_fetch(self, repo_uuid: str) -> Generator[Tuple[str, FSObjectProps], None, None]: pass

    @abc.abstractmethod
    def to_cleanup(self, repo_uuid: str) -> Generator[Tuple[str, FSObjectProps], None, None]: pass


STATUSES_TO_FETCH = [FileStatus.COPY.value, FileStatus.GET.value]


class TOMLHoardFSObjects(HoardFSObjects):
    tree: HoardTree

    def __init__(self, doc: Dict[str, Any]):
        self._doc = doc
        self._objects = dict(
            (f, TOMLHoardFileProps(data) if not data['isdir'] else DirProps(data)) for f, data in self._doc.items())
        self.tree = HoardTree(self._objects.items())

    def __len__(self):
        return len(self._objects)

    def __getitem__(self, key: str) -> FSObjectProps:
        return self._objects[key]

    def __iter__(self) -> Generator[Tuple[str, FSObjectProps], None, None]:
        yield from self._objects.copy().items()

    def to_fetch(self, repo_uuid: str) -> Generator[Tuple[str, FSObjectProps], None, None]:
        for f, props in self:
            if isinstance(props, HoardFileProps) and props.status(repo_uuid).value in STATUSES_TO_FETCH:
                yield f, props

    def to_cleanup(self, repo_uuid: str) -> Generator[Tuple[str, FSObjectProps], None, None]:
        for f, props in self:
            if isinstance(props, HoardFileProps) and props.status(repo_uuid) == FileStatus.CLEANUP:
                yield f, props

    def __contains__(self, item: str) -> bool:
        return item in self._objects

    def add_new_file(
            self, filepath: str, props: RepoFileProps,
            current_uuid: str, repos_to_add_new_files: List[str]) -> HoardFileProps:
        self._doc[filepath] = {
            "isdir": False,
            "size": props.size,
            "fasthash": props.fasthash,
            "status": dict((uuid, FileStatus.GET.value) for uuid in repos_to_add_new_files)
        }

        # mark as present here
        self._doc[filepath]["status"][current_uuid] = FileStatus.AVAILABLE.value

        self._objects[filepath] = TOMLHoardFileProps(self._doc[filepath])
        return self._objects[filepath]

    def add_dir(self, curr_dir: str):
        self._doc[curr_dir] = {"isdir": True}
        self._objects[curr_dir] = DirProps(self._doc[curr_dir])

    def delete(self, curr_path: str):
        self._objects.pop(curr_path)
        self._doc.pop(curr_path)

    def move(self, orig_path: str, new_path: str, props: DirProps | HoardFileProps):
        assert orig_path != new_path
        assert isinstance(props, HoardFileProps) or isinstance(props, DirProps)

        self._doc[new_path] = props.doc
        self._objects[new_path] = props

        self.delete(orig_path)

    def copy(self, from_fullpath: str, to_fullpath: str):
        assert from_fullpath != to_fullpath

        props = self._objects[from_fullpath]
        if isinstance(props, HoardFileProps):
            self._doc[to_fullpath] = {
                "isdir": False,
                "size": props.size,
                "fasthash": props.fasthash,
                "status": dict((uuid, FileStatus.COPY.value) for uuid in props.status_to_copy())
            }
            self._objects[to_fullpath] = TOMLHoardFileProps(self._doc[to_fullpath])
        elif isinstance(props, DirProps):
            self.add_dir(to_fullpath)
        else:
            raise ValueError(f"props type unrecognized: {type(props)}")

    @property
    def num_files(self):
        return len([f for f, p in self if isinstance(p, HoardFileProps)])

    @property
    def num_dirs(self):
        return len([f for f, p in self if isinstance(p, DirProps)])

    @property
    def total_size(self) -> int:
        return sum(p.size for _, p in self if isinstance(p, HoardFileProps))


class SQLHoardFSObjects(HoardFSObjects):
    def __init__(self, parent: "SQLHoardContents"):
        self.parent = parent
        self.tree = HoardTree(self)

    @property
    def num_files(self) -> int:
        return get_singular_value(self.parent.conn, "SELECT count(1) FROM fsobject WHERE isdir=FALSE")

    @property
    def num_dirs(self) -> int:
        return get_singular_value(self.parent.conn, "SELECT count(1) FROM fsobject WHERE isdir=TRUE")

    @property
    def total_size(self) -> int:
        return get_singular_value(self.parent.conn, "SELECT sum(size) FROM fsobject WHERE isdir=FALSE")

    def __len__(self) -> int:
        return get_singular_value(self.parent.conn, "SELECT count(1) FROM fsobject")

    def __getitem__(self, file_path: str) -> FSObjectProps:
        fsobject_id, isdir = self.parent.conn.execute(
            "SELECT fsobject_id, isdir "
            "FROM fsobject "
            "WHERE fsobject.fullpath = ? ",
            (file_path,)).fetchone()

        if isdir:
            return DirProps({})
        else:
            return SQLHoardFileProps(self.parent, fsobject_id)

    def __iter__(self) -> Generator[Tuple[str, FSObjectProps], None, None]:  # fixme maybe optimize to create directly?
        for fsobject_id, fullpath, isdir in self.parent.conn.execute(
                "SELECT fsobject_id, fullpath, isdir FROM fsobject"):
            if isdir:
                yield fullpath, DirProps({})
            else:
                yield fullpath, SQLHoardFileProps(self.parent, fsobject_id)

    def to_fetch(self, repo_uuid: str) -> Generator[Tuple[str, FSObjectProps], None, None]:
        for fsobject_id, fullpath, isdir in self.parent.conn.execute(
                "SELECT fsobject.fsobject_id, fullpath, isdir "
                "FROM fsobject JOIN fspresence on fsobject.fsobject_id = fspresence.fsobject_id "
                "WHERE fspresence.uuid = ? and fspresence.status in (?, ?)", (repo_uuid, *STATUSES_TO_FETCH)):
            assert not isdir
            yield fullpath, SQLHoardFileProps(self.parent, fsobject_id)

    def to_cleanup(self, repo_uuid: str) -> Generator[Tuple[str, FSObjectProps], None, None]:
        for fsobject_id, fullpath, isdir in self.parent.conn.execute(
                "SELECT fsobject.fsobject_id, fullpath, isdir "
                "FROM fsobject JOIN fspresence on fsobject.fsobject_id = fspresence.fsobject_id "
                "WHERE fspresence.uuid = ? and fspresence.status = ?", (repo_uuid, FileStatus.CLEANUP.value)):
            assert not isdir
            yield fullpath, SQLHoardFileProps(self.parent, fsobject_id)

    def __contains__(self, file_path: str) -> bool:
        return self.parent.conn.execute(
            "SELECT count(1) FROM fsobject WHERE fsobject.fullpath = ?",
            (file_path,)).fetchone()[0] > 0

    def add_new_file(
            self, filepath: str, props: RepoFileProps,
            current_uuid: str, repos_to_add_new_files: List[str]) -> HoardFileProps:
        # add fsobject entry
        self.parent.conn.execute(
            "INSERT OR REPLACE INTO fsobject(fullpath, isdir, size, fasthash) VALUES (?, FALSE, ?, ?)",
            (filepath, props.size, props.fasthash))

        fsobject_id: int = self.parent.conn.execute(
            "SELECT fsobject_id FROM fsobject WHERE fullpath = ?", (filepath,)).fetchone()[0]

        # add status for new repos
        self.parent.conn.execute("DELETE FROM fspresence WHERE fsobject_id = ?", (fsobject_id,))
        self.parent.conn.executemany(
            "INSERT INTO fspresence(fsobject_id, uuid, status) VALUES (?, ?, ?)",
            [(fsobject_id, uuid, FileStatus.GET.value) for uuid in repos_to_add_new_files])

        # set status here
        self.parent.conn.execute(
            "INSERT OR REPLACE INTO fspresence(fsobject_id, uuid, status) VALUES (?, ?, ?)",
            (fsobject_id, current_uuid, FileStatus.AVAILABLE.value))

        return SQLHoardFileProps(self.parent, fsobject_id)

    def add_dir(self, curr_dir: str):
        # add fsobject entry
        self.parent.conn.execute(
            "INSERT OR REPLACE INTO fsobject(fullpath, isdir) VALUES (?, TRUE)",
            (curr_dir,))
        fsobject_id: int = self.parent.conn.execute(
            "SELECT fsobject_id FROM fsobject WHERE fullpath = ?", (curr_dir,)).fetchone()[0]
        self.parent.conn.execute("DELETE FROM fspresence WHERE fsobject_id = ?", (fsobject_id,))

    def delete(self, curr_path: str):
        fsobject_id: List[int] = self.parent.conn.execute(
            "SELECT fsobject_id FROM fsobject WHERE fullpath = ?", (curr_path,)).fetchone()
        if fsobject_id is None:
            return
        self.parent.conn.execute("DELETE FROM fsobject WHERE fullpath = ?", (curr_path,))
        self.parent.conn.execute("DELETE FROM fspresence WHERE fsobject_id = ?", (fsobject_id[0],))

    def move(self, orig_path: str, new_path: str, props: DirProps | HoardFileProps):
        assert orig_path != new_path
        assert isinstance(props, HoardFileProps) or isinstance(props, DirProps)

        # delete whatever new_path had
        self.delete(new_path)

        if isinstance(props, HoardFileProps):
            # add fsobject entry
            self.parent.conn.execute(
                "INSERT OR REPLACE INTO fsobject(fullpath, isdir, size, fasthash) VALUES (?, FALSE, ?, ?)",
                (new_path, props.size, props.fasthash))

            # add old presence
            new_path_id: int = self.parent.conn.execute(
                "SELECT fsobject_id FROM fsobject WHERE fullpath = ?",
                (new_path,)).fetchone()[0]
            self.parent.conn.executemany(
                "INSERT INTO fspresence(fsobject_id, uuid, status) VALUES (?, ?, ?)",
                [(new_path_id, uuid, status.value) for uuid, status in props.presence.items()])
        else:
            assert isinstance(props, DirProps)
            self.parent.conn.execute(
                "INSERT OR REPLACE INTO fsobject(fullpath, isdir) VALUES (?, TRUE)",
                (new_path,))

        self.delete(orig_path)

    def copy(self, from_fullpath: str, to_fullpath: str):
        assert from_fullpath != to_fullpath

        self.delete(to_fullpath)

        props = self[from_fullpath]
        if isinstance(props, HoardFileProps):
            # add fsobject entry
            self.parent.conn.execute(
                "INSERT OR REPLACE INTO fsobject(fullpath, isdir, size, fasthash) VALUES (?, FALSE, ?, ?)",
                (to_fullpath, props.size, props.fasthash))

            # add presence tp request
            new_path_id: int = self.parent.conn.execute(
                "SELECT fsobject_id FROM fsobject WHERE fullpath = ?",
                (to_fullpath,)).fetchone()[0]
            self.parent.conn.executemany(
                "INSERT INTO fspresence(fsobject_id, uuid, status) VALUES (?, ?, ?)",
                [(new_path_id, uuid, FileStatus.COPY.value) for uuid in props.status_to_copy()])
        elif isinstance(props, DirProps):
            self.add_dir(to_fullpath)
        else:
            raise ValueError(f"props type unrecognized: {type(props)}")


class HoardContents:
    config: HoardContentsConfig
    fsobjects: HoardFSObjects
    tree: HoardTree

    @staticmethod
    def load(filename: str, write_on_close: bool = True) -> "HoardContents":
        # return TOMLHoardContents.load(filename)
        return SQLHoardContents.load(filename)

    @abc.abstractmethod
    def __enter__(self) -> "HoardContents": pass

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool: pass

    @abc.abstractmethod
    def write(self): pass

    @abc.abstractmethod
    def epoch(self, remote_uuid: str) -> int: pass

    @abc.abstractmethod
    def set_epoch(self, remote_uuid: str, epoch: int): pass


class TOMLHoardContents(HoardContents):
    @staticmethod
    def load(filename: str, write_on_close: bool = True) -> "HoardContents":
        if not os.path.isfile(filename):
            with open(filename, "w", encoding="utf-8") as f:
                config = {"updated": datetime.now().isoformat()}
                rtoml.dump({
                    "config": config,
                    "epochs": {},
                    "fsobjects": {},
                }, f)
        return TOMLHoardContents(filename, write_on_close)

    def __init__(self, filepath: str, write_on_close: bool):
        self.filepath = filepath
        self.write_on_close = write_on_close

        self.config = None
        self.fsobjects = None
        self.epochs = None

    def __enter__(self):
        with open(self.filepath, "r", encoding="utf-8") as f:
            contents_doc = rtoml.load(f)

        self.config = TOMLHoardContentsConfig(contents_doc["config"] if "config" in contents_doc else {})
        self.fsobjects = TOMLHoardFSObjects(contents_doc["fsobjects"] if "fsobjects" in contents_doc else {})
        self.epochs = contents_doc["epochs"] if "epochs" in contents_doc else {}

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.write_on_close:
            self.write()
        self.config = None
        self.fsobjects = None
        self.epochs = None

        return False

    def write(self):
        logging.info(f"Writing contents to file: {self.filepath}")
        with open(self.filepath, "w", encoding="utf-8") as f:
            rtoml.dump({
                "config": self.config.doc,
                "epochs": self.epochs,
                "fsobjects": self.fsobjects._doc
            }, f)

    def epoch(self, remote_uuid: str) -> int:
        return self.epochs.get(remote_uuid, -1)

    def set_epoch(self, remote_uuid: str, epoch: int):
        self.epochs[remote_uuid] = epoch


class SQLHoardContents(HoardContents):
    @staticmethod
    def load(filename: str) -> "HoardContents":
        if not os.path.isfile(filename):
            conn = sqlite3.connect(filename)
            curr = conn.cursor()

            curr.execute(
                "CREATE TABLE fsobject("
                " fsobject_id INTEGER PRIMARY KEY AUTOINCREMENT,"
                " fullpath TEXT NOT NULL UNIQUE,"
                " isdir BOOL NOT NULL,"
                " size INTEGER,"
                " fasthash TEXT)")

            curr.execute(
                "CREATE TABLE fspresence ("
                " fsobject_id INTEGER,"
                " uuid TEXT NOT NULL,"
                " status TEXT NOT NULL,"
                " FOREIGN KEY (fsobject_id) REFERENCES fsobject(id) ON DELETE CASCADE)"
            )
            curr.execute("CREATE UNIQUE INDEX fspresence_fsobject_id__uuid ON fspresence(fsobject_id, uuid)")

            curr.execute(
                "CREATE TABLE config(updated TEXT NOT NULL)")
            curr.execute(
                "INSERT INTO config(updated) VALUES (?)",
                (datetime.now().isoformat(),))

            curr.execute(
                "CREATE TABLE epoch("
                " uuid TEXT PRIMARY KEY,"
                " epoch INTEGER NOT NULL DEFAULT -1)")

            conn.commit()
            conn.close()

        return SQLHoardContents(filename)

    conn: Connection

    def __init__(self, filepath: str):
        self.filepath = filepath

        self.config = None
        self.fsobjects = None
        self.conn = None

    def __enter__(self):
        self.conn = sqlite3.connect(self.filepath)

        self.config = SQLHoardContentsConfig(self)
        self.fsobjects = SQLHoardFSObjects(self)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.config = None
        self.fsobjects = None
        self.epochs = None

        self.write()
        self.conn.close()

        return False

    def write(self):
        self.conn.commit()

    def epoch(self, remote_uuid: str) -> int:
        result = self.conn.execute("SELECT epoch FROM epoch WHERE uuid = ?", (remote_uuid,)).fetchone()
        return result[0] if result is not None else -1

    def set_epoch(self, remote_uuid: str, epoch: int):
        self.conn.execute(
            "INSERT OR REPLACE INTO epoch(uuid, epoch) VALUES (?, ?)",
            (remote_uuid, epoch))
        self.conn.commit()
