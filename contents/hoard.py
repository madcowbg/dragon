import os
import pathlib
import sqlite3
import sys
from datetime import datetime
from sqlite3 import Connection
from typing import Dict, Any, Optional, Tuple, Generator, Iterator, Iterable

import rtoml

from contents.repo_props import RepoFileProps
from contents.hoard_props import HoardDirProps, HoardFileStatus, HoardFileProps
from util import FIRST_VALUE

HOARD_CONTENTS_FILENAME = "hoard.contents"
HOARD_CONTENTS_TOML = "hoard.contents.toml"


class HoardContentsConfig:
    def __init__(self, file: str):
        self.file = file

        with open(file, "r") as f:
            self.doc = rtoml.load(f)

    def write(self):
        with open(self.file, "w") as f:
            rtoml.dump(self.doc, f)

    def touch_updated(self) -> None:
        self.doc["updated"] = datetime.now().isoformat()
        self.write()

    def _remote_config(self, remote_uuid: str) -> Dict[str, Any]:
        if "remotes" not in self.doc:
            self.doc["remotes"] = {}

        if remote_uuid not in self.doc["remotes"]:
            self.doc["remotes"][remote_uuid] = {}
            self.write()

        return self.doc["remotes"][remote_uuid]

    def epoch(self, remote_uuid: str) -> int:
        return self._remote_config(remote_uuid).get("epoch", -1)

    def set_epoch(self, remote_uuid: str, epoch: int, updated: datetime):
        self._remote_config(remote_uuid)["epoch"] = epoch
        self._remote_config(remote_uuid)["updated"] = updated.isoformat()
        self.write()

    def updated(self, remote_uuid: str) -> Optional[datetime]:
        remote = self._remote_config(remote_uuid)
        return datetime.fromisoformat(remote["updated"]) if "updated" in remote else None


class HoardTree:
    def __init__(self, objects: Iterator[Tuple[str, HoardFileProps | HoardDirProps]]):
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
            elif isinstance(props, HoardDirProps):
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


STATUSES_TO_FETCH = [HoardFileStatus.COPY.value, HoardFileStatus.GET.value]


class HoardFSObjects:
    def __init__(self, parent: "HoardContents"):
        self.parent = parent
        self.tree = HoardTree(self)

    @property
    def num_files(self) -> int:
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr.execute("SELECT count(1) FROM fsobject WHERE isdir=FALSE").fetchone()

    @property
    def num_dirs(self) -> int:
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr.execute("SELECT count(1) FROM fsobject WHERE isdir=TRUE").fetchone()

    @property
    def total_size(self) -> int:
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr.execute("SELECT sum(size) FROM fsobject WHERE isdir=FALSE").fetchone()

    def __len__(self) -> int:
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr.execute("SELECT count(1) FROM fsobject").fetchone()

    def _read_as_prop_tuple(self, cursor, row) -> Tuple[str, HoardFileProps]:
        fullpath, fsobject_id, isdir, size, fasthash = row
        if isdir:
            return fullpath, HoardDirProps({})
        else:
            return fullpath, HoardFileProps(self.parent, fsobject_id, size, fasthash)

    def __getitem__(self, file_path: str) -> HoardFileProps | HoardDirProps:
        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_prop_tuple
        return curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash "
            "FROM fsobject "
            "WHERE fsobject.fullpath = ? ",
            (file_path,)).fetchone()[1]

    def by_fasthash(self, fasthash: str) -> Iterable[Tuple[str, HoardFileProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_prop_tuple
        yield from curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash "
            "FROM fsobject "
            "WHERE isdir = FALSE and fasthash = ?", (fasthash,))

    def __iter__(self) -> Iterable[Tuple[str, HoardFileProps | HoardDirProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_prop_tuple
        yield from curr.execute("SELECT fullpath, fsobject_id, isdir, size, fasthash FROM fsobject")

    @property
    def dangling_files(self) -> Iterable[Tuple[str, HoardFileProps | HoardDirProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_prop_tuple
        yield from curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash FROM fsobject "
            "WHERE isdir = FALSE AND "
            "  NOT EXISTS (SELECT 1 FROM fspresence WHERE fspresence.fsobject_id = fsobject.fsobject_id)")

    def with_pending(self, repo_uuid: str) -> Iterable[Tuple[str, HoardFileProps | HoardDirProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_prop_tuple
        yield from curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash FROM fsobject "
            "WHERE isdir = FALSE AND EXISTS ("
            "  SELECT 1 FROM fspresence "
            "  WHERE fspresence.fsobject_id = fsobject.fsobject_id AND "
            "    uuid = ? AND "
            "    status in (?, ?, ?))",
            (repo_uuid, HoardFileStatus.GET.value, HoardFileStatus.COPY.value, HoardFileStatus.CLEANUP.value))

    def in_folder(self, folder: str) -> Iterable[Tuple[str, HoardFileProps | HoardDirProps]]:
        assert os.path.isabs(folder)
        folder_with_trailing = folder if folder.endswith("/") else folder + "/"
        assert folder_with_trailing.endswith('/')

        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_prop_tuple

        yield from curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash FROM fsobject "
            "WHERE fullpath like ? or fullpath = ?",
            (f"{folder_with_trailing}%", folder))

    @property
    def status_by_uuid(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        stats: Dict[str, Dict[str, Dict[str, Any]]] = dict()
        for uuid, nfiles, size in self.parent.conn.execute(
                "SELECT fspresence.uuid, count(fspresence.fsobject_id) as nfiles, sum(size) as total_size "
                "FROM fsobject JOIN fspresence ON fsobject.fsobject_id=fspresence.fsobject_id "
                "WHERE isdir = FALSE "
                "GROUP BY fspresence.uuid"):
            stats[uuid] = {
                "total": {"nfiles": nfiles, "size": size}}

        for uuid, status, nfiles, size in self.parent.conn.execute(
                "SELECT fspresence.uuid, fspresence.status, count(fspresence.fsobject_id) as nfiles, sum(size) as total_size "
                "FROM fsobject JOIN fspresence ON fsobject.fsobject_id=fspresence.fsobject_id "
                "WHERE isdir = FALSE "
                "GROUP BY fspresence.uuid, fspresence.status"):
            stats[uuid][status] = {"nfiles": nfiles, "size": size}
        return stats

    def to_fetch(self, repo_uuid: str) -> Generator[Tuple[str, HoardFileProps | HoardDirProps], None, None]:
        for fsobject_id, fullpath, isdir, size, fasthash in self.parent.conn.execute(
                "SELECT fsobject.fsobject_id, fullpath, isdir, size, fasthash "
                "FROM fsobject JOIN fspresence on fsobject.fsobject_id = fspresence.fsobject_id "
                "WHERE fspresence.uuid = ? and fspresence.status in (?, ?)", (repo_uuid, *STATUSES_TO_FETCH)):
            assert not isdir
            yield fullpath, HoardFileProps(self.parent, fsobject_id, size, fasthash)

    def to_cleanup(self, repo_uuid: str) -> Generator[Tuple[str, HoardFileProps | HoardDirProps], None, None]:
        for fsobject_id, fullpath, isdir, size, fasthash in self.parent.conn.execute(
                "SELECT fsobject.fsobject_id, fullpath, isdir, size, fasthash "
                "FROM fsobject JOIN fspresence on fsobject.fsobject_id = fspresence.fsobject_id "
                "WHERE fspresence.uuid = ? and fspresence.status = ?", (repo_uuid, HoardFileStatus.CLEANUP.value)):
            assert not isdir
            yield fullpath, HoardFileProps(self.parent, fsobject_id, size, fasthash)

    def __contains__(self, file_path: str) -> bool:
        curr = self._first_value_curr()
        return curr.execute(
            "SELECT count(1) > 0 FROM fsobject WHERE fsobject.fullpath = ?",
            (file_path,)).fetchone()

    def add_or_replace_file(self, filepath: str, props: RepoFileProps) -> HoardFileProps:
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE

        # add fsobject entry
        curr.execute(
            "INSERT OR REPLACE INTO fsobject(fullpath, isdir, size, fasthash) VALUES (?, FALSE, ?, ?)",
            (filepath, props.size, props.fasthash))

        # cleanup presence status
        fsobject_id: int = curr.execute(
            "SELECT fsobject_id FROM fsobject WHERE fullpath = ?", (filepath,)).fetchone()
        curr.execute("DELETE FROM fspresence WHERE fsobject_id = ?", (fsobject_id,))
        return HoardFileProps(self.parent, fsobject_id, props.size, props.fasthash)

    def _first_value_curr(self):
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr

    def add_dir(self, curr_dir: str):
        curr = self._first_value_curr()

        # add fsobject entry
        curr.execute(
            "INSERT OR REPLACE INTO fsobject(fullpath, isdir) VALUES (?, TRUE)",
            (curr_dir,))
        fsobject_id: int = curr.execute(
            "SELECT fsobject_id FROM fsobject WHERE fullpath = ?", (curr_dir,)).fetchone()
        curr.execute("DELETE FROM fspresence WHERE fsobject_id = ?", (fsobject_id,))

    def delete(self, curr_path: str):
        curr = self._first_value_curr()
        fsobject_id: Optional[int] = curr.execute(
            "SELECT fsobject_id FROM fsobject WHERE fullpath = ?", (curr_path,)).fetchone()
        if fsobject_id is None:
            return
        curr.execute("DELETE FROM fsobject WHERE fsobject_id = ?", (fsobject_id,))
        curr.execute("DELETE FROM fspresence WHERE fsobject_id = ?", (fsobject_id,))

    def move(self, orig_path: str, new_path: str, props: HoardDirProps | HoardFileProps):
        assert orig_path != new_path
        assert isinstance(props, HoardFileProps) or isinstance(props, HoardDirProps)

        # delete whatever new_path had
        self.delete(new_path)

        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE

        if isinstance(props, HoardFileProps):
            # add fsobject entry
            curr.execute(
                "INSERT OR REPLACE INTO fsobject(fullpath, isdir, size, fasthash) VALUES (?, FALSE, ?, ?)",
                (new_path, props.size, props.fasthash))

            # add old presence
            new_path_id: int = curr.execute(
                "SELECT fsobject_id FROM fsobject WHERE fullpath = ?",
                (new_path,)).fetchone()
            curr.executemany(
                "INSERT INTO fspresence(fsobject_id, uuid, status) VALUES (?, ?, ?)",
                [(new_path_id, uuid, status.value) for uuid, status in props.presence.items()])
        else:
            assert isinstance(props, HoardDirProps)
            curr.execute(
                "INSERT OR REPLACE INTO fsobject(fullpath, isdir) VALUES (?, TRUE)",
                (new_path,))

        self.delete(orig_path)

    def copy(self, from_fullpath: str, to_fullpath: str):
        assert from_fullpath != to_fullpath

        self.delete(to_fullpath)

        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE

        props = self[from_fullpath]
        if isinstance(props, HoardFileProps):
            # add fsobject entry
            curr.execute(
                "INSERT OR REPLACE INTO fsobject(fullpath, isdir, size, fasthash) VALUES (?, FALSE, ?, ?)",
                (to_fullpath, props.size, props.fasthash))

            # add presence tp request
            new_path_id: int = curr.execute(
                "SELECT fsobject_id FROM fsobject WHERE fullpath = ?",
                (to_fullpath,)).fetchone()

            previously_added_repos = props.repos_having_status(HoardFileStatus.COPY, HoardFileStatus.GET,
                                                               HoardFileStatus.AVAILABLE)
            curr.executemany(
                "INSERT INTO fspresence(fsobject_id, uuid, status) VALUES (?, ?, ?)",
                [(new_path_id, uuid, HoardFileStatus.COPY.value) for uuid in previously_added_repos])
        elif isinstance(props, HoardDirProps):
            self.add_dir(to_fullpath)
        else:
            raise ValueError(f"props type unrecognized: {type(props)}")


class HoardContents:
    @staticmethod
    def load(folder: str) -> "HoardContents":
        config_filename = os.path.join(folder, HOARD_CONTENTS_FILENAME)

        if not os.path.isfile(config_filename):
            conn = sqlite3.connect(config_filename)
            curr = conn.cursor()

            curr.execute(
                "CREATE TABLE fsobject("
                " fsobject_id INTEGER PRIMARY KEY AUTOINCREMENT,"
                " fullpath TEXT NOT NULL UNIQUE,"
                " isdir BOOL NOT NULL,"
                " size INTEGER,"
                " fasthash TEXT)")

            # for fasthash lookups in melding
            curr.execute("CREATE INDEX index_fsobject_fasthash ON fsobject (fasthash) ")

            curr.execute(
                "CREATE TABLE fspresence ("
                " fsobject_id INTEGER,"
                " uuid TEXT NOT NULL,"
                " status TEXT NOT NULL,"
                " FOREIGN KEY (fsobject_id) REFERENCES fsobject(id) ON DELETE CASCADE)"
            )
            curr.execute("CREATE UNIQUE INDEX fspresence_fsobject_id__uuid ON fspresence(fsobject_id, uuid)")

            conn.commit()
            conn.close()

        toml_filename = os.path.join(folder, HOARD_CONTENTS_TOML)
        if not os.path.isfile(toml_filename):
            with open(toml_filename, "w") as f:
                rtoml.dump({
                    "updated": datetime.now().isoformat()
                }, f)

        return HoardContents(folder)

    conn: Connection
    config: HoardContentsConfig
    fsobjects: HoardFSObjects

    def __init__(self, folder: str):
        self.folder = folder

        self.config = None
        self.fsobjects = None
        self.conn = None

    def __enter__(self):
        self.conn = sqlite3.connect(os.path.join(self.folder, HOARD_CONTENTS_FILENAME))

        self.config = HoardContentsConfig(os.path.join(self.folder, HOARD_CONTENTS_TOML))
        self.fsobjects = HoardFSObjects(self)

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
