import os
import pathlib
import sqlite3
import sys
from datetime import datetime
from functools import cached_property
from pathlib import PurePosixPath
from sqlite3 import Connection
from typing import Dict, Any, Optional, Tuple, Generator, Iterator, Iterable, List, Set

import rtoml

from contents.repo import RepoContentsConfig
from contents.repo_props import RepoFileProps
from contents.hoard_props import HoardDirProps, HoardFileStatus, HoardFileProps
from util import FIRST_VALUE, custom_isabs

HOARD_CONTENTS_FILENAME = "hoard.contents"
HOARD_CONTENTS_TOML = "hoard.contents.toml"


class HoardContentsConfig:
    def __init__(self, file: pathlib.Path):
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

    def remote_epoch(self, remote_uuid: str) -> int:
        return self._remote_config(remote_uuid).get("epoch", -1)

    def set_remote_epoch(self, remote_uuid: str, epoch: int, updated: datetime):
        self._remote_config(remote_uuid)["epoch"] = epoch
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
        config.doc["epoch"] = self.remote_epoch(config.uuid)
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


def _augment_with_fake_props(objects) -> Tuple[Set[str], List[str]]:
    # augment all objects with dummy folders as the hoard is incomplete by design
    all_dirs: Set[str] = set()
    all_files: List[str] = []
    for path, is_dir in list(objects.str_to_props()):
        if is_dir:
            all_dirs.add(path)
        else:
            all_files.append(path)

        current_path, _ = path.rsplit("/", 1)

        while len(current_path) > 0:
            if current_path in all_dirs:
                break

            all_dirs.add(current_path)
            current_path, _ = current_path.rsplit("/", 1)
    return all_dirs, all_files


def _add_all_files_and_folders(root, all_dirs: Set[str], all_files: List[str], fsobjects: "HoardFSObjects"):
    # add all files and folders, ordered by path so that parent folders come before children
    folders_cache: dict[str, HoardDir] = {"": root}
    for path in sorted(all_dirs):
        parent_path, child_name = path.rsplit("/", 1)

        # assert isinstance(props, HoardDirProps)
        folders_cache[path] = folders_cache[parent_path].create_dir(child_name)

    for path in all_files:
        parent_path, child_name = path.rsplit("/", 1)
        folders_cache[parent_path].create_file(child_name, path, fsobjects)


class HoardTree:
    def __init__(self, objects: "HoardFSObjects"):
        self.root = HoardDir(None, "", self)

        all_dirs, all_files = _augment_with_fake_props(objects)

        _add_all_files_and_folders(self.root, all_dirs, all_files, objects)

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
    def __init__(self, parent: "HoardDir", name: str, fullname: str, fsobjects: "HoardFSObjects"):
        self.parent = parent
        self.name = name

        self.fullname = fullname
        self.fsobjects = fsobjects

    @cached_property
    def props(self) -> HoardFileProps:
        return self.fsobjects[PurePosixPath(self.fullname)]


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

    def get_dir(self, subname: str) -> Optional["HoardDir"]:
        return self.dirs.get(subname, None)

    def create_dir(self, subname: str) -> "HoardDir":
        assert subname not in self.dirs and subname not in self.files
        new_dir = HoardDir(self, subname, self.tree)
        self.dirs[subname] = new_dir
        return new_dir

    def create_file(self, filename: str, fullname: str, fsobjects: "HoardFSObjects") -> HoardFile:
        assert filename not in self.dirs and filename not in self.files
        new_file = HoardFile(self, filename, fullname, fsobjects)
        self.files[filename] = new_file
        return new_file

    def walk(self, depth: int) -> Generator[Tuple[Optional["HoardDir"], Optional["HoardFile"]], None, None]:
        yield self, None
        if depth <= 0:
            return
        for hoard_file in self.files.values():
            yield None, hoard_file
        for hoard_dir in self.dirs.values():
            yield from hoard_dir.walk(depth - 1)


STATUSES_TO_FETCH = [HoardFileStatus.COPY.value, HoardFileStatus.GET.value, HoardFileStatus.MOVE.value]


class HoardFSObjects:
    def __init__(self, parent: "HoardContents"):
        self.parent = parent

    @cached_property
    def tree(self) -> HoardTree: return HoardTree(self)

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

    def _read_as_path_to_props(self, cursor, row) -> Tuple[PurePosixPath, HoardFileProps | HoardDirProps]:
        fullpath, fsobject_id, isdir, size, fasthash = row
        if isdir:
            return PurePosixPath(fullpath), HoardDirProps({})
        else:
            return PurePosixPath(fullpath), HoardFileProps(self.parent, fsobject_id, size, fasthash)

    def __getitem__(self, file_path: PurePosixPath) -> HoardFileProps | HoardDirProps:
        assert file_path.is_absolute()

        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_path_to_props
        return curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash "
            "FROM fsobject "
            "WHERE fsobject.fullpath = ? ",
            (file_path.as_posix(),)).fetchone()[1]

    def by_fasthash(self, fasthash: str) -> Iterable[Tuple[PurePosixPath, HoardFileProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_path_to_props
        yield from curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash "
            "FROM fsobject "
            "WHERE isdir = FALSE and fasthash = ?", (fasthash,))

    def __iter__(self) -> Iterable[Tuple[PurePosixPath, HoardFileProps | HoardDirProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_path_to_props
        yield from curr.execute("SELECT fullpath, fsobject_id, isdir, size, fasthash FROM fsobject")

    @property
    def dangling_files(self) -> Iterable[Tuple[PurePosixPath, HoardFileProps | HoardDirProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_path_to_props
        yield from curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash FROM fsobject "
            "WHERE isdir = FALSE AND "
            "  NOT EXISTS (SELECT 1 FROM fspresence WHERE fspresence.fsobject_id = fsobject.fsobject_id)")

    def with_pending(self, repo_uuid: str) -> Iterable[Tuple[PurePosixPath, HoardFileProps | HoardDirProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_path_to_props
        yield from curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash FROM fsobject "
            "WHERE isdir = FALSE AND EXISTS ("
            "  SELECT 1 FROM fspresence "
            "  WHERE fspresence.fsobject_id = fsobject.fsobject_id AND "
            "    uuid = ? AND "
            "    status in (?, ?, ?, ?))",
            (repo_uuid, HoardFileStatus.GET.value, HoardFileStatus.COPY.value, HoardFileStatus.MOVE.value,
             HoardFileStatus.CLEANUP.value))

    def available_in_repo(self, remote_uuid: str) -> Iterable[Tuple[PurePosixPath, HoardFileProps | HoardDirProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_path_to_props
        yield from curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash FROM fsobject "
            "WHERE EXISTS ("
            "  SELECT 1 FROM fspresence "
            "  WHERE fspresence.fsobject_id = fsobject.fsobject_id AND uuid = ? AND status = ?)",
            (remote_uuid, HoardFileStatus.AVAILABLE.value))

    def in_folder(self, folder: PurePosixPath) -> Iterable[Tuple[PurePosixPath, HoardFileProps | HoardDirProps]]:
        assert custom_isabs(folder.as_posix())  # from 3.13 behavior change...
        folder = folder.as_posix()
        folder_with_trailing = folder if folder.endswith("/") else folder + "/"
        assert folder_with_trailing.endswith('/')

        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_path_to_props

        yield from curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash FROM fsobject "
            "WHERE fullpath like ? or fullpath = ?",
            (f"{folder_with_trailing}%", folder))

    def str_to_props(self) -> Iterable[Tuple[str, bool]]:
        curr = self.parent.conn.cursor()

        return curr.execute("SELECT fullpath, isdir FROM fsobject ").fetchall()

    def status_by_uuid(self, folder_path: PurePosixPath | None) -> Dict[str, Dict[str, Dict[str, Any]]]:
        if folder_path is not None:
            AND_CLAUSE = " AND ? < fsobject.fullpath AND fsobject.fullpath < ? "
            args = (fast_between_filter_left(folder_path), fast_between_filter_right(folder_path))
        else:
            AND_CLAUSE = ""
            args = tuple()

        stats: Dict[str, Dict[str, Dict[str, Any]]] = dict()
        for uuid, nfiles, size in self.parent.conn.execute(
                "SELECT fspresence.uuid, count(fspresence.fsobject_id) as nfiles, sum(size) as total_size "
                "FROM fsobject JOIN fspresence ON fsobject.fsobject_id=fspresence.fsobject_id "
                f"WHERE isdir = FALSE {AND_CLAUSE} "
                "GROUP BY fspresence.uuid", args):
            stats[uuid] = {
                "total": {"nfiles": nfiles, "size": size}}

        for uuid, status, nfiles, size in self.parent.conn.execute(
                "SELECT fspresence.uuid, fspresence.status, count(fspresence.fsobject_id) as nfiles, sum(size) as total_size "
                "FROM fsobject JOIN fspresence ON fsobject.fsobject_id=fspresence.fsobject_id "
                f"WHERE isdir = FALSE {AND_CLAUSE} "
                "GROUP BY fspresence.uuid, fspresence.status", args):
            stats[uuid][status] = {"nfiles": nfiles, "size": size}
        return stats

    def to_fetch(self, repo_uuid: str) -> Generator[Tuple[str, HoardFileProps | HoardDirProps], None, None]:
        for fsobject_id, fullpath, isdir, size, fasthash in self.parent.conn.execute(
                "SELECT fsobject.fsobject_id, fullpath, isdir, size, fasthash "
                "FROM fsobject JOIN fspresence on fsobject.fsobject_id = fspresence.fsobject_id "
                "WHERE fspresence.uuid = ? and fspresence.status in (?, ?, ?)", (repo_uuid, *STATUSES_TO_FETCH)):
            assert not isdir
            yield fullpath, HoardFileProps(self.parent, fsobject_id, size, fasthash)

    def to_cleanup(self, repo_uuid: str) -> Generator[Tuple[str, HoardFileProps | HoardDirProps], None, None]:
        for fsobject_id, fullpath, isdir, size, fasthash in self.parent.conn.execute(
                "SELECT fsobject.fsobject_id, fullpath, isdir, size, fasthash "
                "FROM fsobject JOIN fspresence ON fsobject.fsobject_id = fspresence.fsobject_id "
                "WHERE fspresence.uuid = ? AND fspresence.status = ?", (repo_uuid, HoardFileStatus.CLEANUP.value)):
            assert not isdir
            yield fullpath, HoardFileProps(self.parent, fsobject_id, size, fasthash)

    def where_to_move(self, remote: str, hoard_file: PurePosixPath) -> List[str]:
        assert hoard_file.is_absolute()
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr.execute(
            f"SELECT fsobject.fullpath "
            f"FROM fspresence JOIN fsobject on fspresence.fsobject_id = fsobject.fsobject_id "
            f"WHERE status = ? AND move_from = ? AND uuid = ?",
            (HoardFileStatus.MOVE.value, hoard_file.as_posix(), remote)).fetchall()

    def __contains__(self, file_path: PurePosixPath) -> bool:
        assert file_path.is_absolute()

        curr = self._first_value_curr()
        return curr.execute(
            "SELECT count(1) > 0 FROM fsobject WHERE fsobject.fullpath = ?",
            (file_path.as_posix(),)).fetchone()

    def add_or_replace_file(self, filepath: PurePosixPath, props: RepoFileProps) -> HoardFileProps:
        assert filepath.is_absolute()
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE

        # add fsobject entry
        curr.execute(
            "INSERT OR REPLACE INTO fsobject(fullpath, isdir, size, fasthash) VALUES (?, FALSE, ?, ?)",
            (filepath.as_posix(), props.size, props.fasthash))

        # cleanup presence status
        fsobject_id: int = curr.execute(
            "SELECT fsobject_id FROM fsobject WHERE fullpath = ?", (filepath.as_posix(),)).fetchone()
        curr.execute("DELETE FROM fspresence WHERE fsobject_id = ?", (fsobject_id,))
        return HoardFileProps(self.parent, fsobject_id, props.size, props.fasthash)

    def _first_value_curr(self):
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr

    def add_dir(self, curr_dir: PurePosixPath):
        assert curr_dir.is_absolute()
        curr = self._first_value_curr()

        # add fsobject entry
        curr.execute(
            "INSERT OR REPLACE INTO fsobject(fullpath, isdir) VALUES (?, TRUE)",
            (curr_dir.as_posix(),))
        fsobject_id: int = curr.execute(
            "SELECT fsobject_id FROM fsobject WHERE fullpath = ?", (curr_dir.as_posix(),)).fetchone()
        curr.execute("DELETE FROM fspresence WHERE fsobject_id = ?", (fsobject_id,))

    def delete(self, curr_path: PurePosixPath):
        assert curr_path.is_absolute()

        curr = self._first_value_curr()
        fsobject_id: Optional[int] = curr.execute(
            "SELECT fsobject_id FROM fsobject WHERE fullpath = ?", (curr_path.as_posix(),)).fetchone()
        if fsobject_id is None:
            return
        curr.execute("DELETE FROM fsobject WHERE fsobject_id = ?", (fsobject_id,))
        curr.execute("DELETE FROM fspresence WHERE fsobject_id = ?", (fsobject_id,))

    def move_via_mounts(self, orig_path: PurePosixPath, new_path: PurePosixPath, props: HoardDirProps | HoardFileProps):
        assert orig_path.is_absolute()
        assert new_path.is_absolute()
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
                (new_path.as_posix(), props.size, props.fasthash))

            # add old presence
            new_path_id: int = curr.execute(
                "SELECT fsobject_id FROM fsobject WHERE fullpath = ?",
                (new_path.as_posix(),)).fetchone()
            curr.executemany(
                "INSERT INTO fspresence(fsobject_id, uuid, status) VALUES (?, ?, ?)",
                [(new_path_id, uuid, status.value) for uuid, status in props.presence.items()])
        else:
            assert isinstance(props, HoardDirProps)
            curr.execute(
                "INSERT OR REPLACE INTO fsobject(fullpath, isdir) VALUES (?, TRUE)",
                (new_path.as_posix(),))

        self.delete(orig_path)

    def copy(self, from_fullpath: PurePosixPath, to_fullpath: PurePosixPath):
        assert from_fullpath.is_absolute()
        assert to_fullpath.is_absolute()
        assert from_fullpath != to_fullpath

        self.delete(to_fullpath)

        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE

        props = self[PurePosixPath(from_fullpath)]
        if isinstance(props, HoardFileProps):
            # add fsobject entry
            curr.execute(
                "INSERT OR REPLACE INTO fsobject(fullpath, isdir, size, fasthash) VALUES (?, FALSE, ?, ?)",
                (to_fullpath.as_posix(), props.size, props.fasthash))

            # add presence tp request
            new_path_id: int = curr.execute(
                "SELECT fsobject_id FROM fsobject WHERE fullpath = ?",
                (to_fullpath.as_posix(),)).fetchone()

            previously_added_repos = props.repos_having_status(
                HoardFileStatus.COPY, HoardFileStatus.GET, HoardFileStatus.AVAILABLE)
            curr.executemany(
                "INSERT INTO fspresence(fsobject_id, uuid, status) VALUES (?, ?, ?)",
                [(new_path_id, uuid, HoardFileStatus.COPY.value) for uuid in previously_added_repos])
        elif isinstance(props, HoardDirProps):
            self.add_dir(to_fullpath)
        else:
            raise ValueError(f"props type unrecognized: {type(props)}")

    def used_size(self, repo_uuid: str) -> int:
        used_size = self.parent.conn.execute(
            "SELECT SUM(fsobject.size) "
            "FROM fsobject "
            "WHERE isdir = FALSE AND EXISTS ("
            "  SELECT 1 FROM fspresence "
            "  WHERE fspresence.fsobject_id = fsobject.fsobject_id AND "
            "    uuid = ? AND "
            f"    status in ({', '.join(['?'] * len(STATUSES_THAT_USE_SIZE))}))",
            (repo_uuid, *STATUSES_THAT_USE_SIZE)) \
            .fetchone()[0]
        return used_size if used_size is not None else 0

    def stats_in_folder(self, folder_path: PurePosixPath) -> Tuple[int, int]:
        assert folder_path.is_absolute()

        return self.parent.conn.execute(
            "SELECT COUNT(1), IFNULL(SUM(fsobject.size), 0) FROM fsobject "
            "WHERE isdir = FALSE AND ? < fullpath AND fullpath < ?",  # fast search using the index
            (fast_between_filter_left(folder_path), fast_between_filter_right(folder_path))).fetchone()


def fast_between_filter_left(folder_path: PurePosixPath):
    folder_path = "" if folder_path.as_posix() == "/" else folder_path.as_posix()
    return folder_path + "/"


def fast_between_filter_right(folder_path: PurePosixPath):
    folder_path = "" if folder_path.as_posix() == "/" else folder_path.as_posix()
    return folder_path + "0"


STATUSES_THAT_USE_SIZE = [
    HoardFileStatus.AVAILABLE.value, HoardFileStatus.GET.value, HoardFileStatus.COPY.value,
    HoardFileStatus.CLEANUP.value]


class HoardContents:
    @staticmethod
    def load(folder: str, is_readonly: bool) -> "HoardContents":
        config_filename = os.path.join(folder, HOARD_CONTENTS_FILENAME)

        if not os.path.isfile(config_filename):
            if is_readonly:
                raise ValueError("Cannot create a read-only hoard!")

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
                " move_from TEXT,"
                " FOREIGN KEY (fsobject_id) REFERENCES fsobject(id) ON DELETE CASCADE)"
            )
            curr.execute("CREATE UNIQUE INDEX fspresence_fsobject_id__uuid ON fspresence(fsobject_id, uuid)")

            conn.commit()
            conn.close()

        toml_filename = os.path.join(folder, HOARD_CONTENTS_TOML)
        if not os.path.isfile(toml_filename):
            if is_readonly:
                raise ValueError("Cannot create a read-only hoard!")

            with open(toml_filename, "w") as f:
                rtoml.dump({
                    "updated": datetime.now().isoformat()
                }, f)

        return HoardContents(folder, is_readonly)

    conn: Connection
    config: HoardContentsConfig
    fsobjects: HoardFSObjects

    def __init__(self, folder: str, is_readonly: bool):
        self.folder = pathlib.Path(folder)
        self.is_readonly = is_readonly

        self.config = None
        self.fsobjects = None
        self.conn = None

    def __enter__(self):
        self.conn = sqlite3.connect(
            f"file:{os.path.join(self.folder, HOARD_CONTENTS_FILENAME)}{'?mode=ro' if self.is_readonly else ''}",
            uri=True)

        self.config = HoardContentsConfig(self.folder.joinpath(HOARD_CONTENTS_TOML))
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
