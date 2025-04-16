import logging
import os
import pathlib
import sys
from datetime import datetime
from functools import cached_property
from sqlite3 import Connection
from typing import Dict, Any, Optional, Tuple, Generator, Iterable, List, Set, AsyncGenerator

import rtoml

from command.fast_path import FastPosixPath
from contents.hoard_props import HoardFileStatus, HoardFileProps
from contents.repo import RepoContentsConfig
from contents.repo_props import RepoFileProps
from sql_util import SubfolderFilter, NoFilter, sqlite3_standard
from util import FIRST_VALUE, custom_isabs

HOARD_CONTENTS_FILENAME = "hoard.contents"
HOARD_CONTENTS_TOML = "hoard.contents.toml"


class HoardContentsConfig:
    def __init__(self, file: pathlib.Path, is_readonly: bool):
        self.file = file
        self.is_readonly = is_readonly

        with open(file, "r") as f:
            self.doc = rtoml.load(f)

    def write(self):
        if self.is_readonly:
            raise ValueError("Cannot write to read-only contents!")

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

    @property
    def hoard_epoch(self) -> int:
        return self.doc.get("hoard_epoch", 0)

    @hoard_epoch.setter
    def hoard_epoch(self, epoch: int) -> None:
        self.doc["hoard_epoch"] = epoch
        self.write()

    def remote_epoch(self, remote_uuid: str) -> int:
        return self._remote_config(remote_uuid).get("epoch", -1)

    def last_hoard_epoch_for_remote(self, remote_uuid: str) -> int:
        return self._remote_config(remote_uuid).get("last_hoard_epoch", -1)

    def mark_up_to_date(self, remote_uuid: str, epoch: int, updated: datetime):
        self._remote_config(remote_uuid)["epoch"] = epoch
        self._remote_config(remote_uuid)["last_hoard_epoch"] = self.hoard_epoch
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

    def bump_hoard_epoch(self):
        self.hoard_epoch += 1


class HoardTree:
    def __init__(self, objects: "HoardFSObjects"):
        self.root = HoardDir(None, "", self)
        self.objects = objects

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

        self._props = None

    @property
    def props(self) -> HoardFileProps:
        if self._props is None:
            self._props = self.fsobjects[FastPosixPath(self.fullname)]
        return self._props

    def reload_props(self):
        self._props = self.fsobjects[FastPosixPath(self.fullname)]


class HoardDir:
    @cached_property
    def fullname(self) -> str:  # fixme replace with FullPosixPath
        parent_path = pathlib.Path(self.parent.fullname) if self.parent is not None else pathlib.Path("/")
        return parent_path.joinpath(self.name).as_posix()

    def __init__(self, parent: Optional["HoardDir"], name: str, tree: HoardTree):
        self.tree = tree
        self.name = name
        self.parent = parent

    @property
    def dirs(self) -> Dict[str, "HoardDir"]:
        return dict(
            (_filename(subname), HoardDir(self, _filename(subname), self.tree))
            for subname in self.tree.objects.get_sub_dirs(self._as_parent))

    @property
    def _as_parent(self):
        return self.fullname if self.fullname != "/" else ""

    @property
    def files(self) -> Dict[str, HoardFile]:
        return dict(
            (_filename(subname), HoardFile(self, _filename(subname), subname, self.tree.objects))
            for subname in self.tree.objects.get_sub_files(self._as_parent))

    def get_dir(self, subname: str) -> Optional["HoardDir"]:  # FIXME remove, slow and obsolete
        return self.dirs.get(subname, None)

    def walk(self, depth: int) -> Generator[Tuple[Optional["HoardDir"], Optional["HoardFile"]], None, None]:
        yield self, None
        if depth <= 0:
            return
        for hoard_file in self.files.values():
            yield None, hoard_file
        for hoard_dir in self.dirs.values():
            yield from hoard_dir.walk(depth - 1)


def _filename(filepath: str) -> str:
    _, name = os.path.split(filepath)
    return name


STATUSES_TO_FETCH = [HoardFileStatus.COPY.value, HoardFileStatus.GET.value, HoardFileStatus.MOVE.value]


class ReadonlyHoardFSObjects:
    def __init__(self, parent: "HoardContents"):
        self.parent = parent

    @cached_property
    async def tree(self) -> HoardTree:
        return HoardTree(self)

    @property
    def num_files(self) -> int:
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr.execute("SELECT count(1) FROM fsobject WHERE isdir=FALSE").fetchone()

    @property
    def total_size(self) -> int:
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr.execute("SELECT sum(size) FROM fsobject WHERE isdir=FALSE").fetchone()

    def __len__(self) -> int:
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr.execute("SELECT count(1) FROM fsobject WHERE isdir = FALSE").fetchone()

    def _read_as_path_to_props(self, cursor, row) -> Tuple[FastPosixPath, HoardFileProps]:
        fullpath, fsobject_id, isdir, size, fasthash = row
        assert isdir == False
        return FastPosixPath(fullpath), HoardFileProps(self.parent, fsobject_id, size, fasthash)

    def __getitem__(self, file_path: FastPosixPath) -> HoardFileProps:
        assert file_path.is_absolute()

        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_path_to_props
        return curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash "
            "FROM fsobject "
            "WHERE fsobject.fullpath = ? AND isdir = FALSE ",
            (file_path.as_posix(),)).fetchone()[1]

    def by_fasthash(self, fasthash: str) -> Iterable[Tuple[FastPosixPath, HoardFileProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_path_to_props
        yield from curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash "
            "FROM fsobject "
            "WHERE fasthash = ? AND isdir = FALSE ", (fasthash,))

    def __iter__(self) -> Iterable[Tuple[FastPosixPath, HoardFileProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_path_to_props
        yield from curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash FROM fsobject WHERE isdir = FALSE ")

    @property
    def dangling_files(self) -> Iterable[Tuple[FastPosixPath, HoardFileProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_path_to_props
        yield from curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash FROM fsobject "
            "WHERE isdir = FALSE AND "
            "  NOT EXISTS (SELECT 1 FROM fspresence WHERE fspresence.fsobject_id = fsobject.fsobject_id)")

    def with_pending(self, repo_uuid: str) -> Iterable[Tuple[FastPosixPath, HoardFileProps]]:
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

    def available_in_repo(self, remote_uuid: str) -> Iterable[Tuple[FastPosixPath, HoardFileProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_path_to_props
        yield from curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash FROM fsobject "
            "WHERE isdir = FALSE AND EXISTS ("
            "  SELECT 1 FROM fspresence "
            "  WHERE fspresence.fsobject_id = fsobject.fsobject_id AND uuid = ? AND status = ?)",
            (remote_uuid, HoardFileStatus.AVAILABLE.value))

    def to_get_in_repo(self, remote_uuid: str) -> Iterable[Tuple[FastPosixPath, HoardFileProps]]:
        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_path_to_props
        yield from curr.execute(
            "SELECT fullpath, fsobject_id, isdir, size, fasthash FROM fsobject "
            "WHERE isdir = FALSE AND EXISTS ("
            "  SELECT 1 FROM fspresence "
            "  WHERE fspresence.fsobject_id = fsobject.fsobject_id AND uuid = ? AND status = ?)",
            (remote_uuid, HoardFileStatus.GET.value))

    async def in_folder(self, folder: FastPosixPath) -> AsyncGenerator[
        Tuple[FastPosixPath, HoardFileProps]]:
        assert custom_isabs(folder.as_posix())  # from 3.13 behavior change...
        folder = folder.as_posix()
        folder_with_trailing = folder if folder.endswith("/") else folder + "/"
        assert folder_with_trailing.endswith('/')

        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_path_to_props

        for fp in curr.execute(
                "SELECT fullpath, fsobject_id, isdir, size, fasthash FROM fsobject "
                "WHERE isdir = FALSE AND fullpath like ? or fullpath = ?",
                (f"{folder_with_trailing}%", folder)):
            yield fp

    async def in_folder_non_deleted(self, folder: FastPosixPath) -> AsyncGenerator[
        Tuple[FastPosixPath, HoardFileProps]]:
        assert custom_isabs(folder.as_posix())  # from 3.13 behavior change...

        folder = folder.as_posix()
        folder_with_trailing = folder if folder.endswith("/") else folder + "/"
        assert folder_with_trailing.endswith('/')

        curr = self.parent.conn.cursor()
        curr.row_factory = self._read_as_path_to_props

        for fp in curr.execute(
                "SELECT fullpath, fsobject_id, isdir, size, fasthash FROM fsobject "
                "WHERE isdir = FALSE "
                "  AND (fullpath like ? or fullpath = ?) "
                "  AND EXISTS ("
                "    SELECT 1 FROM fspresence "
                "    WHERE fsobject.fsobject_id = fspresence.fsobject_id "
                "      AND status != ?)",
                (f"{folder_with_trailing}%", folder, HoardFileStatus.CLEANUP.value)):
            yield fp

    def str_to_props(self) -> Iterable[Tuple[str, bool]]:
        curr = self.parent.conn.cursor()

        return curr.execute("SELECT fullpath, isdir FROM fsobject ").fetchall()

    def status_by_uuid(self, folder_path: FastPosixPath | None) -> Dict[str, Dict[str, Dict[str, Any]]]:
        if folder_path is not None:
            subfolder_filter = SubfolderFilter('fsobject.fullpath', folder_path)
        else:
            subfolder_filter = NoFilter()

        stats: Dict[str, Dict[str, Dict[str, Any]]] = dict()
        for uuid, nfiles, size in self.parent.conn.execute(
                "SELECT fspresence.uuid, count(fspresence.fsobject_id) as nfiles, sum(size) as total_size "
                "FROM fsobject JOIN fspresence ON fsobject.fsobject_id=fspresence.fsobject_id "
                f"WHERE isdir = FALSE AND {subfolder_filter.where_clause} "
                "GROUP BY fspresence.uuid",
                subfolder_filter.params):
            stats[uuid] = {
                "total": {"nfiles": nfiles, "size": size}}

        for uuid, status, nfiles, size in self.parent.conn.execute(
                "SELECT fspresence.uuid, fspresence.status, count(fspresence.fsobject_id) as nfiles, sum(size) as total_size "
                "FROM fsobject JOIN fspresence ON fsobject.fsobject_id=fspresence.fsobject_id "
                f"WHERE isdir = FALSE AND {subfolder_filter.where_clause} "
                "GROUP BY fspresence.uuid, fspresence.status",
                subfolder_filter.params):
            stats[uuid][status] = {"nfiles": nfiles, "size": size}
        return stats

    def to_fetch(self, repo_uuid: str) -> Generator[Tuple[str, HoardFileProps], None, None]:
        for fsobject_id, fullpath, isdir, size, fasthash in self.parent.conn.execute(
                "SELECT fsobject.fsobject_id, fullpath, isdir, size, fasthash "
                "FROM fsobject JOIN fspresence on fsobject.fsobject_id = fspresence.fsobject_id "
                "WHERE fspresence.uuid = ? and fspresence.status in (?, ?, ?) AND isdir = FALSE ",
                (repo_uuid, *STATUSES_TO_FETCH)):
            assert not isdir
            yield fullpath, HoardFileProps(self.parent, fsobject_id, size, fasthash)

    def to_cleanup(self, repo_uuid: str) -> Generator[Tuple[FastPosixPath, HoardFileProps], None, None]:
        for fsobject_id, fullpath, isdir, size, fasthash in self.parent.conn.execute(
                "SELECT fsobject.fsobject_id, fullpath, isdir, size, fasthash "
                "FROM fsobject JOIN fspresence ON fsobject.fsobject_id = fspresence.fsobject_id "
                "WHERE fspresence.uuid = ? AND fspresence.status = ? AND isdir = FALSE ",
                (repo_uuid, HoardFileStatus.CLEANUP.value)):
            assert not isdir
            yield FastPosixPath(fullpath), HoardFileProps(self.parent, fsobject_id, size, fasthash)

    def where_to_move(self, remote: str, hoard_file: FastPosixPath) -> List[str]:
        assert hoard_file.is_absolute()
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr.execute(
            f"SELECT fsobject.fullpath "
            f"FROM fspresence JOIN fsobject on fspresence.fsobject_id = fsobject.fsobject_id "
            f"WHERE status = ? AND move_from = ? AND uuid = ? AND isdir = FALSE ",
            (HoardFileStatus.MOVE.value, hoard_file.as_posix(), remote)).fetchall()

    def __contains__(self, file_path: FastPosixPath) -> bool:
        assert file_path.is_absolute()

        curr = self._first_value_curr()
        return curr.execute(
            "SELECT count(1) > 0 FROM fsobject WHERE fsobject.fullpath = ? AND isdir = FALSE ",
            (file_path.as_posix(),)).fetchone()

    def _first_value_curr(self):
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr

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

    def stats_in_folder(self, folder_path: FastPosixPath) -> Tuple[int, int]:
        assert folder_path.is_absolute()
        subfolder_filter = SubfolderFilter('fullpath', folder_path)

        return self.parent.conn.execute(
            "SELECT COUNT(1), IFNULL(SUM(fsobject.size), 0) FROM fsobject "
            f"WHERE isdir = FALSE AND {subfolder_filter.where_clause}",  # fast search using the index
            subfolder_filter.params).fetchone()

    @cached_property
    def query(self) -> "Query":
        return Query(self.parent.conn)

    def get_sub_dirs(self, fullpath: str) -> Iterable[str]:
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        yield from curr.execute(
            "SELECT fullpath FROM folder_structure "
            "WHERE parent = ? AND isdir = TRUE",
            (fullpath,))

    def get_sub_files(self, fullpath: str) -> Iterable[str]:
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE
        yield from curr.execute(
            "SELECT fullpath FROM folder_structure "
            "WHERE parent = ? AND isdir = FALSE",
            (fullpath,))


class HoardFSObjects(ReadonlyHoardFSObjects):
    def __init__(self, parent: "HoardContents"):
        super().__init__(parent)

    def add_or_replace_file(self, filepath: FastPosixPath, props: RepoFileProps) -> HoardFileProps:
        assert filepath.is_absolute()
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE

        # add fsobject entry
        curr.execute(
            "INSERT INTO fsobject(fullpath, isdir, size, fasthash, last_epoch_updated) VALUES (?, FALSE, ?, ?, ?) "
            "ON CONFLICT (fullpath) DO UPDATE "
            "SET isdir = excluded.isdir, size = excluded.size, fasthash = excluded.fasthash, last_epoch_updated = excluded.last_epoch_updated ",
            (filepath.as_posix(), props.size, props.fasthash, self.parent.config.hoard_epoch))

        # cleanup presence status
        fsobject_id: int = curr.execute(
            "SELECT fsobject_id FROM fsobject WHERE fullpath = ?", (filepath.as_posix(),)).fetchone()
        curr.execute("DELETE FROM fspresence WHERE fsobject_id = ?", (fsobject_id,))
        return HoardFileProps(self.parent, fsobject_id, props.size, props.fasthash)

    def delete(self, curr_path: FastPosixPath):
        assert curr_path.is_absolute()

        curr = self._first_value_curr()
        fsobject_id: Optional[int] = curr.execute(
            "SELECT fsobject_id FROM fsobject WHERE fullpath = ?", (curr_path.as_posix(),)).fetchone()
        if fsobject_id is None:
            return
        curr.execute("DELETE FROM fspresence WHERE fsobject_id = ?", (fsobject_id,))
        curr.execute("DELETE FROM fsobject WHERE fsobject_id = ?", (fsobject_id,))

    def move_via_mounts(self, orig_path: FastPosixPath, new_path: FastPosixPath, props: HoardFileProps):
        assert orig_path.is_absolute()
        assert new_path.is_absolute()
        assert orig_path != new_path
        assert isinstance(props, HoardFileProps)

        # delete whatever new_path had
        self.delete(new_path)

        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE

        assert isinstance(props, HoardFileProps)
        # add fsobject entry
        curr.execute(
            "INSERT INTO fsobject(fullpath, isdir, size, fasthash, last_epoch_updated) VALUES (?, FALSE, ?, ?, ?)",
            (new_path.as_posix(), props.size, props.fasthash, self.parent.config.hoard_epoch))

        # add old presence
        new_path_id: int = curr.execute(
            "SELECT fsobject_id FROM fsobject WHERE fullpath = ?",
            (new_path.as_posix(),)).fetchone()
        curr.executemany(
            "INSERT INTO fspresence(fsobject_id, uuid, status) VALUES (?, ?, ?)",
            [(new_path_id, uuid, status.value) for uuid, status in props.presence.items()])

        self.delete(orig_path)

    def copy(self, from_fullpath: FastPosixPath, to_fullpath: FastPosixPath):
        assert from_fullpath.is_absolute()
        assert to_fullpath.is_absolute()
        assert from_fullpath != to_fullpath

        self.delete(to_fullpath)

        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE

        props = self[FastPosixPath(from_fullpath)]
        assert isinstance(props, HoardFileProps)
        # add fsobject entry
        curr.execute(
            "INSERT INTO fsobject(fullpath, isdir, size, fasthash, last_epoch_updated) VALUES (?, FALSE, ?, ?, ?)",
            (to_fullpath.as_posix(), props.size, props.fasthash, self.parent.config.hoard_epoch))

        # add presence tp request
        new_path_id: int = curr.execute(
            "SELECT fsobject_id FROM fsobject WHERE fullpath = ?",
            (to_fullpath.as_posix(),)).fetchone()

        previously_added_repos = props.repos_having_status(
            HoardFileStatus.COPY, HoardFileStatus.GET, HoardFileStatus.AVAILABLE)
        curr.executemany(
            "INSERT INTO fspresence(fsobject_id, uuid, status) VALUES (?, ?, ?)",
            [(new_path_id, uuid, HoardFileStatus.COPY.value) for uuid in previously_added_repos])


class Query:
    def __init__(self, conn: Connection):
        self.conn = conn
        conn.execute(
            f"CREATE TEMPORARY VIEW IF NOT EXISTS file_stats_query AS "
            f"SELECT "
            f"  (SELECT COUNT(1) from fspresence WHERE fsobject.fsobject_id = fspresence.fsobject_id AND status IN ('available', 'move')) as source_count,"
            f"  NOT EXISTS (SELECT 1 from fspresence WHERE fsobject.fsobject_id = fspresence.fsobject_id and status NOT IN ('cleanup')) as is_deleted,"
            f"  fullpath, fsobject_id "
            f"FROM fsobject "
            f"WHERE isdir = 0 ")

    def count_non_deleted(self, folder_name: FastPosixPath) -> int:
        subfolder_filter = SubfolderFilter('fullpath', folder_name)
        return int(self.cursor().execute(
            "SELECT IFNULL(SUM(is_deleted == 0), 0) AS non_deleted_count "
            f"FROM file_stats_query WHERE {subfolder_filter.where_clause} ",
            subfolder_filter.params).fetchone())

    def cursor(self):
        curr = self.conn.cursor()
        curr.row_factory = FIRST_VALUE
        return curr

    def num_without_source(self, folder_name: FastPosixPath) -> int:
        subfolder_filter = SubfolderFilter('fullpath', folder_name)
        return int(self.cursor().execute(
            "SELECT IFNULL(SUM(source_count == 0), 0) AS without_source_count "
            f"FROM file_stats_query WHERE {subfolder_filter.where_clause}",
            subfolder_filter.params).fetchone())

    def is_deleted(self, file_name: FastPosixPath) -> bool:
        logging.warning(file_name.as_posix())
        return bool(self.cursor().execute(
            "SELECT is_deleted FROM file_stats_query WHERE fullpath = ?",
            (file_name.as_posix(),)).fetchone())


STATUSES_THAT_USE_SIZE = [
    HoardFileStatus.AVAILABLE.value, HoardFileStatus.GET.value, HoardFileStatus.COPY.value,
    HoardFileStatus.CLEANUP.value]


class ReadonlyHoardContentsConn:
    def __init__(self, folder: pathlib.Path):
        self.folder = folder

    async def __aenter__(self) -> "HoardContents":
        self.contents = HoardContents(self.folder, True)
        return self.contents

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.contents.close(False)
        return None

    def writeable(self):
        return HoardContentsConn(self.folder)

class HoardContentsConn:
    def __init__(self, folder: pathlib.Path):
        config_filename = os.path.join(folder, HOARD_CONTENTS_FILENAME)

        if not os.path.isfile(config_filename):
            conn = sqlite3_standard(config_filename)
            curr = conn.cursor()

            init_hoard_db_tables(curr)

            conn.commit()
            conn.close()

        toml_filename = os.path.join(folder, HOARD_CONTENTS_TOML)
        if not os.path.isfile(toml_filename):
            with open(toml_filename, "w") as f:
                rtoml.dump({
                    "updated": datetime.now().isoformat()
                }, f)

        self.folder = folder

    async def __aenter__(self) -> "HoardContents":
        self.contents = HoardContents(self.folder, False)
        return self.contents

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.contents.close(True)
        return None


class HoardContents:
    conn: Connection
    config: HoardContentsConfig
    fsobjects: HoardFSObjects

    def __init__(self, folder: pathlib.Path, is_readonly: bool):
        self.conn = sqlite3_standard(
            f"file:{os.path.join(folder, HOARD_CONTENTS_FILENAME)}{'?mode=ro' if is_readonly else ''}",
            uri=True)

        self.config = HoardContentsConfig(folder.joinpath(HOARD_CONTENTS_TOML), is_readonly)
        self.fsobjects = HoardFSObjects(self) if not is_readonly else ReadonlyHoardFSObjects(self)

    def close(self, writeable: bool):
        if writeable:
            self.config.bump_hoard_epoch()
            self.config.write()

        self.conn.commit()
        self.conn.close()

        self.config = None
        self.fsobjects = None
        self.conn = None


def init_hoard_db_tables(curr):
    curr.execute(
        "CREATE TABLE fsobject("
        " fsobject_id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " fullpath TEXT NOT NULL UNIQUE,"
        " isdir BOOL NOT NULL,"
        " size INTEGER,"
        " fasthash TEXT,"
        " last_epoch_updated INTEGER)")
    # for fasthash lookups in melding
    curr.execute("CREATE INDEX index_fsobject_fasthash ON fsobject (fasthash) ")
    curr.execute(
        "CREATE TABLE fspresence ("
        " fsobject_id INTEGER,"
        " uuid TEXT NOT NULL,"
        " status TEXT NOT NULL,"
        " move_from TEXT,"
        " FOREIGN KEY (fsobject_id) REFERENCES fsobject(fsobject_id) ON DELETE RESTRICT)"
    )

    curr.execute("CREATE UNIQUE INDEX fspresence_fsobject_id__uuid ON fspresence(fsobject_id, uuid)")

    curr.executescript("""
            CREATE TABLE folder_structure (
              fullpath TEXT NOT NULL PRIMARY KEY,
              fsobject_id INTEGER,
              ISDIR BOOL NOT NULL,
              parent TEXT,
              FOREIGN KEY(parent) REFERENCES folder_structure(fullpath) ON DELETE RESTRICT
              );
            CREATE INDEX _folder_structure_parent ON folder_structure (parent);

            -- add to folder structure
            CREATE TRIGGER add_missing__folder_structure_on_fsobject AFTER INSERT ON fsobject 
            BEGIN
              INSERT INTO folder_structure (fullpath, parent, isdir)
              SELECT new.fullpath, CASE WHEN LENGTH(new.fullpath) == 0 THEN NULL ELSE rtrim(rtrim(new.fullpath, replace(new.fullpath, '/', '')), '/') END, new.isdir
              WHERE NOT EXISTS (SELECT 1 FROM folder_structure WHERE folder_structure.fullpath = new.fullpath);
              
              UPDATE folder_structure SET fsobject_id = new.fsobject_id WHERE fullpath = new.fullpath; 
            END;

            CREATE TRIGGER add_missing__folder_structure_parent BEFORE INSERT ON folder_structure
            WHEN new.parent IS NOT NULL AND NOT EXISTS (SELECT 1 FROM folder_structure WHERE folder_structure.fullpath = new.parent)
            BEGIN
              INSERT OR REPLACE INTO folder_structure(fullpath, parent, isdir)
              VALUES (new.parent, CASE WHEN LENGTH(new.parent)=0 THEN NULL ELSE rtrim(rtrim(new.parent, replace(new.parent, '/', '')), '/') END, TRUE);
            END;

            CREATE TRIGGER remove_obsolete__folder_structure_on_fsobject_can_delete AFTER DELETE ON fsobject
            WHEN NOT EXISTS(SELECT 1 FROM folder_structure WHERE parent == old.fullpath)
            BEGIN
              DELETE FROM folder_structure WHERE folder_structure.fullpath = old.fullpath; -- no child folders
            END;
            
            CREATE TRIGGER remove_obsolete__folder_structure_on_fsobject AFTER DELETE ON fsobject
            BEGIN
              UPDATE folder_structure SET fsobject_id = NULL WHERE folder_structure.fullpath = old.fullpath;
            END;

            CREATE TRIGGER remove_obsolete__folder_structure_on_no_other_children AFTER DELETE ON folder_structure
            WHEN NOT EXISTS (SELECT 1 FROM fsobject WHERE fsobject.fullpath = old.parent) 
              AND NOT EXISTS (SELECT 1 FROM folder_structure WHERE folder_structure.parent = old.parent)
            BEGIN
              DELETE FROM folder_structure 
              WHERE folder_structure.fullpath = old.parent;
            END;""")
