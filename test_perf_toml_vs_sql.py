import os
import sqlite3
import time
import unittest
from os.path import join
from sqlite3 import Cursor, Connection
from typing import Callable, Dict

from alive_progress import alive_bar

from contents_hoard import HoardContents, HoardTree
from contents_props import DirProps, HoardFileProps


def _create_tables(conn: Connection):
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
        "CREATE TABLE fspresence ("
        " fsobject_id INTEGER,"
        " uuid TEXT NOT NULL,"
        " state TEXT NOT NULL,"
        " FOREIGN KEY (fsobject_id) REFERENCES fsobject(id) ON DELETE CASCADE)"
    )
    curr.execute("CREATE UNIQUE INDEX fspresence_fsobject_id__uuid ON fspresence(fsobject_id, uuid)")
    conn.commit()
    return curr


def _copy_hoard_to_db(curr: Cursor, hoard: HoardContents, bar: Callable[[], None]):
    for f, props in hoard.fsobjects:
        bar()
        if isinstance(props, HoardFileProps):
            curr.execute(
                "INSERT INTO fsobject(fullpath, isdir, size, fasthash) VALUES (?, FALSE, ?, ?)",
                (f, props.size, props.fasthash))

            file_id = curr.execute("SELECT fsobject_id FROM fsobject WHERE fullpath = ?", (f,)).fetchone()[0]
            for uuid, presence in props.presence.items():
                curr.execute(
                    "INSERT INTO fspresence (fsobject_id, uuid, state) VALUES (?, ?, ?)",
                    (file_id, uuid, presence.value))

        elif isinstance(props, DirProps):
            curr.execute(
                "INSERT INTO fsobject(fullpath, isdir) VALUES (?, TRUE)",
                (f,))
        else:
            raise TypeError(props)


def _test_perf_create_and_lookup(conn, hoard):
    print("Testing perf_create_and_lookup...")
    curr = _create_tables(conn)
    t_before_addall = time.perf_counter()
    with alive_bar(len(hoard.fsobjects)) as bar:
        _copy_hoard_to_db(curr, hoard, bar)

    fscount = conn.execute("SELECT count(*) FROM fsobject").fetchone()[0]
    print(f"Loaded {fscount} objects!")
    conn.commit()

    f_after_addall = time.perf_counter()
    print(f" Real time to init DB: {f_after_addall - t_before_addall:.2f} seconds")
    print(f" Time per file entry: {(f_after_addall - t_before_addall) / len(hoard.fsobjects) * 1000:.2f} ms")

    return conn


def _load_fsobjects(conn: Connection):
    print("Testing load fs objects in bulk...")
    t_before = time.perf_counter()

    cursor = conn.execute(
        "SELECT fullpath, isdir, size, fasthash, group_concat(fspresence.uuid, '|'), group_concat(fspresence.state, '|') "
        "FROM fsobject LEFT OUTER JOIN fspresence on fsobject.fsobject_id = fspresence.fsobject_id "
        "GROUP BY fsobject.fsobject_id")
    _objects: Dict[str, DirProps | HoardFileProps] = dict()
    for data in cursor:
        fullpath, isdir, size, fasthash, group_uuid, group_fspresence = data
        # print(f"reading {fullpath}")
        # print(f"{fullpath} {isdir} {size} {fasthash} {group_uuid} {group_fspresence}")

        if not isdir:
            _objects[fullpath] = HoardFileProps({
                "isdir": isdir, "size": size, "fasthash": fasthash,
                "status": dict((uuid, state) for uuid, state in zip(group_uuid.split("|"), group_fspresence.split("|")))
            })
        else:
            _objects[fullpath] = DirProps({})

    print(f"Loaded {len(_objects)} file entries!")
    # _tree = HoardTree(_objects)
    t_after = time.perf_counter()

    print(f" Real time to init DB: {t_after - t_before:.2f} seconds")
    print(f" Time per file entry: {(t_after - t_before) / len(_objects) * 1000:.2f} ms")

    return _objects, None  # _tree


def _load_fsobjects_individually(conn: Connection):
    print("Testing load fs objects individually...")

    query_plan = conn.execute(
        "EXPLAIN QUERY PLAN "
        "SELECT fullpath, isdir, size, fasthash, group_concat(fspresence.uuid, '|'), group_concat(fspresence.state, '|') "
        "FROM fsobject LEFT OUTER JOIN fspresence on fsobject.fsobject_id = fspresence.fsobject_id "
        "WHERE fsobject.fullpath = ? "
        "GROUP BY fsobject.fsobject_id",
        ("something",)).fetchall()
    print("QUERY PLAN:")
    print("\n".join(["\t".join(str(sv) for sv in query_step) for query_step in query_plan]))
    print("END QUERY PLAN:")

    t_before = time.perf_counter()

    _objects: Dict[str, DirProps | HoardFileProps] = dict()
    with alive_bar(0) as bar:
        for id_ in conn.execute("SELECT fullpath FROM fsobject"):
            bar()
            fullpath = id_[0]
            # print(f"Loading {fullpath}")

            cursor = conn.execute(
                "SELECT fullpath, isdir, size, fasthash, group_concat(fspresence.uuid, '|'), group_concat(fspresence.state, '|') "
                "FROM fsobject LEFT OUTER JOIN fspresence on fsobject.fsobject_id = fspresence.fsobject_id "
                "WHERE fsobject.fullpath = ? "
                "GROUP BY fsobject.fsobject_id",
                (fullpath,))
            data = cursor.fetchone()

            fullpath, isdir, size, fasthash, group_uuid, group_fspresence = data
            # print(f"reading {fullpath}")
            # print(f"{fullpath} {isdir} {size} {fasthash} {group_uuid} {group_fspresence}")

            if not isdir:
                _objects[fullpath] = HoardFileProps({
                    "isdir": isdir, "size": size, "fasthash": fasthash,
                    "status": dict(
                        (uuid, state) for uuid, state in zip(group_uuid.split("|"), group_fspresence.split("|")))
                })
            else:
                _objects[fullpath] = DirProps({})

    print(f"Loaded {len(_objects)} file entries!")
    # _tree = HoardTree(_objects)
    t_after = time.perf_counter()

    print(f" Real time to init DB: {t_after - t_before:.2f} seconds")
    print(f" Time per file entry: {(t_after - t_before) / len(_objects) * 1000:.2f} ms")

    return _objects, None  # _tree


@unittest.skip("Do not run performance testing part of all tests")
class TestPerfTomlVsSql(unittest.TestCase):

    def setUp(self):
        self.path = "tests/largehoard/"

    def test_using_toml(self):
        t_before = time.perf_counter()
        with HoardContents.load(join(self.path, "hoard.contents"), write_on_close=False) as hoard:
            t_after = time.perf_counter()
            print(f" Real time to load TOML: {t_after - t_before:.2f} seconds")
            print(f" Time per file entry: {(t_after - t_before) / len(hoard.fsobjects) * 1000:.2f} ms")

            print("Test with in-memory db...")
            conn = sqlite3.connect(":memory:")
            #  conn.execute('pragma journal_mode=wal')
            conn = _test_perf_create_and_lookup(conn, hoard)

            _objects, _ = _load_fsobjects(conn)
            self.assertEqual(len(_objects), len(hoard.fsobjects))

            _objects, _ = _load_fsobjects_individually(conn)
            self.assertEqual(len(_objects), len(hoard.fsobjects))

            conn.close()

            print("Test with on-disk db...")
            if os.path.exists(join(self.path, "dummy.db")):
                os.remove(join(self.path, "dummy.db"))
            conn = sqlite3.connect(join(self.path, "dummy.db"))
            #  conn.execute('pragma journal_mode=wal')
            conn = _test_perf_create_and_lookup(conn, hoard)
            conn.close()

            conn = sqlite3.connect(join(self.path, "dummy.db"))
            #  conn.execute('pragma journal_mode=wal')
            _objects, _ = _load_fsobjects(conn)
            assert len(_objects) == len(hoard.fsobjects)
            conn.close()

            conn = sqlite3.connect(join(self.path, "dummy.db"))
            #  conn.execute('pragma journal_mode=wal')
            _objects, _ = _load_fsobjects_individually(conn)
            self.assertEqual(len(_objects), len(hoard.fsobjects))
            conn.close()
