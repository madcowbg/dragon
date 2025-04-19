import hashlib
import logging
import unittest
from typing import List, Tuple, Iterable

import lmdb
import msgpack
from alive_progress import alive_it, alive_bar
from lmdb import Transaction

from contents.hoard_props import HoardFileStatus
from lmdb_storage.tree_diff import Diff, AreSame
from lmdb_storage.tree_structure import ObjectType, TreeObject, FileObject, ExpandableTreeObject
from sql_util import sqlite3_standard
from util import FIRST_VALUE


# @unittest.skip("Made to run only locally to benchmark")
def _list_uuids(conn) -> List[str]:
    curr = conn.cursor()
    curr.row_factory = FIRST_VALUE
    all_repos = list(curr.execute("SELECT uuid FROM fspresence GROUP BY uuid ORDER BY uuid"))
    return all_repos


class MyTestCase(unittest.TestCase):

    # @unittest.skip("Made to run only locally to benchmark")
    def test_create_lmdb(self):
        env = ObjectStorage("test/example.lmdb", map_size=(1 << 30))
        path = r"C:\Users\Bono\hoard\hoard.contents"
        is_readonly = True

        with sqlite3_standard(f"file:{path}{'?mode=ro' if is_readonly else ''}", uri=True) as conn:
            all_data = list(alive_it(
                conn.execute("SELECT fullpath, fasthash, size FROM fsobject ORDER BY fullpath"),
                title="loading from sqlite"))

            with env.objects_txn(write=True) as txn:
                root_id = add_all(all_data, txn)

            with env.repos_txn(write=True) as txn:
                txn.put("HEAD".encode(), root_id)

            all_repos = _list_uuids(conn)
            logging.info("# repos: {}".format(len(all_repos)))

            for uuid in all_repos:
                curr = conn.execute(
                    "SELECT fullpath, fasthash, size FROM fsobject "
                    "WHERE EXISTS ("
                    "  SELECT 1 FROM fspresence "
                    "  WHERE fsobject.fsobject_id == fspresence.fsobject_id AND uuid = ? AND status = ?)"
                    "ORDER BY fullpath",
                    (uuid, HoardFileStatus.AVAILABLE.value))
                uuid_data = list(alive_it(curr, title=f"Loading for uuid {uuid}"))

                with env.objects_txn(write=True) as txn:
                    uuid_root_id = add_all(uuid_data, txn)

                with env.repos_txn(write=True) as txn:
                    txn.put(uuid.encode(), uuid_root_id)

    # @unittest.skip("Made to run only locally to benchmark")
    def test_fully_load_lmdb(self):
        env = ObjectStorage("test/example.lmdb")  # , map_size=(1 << 30) // 4)

        with env.repos_txn(write=False) as txn:
            root_id = txn.get("HEAD".encode())

        with env.objects_txn(write=False) as txn:
            root = ExpandableTreeObject.create(root_id, txn)

            def all_files(tree: ExpandableTreeObject) -> Iterable[FileObject]:
                yield from tree.files.values()
                for subtree in tree.dirs.values():
                    yield from all_files(subtree)

            all_files = list(alive_it(all_files(root), title="loading from lmdb..."))
            logging.warning(f"# all_files: {len(all_files)}")

    # @unittest.skip("Made to run only locally to benchmark")
    def test_dump_lmdb(self):
        env = ObjectStorage("test/example.lmdb")  # , map_size=(1 << 30) // 4)
        with env.objects_txn(write=False) as txn:
            with txn.cursor() as curr:
                with open("test/dbdump.msgpack", "wb") as f:
                    # msgpack.dump(((k, v) for k, v in alive_it(curr, title="loading from lmdb...")), f)
                    msgpack.dump(list(((k, v) for k, v in curr)), f)

    def test_tree_compare(self):
        env = ObjectStorage("test/example.lmdb")
        # uuid = "f8f42230-2dc7-48f4-b1b7-5298a309e3fd"
        uuid = "726613d5-2b92-451e-b863-833a579456f5"

        with env.repos_txn(write=False) as txn:
            hoard_id = txn.get("HEAD".encode())
            repo_id = txn.get(uuid.encode())

        with env.objects_txn(write=False) as txn:
            root_diff = Diff.compute("root", hoard_id, repo_id)

            for diff in alive_it(root_diff.expand(txn)):
                if isinstance(diff, AreSame):
                    continue
                print(diff)

    def test_gc(self):
        objs = ObjectStorage("test/example.lmdb")
        objs.gc()


class ObjectStorage:
    def __init__(self, path: str, *, map_size: int | None = None, max_dbs=5):
        self.env = lmdb.open(path, max_dbs=max_dbs, map_size=map_size)

    def gc(self):
        live_ids = set()
        q = list()
        with self.repos_txn(write=False) as txn:
            for k, root_id in txn.cursor():
                q.append(root_id)
                live_ids.add(root_id)

        logging.info(f"found {len(q)} live top-level refs")

        with self.objects_txn(write=True) as txn:
            with alive_bar(title="iterating live objects") as bar:
                while len(q) > 0:
                    current_id = q.pop()

                    bar()
                    live_obj = self[current_id, txn]
                    if isinstance(live_obj, TreeObject):
                        for child_id in live_obj.children.values():
                            if child_id not in live_ids:
                                live_ids.add(child_id)
                                q.append(child_id)
                    else:
                        assert isinstance(live_obj, FileObject)
            with alive_bar(title="deleting objects") as bar:
                for obj_id, _ in txn.cursor():
                    if obj_id not in live_ids:
                        txn.delete(obj_id)
                        bar()

    def objects_txn(self, write: bool):
        return self.env.begin(db=self.env.open_db("objects".encode()), write=write)

    def repos_txn(self, write: bool):
        return self.env.begin(db=self.env.open_db("repos".encode()), write=write)


def add_all(all_data: Tuple[str, str, int], txn: Transaction) -> bytes:
    # every element is a partially-constructed object
    # (name, partial TreeObject)
    stack: List[Tuple[str | None, TreeObject]] = [("", TreeObject(dict()))]
    for fullpath, fasthash, size in alive_it(all_data, title="adding all data..."):
        pop_and_write_nonparents(txn, stack, fullpath)

        top_obj_path, children = stack[-1]

        assert is_child_of(fullpath, top_obj_path)
        file_name = fullpath[fullpath.rfind("/") + 1:]

        # add needed subfolders to stack
        current_path = top_obj_path
        rel_path = fullpath[len(top_obj_path) + 1:-len(file_name)].split("/")
        for path_elem in rel_path[:-1]:
            current_path += "/" + path_elem
            stack.append((current_path, TreeObject(dict())))

        # add file to current's children
        file = FileObject.create(fasthash, size)
        txn.put(file.file_id, file.serialized)

        top_obj_path, tree_obj = stack[-1]
        assert is_child_of(fullpath, top_obj_path) and fullpath[len(top_obj_path) + 1:].find("/") == -1
        tree_obj.children[file_name] = file.file_id

    pop_and_write_nonparents(txn, stack, "/")  # commits the stack
    assert len(stack) == 1

    obj_id, _ = pop_and_write_obj(stack, txn)
    return obj_id


def pop_and_write_nonparents(txn, stack: List[Tuple[str | None, TreeObject]], fullpath: str):
    while not is_child_of(fullpath, stack[-1][0]):  # this is not a common ancestor
        child_id, child_path = pop_and_write_obj(stack, txn)

        # add to parent
        _, parent_obj = stack[-1]
        child_name = child_path[child_path.rfind("/") + 1:]
        parent_obj.children[child_name] = child_id


def is_child_of(fullpath, parent) -> bool:
    return fullpath.startswith(parent) and fullpath[len(parent)] == "/"


def pop_and_write_obj(stack: List[Tuple[str | None, TreeObject]], txn):
    top_obj_path, tree_obj = stack.pop()

    # store currently constructed object in tree
    obj_packed = tree_obj.serialized
    obj_id = hashlib.sha1(obj_packed).digest()
    txn.put(obj_id, obj_packed)

    return obj_id, top_obj_path


if __name__ == '__main__':
    unittest.main()
