import dataclasses
import enum
import hashlib
import logging
import unittest
from typing import List, Tuple, Dict

import lmdb
import msgpack
from alive_progress import alive_it, alive_bar
from lmdb import Transaction

from contents.hoard_props import HoardFileStatus
from sql_util import sqlite3_standard
from util import FIRST_VALUE


class ObjectType(enum.Enum):
    TREE = 1
    FILE = 2


## LMDB format
#  object_id    blob
#
# blob is file:
#  Type.FILE, (fasthash, size)
#
# blob is tree:
#  Type.TREE, Dict[obj name to object_id]

# @unittest.skip("Made to run only locally to benchmark")
class MyTestCase(unittest.TestCase):

    # @unittest.skip("Made to run only locally to benchmark")
    def test_create_lmdb(self):
        env = lmdb.open("test/example.lmdb", map_size=(1 << 30), max_dbs=5)

        with env.begin(write=True) as txn:
            txn.put("hi, my name is".encode(), "tikitikitiki".encode())

        path = r"C:\Users\Bono\hoard\hoard.contents"
        is_readonly = True

        with sqlite3_standard(f"file:{path}{'?mode=ro' if is_readonly else ''}", uri=True) as conn:
            all_data = list(alive_it(
                conn.execute("SELECT fullpath, fasthash, size FROM fsobject ORDER BY fullpath"),
                title="loading from sqlite"))

            with env.begin(db=env.open_db("objects".encode()), write=True) as txn:
                root_id = add_all(all_data, txn)

            with env.begin(db=env.open_db("repos".encode()), write=True) as txn:
                txn.put("HEAD".encode(), root_id)

            all_repos = self._list_uuids(conn)
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
                with env.begin(db=env.open_db("objects".encode()), write=True) as txn:
                    uuid_root_id = add_all(uuid_data, txn)

                with env.begin(db=env.open_db("repos".encode()), write=True) as txn:
                    txn.put(uuid.encode(), uuid_root_id)

            # for fullpath, fasthash, size in all_data:
            #     path = fullpath.split("/")
            #
            #     root_id = up_to_date_object(txn, root_id, path, (fasthash, size))
            #
            #     txn.put("HEAD".encode(), root_id)

    def _list_uuids(self, conn) -> List[str]:
        curr = conn.cursor()
        curr.row_factory = FIRST_VALUE
        all_repos = list(curr.execute("SELECT uuid FROM fspresence GROUP BY uuid ORDER BY uuid"))
        return all_repos

    def test_fully_load_lmdb(self):
        env = lmdb.open("test/example.lmdb", max_dbs=5)  # , map_size=(1 << 30) // 4)

        with alive_bar(title="loading from lmdb...") as bar:
            with env.begin(db=env.open_db("repos".encode()), write=False) as txn:
                root_id = txn.get("HEAD".encode())

            with env.begin(db=env.open_db("objects".encode()), write=False) as txn:
                root = load_recursive(txn, root_id, bar)

    # @unittest.skip("Made to run only locally to benchmark")
    def test_dump_lmdb(self):
        env = lmdb.open("test/example.lmdb", max_dbs=5)  # , map_size=(1 << 30) // 4)
        with env.begin(db=env.open_db("objects".encode()), write=False) as txn:
            with txn.cursor() as curr:
                with open("test/dbdump.msgpack", "wb") as f:
                    # msgpack.dump(((k, v) for k, v in alive_it(curr, title="loading from lmdb...")), f)
                    msgpack.dump(list(((k, v) for k, v in curr)), f)


def load_recursive(txn, obj_id: bytes, bar) -> Dict[str, any] | Tuple[any]:
    bar()
    obj_packed = txn.get(obj_id)
    obj_data = msgpack.loads(obj_packed)
    if obj_data[0] == ObjectType.FILE.value:
        return obj_data
    elif obj_data[0] == ObjectType.TREE.value:
        res = dict()
        for child_name, child_id in obj_data[1].items():
            res[child_name] = load_recursive(txn, child_id, bar)
        return res
    else:
        raise ValueError(f"Unrecognized type {obj_data[0]}")


def add_all(all_data, txn: Transaction) -> bytes:
    # every element is a partially-constructed object
    # (name, {sub: sub_id})
    stack: List[Tuple[str | None, Dict[str, bytes]]] = [("", dict())]
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
            stack.append((current_path, dict()))

        # add file to current's children
        file_data = (ObjectType.FILE.value, fasthash, size)
        file_packed = msgpack.packb(file_data)
        file_id = hashlib.sha1(file_packed).digest()

        txn.put(file_id, file_packed)

        top_obj_path, children = stack[-1]
        assert is_child_of(fullpath, top_obj_path) and fullpath[len(top_obj_path) + 1:].find("/") == -1
        children[file_name] = file_id

    pop_and_write_nonparents(txn, stack, "/")  # commits the stack
    assert len(stack) == 1

    obj_id, _ = pop_and_write_obj(stack, txn)
    return obj_id


def pop_and_write_nonparents(txn, stack, fullpath):
    while not is_child_of(fullpath, stack[-1][0]):  # this is not a common ancestor
        child_id, child_path = pop_and_write_obj(stack, txn)

        # add to parent
        _, parent_children = stack[-1]
        child_name = child_path[child_path.rfind("/") + 1:]
        parent_children[child_name] = child_id


def is_child_of(fullpath, parent) -> bool:
    return fullpath.startswith(parent) and fullpath[len(parent)] == "/"


def pop_and_write_obj(stack, txn):
    top_obj_path, children = stack.pop()

    # store currently constructed object in tree
    obj_data = (ObjectType.TREE.value, children)
    obj_packed = msgpack.packb(obj_data)
    obj_id = hashlib.sha1(obj_packed).digest()
    txn.put(obj_id, obj_packed)

    return obj_id, top_obj_path


def up_to_date_object(txn: Transaction, current_id: bytes | None, path: List[str], file: any) -> bytes:
    current_object_s = txn.get(current_id) if current_id is not None else None

    if len(path) == 0:  # is current file
        new_obj_s = msgpack.dumps((ObjectType.FILE.value, file))
        if new_obj_s == current_object_s:
            return current_id
        else:  # a new/modified file
            new_id = hashlib.md5(new_obj_s).digest()
            if not txn.get(new_id):
                txn.put(new_id, new_obj_s)
            return new_id
    else:
        current_object = msgpack.loads(current_object_s) if current_object_s is not None else (ObjectType.TREE.value, dict())
        current_child_name = path[0]

        current_child_id = current_object[1].get(current_child_name, None)

        new_child_id = up_to_date_object(txn, current_child_id, path[1:], file)
        current_object[1][current_child_name] = new_child_id

        # store the new object
        new_obj_data = msgpack.dumps(current_object)
        new_obj_id = hashlib.md5(new_obj_data).digest()

        if not txn.get(new_obj_id):
            txn.put(new_obj_id, new_obj_data)
        return new_obj_id


if __name__ == '__main__':
    unittest.main()
