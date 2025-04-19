import abc
import dataclasses
import enum
import hashlib
import logging
import unittest
from abc import abstractmethod
from typing import List, Tuple, Dict, Iterable

import lmdb
import msgpack
from alive_progress import alive_it, alive_bar
from lmdb import Transaction
from propcache import cached_property

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

type ObjectID = bytes


@dataclasses.dataclass
class TreeObject:
    tree_id: bytes
    children: Dict[str, ObjectID]

    @cached_property
    def serialized(self) -> bytes:
        return msgpack.packb((ObjectType.TREE.value, self.children))

    @staticmethod
    def load(obj_id: bytes, data: bytes) -> "TreeObject":
        object_type, children = msgpack.unpackb(data)
        assert object_type == ObjectType.TREE.value
        return TreeObject(obj_id, children=children)


@dataclasses.dataclass
class FileObject:
    file_id: bytes
    fasthash: str
    size: int

    @cached_property
    def serialized(self) -> bytes:
        return msgpack.packb((ObjectType.FILE.value, self.fasthash, self.size))

    @staticmethod
    def load(file_id: bytes, data: bytes) -> "FileObject":
        object_type, fasthash, size = msgpack.unpackb(data)
        assert object_type == ObjectType.FILE.value
        return FileObject(file_id, fasthash, size)


def load_tree_or_file(obj_id: bytes, txn: Transaction) -> FileObject | TreeObject:
    obj_packed = txn.get(obj_id)  # todo use streaming op
    obj_data = msgpack.loads(obj_packed)  # fixme make this faster by extracting type away
    if obj_data[0] == ObjectType.FILE.value:
        return FileObject.load(obj_id, obj_packed)
    elif obj_data[0] == ObjectType.TREE.value:
        return TreeObject.load(obj_id, obj_packed)
    else:
        raise ValueError(f"Unrecognized type {obj_data[0]}")


class ExpandableTreeObject:
    def __init__(self, data: TreeObject, txn: Transaction):
        self.txn = txn
        self.children: Dict[str, ObjectID] = data.children

        self._files: Dict[str, FileObject] | None = None
        self._dirs: Dict[str, ExpandableTreeObject] | None = None

    @property
    def files(self) -> Dict[str, FileObject]:
        if self._files is None:
            self._load()
        return self._files

    @property
    def dirs(self) -> Dict[str, "ExpandableTreeObject"]:
        if self._dirs is None:
            self._load()
        return self._dirs

    def _load(self):
        self._files = dict()
        self._dirs = dict()

        for name, obj_id in self.children.items():
            obj = load_tree_or_file(obj_id, self.txn)
            if isinstance(obj, FileObject):
                self._files[name] = obj
            elif isinstance(obj, TreeObject):
                self._dirs[name] = ExpandableTreeObject(obj, self.txn)
            else:
                raise TypeError(f"Unexpected type {type(obj)}")

    @staticmethod
    def create(obj_id: bytes, txn: Transaction) -> "ExpandableTreeObject":
        return ExpandableTreeObject(TreeObject.load(obj_id, txn.get(obj_id)), txn)


# @unittest.skip("Made to run only locally to benchmark")
class MyTestCase(unittest.TestCase):

    @unittest.skip("Made to run only locally to benchmark")
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

    def _list_uuids(self, conn) -> List[str]:
        curr = conn.cursor()
        curr.row_factory = FIRST_VALUE
        all_repos = list(curr.execute("SELECT uuid FROM fspresence GROUP BY uuid ORDER BY uuid"))
        return all_repos

    def test_fully_load_lmdb(self):
        env = lmdb.open("test/example.lmdb", max_dbs=5)  # , map_size=(1 << 30) // 4)

        with env.begin(db=env.open_db("repos".encode()), write=False) as txn:
            root_id = txn.get("HEAD".encode())

        with env.begin(db=env.open_db("objects".encode()), write=False) as txn:
            root = ExpandableTreeObject.create(root_id, txn)

            def all_files(tree: ExpandableTreeObject) -> Iterable[FileObject]:
                yield from tree.files.values()
                for subtree in tree.dirs.values():
                    yield from all_files(subtree)

            all_files = list(alive_it(all_files(root), title="loading from lmdb..."))
            logging.warning(f"# all_files: {len(all_files)}")

    # @unittest.skip("Made to run only locally to benchmark")
    def test_dump_lmdb(self):
        env = lmdb.open("test/example.lmdb", max_dbs=5)  # , map_size=(1 << 30) // 4)
        with env.begin(db=env.open_db("objects".encode()), write=False) as txn:
            with txn.cursor() as curr:
                with open("test/dbdump.msgpack", "wb") as f:
                    # msgpack.dump(((k, v) for k, v in alive_it(curr, title="loading from lmdb...")), f)
                    msgpack.dump(list(((k, v) for k, v in curr)), f)

    def test_tree_compare(self):
        env = lmdb.open("test/example.lmdb", max_dbs=5)
        # uuid = "f8f42230-2dc7-48f4-b1b7-5298a309e3fd"
        uuid = "726613d5-2b92-451e-b863-833a579456f5"

        with env.begin(db=env.open_db("repos".encode()), write=False) as txn:
            hoard_id = txn.get("HEAD".encode())
            repo_id = txn.get(uuid.encode())

        with env.begin(db=env.open_db("objects".encode()), write=False) as txn:
            root_diff = Diff.compute("root", hoard_id, repo_id, txn)

            for diff in alive_it(root_diff.expand(txn)):
                if isinstance(diff, AreSame):
                    continue
                print(diff)


class Diff:
    @staticmethod
    def compute(path: str, left_id: ObjectID, right_id: ObjectID, txn: Transaction) -> "Diff":
        if left_id == right_id:
            return AreSame(path, left_id)

        left_obj = load_tree_or_file(left_id, txn)
        right_obj = load_tree_or_file(right_id, txn)
        if type(left_obj) != type(right_obj):
            return DifferentTypes(path, left_id, right_id)
        else:
            if type(left_obj) == FileObject:
                assert type(right_obj) == FileObject
                return FilesAreDiff(path, left_id, right_id)
            else:
                assert type(left_obj) == TreeObject and type(right_obj) == TreeObject
                return FoldersAreDiff(
                    path,
                    left_id, ExpandableTreeObject.create(left_id, txn),
                    right_id, ExpandableTreeObject.create(right_id, txn))

    @abc.abstractmethod
    def expand(self, txn: Transaction) -> Iterable["Diff"]:
        pass

    def __str__(self):
        return f"{self.__class__.__name__}[{self.path}]"


@dataclasses.dataclass
class AreSame(Diff):
    path: str
    id: ObjectID

    def expand(self, txn: Transaction) -> Iterable["Diff"]:
        yield self


@dataclasses.dataclass
class FilesAreDiff(Diff):
    path: str
    left_id: ObjectID
    right_id: ObjectID

    def expand(self, txn: Transaction) -> Iterable["Diff"]:
        yield self


@dataclasses.dataclass
class DifferentTypes(Diff):
    path: str
    left_id: ObjectID
    right_id: ObjectID

    def expand(self, txn: Transaction) -> Iterable["Diff"]:
        yield self


@dataclasses.dataclass
class FoldersAreDiff(Diff):
    path: str
    left_id: ObjectID
    left_folder: ExpandableTreeObject
    right_id: ObjectID
    right_folder: ExpandableTreeObject

    def expand(self, txn: Transaction) -> Iterable["Diff"]:
        yield self

        for left_sub_name, left_file_id in self.left_folder.children.items():
            if left_sub_name in self.right_folder.children:
                yield from Diff.compute(
                    self.path + "/" + left_sub_name, left_file_id, self.right_folder.children[left_sub_name], txn) \
                    .expand(txn)
            else:
                yield LeftMissingInRight(self.path + "/" + left_sub_name, left_file_id)

        for right_sub_name, right_file_id in self.right_folder.children.items():
            if right_sub_name in self.left_folder.children:
                pass  # already returned
            else:
                yield RightMissingInLeft(self.path + "/" + right_sub_name, right_file_id)


@dataclasses.dataclass
class LeftMissingInRight(Diff):
    path: str
    left_obj: ObjectID

    def expand(self, txn: Transaction) -> Iterable["Diff"]:
        yield self


@dataclasses.dataclass
class RightMissingInLeft(Diff):
    path: str
    right_obj: ObjectID

    def expand(self, txn: Transaction) -> Iterable["Diff"]:
        yield self


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
        file_data = (ObjectType.FILE.value, fasthash, size)
        file_packed = msgpack.packb(file_data)
        file_id = hashlib.sha1(file_packed).digest()

        txn.put(file_id, file_packed)

        top_obj_path, tree_obj = stack[-1]
        assert is_child_of(fullpath, top_obj_path) and fullpath[len(top_obj_path) + 1:].find("/") == -1
        tree_obj.children[file_name] = file_id

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
