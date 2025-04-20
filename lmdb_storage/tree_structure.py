import dataclasses
import enum
import hashlib
from typing import Dict, Iterable, Tuple, List

import msgpack
from lmdb import Transaction
from propcache import cached_property

## LMDB object format
#  object_id    blob
#
# blob is file:
#  Type.FILE, (fasthash, size)
#
# blob is tree:
#  Type.TREE, Dict[obj name to object_id]

type ObjectID = bytes


class ObjectType(enum.Enum):
    TREE = 1
    FILE = 2


@dataclasses.dataclass
class TreeObject:
    children: Dict[str, ObjectID]

    @cached_property
    def serialized(self) -> bytes:
        return msgpack.packb((ObjectType.TREE.value, self.children))

    @staticmethod
    def load(data: bytes) -> "TreeObject":
        object_type, children = msgpack.unpackb(data)
        assert object_type == ObjectType.TREE.value
        return TreeObject(children=children)


@dataclasses.dataclass
class FileObject:
    file_id: bytes
    fasthash: str
    size: int

    @cached_property
    def serialized(self) -> bytes:
        return msgpack.packb((ObjectType.FILE.value, self.fasthash, self.size))

    @staticmethod
    def create(fasthash: str, size: int) -> "FileObject":
        file_packed = msgpack.packb((ObjectType.FILE.value, fasthash, size))
        file_id = hashlib.sha1(file_packed).digest()
        return FileObject(file_id=file_id, fasthash=fasthash, size=size)

    @staticmethod
    def load(file_id: bytes, data: bytes) -> "FileObject":
        object_type, fasthash, size = msgpack.unpackb(data)
        assert object_type == ObjectType.FILE.value
        return FileObject(file_id, fasthash, size)


class ExpandableTreeObject:
    def __init__(self, data: TreeObject, objects: "Objects"):
        self.objects = objects
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
            obj = self.objects[obj_id]
            if isinstance(obj, FileObject):
                self._files[name] = obj
            elif isinstance(obj, TreeObject):
                self._dirs[name] = ExpandableTreeObject(obj, self.objects)
            else:
                raise TypeError(f"Unexpected type {type(obj)}")

    @staticmethod
    def create(obj_id: bytes, objects: "Objects") -> "ExpandableTreeObject":
        return ExpandableTreeObject(objects[obj_id], objects)


def do_nothing[T](x: T, *, title) -> T: return x


class Objects:
    def __init__(self, storage: "ObjectStorage", write: bool):
        self.txn = storage.objects_txn(write=write)

    def __enter__(self):
        self.txn.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.txn.__exit__(exc_type, exc_val, exc_tb)
        return None

    def __getitem__(self, obj_id: bytes) -> FileObject | TreeObject:
        obj_packed = self.txn.get(obj_id)  # todo use streaming op
        obj_data = msgpack.loads(obj_packed)  # fixme make this faster by extracting type away
        if obj_data[0] == ObjectType.FILE.value:
            return FileObject(obj_id, obj_data[1], obj_data[2])
        elif obj_data[0] == ObjectType.TREE.value:
            return TreeObject(obj_data[1])
        else:
            raise ValueError(f"Unrecognized type {obj_data[0]}")

    def __setitem__(self, obj_id: bytes, obj: FileObject | TreeObject):
        self.txn.put(obj_id, obj.serialized)

    def __delitem__(self, obj_id: bytes) -> None:
        self.txn.delete(obj_id)

    def mktree_from_tuples(self, all_data: Iterable[Tuple[str, FileObject]], alive_it=do_nothing) -> bytes:
        # every element is a partially-constructed object
        # (name, partial TreeObject)
        stack: List[Tuple[str | None, TreeObject]] = [("", TreeObject(dict()))]
        for fullpath, file in alive_it(all_data, title="adding all data..."):
            pop_and_write_nonparents(self, stack, fullpath)

            top_obj_path, children = stack[-1]

            assert is_child_of(fullpath, top_obj_path)
            file_name = fullpath[fullpath.rfind("/") + 1:]

            # add needed subfolders to stack
            current_path = top_obj_path
            rel_path = fullpath[len(current_path) + 1:-len(file_name)].split("/")
            for path_elem in rel_path[:-1]:
                current_path += "/" + path_elem
                stack.append((current_path, TreeObject(dict())))

            # add file to current's children
            self[file.file_id] = file

            top_obj_path, tree_obj = stack[-1]
            assert is_child_of(fullpath, top_obj_path) and fullpath[len(top_obj_path) + 1:].find("/") == -1
            tree_obj.children[file_name] = file.file_id

        pop_and_write_nonparents(self, stack, "/")  # commits the stack
        assert len(stack) == 1

        obj_id, _ = pop_and_write_obj(stack, self)
        return obj_id


def pop_and_write_nonparents(objects: Objects, stack: List[Tuple[str | None, TreeObject]], fullpath: str):
    while not is_child_of(fullpath, stack[-1][0]):  # this is not a common ancestor
        child_id, child_path = pop_and_write_obj(stack, objects)

        # add to parent
        _, parent_obj = stack[-1]
        child_name = child_path[child_path.rfind("/") + 1:]
        parent_obj.children[child_name] = child_id


def is_child_of(fullpath: str, parent: str) -> bool:
    return fullpath.startswith(parent) and fullpath[len(parent)] == "/"


def pop_and_write_obj(stack: List[Tuple[str | None, TreeObject]], objects: Objects):
    top_obj_path, tree_obj = stack.pop()

    # store currently constructed object in tree
    obj_packed = tree_obj.serialized
    obj_id = hashlib.sha1(obj_packed).digest()
    objects[obj_id] = tree_obj

    return obj_id, top_obj_path
