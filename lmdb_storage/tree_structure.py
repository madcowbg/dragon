import dataclasses
import enum
import hashlib
from typing import Dict, Iterable, Tuple, List, Callable, Union

import msgpack
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
    BLOB = 2


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


class ExpandableTreeObject[F]:
    def __init__(self, data: TreeObject, objects: "Objects[F]"):
        self.objects = objects
        self.children: Dict[str, ObjectID] = data.children

        self._files: Dict[str, F] | None = None
        self._dirs: Dict[str, ExpandableTreeObject] | None = None

    @property
    def files(self) -> Dict[str, F]:
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
            if isinstance(obj, TreeObject):
                self._dirs[name] = ExpandableTreeObject(obj, self.objects)
            else:
                self._files[name] = obj

    @staticmethod
    def create(obj_id: bytes, objects: "Objects[F]") -> "ExpandableTreeObject[F]":
        return ExpandableTreeObject[F](objects[obj_id], objects)


def do_nothing[T](x: T, *, title) -> T: return x


class Objects[F]:
    def __init__(self, storage: "ObjectStorage", write: bool, object_builder: Callable[[ObjectID, any], F]):
        self.txn = storage.objects_txn(write=write)
        self.object_builder = object_builder

    def __enter__(self):
        self.txn.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.txn.__exit__(exc_type, exc_val, exc_tb)
        return None

    def __getitem__(self, obj_id: bytes) -> Union[F, TreeObject]:
        obj_packed = self.txn.get(obj_id)  # todo use streaming op
        obj_data = msgpack.loads(obj_packed)  # fixme make this faster by extracting type away
        if obj_data[0] == ObjectType.BLOB.value:
            return self.object_builder(obj_id, obj_data[1])
        elif obj_data[0] == ObjectType.TREE.value:
            return TreeObject(obj_data[1])
        else:
            raise ValueError(f"Unrecognized type {obj_data[0]}")

    def __setitem__(self, obj_id: bytes, obj: Union[F, TreeObject]):
        self.txn.put(obj_id, obj.serialized)

    def __delitem__(self, obj_id: bytes) -> None:
        self.txn.delete(obj_id)

    def mktree_from_tuples(self, all_data: Iterable[Tuple[str, F]], alive_it=do_nothing) -> bytes:
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
