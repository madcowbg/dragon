import abc
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
        return msgpack.packb((ObjectType.TREE.value, list(sorted(self.children.items()))))

    @staticmethod
    def load(data: bytes) -> "TreeObject":
        object_type, children_list = msgpack.unpackb(data)
        assert object_type == ObjectType.TREE.value
        return TreeObject(children=dict(children_list))

    @property
    def id(self) -> bytes:
        return hashlib.sha1(self.serialized).digest()


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
    @abc.abstractmethod
    def __enter__(self) -> "Objects[F]":
        pass

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abc.abstractmethod
    def __contains__(self, obj_id: bytes) -> bool:
        pass

    @abc.abstractmethod
    def __getitem__(self, obj_id: bytes) -> Union[F, TreeObject, None]:
        pass

    @abc.abstractmethod
    def __setitem__(self, obj_id: bytes, obj: Union[F, TreeObject]):
        pass

    @abc.abstractmethod
    def __delitem__(self, obj_id: bytes) -> None:
        pass

    def mktree_from_tuples(self, all_data: Iterable[Tuple[str, F]], alive_it=do_nothing) -> bytes:
        all_data = sorted(all_data, key=lambda t: t[0])

        # every element is a partially-constructed object
        # (name, partial TreeObject)

        stack: List[Tuple[ObjPath, TreeObject]] = [([], TreeObject(dict()))]
        for fullpath, file in alive_it(all_data, title="adding all data..."):
            assert fullpath == "" or fullpath[0] == "/", f"[{fullpath}] is not absolute path!"
            fullpath = fullpath.split("/")[1:]

            pop_and_write_nonparents(self, stack, fullpath)

            top_obj_path, children = stack[-1]

            assert ASSERTS_DISABLED or is_child_of(fullpath, top_obj_path)
            file_name = fullpath[-1]

            # add needed subfolders to stack
            current_path = top_obj_path
            rel_path = fullpath[len(current_path):-1]
            for path_elem in rel_path:
                current_path = current_path + [path_elem]
                stack.append((current_path, TreeObject(dict())))

            # add file to current's children
            self[file.file_id] = file

            top_obj_path, tree_obj = stack[-1]
            assert ASSERTS_DISABLED or is_child_of(fullpath, top_obj_path) and len(top_obj_path) + 1 == len(fullpath)
            tree_obj.children[file_name] = file.file_id

        pop_and_write_nonparents(self, stack, [])  # commits the stack
        assert len(stack) == 1

        obj_id, _ = pop_and_write_obj(stack, self)
        return obj_id


class StoredObjects[F](Objects[F]):
    def __init__(self, storage: "ObjectStorage", write: bool, object_builder: Callable[[ObjectID, any], F]):
        self.storage = storage
        self.write = write
        self.object_builder = object_builder

    def __enter__(self):
        self.txn = self.storage.begin(db_name="objects", write=self.write)
        self.txn.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.txn.__exit__(exc_type, exc_val, exc_tb)
        self.txn = None
        return None

    def __contains__(self, obj_id: bytes) -> bool:
        assert type(obj_id) is bytes, type(obj_id)
        return self.txn.get(obj_id) is not None

    def __getitem__(self, obj_id: bytes) -> Union[F, TreeObject, None]:
        assert type(obj_id) is bytes, f"{obj_id} -> {type(obj_id)}"
        obj_packed = self.txn.get(obj_id)  # todo use streaming op
        if obj_packed is None:
            return None

        obj_data = msgpack.loads(obj_packed)  # fixme make this faster by extracting type away
        if obj_data[0] == ObjectType.BLOB.value:
            return self.object_builder(obj_id, obj_data[1])
        elif obj_data[0] == ObjectType.TREE.value:
            return TreeObject(dict(obj_data[1]))
        else:
            raise ValueError(f"Unrecognized type {obj_data[0]}")

    def __setitem__(self, obj_id: bytes, obj: Union[F, TreeObject]):
        if self[obj_id] is None:
            self.txn.put(obj_id, obj.serialized)

    def __delitem__(self, obj_id: bytes) -> None:
        self.txn.delete(obj_id)


type ObjPath = List[str]
ASSERTS_DISABLED = True


def pop_and_write_nonparents(objects: Objects, stack: List[Tuple[ObjPath, TreeObject]], fullpath: ObjPath):
    while not is_child_of(fullpath, stack[-1][0]):  # this is not a common ancestor
        child_id, child_path = pop_and_write_obj(stack, objects)

        # add to parent
        _, parent_obj = stack[-1]
        child_name = child_path[-1]
        parent_obj.children[child_name] = child_id


def is_child_of(fullpath: ObjPath, parent: ObjPath) -> bool:
    if len(fullpath) < len(parent):
        return False

    for i in range(len(parent)):
        if fullpath[i] != parent[i]:
            return False
    return True


def pop_and_write_obj(stack: List[Tuple[ObjPath, TreeObject]], objects: Objects):
    top_obj_path, tree_obj = stack.pop()

    # store currently constructed object in tree
    obj_id = tree_obj.id
    objects[obj_id] = tree_obj

    return obj_id, top_obj_path


def add_file_object[F](objects: Objects[F], tree_id: ObjectID | None, filepath: ObjPath, file: F) -> ObjectID:
    objects[file.file_id] = file
    return add_object(objects, tree_id, filepath, file.file_id)


def add_object[F](objects: Objects[F], tree_id: ObjectID | None, path: ObjPath, obj_id: ObjectID) -> ObjectID:
    if len(path) == 0:  # is here
        return obj_id

    tree_obj = objects[tree_id] if tree_id is not None else TreeObject(dict())
    assert isinstance(tree_obj, TreeObject)

    sub_name = path[0]
    assert sub_name != ''
    tree_obj.children[sub_name] = add_object(objects, tree_obj.children.get(sub_name, None), path[1:], obj_id)

    new_tree_id = tree_obj.id
    if new_tree_id != tree_id:
        objects[new_tree_id] = tree_obj

    return new_tree_id


def remove_file_object[F](objects: Objects[F], tree_id: ObjectID, filepath: ObjPath) -> ObjectID:
    assert len(filepath) > 0

    sub_name = filepath[0]
    tree_obj = objects[tree_id]
    if len(filepath) == 1:
        tree_obj.children.pop(sub_name, None)
    elif sub_name not in tree_obj.children:  # do nothing for empty folders
        pass
    else:
        tree_obj.children[sub_name] = remove_file_object(objects, tree_obj.children.get(sub_name, None), filepath[1:])

    new_tree_id = tree_obj.id
    if new_tree_id != tree_id:
        objects[new_tree_id] = tree_obj
    return new_tree_id
