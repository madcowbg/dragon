import abc
from typing import Iterable, Tuple, List, Callable, Union

from lmdb import Transaction

from lmdb_storage.object_serialization import construct_tree_object
from lmdb_storage.tree_object import StoredObject, TreeObject, ObjectType, TreeObjectBuilder, ObjectID


def do_nothing[T](x: T, *, title) -> T: return x


class Objects:
    txn: Transaction

    @abc.abstractmethod
    def __enter__(self) -> "Objects":
        pass

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abc.abstractmethod
    def __contains__(self, obj_id: bytes) -> bool:
        pass

    @abc.abstractmethod
    def __getitem__(self, obj_id: bytes) -> StoredObject | None:
        pass

    @abc.abstractmethod
    def __setitem__(self, obj_id: bytes, obj: StoredObject):
        pass

    @abc.abstractmethod
    def __delitem__(self, obj_id: bytes) -> None:
        pass

    def mktree_from_tuples(self, all_data: Iterable[Tuple[str, StoredObject]], alive_it=do_nothing) -> bytes:
        all_data = sorted(all_data, key=lambda t: t[0])

        # every element is a partially-constructed object
        # (name, partial TreeObject)

        stack: List[Tuple[ObjPath, TreeObjectBuilder]] = [([], dict())]
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
                stack.append((current_path, dict()))

            # add file to current's children
            self[file.id] = file

            top_obj_path, tree_obj_builder = stack[-1]
            assert ASSERTS_DISABLED or is_child_of(fullpath, top_obj_path) and len(top_obj_path) + 1 == len(fullpath)
            tree_obj_builder[file_name] = file.id

        pop_and_write_nonparents(self, stack, [])  # commits the stack
        assert len(stack) == 1

        obj_id, _ = pop_and_write_obj(stack, self)
        return obj_id


class TransactionCreator:
    @abc.abstractmethod
    def begin(self, db_name: str, write: bool) -> Transaction: pass


class StoredObjects(Objects):
    def __init__(
            self, storage: TransactionCreator, db_name: str, write: bool,
            object_reader: Callable[[ObjectID, bytes], StoredObject],
            object_writer: Callable[[StoredObject], bytes]):
        self._storage = storage
        self.db_name = db_name
        self.write = write
        self._object_reader = object_reader
        self._object_writer = object_writer

    def __enter__(self):
        self.txn = self._storage.begin(db_name=self.db_name, write=self.write)
        self.txn.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.txn.__exit__(exc_type, exc_val, exc_tb)
        self.txn = None
        return None

    def __contains__(self, obj_id: bytes) -> bool:
        assert type(obj_id) is bytes, type(obj_id)
        return self.txn.get(obj_id) is not None

    def __getitem__(self, obj_id: bytes) -> StoredObject | None:
        assert type(obj_id) is bytes, f"{obj_id} -> {type(obj_id)}"
        obj_packed = self.txn.get(obj_id)  # todo use streaming op
        if obj_packed is None:
            return None
        return self._object_reader(obj_id, obj_packed)

    def __setitem__(self, obj_id: bytes, obj: StoredObject):
        if self[obj_id] is None:
            self.txn.put(obj_id, self._object_writer(obj))

    def __delitem__(self, obj_id: bytes) -> None:
        self.txn.delete(obj_id)


type ObjPath = List[str]
ASSERTS_DISABLED = True


def pop_and_write_nonparents(objects: Objects, stack: List[Tuple[ObjPath, TreeObjectBuilder]], fullpath: ObjPath):
    while not is_child_of(fullpath, stack[-1][0]):  # this is not a common ancestor
        child_id, child_path = pop_and_write_obj(stack, objects)

        # add to parent
        _, parent_obj = stack[-1]
        child_name = child_path[-1]
        parent_obj[child_name] = child_id


def is_child_of(fullpath: ObjPath, parent: ObjPath) -> bool:
    if len(fullpath) < len(parent):
        return False

    for i in range(len(parent)):
        if fullpath[i] != parent[i]:
            return False
    return True


def pop_and_write_obj(stack: List[Tuple[ObjPath, TreeObjectBuilder]], objects: Objects) -> Tuple[ObjectID, ObjPath]:
    top_obj_path, tree_obj_builder = stack.pop()

    # store currently constructed object in tree
    tree_obj = construct_tree_object(tree_obj_builder)
    obj_id = tree_obj.id
    objects[obj_id] = tree_obj

    return obj_id, top_obj_path


def add_file_object[O](objects: Objects, tree_id: ObjectID | None, filepath: ObjPath, file: O) -> ObjectID:
    objects[file.file_id] = file
    return add_object(objects, tree_id, filepath, file.file_id)


def add_object(objects: Objects, tree_id: ObjectID | None, path: ObjPath, obj_id: ObjectID) -> ObjectID | None:
    if len(path) == 0:  # is here
        return obj_id

    if tree_id is not None:
        current_tree_object: StoredObject = objects[tree_id]

        assert current_tree_object.object_type == ObjectType.TREE
        current_tree_object: TreeObject
        tree_data: TreeObjectBuilder = dict(current_tree_object.children)
    else:
        tree_data = dict()

    sub_name = path[0]
    assert sub_name != ''
    new_child_id = add_object(objects, tree_data.get(sub_name, None), path[1:], obj_id)

    if new_child_id is None:
        if sub_name in tree_data:
            del tree_data[sub_name]
    else:
        tree_data[sub_name] = new_child_id

    if len(tree_data) == 0:
        return None

    tree_obj = construct_tree_object(tree_data)
    new_tree_id = tree_obj.id
    if new_tree_id != tree_id:
        objects[new_tree_id] = tree_obj

    return new_tree_id


def remove_file_object(objects: Objects, tree_id: ObjectID, filepath: ObjPath) -> ObjectID:
    assert len(filepath) > 0

    sub_name = filepath[0]
    tree_obj_builder: TreeObjectBuilder = dict(objects[tree_id].children)
    if len(filepath) == 1:
        tree_obj_builder.pop(sub_name, None)
    elif sub_name not in tree_obj_builder:  # do nothing for empty folders
        pass
    else:
        tree_obj_builder[sub_name] = remove_file_object(objects, tree_obj_builder.get(sub_name, None), filepath[1:])

    new_tree = construct_tree_object(tree_obj_builder)
    if new_tree.id != tree_id:
        objects[new_tree.id] = new_tree
    return new_tree.id
