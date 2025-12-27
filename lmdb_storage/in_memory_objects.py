from typing import Dict, Union, Iterable, Tuple

from lmdb import Transaction

from lmdb_storage.file_object import BlobObject
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.tree_object import ObjectID, TreeObject
from lmdb_storage.tree_structure import Objects, do_nothing


class InMemoryObjectsExtension(Objects):
    def __init__(self, env: ObjectStorage) -> None:
        self.stored_objects = env.objects(write=False)
        self.in_mem: Dict[ObjectID, BlobObject | TreeObject] = dict()

    def __enter__(self) -> Objects:
        self.stored_objects.__enter__()
        return self

    @property
    def txn(self) -> Transaction:
        return self.stored_objects.txn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stored_objects.__exit__(exc_type, exc_val, exc_tb)
        return None

    def __contains__(self, obj_id: bytes) -> bool:
        return obj_id in self.in_mem or obj_id in self.stored_objects

    def __getitem__(self, obj_id: bytes) -> Union[BlobObject, TreeObject, None]:
        in_mem = self.in_mem.get(obj_id, None)
        return in_mem if in_mem is not None else self.stored_objects[obj_id]

    def __setitem__(self, obj_id: bytes, obj: Union[BlobObject, TreeObject]):
        stored = self.stored_objects[obj_id]
        if stored is None:
            self.in_mem[obj_id] = obj
        elif obj != stored:
            raise ValueError("Cannot change a stored object!")

    def __delitem__(self, obj_id: bytes) -> None:
        raise ValueError("Cannot delete InMemory objects")

