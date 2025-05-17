import logging
from typing import Collection

import lmdb
from alive_progress import alive_bar
from lmdb import Transaction

from lmdb_storage.roots import Roots
from lmdb_storage.tree_structure import TreeObject, Objects, ObjectID, StoredObjects
from lmdb_storage.file_object import FileObject


class InconsistentObjectStorage(BaseException):
    pass


class ObjectStorage:
    def __init__(self, path: str, *, map_size: int | None = None, max_dbs=5):
        self.env = lmdb.open(path, max_dbs=max_dbs, map_size=map_size, readonly=False)

    def gc(self):
        root_ids = self.roots(write=False).all_live
        logging.info(f"found {len(root_ids)} live top-level refs.")

        with self.objects(write=False) as objects:
            self.validate_storage(objects, root_ids)

            live_ids = find_all_live(objects, root_ids)

        logging.info(f"retaining {len(live_ids)} live objects.")
        with self.objects(write=True) as objects:
            with alive_bar(title="deleting objects") as bar:
                for obj_id, _ in objects.txn.cursor():
                    if obj_id not in live_ids:
                        del objects[obj_id]
                        bar()

            self.validate_storage(objects, root_ids)

    def validate_storage(self, objects, root_ids):
        for root_id in root_ids:
            if objects[root_id] is None:
                raise InconsistentObjectStorage(f"Missing root ID {root_id}: not in stored objects!")

    def copy_trees_from(self, other: "ObjectStorage", root_ids: Collection[ObjectID]):
        assert isinstance(root_ids, Collection)
        with other.objects(write=False) as other_objects:
            other_live_ids = find_all_live(other_objects, root_ids)

            with self.objects(write=True) as self_objects:
                for live_id in other_live_ids:
                    if live_id not in self_objects:
                        self_objects[live_id] = other_objects[live_id]

    def begin(self, db_name: str, write: bool) -> Transaction:
        return self.env.begin(db=self.env.open_db(db_name.encode()), write=write)

    def objects(self, write: bool) -> StoredObjects:
        return StoredObjects(self, write, FileObject)

    def roots(self, write: bool) -> Roots:
        return Roots(self, write)


def find_all_live[F](objects: Objects[F], root_ids: Collection[ObjectID]) -> Collection[ObjectID]:
    live_ids = set(root_ids)
    q = list(live_ids)
    with alive_bar(title="iterating live objects") as bar:
        while len(q) > 0:
            current_id = q.pop()

            bar()
            live_obj = objects[current_id]
            if isinstance(live_obj, TreeObject):
                # add all children to queue
                for child_id in live_obj.children.values():
                    if child_id not in live_ids:
                        live_ids.add(child_id)
                        q.append(child_id)
            else:
                # do nothing on files, just verify that they exist
                assert isinstance(live_obj, FileObject)
    return live_ids
