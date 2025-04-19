import logging

import lmdb
from alive_progress import alive_bar

from lmdb_storage.tree_structure import TreeObject, FileObject, Objects


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

        with self.objects(write=True) as objects:
            with alive_bar(title="iterating live objects") as bar:
                while len(q) > 0:
                    current_id = q.pop()

                    bar()
                    live_obj = objects[current_id]
                    if isinstance(live_obj, TreeObject):
                        for child_id in live_obj.children.values():
                            if child_id not in live_ids:
                                live_ids.add(child_id)
                                q.append(child_id)
                    else:
                        assert isinstance(live_obj, FileObject)
            with alive_bar(title="deleting objects") as bar:
                for obj_id, _ in objects.txn.cursor():
                    if obj_id not in live_ids:
                        del objects[obj_id]
                        bar()

    def objects_txn(self, write: bool):
        return self.env.begin(db=self.env.open_db("objects".encode()), write=write)

    def objects(self, write: bool) -> "Objects":
        return Objects(self, write)

    def repos_txn(self, write: bool):
        return self.env.begin(db=self.env.open_db("repos".encode()), write=write)
