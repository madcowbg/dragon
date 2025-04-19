import logging

import lmdb
from alive_progress import alive_bar

from lmdb_storage.tree_structure import load_tree_or_file, TreeObject, FileObject


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
                    live_obj = load_tree_or_file(current_id, txn)
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

    def objects(self, write: bool):
        return Objects(self, write)

    def repos_txn(self, write: bool):
        return self.env.begin(db=self.env.open_db("repos".encode()), write=write)


class Objects:
    def __init__(self, storage: ObjectStorage, write: bool):
        self.txn = storage.objects_txn(write=write)

    def __enter__(self):
        self.txn.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.txn.__exit__(exc_type, exc_val, exc_tb)
        return None
