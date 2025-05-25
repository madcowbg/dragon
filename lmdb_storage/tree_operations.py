from typing import List

from lmdb_storage.file_object import BlobObject
from lmdb_storage.tree_structure import Objects, ObjectID, TreeObject, ObjectType


def get_child(objects: Objects, path: List[str], root_id: ObjectID | None) -> ObjectID | None:
    idx = 0

    curr_id = root_id
    while curr_id is not None and idx < len(path):
        obj = objects[curr_id]
        if obj.object_type == ObjectType.TREE:
            curr_id = obj.children.get(path[idx])
            idx += 1
        else:
            assert obj.object_type == ObjectType.BLOB
            return None

    return curr_id

def graft_in_tree(
        objects: Objects, old_root_id: ObjectID | None, path: List[str],
        donor_root_id: ObjectID | None) -> ObjectID | None:
    if len(path) == 0:  # we are here...
        return donor_root_id

    child_name = path[0]

    donor_child_obj = objects[donor_root_id] if donor_root_id is not None else None
    donor_child_id = donor_child_obj.children.get(child_name) if isinstance(donor_child_obj, TreeObject) else None

    old_obj = objects[old_root_id] if old_root_id is not None else None
    if old_obj and old_obj.object_type == ObjectType.TREE:
        old_child_id = old_obj.children.get(child_name)
    else:
        assert old_root_id is None or (old_obj and old_obj.object_type == ObjectType.BLOB)
        old_child_id = None

    new_child_id = graft_in_tree(objects, old_child_id, path[1:], donor_child_id)

    if old_obj is None:
        return package_existing_as_tree_object(objects, child_name, new_child_id)
    elif old_obj.object_type == ObjectType.TREE:
        # is a tree object, then graft the result and return
        if new_child_id is None:
            if child_name in old_obj.children:
                del old_obj.children[child_name]
        else:
            old_obj.children[child_name] = new_child_id

        if len(old_obj.children) == 0:
            # do not return empty folders
            return None

        objects[old_obj.id] = old_obj
        return old_obj.id
    else:
        assert old_obj.object_type == ObjectType.BLOB

        # was a file, we return new instead
        return package_existing_as_tree_object(objects, child_name, new_child_id)


def package_existing_as_tree_object(objects: Objects, child_name: str, new_child_id: ObjectID | None):
    if new_child_id is None:
        # was not there, is missing now - return it as missing
        return None
    else:
        # is here now, package into a new tree and then return
        new_obj = TreeObject({child_name: new_child_id})
        objects[new_obj.id] = new_obj
        return new_obj.id


def remove_child(objects: Objects, path: List[str], root_id: ObjectID | None) -> ObjectID | None:
    return graft_in_tree(objects, root_id, path, None)
