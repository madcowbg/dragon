import enum
from typing import Iterable, Callable, Tuple, List

from lmdb_storage.tree_structure import ObjectID, Objects
from lmdb_storage.tree_object import ObjectType, StoredObject, TreeObject

type SkipFun = Callable[[], None]


def dfs(
        objects: Objects, path: str,
        obj_id: bytes) -> Iterable[Tuple[str, ObjectType, ObjectID, StoredObject, SkipFun]]:
    if obj_id is None:
        return
    assert type(obj_id) is bytes

    obj = objects[obj_id]
    if obj is None:
        raise ValueError(f"{obj_id} is missing!")

    if not isinstance(obj, TreeObject):
        yield path, ObjectType.BLOB, obj_id, obj, CANT_SKIP
        return

    should_skip = False

    def skip_children() -> None:
        nonlocal should_skip
        should_skip = True

    yield path, ObjectType.TREE, obj_id, obj, skip_children
    if should_skip:
        return

    for child_name, child_id in obj.children:
        yield from dfs(objects, path + "/" + child_name, child_id)


class DiffType(enum.Enum):
    SAME = "same"
    DIFFERENT = "different"
    LEFT_MISSING = "left_missing"
    RIGHT_MISSING = "right_missing"


CANT_SKIP = lambda: None


def zip_dfs(
        objects: Objects, path: str,
        left_id: bytes | None, right_id: bytes | None,
        drilldown_same: bool = False) -> Iterable[Tuple[str, DiffType, ObjectID | None, ObjectID | None, SkipFun]]:
    for sub_path, sub_obj_ids, skip_children in zip_trees_dfs(objects, path, [left_id, right_id], drilldown_same):
        sub_left_id, sub_right_id = sub_obj_ids
        if sub_left_id is None:
            yield sub_path, DiffType.LEFT_MISSING, None, sub_right_id, skip_children
        elif sub_right_id is None:
            yield sub_path, DiffType.RIGHT_MISSING, sub_left_id, None, skip_children
        elif sub_left_id == sub_right_id:
            yield sub_path, DiffType.SAME, sub_left_id, sub_right_id, skip_children
        else:
            yield sub_path, DiffType.DIFFERENT, sub_left_id, sub_right_id, skip_children

type ObjectIDs = List[ObjectID | None]

def zip_trees_dfs(
        objects: Objects, path: str, obj_ids: ObjectIDs,
        drilldown_same: bool = True) -> Iterable[Tuple[str, ObjectIDs, SkipFun]]:

    if not any(obj_id is not None for obj_id in obj_ids):
        return  # nothing more to yield

    if len(set(obj_ids)) <= 1 and not drilldown_same:  # we got same value for all
        yield path, obj_ids, CANT_SKIP
        return

    all_objs = [objects[obj_id] if obj_id else None for obj_id in obj_ids]
    if any(isinstance(obj, TreeObject) for obj in all_objs):
        # has tree
        should_skip = False

        def skip_children() -> None:
            nonlocal should_skip
            should_skip = True

        yield path, obj_ids, skip_children

        if should_skip:
            return

        child_names = set(sum([[key for key, _ in obj.children] for obj in all_objs if isinstance(obj, TreeObject)], []))

        for child_name in sorted(child_names):
            yield from zip_trees_dfs(
                objects, path + "/" + child_name,
                [obj.get(child_name) if isinstance(obj, TreeObject) else None for obj in all_objs],
                drilldown_same)
    else:
        # only one or more filesfiles
        yield path, obj_ids, CANT_SKIP
