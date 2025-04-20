import enum
from typing import Iterable, Callable, Tuple

from lmdb_storage.tree_structure import ObjectID, Objects, TreeObject, ObjectType

type SkipFun = Callable[[], None]


def dfs[F](
        objects: Objects[F], path: str,
        obj_id: bytes) -> Iterable[Tuple[str, ObjectType, ObjectID, F | TreeObject, SkipFun]]:
    if obj_id is None:
        return
    assert type(obj_id) is bytes

    obj = objects[obj_id]
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

    for child_name, child_id in obj.children.items():
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
    if left_id is None:
        if right_id is None:
            return
        for sub_path, _, obj_id, obj, skip_children in dfs(objects, path, right_id):
            yield sub_path, DiffType.LEFT_MISSING, None, obj_id, skip_children

        return
    if right_id is None:
        for sub_path, _, obj_id, obj, skip_children in dfs(objects, path, left_id):
            yield sub_path, DiffType.RIGHT_MISSING, obj_id, None, skip_children

        return

    assert left_id is not None
    assert right_id is not None


    left_obj = objects[left_id]
    right_obj = objects[right_id]

    if left_id == right_id:
        if drilldown_same and isinstance(left_obj, TreeObject):
            for sub_path, _, obj_id, obj, skip_children in dfs(objects, path, left_id):
                yield sub_path, DiffType.SAME, obj_id, obj_id, skip_children
        else:
            yield path, DiffType.SAME, left_id, right_id, CANT_SKIP
        return

    if not (isinstance(left_obj, TreeObject) and isinstance(right_obj, TreeObject)):
        yield path, DiffType.DIFFERENT, left_id, right_id, CANT_SKIP
        return

    should_skip = False

    def skip_children() -> None:
        nonlocal should_skip
        should_skip = True

    yield path, DiffType.DIFFERENT, left_id, right_id, skip_children

    if should_skip:
        return

    # are both dirs, drilldown...
    for left_sub_name, left_sub_id in left_obj.children.items():
        if left_sub_name in right_obj.children:
            yield from zip_dfs(
                objects,
                path=path + "/" + left_sub_name,
                left_id=left_sub_id, right_id=right_obj.children[left_sub_name],
                drilldown_same=drilldown_same)
        else:
            yield from zip_dfs(objects, path + "/" + left_sub_name, left_sub_id, None, drilldown_same)

    for right_sub_name, right_sub_id in right_obj.children.items():
        if right_sub_name in left_obj.children:
            pass  # already returned
        else:
            yield from zip_dfs(objects, path + "/" + right_sub_name, None, right_sub_id, drilldown_same)
