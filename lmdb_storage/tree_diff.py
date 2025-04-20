import abc
import dataclasses
import enum
from typing import Iterable, Callable, Tuple

from lmdb_storage.tree_structure import ObjectID, FileObject, Objects

type SkipFun = Callable[[], None]


class DiffType(enum.Enum):
    SAME = "same"
    DIFFERENT = "different"
    LEFT_MISSING = "left_missing"
    RIGHT_MISSING = "right_missing"


def zip_trees(
        objects: Objects, root_name: str,
        left_id: bytes, right_id: bytes) -> Iterable[Tuple[str, DiffType, ObjectID | None, ObjectID | None, SkipFun]]:
    assert left_id is not None
    assert right_id is not None

    yield from zip_dfs(objects, root_name, left_id, right_id)


CANT_SKIP = lambda: None


def zip_dfs(
        objects: Objects, path: str,
        left_id: bytes, right_id: bytes) -> Iterable[Tuple[str, DiffType, ObjectID | None, ObjectID | None, SkipFun]]:
    if left_id == right_id:
        yield path, DiffType.SAME, left_id, right_id, CANT_SKIP
        return

    left_obj = objects[left_id]
    right_obj = objects[right_id]

    if isinstance(left_obj, FileObject) or isinstance(right_obj, FileObject):
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
    for left_sub_name, left_file_id in left_obj.children.items():
        if left_sub_name in right_obj.children:
            yield from zip_dfs(
                objects,
                path=path + "/" + left_sub_name,
                left_id=left_file_id, right_id=right_obj.children[left_sub_name])
        else:
            yield path + "/" + left_sub_name, DiffType.RIGHT_MISSING, left_file_id, None, CANT_SKIP

    for right_sub_name, right_file_id in right_obj.children.items():
        if right_sub_name in left_obj.children:
            pass  # already returned
        else:
            yield path + "/" + right_sub_name, DiffType.RIGHT_MISSING, None, right_file_id, CANT_SKIP
