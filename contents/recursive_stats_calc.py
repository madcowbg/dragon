from typing import Iterable, Tuple

from lmdb_storage.tree_calculation import RecursiveReader, RecursiveCalculator, CachedCalculator
from lmdb_storage.tree_object import TreeObject, ObjectType, MaybeObjectID, StoredObject

type NodeID = Tuple[MaybeObjectID, MaybeObjectID]
type NodeObj = Tuple[StoredObject | None, StoredObject | None]


class UsedSize:
    def __init__(self, value: int):
        self.value = value


def get_used_size(obj: NodeObj) -> UsedSize:
    """ Returns the larger of the desired or the current size for that object. Assumes they are blobs"""
    assert not obj[0] or obj[0].object_type == ObjectType.BLOB
    assert not obj[1] or obj[1].object_type == ObjectType.BLOB
    return UsedSize(max(obj[0].size if obj[0] else 0, obj[1].size if obj[1] else 0))


class CurrentAndDesiredReader(RecursiveReader[NodeID, NodeObj]):
    def __init__(self, contents: "HoardContent"):
        self.contents = contents

    def convert(self, obj: NodeID) -> NodeObj:
        with self.contents.env.objects(write=False) as objects:
            return objects[obj[0]] if obj[0] else None, objects[obj[1]] if obj[1] else None

    def children(self, obj: NodeID) -> Iterable[Tuple[str, NodeID]]:
        left, right = self.convert(obj)

        if left is None:
            assert isinstance(right, TreeObject)
            yield from [(child_name, (None, right_child)) for child_name, right_child in right.children]
            return

        if left.object_type == ObjectType.BLOB:
            yield "$LEFT$", (obj[0], None)  # returns left blob

            assert isinstance(right, TreeObject)
            yield from [(child_name, (None, right_child)) for child_name, right_child in right.children]
            return

        assert left.object_type == ObjectType.TREE

        if right is None:
            assert isinstance(left, TreeObject)
            yield from [(child_name, (left_child, None)) for child_name, left_child in left.children]
            return

        if right.object_type == ObjectType.BLOB:
            yield "$RIGHT", (None, obj[1])  # returns right blob

            assert isinstance(left, TreeObject)
            yield from [(child_name, (left_child, None)) for child_name, left_child in left.children]
            return

        assert right.object_type == ObjectType.TREE
        left_map = dict(left.children)
        right_map = dict(right.children)
        all_children = sorted(set(list(left_map.keys()) + list(right_map.keys())))
        for child_name in all_children:
            yield child_name, (left_map.get(child_name), right_map.get(child_name))

    def is_compound(self, obj: NodeID) -> bool:
        left, right = self.convert(obj)

        return (left and left.object_type == ObjectType.TREE) \
            or (right and right.object_type == ObjectType.TREE)

    def is_atom(self, obj: NodeID) -> bool:
        return not self.is_compound(obj)


class UsedSizeCalculator(RecursiveCalculator[NodeID, NodeObj, UsedSize]):
    def aggregate(self, items: Iterable[Tuple[str, UsedSize]]) -> UsedSize:
        return UsedSize(sum(v.value for _, v in items))

    def for_none(self, calculator: "CachedCalculator[NodeObj, UsedSize]") -> UsedSize:
        return UsedSize(0)

    def __init__(self, contents: "HoardContent"):
        super().__init__(get_used_size, CurrentAndDesiredReader(contents))
