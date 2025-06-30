import enum
import logging
import sys
from functools import cached_property
from typing import Dict, Iterable, Tuple, Union, List

type ObjectID = bytes
type MaybeObjectID = Union[ObjectID, None]

class ObjectType(enum.Enum):
    TREE = 1
    BLOB = 2


class StoredObject:
    object_type: ObjectType
    id: ObjectID


type TreeObjectBuilder = Dict[str, ObjectID]

class TreeObject(StoredObject):
    object_type: ObjectType = ObjectType.TREE

    def __init__(self, id: ObjectID, sorted_children_pairs: List[Tuple[str, ObjectID]]):
        self._id = id
        self._sorted_children = sorted_children_pairs
        self._children = dict(self._sorted_children)

    @property
    def id(self) -> bytes:
        return self._id

    @property
    def children(self) -> List[Tuple[str, ObjectID]]:
        return self._sorted_children

    def get(self, child_name: str) -> MaybeObjectID:
        return self._children.get(child_name)

    def __contains__(self, child_name: str) -> bool:
        return child_name in self._children

    def __eq__(self, other):
        return isinstance(other, TreeObject) and self._children == other._children

    def __hash__(self):
        # fixme it is not well defined, what about self._id?
        return sum(hash(name) + hash(child) for name, child in self.children)
