import enum
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

    def __init__(self, id: ObjectID, children: Dict[str, ObjectID]):
        self._id = id
        self._children = children

    @property
    def id(self) -> bytes:
        return self._id

    @property
    def children(self) -> Iterable[Tuple[str, ObjectID]]:
        return self._children.items()

    @cached_property
    def sorted_children_names(self) -> List[str]:
        return sorted(self._children.keys())

    def get(self, child_name: str) -> MaybeObjectID:
        return self._children.get(child_name)

    def __contains__(self, child_name: str) -> bool:
        return child_name in self._children

    def __eq__(self, other):
        return isinstance(other, TreeObject) and self.children == other.children

    def __hash__(self):
        return self.children.__hash__()
