import hashlib
from abc import abstractmethod
from typing import Dict, Iterable, Tuple, List

from msgspec import msgpack

from contents.hashable_key import HashableKey
from lmdb_storage.file_object import FileObject
from lmdb_storage.operations.util import remap
from lmdb_storage.tree_object import ObjectID, StoredObject, MaybeObjectID, ObjectType, TreeObject


class ObjectReader:
    @abstractmethod
    def read(self, object_id: ObjectID) -> StoredObject:
        pass

    def maybe_read(self, object_id: MaybeObjectID) -> StoredObject | None:
        return self.read(object_id) if object_id else None


class CompositeNodeID(HashableKey):
    def __init__(
            self, hoard_obj_id: MaybeObjectID,
            current_roots: Dict[str, ObjectID], desired_roots: Dict[str, ObjectID]) -> None:
        self._hoard_obj_id = hoard_obj_id
        self._current_roots: Dict[str, ObjectID] = current_roots
        self._desired_roots: Dict[str, ObjectID] = desired_roots
        self._hashed: bytes | None = None

    @property
    def hashed(self) -> bytes:
        if self._hashed is None:
            packed = msgpack.encode((
                self._hoard_obj_id,
                sorted(self._current_roots.items()),
                sorted(self._desired_roots.items())))
            self._hashed = hashlib.md5(packed).digest()
        return self._hashed

    @property
    def roots(self) -> Iterable[Tuple[str, Tuple[MaybeObjectID, MaybeObjectID]]]:
        for uuid in set(list(self._current_roots.keys()) + list(self._desired_roots.keys())):
            current_id = self._current_roots.get(uuid, None)
            desired_id = self._desired_roots.get(uuid, None)
            yield uuid, (current_id, desired_id)

    def __hash__(self) -> int:
        return hash(self.hashed)

    def __eq__(self, other) -> bool:
        return isinstance(other, CompositeNodeID) and self.hashed == other.hashed


def get_existing_children(rts: Dict[str, StoredObject], child_name: str) -> dict[str, bytes]:
    current_roots: Dict[str, ObjectID] = {}
    for uuid, child_current in rts.items():
        current_child = get_child_if_exists(child_name, child_current)
        if current_child is not None:
            current_roots[uuid] = current_child
    return current_roots


class CompositeObject:
    @staticmethod
    def expand(node_id: CompositeNodeID, objects: ObjectReader) -> "CompositeObject":
        return CompositeObject(node_id, objects)

    def __init__(self, node_id: CompositeNodeID, objects: ObjectReader):
        self.node_id = node_id
        self._hoard_obj = objects.maybe_read(node_id._hoard_obj_id)
        self._current_roots = remap(node_id._current_roots, objects.read)
        self._desired_roots = remap(node_id._desired_roots, objects.read)

    def children(self) -> Iterable[Tuple[str, "CompositeNodeID"]]:
        children_names = set(
            child_names(self._hoard_obj)
            + sum((child_names(obj) for obj in self._current_roots.values()), [])
            + sum((child_names(obj) for obj in self._desired_roots.values()), []))

        for child_name in children_names:
            child_node = self.get_child(child_name)
            if child_node is not None:
                yield child_name, child_node

    def get_child(self, child_name: str) -> "CompositeNodeID":
        current_roots: Dict[str, ObjectID] = get_existing_children(self._current_roots, child_name)
        desired_roots: Dict[str, ObjectID] = get_existing_children(self._desired_roots, child_name)

        return CompositeNodeID(get_child_if_exists(child_name, self._hoard_obj), current_roots, desired_roots)

    def __hash__(self) -> int:
        return hash(self.node_id.hashed)

    def __eq__(self, other) -> bool:
        return isinstance(other, CompositeObject) and self.node_id == other.node_id

    def is_any_tree(self):
        if self._hoard_obj is not None:
            return isinstance(self._hoard_obj, TreeObject)

        for obj in self._current_roots.values():
            if isinstance(obj, TreeObject):
                return True

        for obj in self._desired_roots.values():
            if isinstance(obj, TreeObject):
                return True
        return False


def get_child_if_exists(child_name: str, hoard_obj: StoredObject | None) -> MaybeObjectID:
    if hoard_obj and hoard_obj.object_type == ObjectType.TREE:
        hoard_obj: TreeObject
        return hoard_obj.get(child_name)
    return None


def child_names(obj: StoredObject) -> List[str]:
    return list(n for n, _ in obj.children) if obj and obj.object_type == ObjectType.TREE else []
