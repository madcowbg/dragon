import hashlib
from abc import abstractmethod
from typing import Dict, Iterable, Tuple, List

from msgspec import msgpack

from contents.hashable_key import HashableKey
from lmdb_storage.operations.util import remap
from lmdb_storage.tree_object import ObjectID, StoredObject, MaybeObjectID, ObjectType, TreeObject


class ObjectReader:
    @abstractmethod
    def read(self, object_id: ObjectID) -> StoredObject:
        pass

    def maybe_read(self, object_id: MaybeObjectID) -> StoredObject | None:
        return self.read(object_id) if object_id else None


class CompositeNodeID(HashableKey):
    def __init__(self, hoard_obj_id: MaybeObjectID) -> None:
        self._hoard_obj_id = hoard_obj_id
        self._current_roots: Dict[str, ObjectID] = {}
        self._desired_roots: Dict[str, ObjectID] = {}
        self._hashed: bytes | None = None

    def set_root_current(self, uuid: str, node_id: MaybeObjectID) -> None:
        self._hashed = None
        if node_id is not None:
            self._current_roots[uuid] = node_id
        elif uuid in self._current_roots:
            del self._current_roots[uuid]

    def set_root_desired(self, uuid: str, node_id: MaybeObjectID) -> None:
        self._hashed = None
        if node_id is not None:
            self._desired_roots[uuid] = node_id
        elif uuid in self._desired_roots:
            del self._desired_roots[uuid]

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
        child_node = CompositeNodeID(get_child_if_exists(child_name, self._hoard_obj))

        for uuid, child_current in self._current_roots.items():
            current_child = get_child_if_exists(child_name, child_current)
            if current_child is not None:
                child_node.set_root_current(uuid, current_child)

        for uuid, child_desired in self._desired_roots.items():
            desired_child = get_child_if_exists(child_name, child_desired)
            if desired_child is not None:
                child_node.set_root_desired(uuid, desired_child)

        return child_node

    def __hash__(self) -> int:
        return hash(self.node_id.hashed)

    def __eq__(self, other) -> bool:
        return isinstance(other, CompositeObject) and self.node_id == other.node_id


def get_child_if_exists(child_name: str, hoard_obj: StoredObject | None) -> MaybeObjectID:
    if hoard_obj and hoard_obj.object_type == ObjectType.TREE:
        hoard_obj: TreeObject
        return hoard_obj.get(child_name)
    return None


def child_names(obj: StoredObject) -> List[str]:
    return list(n for n, _ in obj.children) if obj and obj.object_type == ObjectType.TREE else []
