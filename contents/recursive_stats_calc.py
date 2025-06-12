import dataclasses
import hashlib
import logging
from abc import abstractmethod, ABC
from functools import cached_property
from typing import Iterable, Tuple, Dict, List, Callable

from msgspec import msgpack, Struct

from contents.hoard_props import HoardFileStatus, compute_status
from lmdb_storage.file_object import BlobObject, FileObject
from lmdb_storage.operations.util import remap
from lmdb_storage.tree_calculation import RecursiveReader, RecursiveCalculator, StatGetter
from lmdb_storage.tree_object import TreeObject, ObjectType, MaybeObjectID, StoredObject, ObjectID
from lmdb_storage.tree_structure import Objects


class HashableKey:
    @property
    @abstractmethod
    def hashed(self) -> bytes: pass


class Storeable:
    @abstractmethod
    def should_store(self) -> bool:    pass


@dataclasses.dataclass(frozen=True)
class NodeID(HashableKey):
    current: MaybeObjectID
    desired: MaybeObjectID

    @cached_property
    def hashed(self) -> bytes:
        return msgpack.encode((self.current, self.desired))

    @property
    def children(self) -> Iterable[Tuple[str, "NodeID"]]:
        pass


class RecursiveObject[RID]:
    @property
    @abstractmethod
    def has_children(self) -> bool: pass

    @property
    @abstractmethod
    def children(self) -> Iterable[Tuple[str, RID]]: pass


@dataclasses.dataclass(frozen=True)
class NodeObj(RecursiveObject[NodeID]):
    current: StoredObject | None
    desired: StoredObject | None

    @property
    def children(self) -> Iterable[Tuple[str, NodeID]]:
        if self.current is None and self.desired is None:
            return

        if self.current is None:
            assert isinstance(self.desired, TreeObject)
            yield from [(child_name, NodeID(None, right_child)) for child_name, right_child in self.desired.children]
            return

        if self.current.object_type == ObjectType.BLOB:
            yield "$LEFT$", NodeID(self.current.id, None)  # returns self.current blob

            assert isinstance(self.desired, TreeObject)
            yield from [(child_name, NodeID(None, right_child)) for child_name, right_child in self.desired.children]
            return

        assert self.current.object_type == ObjectType.TREE

        if self.desired is None:
            assert isinstance(self.current, TreeObject)
            yield from [(child_name, NodeID(left_child, None)) for child_name, left_child in self.current.children]
            return

        if self.desired.object_type == ObjectType.BLOB:
            yield "$RIGHT", NodeID(None, self.desired.id)  # returns right blob

            assert isinstance(self.current, TreeObject)
            yield from [(child_name, NodeID(left_child, None)) for child_name, left_child in self.current.children]
            return

        assert self.desired.object_type == ObjectType.TREE
        left_map = dict(self.current.children)
        right_map = dict(self.desired.children)
        all_children = sorted(set(list(left_map.keys()) + list(right_map.keys())))
        for child_name in all_children:
            yield child_name, NodeID(left_map.get(child_name), right_map.get(child_name))

    @property
    def has_children(self) -> bool:
        return (self.current and self.current.object_type == ObjectType.TREE) \
            or (self.desired and self.desired.object_type == ObjectType.TREE)


@dataclasses.dataclass(frozen=True)
class UsedSize(Storeable):
    used_size: int
    count: int

    def should_store(self) -> bool:
        return self.count > 100


def get_used_size(obj: NodeObj) -> UsedSize:
    """ Returns the larger of the desired or the current size for that object. Assumes they are blobs"""
    assert not obj.current or obj.current.object_type == ObjectType.BLOB
    assert not obj.desired or obj.desired.object_type == ObjectType.BLOB
    return UsedSize(max(obj.current.size if obj.current else 0, obj.desired.size if obj.desired else 0), 1)


class CurrentAndDesiredReader(RecursiveReader[NodeID, NodeObj]):
    def __init__(self, contents: "HoardContent"):
        self.contents = contents

    def convert(self, obj: NodeID) -> NodeObj:
        with self.contents.env.objects(write=False) as objects:
            return NodeObj(objects[obj.current] if obj.current else None, objects[obj.desired] if obj.desired else None)

    def children(self, obj_id: NodeID) -> Iterable[Tuple[str, NodeID]]:
        return self.convert(obj_id).children

    def is_compound(self, obj_id: NodeID) -> bool:
        return self.convert(obj_id).has_children

    def is_atom(self, obj: NodeID) -> bool:
        return not self.is_compound(obj)


class UsedSizeCalculator(RecursiveCalculator[NodeID, NodeObj, UsedSize]):
    def aggregate(self, items: Iterable[Tuple[str, UsedSize]]) -> UsedSize:
        used_size, count = 0, 0
        for _, v in items:
            used_size += v.used_size
            count += v.count
        return UsedSize(used_size, count)

    def for_none(self, calculator: "StatGetter[NodeObj, UsedSize]") -> UsedSize:
        return UsedSize(0, 0)

    def __init__(self, contents: "HoardContent"):
        super().__init__(get_used_size, CurrentAndDesiredReader(contents))

    @cached_property
    def stat_cache_key(self) -> bytes:
        return "UsedSizeCalculator-V01".encode()


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

    def as_object(self, objects: ObjectReader) -> "CompositeObject":
        return CompositeObject(self, objects)

    def __hash__(self) -> int:
        return hash(self.hashed)

    def __eq__(self, other) -> bool:
        return isinstance(other, CompositeNodeID) and self.hashed == other.hashed


class CompositeObject:
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


@dataclasses.dataclass
class FileStats:
    is_deleted: bool
    num_sources: int
    used_size: int


@dataclasses.dataclass
class FolderStats:
    count: int
    used_size: int

    count_non_deleted: int
    num_without_sources: int


@dataclasses.dataclass
class QueryStats(Storeable, ABC):
    file: FileStats | None = None
    folder: FolderStats | None = None

    def should_store(self) -> bool:
        return self.folder and self.folder.count > 100


class HoardFilePresence:
    def __init__(self, file_obj: FileObject, node_id: CompositeNodeID):
        self.file_obj = file_obj
        self.presence = None

        hoard_id = node_id._hoard_obj_id
        self.presence = dict()
        for uuid, (current_id, desired_id) in node_id.roots:
            status = compute_status(hoard_id, current_id, desired_id)
            if status is not None:
                self.presence[uuid] = status


class CachedReader(ObjectReader):
    def __init__(self, parent: "HoardContents") -> None:
        self.parent = parent
        self._cache = dict()

    def read(self, object_id: ObjectID) -> StoredObject:
        if object_id not in self._cache:
            with self.parent.env.objects(write=False) as objects:
                self._cache[object_id] = objects[object_id]
        return self._cache[object_id]


def read_hoard_file_presence(reader: CachedReader, node_id: CompositeNodeID) -> HoardFilePresence | None:
    file_obj: BlobObject | None = reader.maybe_read(node_id._hoard_obj_id)

    if file_obj is None:
        # fixme this is the legacy case where we iterate over current but not desired files, required by hoard file props. remove!
        existing_current = (reader.maybe_read(root_id) for root_id in node_id._current_roots.values())

        file_obj: BlobObject | None = next((obj for obj in existing_current if obj is not None), None)

    if not file_obj or file_obj.object_type != ObjectType.BLOB:
        logging.debug("Error - path %s as it is not a BlobObject", node_id)
        return None  # assert False, f"Error - path {path} as it is not a BlobObject"

    assert isinstance(file_obj, FileObject)
    return HoardFilePresence(file_obj, node_id)


class CompositeTreeReader[T](RecursiveReader[CompositeNodeID, HoardFilePresence | None]):
    def __init__(self, parent: "HoardContents", converter: Callable[[CachedReader, CompositeNodeID], T]):
        self._reader = CachedReader(parent)
        self._converter = converter

    def convert(self, node: CompositeNodeID) -> HoardFilePresence | None:
        return self._converter(self._reader, node)

    def children(self, obj: CompositeNodeID) -> Iterable[Tuple[str, CompositeNodeID]]:
        return [
            (child_name, child_obj)
            for child_name, child_obj in obj.as_object(self._reader).children()]

    def is_compound(self, obj: CompositeNodeID) -> bool:
        return self.convert(obj) is None  # len(list(self.children(obj))) == 0

    def is_atom(self, obj: CompositeNodeID) -> bool:
        return not self.is_compound(obj)


def calc_query_stats(props: HoardFilePresence) -> QueryStats:
    presence = props.presence
    is_deleted = len([uuid for uuid, status in presence.items() if status != HoardFileStatus.CLEANUP]) == 0
    num_sources = len([uuid for uuid, status in presence.items() if status == HoardFileStatus.AVAILABLE])
    used_size = props.file_obj.size

    return QueryStats(file=FileStats(is_deleted, num_sources, used_size))


class QueryStatsCalculator(RecursiveCalculator[CompositeNodeID, HoardFilePresence, QueryStats]):
    def aggregate(self, items: Iterable[Tuple[str, QueryStats]]) -> QueryStats:
        count = 0
        used_size = 0
        count_non_deleted = 0
        num_without_sources = 0
        for _, child in items:
            if child.file:
                assert not child.folder
                count += 1
                used_size += child.file.used_size
                if not child.file.is_deleted:
                    count_non_deleted += 1
                if child.file.num_sources == 0:
                    num_without_sources += 1
            elif child.folder:
                count += child.folder.count
                used_size += child.folder.used_size
                count_non_deleted += child.folder.count_non_deleted
                num_without_sources += child.folder.num_without_sources
            else:
                raise ValueError(f"Unrecognized child type: {child}")

        return QueryStats(folder=FolderStats(
            count=count, used_size=used_size, count_non_deleted=count_non_deleted,
            num_without_sources=num_without_sources))

    def for_none(self, calculator: "StatGetter[HoardFilePresence, QueryStats]") -> QueryStats:
        return QueryStats(folder=FolderStats(0, 0, 0, 0))

    def __init__(self, contents: "HoardContent"):
        super().__init__(calc_query_stats,
                         CompositeTreeReader[HoardFilePresence | None](contents, read_hoard_file_presence))

    @cached_property
    def stat_cache_key(self) -> bytes:
        return "QueryStatsCalculator-V01".encode()


def get_child_if_exists(child_name: str, hoard_obj: StoredObject | None) -> MaybeObjectID:
    if hoard_obj and hoard_obj.object_type == ObjectType.TREE:
        hoard_obj: TreeObject
        return hoard_obj.get(child_name)
    return None


def child_names(obj: StoredObject) -> List[str]:
    return list(n for n, _ in obj.children) if obj and obj.object_type == ObjectType.TREE else []


def composite_from_roots(contents: "HoardContents") -> CompositeNodeID:
    roots = contents.env.roots(write=False)
    result = CompositeNodeID(roots["HOARD"].desired)

    for remote in contents.hoard_config.remotes.all():
        result.set_root_current(remote.uuid, roots[remote.uuid].current)
        result.set_root_desired(remote.uuid, roots[remote.uuid].desired)
    return result


def drilldown(contents: "HoardContents", node_at_path: CompositeNodeID, path: List[str]) -> CompositeNodeID | None:
    with contents.env.objects(write=False) as objects:
        class TmpReader(ObjectReader):
            def read(self, object_id: ObjectID) -> StoredObject:
                return objects[object_id]

        reader = TmpReader()

        current_node_id = node_at_path
        if current_node_id is None:
            return None

        for child in path:
            current_node_id = current_node_id.as_object(reader).get_child(child)
            if current_node_id is None:
                return None

        return current_node_id


@dataclasses.dataclass()
class SizeCount:
    nfiles: int
    size: int

    def __iadd__(self, other: "SizeCount"):
        assert isinstance(other, SizeCount)
        self.nfiles += other.nfiles
        self.size += other.size

        return self


@dataclasses.dataclass()
class SizeCountPresenceForRemoteStats:
    total: SizeCount
    presence: Dict[HoardFileStatus, SizeCount]

    def add(self, other):
        assert isinstance(other, SizeCountPresenceForRemoteStats)
        self.total += other.total
        for status, size_count in other.presence.items():
            if status not in self.presence:
                self.presence[status] = SizeCount(0, 0)
            self.presence[status] += size_count


class SizeCountPresenceStats(Struct, Storeable):
    def should_store(self) -> bool:
        return self.total > 100

    total: int
    _per_remote: Dict[str, SizeCountPresenceForRemoteStats] = dict()

    def for_remote(self, uuid: str) -> SizeCountPresenceForRemoteStats:
        if uuid not in self._per_remote:
            self._per_remote[uuid] = SizeCountPresenceForRemoteStats(SizeCount(0, 0), dict())
        return self._per_remote[uuid]

    def declared_remotes(self) -> Iterable[str]:
        return self._per_remote.keys()

    def __iadd__(self, other):
        assert isinstance(other, SizeCountPresenceStats), other
        self.total += other.total
        for remote, stat in other._per_remote.items():
            self.for_remote(remote).add(stat)
        return self


def calc_size_count_stats(props: HoardFilePresence) -> SizeCountPresenceStats:
    result = SizeCountPresenceStats(1)
    presence = props.presence
    for uuid, status in presence.items():
        single_file_stat = SizeCount(1, props.file_obj.size)
        result.for_remote(uuid).total = single_file_stat
        result.for_remote(uuid).presence = {status: single_file_stat}

    return result


class SizeCountPresenceStatsCalculator(RecursiveCalculator[CompositeNodeID, HoardFilePresence, SizeCountPresenceStats]):
    def aggregate(self, items: Iterable[Tuple[str, SizeCountPresenceStats]]) -> SizeCountPresenceStats:
        result = SizeCountPresenceStats(0)

        for _, child_result in items:
            result += child_result
        return result

    def for_none(self, calculator: "StatGetter[HoardFilePresence, SizeCountPresenceStats]") -> SizeCountPresenceStats:
        return SizeCountPresenceStats(0)

    def __init__(self, contents: "HoardContent"):
        super().__init__(calc_size_count_stats,
                         CompositeTreeReader[HoardFilePresence | None](contents, read_hoard_file_presence))

    @cached_property
    def stat_cache_key(self) -> bytes:
        return "SizeCountPresenceStats-V01".encode("UTF-8")
