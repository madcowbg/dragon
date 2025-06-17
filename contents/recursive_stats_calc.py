import abc
import dataclasses
import logging
from abc import abstractmethod, ABC
from functools import cached_property
from typing import Iterable, Tuple, Dict, List

from msgspec import msgpack, Struct

from contents.hoard_composite_node import ObjectReader, CompositeNodeID, CompositeObject
from contents.hashable_key import HashableKey
from contents.hoard_props import HoardFileStatus, compute_status
from lmdb_storage.file_object import BlobObject, FileObject
from lmdb_storage.tree_calculation import RecursiveReader, RecursiveCalculator, StatGetter, ValueCalculator
from lmdb_storage.tree_object import TreeObject, ObjectType, MaybeObjectID, StoredObject, ObjectID


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


def read_hoard_file_presence(node: CompositeObject) -> HoardFilePresence | None:
    file_obj: BlobObject | None = node._hoard_obj

    if file_obj is None:
        # fixme this is the legacy case where we iterate over current but not desired files, required by hoard file props. remove!
        existing_current = (root_obj for root_obj in node._current_roots.values())

        file_obj: BlobObject | None = next((obj for obj in existing_current if obj is not None), None)

    if not file_obj or file_obj.object_type != ObjectType.BLOB:
        # fixme remove this case, it triggers for all folders now
        logging.debug("Error - path %s as it is not a BlobObject", node.node_id)
        return None  # assert False, f"Error - path {path} as it is not a BlobObject"

    assert isinstance(file_obj, FileObject)
    return HoardFilePresence(file_obj, node.node_id)


class CompositeNodeCalculator[R](ValueCalculator[CompositeObject, R]):
    def __init__(self, hoard_contents: "HoardContents"):
        self.object_reader = CachedReader(hoard_contents)

    def calculate(self, calculator: "StatGetter[CompositeNodeID, R]", item: CompositeNodeID) -> R:
        item_object = CompositeObject.expand(item, self.object_reader)
        if self.treat_as_composite(item_object):
            return self.aggregate(
                (child_name, calculator[child_node_at_path]) for child_name, child_node_at_path in
                item_object.children())
        else:
            return self.calculate_for_atom(item_object)

    @abc.abstractmethod
    def treat_as_composite(self, obj: CompositeObject) -> bool:
        pass

    @abc.abstractmethod
    def calculate_for_atom(self, obj: CompositeObject) -> R:
        pass

    @abc.abstractmethod
    def aggregate(self, items: Iterable[Tuple[str, R]]) -> R:
        pass

    @property
    @abc.abstractmethod
    def stat_cache_key(self) -> bytes:
        pass


class QueryStatsCalculator(CompositeNodeCalculator[QueryStats]):
    def treat_as_composite(self, obj: CompositeObject) -> bool:
        # fixme weird (checking if we found a file), but what if names collide?
        return read_hoard_file_presence(obj) is None

    def calculate_for_atom(self, obj: CompositeObject) -> QueryStats:
        hfp = read_hoard_file_presence(obj)

        assert isinstance(hfp, HoardFilePresence)
        is_deleted = len([uuid for uuid, status in hfp.presence.items() if status != HoardFileStatus.CLEANUP]) == 0
        num_sources = len([uuid for uuid, status in hfp.presence.items() if status == HoardFileStatus.AVAILABLE])
        used_size = hfp.file_obj.size

        return QueryStats(file=FileStats(is_deleted, num_sources, used_size))

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

    @cached_property
    def stat_cache_key(self) -> bytes:
        return "QueryStatsCalculator-V02".encode()


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
            current_node_id = CompositeObject.expand(current_node_id, reader).get_child(child)
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


class SizeCountPresenceStatsCalculator(CompositeNodeCalculator[SizeCountPresenceStats]):
    def treat_as_composite(self, obj: CompositeObject) -> bool:
        # fixme weird (checking if we found a file), but what if names collide?
        return read_hoard_file_presence(obj) is None


    def calculate_for_atom(self, obj: CompositeObject) -> SizeCountPresenceStats:
        props = read_hoard_file_presence(obj)

        result = SizeCountPresenceStats(1)
        presence = props.presence
        for uuid, status in presence.items():
            single_file_stat = SizeCount(1, props.file_obj.size)
            result.for_remote(uuid).total = single_file_stat
            result.for_remote(uuid).presence = {status: single_file_stat}

        return result

    def aggregate(self, items: Iterable[Tuple[str, SizeCountPresenceStats]]) -> SizeCountPresenceStats:
        result = SizeCountPresenceStats(0)

        for _, child_result in items:
            result += child_result
        return result

    def for_none(self, calculator: "StatGetter[HoardFilePresence, SizeCountPresenceStats]") -> SizeCountPresenceStats:
        return SizeCountPresenceStats(0)

    @cached_property
    def stat_cache_key(self) -> bytes:
        return "SizeCountPresenceStats-V02".encode("UTF-8")
