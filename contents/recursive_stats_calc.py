import dataclasses
import hashlib
import logging
from abc import abstractmethod
from functools import cached_property
from typing import Iterable, Tuple, Dict, List, Any

from msgspec import msgpack, Struct

from contents.hoard_props import HoardFileStatus, compute_status
from lmdb_storage.file_object import BlobObject, FileObject
from lmdb_storage.operations.util import remap
from lmdb_storage.tree_calculation import RecursiveReader, RecursiveCalculator, StatGetter
from lmdb_storage.tree_object import TreeObject, ObjectType, MaybeObjectID, StoredObject, ObjectID

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

    def for_none(self, calculator: "StatGetter[NodeObj, UsedSize]") -> UsedSize:
        return UsedSize(0)

    def __init__(self, contents: "HoardContent"):
        super().__init__(get_used_size, CurrentAndDesiredReader(contents))


class ObjectReader:
    @abstractmethod
    def read(self, object_id: ObjectID) -> StoredObject:
        pass

    def maybe_read(self, object_id: MaybeObjectID) -> StoredObject | None:
        return self.read(object_id) if object_id else None


class CompositeNodeID:
    def __init__(self, hoard_obj_id: MaybeObjectID) -> None:
        self._hoard_obj_id = hoard_obj_id
        self._roots: Dict[str, List[MaybeObjectID]] = {}
        self._hashed: bytes | None = None

    def set_root_current(self, uuid: str, node_id: MaybeObjectID) -> None:
        self._hashed = None
        if uuid not in self._roots:
            self._roots[uuid] = [node_id, None]
        else:
            self._roots[uuid][0] = node_id

    def set_root_desired(self, uuid: str, node_id: MaybeObjectID) -> None:
        self._hashed = None
        if uuid not in self._roots:
            self._roots[uuid] = [None, node_id]
        else:
            self._roots[uuid][1] = node_id

    @property
    def hashed(self) -> bytes:
        if self._hashed is None:
            packed = msgpack.encode((
                self._hoard_obj_id,
                sorted((name, oids) for name, oids in self._roots.items()
                       if oids[0] is not None or oids[1] is not None)))
            self._hashed = hashlib.md5(packed).digest()
        return self._hashed

    def children(self, objects: ObjectReader) -> Iterable[Tuple[str, "CompositeNodeID"]]:
        hoard_obj = objects.maybe_read(self._hoard_obj_id)
        current_roots = remap(self._roots, lambda oids: objects.read(oids[0]) if oids[0] is not None else None)
        desired_roots = remap(self._roots, lambda oids: objects.read(oids[1]) if oids[1] is not None else None)
        children_names = set(
            child_names(hoard_obj)
            + sum((child_names(obj) for obj in current_roots.values()), [])
            + sum((child_names(obj) for obj in desired_roots.values()), []))

        for child_name in children_names:
            child_node = self.get_child(objects, child_name)
            if child_node is not None:
                yield child_name, child_node

    def get_child(self, objects: ObjectReader, child_name) -> "CompositeNodeID":
        assert isinstance(objects, ObjectReader)
        hoard_obj = objects.maybe_read(self._hoard_obj_id)
        child_node = CompositeNodeID(get_child_if_exists(child_name, hoard_obj))

        for uuid, roots_ids in self._roots.items():
            current_child = get_child_if_exists(child_name, objects.maybe_read(roots_ids[0]))
            if current_child is not None:
                child_node.set_root_current(uuid, current_child)

            desired_child = get_child_if_exists(child_name, objects.maybe_read(roots_ids[1]))
            if desired_child is not None:
                child_node.set_root_desired(uuid, desired_child)

        return child_node

    def __hash__(self) -> int:
        return hash(self.hashed)

    def __eq__(self, other) -> bool:
        return isinstance(other, CompositeNodeID) and self.hashed == other.hashed


class QueryStats:
    pass


@dataclasses.dataclass
class FileStats(QueryStats):
    is_deleted: bool
    num_sources: int
    used_size: int


@dataclasses.dataclass
class FolderStats(QueryStats):
    count_non_deleted: int | None


class HoardFilePresence:
    def __init__(self, file_obj: FileObject, node_id: CompositeNodeID):
        self.file_obj = file_obj
        self.presence = None

        hoard_id = node_id._hoard_obj_id
        self.presence = dict()
        for uuid, (current_id, desired_id) in node_id._roots.items():
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


class CompositeTreeReader(RecursiveReader[CompositeNodeID, HoardFilePresence | None]):
    def __init__(self, parent: "HoardContents"):
        self._reader = CachedReader(parent)

    def convert(self, node: CompositeNodeID) -> HoardFilePresence | None:
        file_obj: BlobObject | None = self._reader.maybe_read(node._hoard_obj_id)

        if file_obj is None:
            # fixme this is the legacy case where we iterate over current but not desired files, required by hoard file props. remove!
            existing_current = (self._reader.maybe_read(root_ids[0]) for root_ids in node._roots.values())

            file_obj: BlobObject | None = next((obj for obj in existing_current if obj is not None), None)

        if not file_obj or file_obj.object_type != ObjectType.BLOB:
            logging.debug("Error - path %s as it is not a BlobObject", node)
            return None  # assert False, f"Error - path {path} as it is not a BlobObject"

        assert isinstance(file_obj, FileObject)
        return HoardFilePresence(file_obj, node)

    def children(self, obj: CompositeNodeID) -> Iterable[Tuple[str, CompositeNodeID]]:
        return [
            (child_name, child_obj)
            for child_name, child_obj in obj.children(self._reader)]

    def is_compound(self, obj: CompositeNodeID) -> bool:
        return self.convert(obj) is None  # len(list(self.children(obj))) == 0

    def is_atom(self, obj: CompositeNodeID) -> bool:
        return not self.is_compound(obj)


def calc_query_stats(props: HoardFilePresence) -> FileStats:
    presence = props.presence
    is_deleted = len([uuid for uuid, status in presence.items() if status != HoardFileStatus.CLEANUP]) == 0
    num_sources = len(
        [uuid for uuid, status in presence.items() if status in (HoardFileStatus.AVAILABLE, HoardFileStatus.MOVE)])
    used_size = props.size

    return FileStats(is_deleted, num_sources, used_size)


class QueryStatsCalculator(RecursiveCalculator[CompositeNodeID, HoardFilePresence, QueryStats]):
    def aggregate(self, items: Iterable[Tuple[str, QueryStats]]) -> FolderStats:
        count_non_deleted = 0
        for _, child in items:
            if isinstance(child, FileStats):
                if not child.is_deleted:
                    count_non_deleted += 1
            elif isinstance(child, FolderStats):
                count_non_deleted += child.count_non_deleted
            else:
                raise ValueError(f"Unrecognized child type: {child}")

        return FolderStats(count_non_deleted)

    def for_none(self, calculator: "StatGetter[HoardFilePresence, QueryStats]") -> QueryStats:
        return FolderStats(count_non_deleted=0)

    def __init__(self, contents: "HoardContent"):
        super().__init__(calc_query_stats, CompositeTreeReader(contents))


def get_child_if_exists(child_name: str, hoard_obj: StoredObject | None):
    return hoard_obj.get(child_name) if hoard_obj and hoard_obj.object_type == ObjectType.TREE else None


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
            current_node_id = current_node_id.get_child(reader, child)
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


class SizeCountPresenceStats(Struct):
    @classmethod
    def should_store(cls, item: "SizeCountPresenceStats") -> bool:
        return item.total > 100

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
        super().__init__(calc_size_count_stats, CompositeTreeReader(contents))

    @cached_property
    def stat_cache_key(self) -> bytes:
        return "SizeCountPresenceStats-V01".encode("UTF-8")
