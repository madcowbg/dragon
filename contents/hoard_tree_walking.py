import logging
import pathlib
import sys
from functools import cached_property
from typing import Generator, Tuple, Optional, Dict

from contents.recursive_stats_calc import CachedReader, read_hoard_file_presence
from contents.hoard_composite_node import CompositeNodeID, CompositeObject
from lmdb_storage.file_object import FileObject
from util import custom_isabs


def walk(current: "HoardDir", objects_reader: CachedReader, from_path: str = "/", depth: int = sys.maxsize) -> \
        Generator[Tuple[Optional["HoardDir"], Optional["HoardFile"]], None, None]:
    assert custom_isabs(from_path)
    for folder in pathlib.Path(from_path).parts[1:]:
        current = current.get_dir(objects_reader, folder)
        if current is None:
            return

    yield from current.walk(objects_reader, depth)


class HoardFile:
    @cached_property
    def fullname(self) -> str:  # fixme replace with FullPosixPath
        parent_path = pathlib.Path(self.parent.fullname) if self.parent is not None else pathlib.Path("/")
        return parent_path.joinpath(self.name).as_posix()

    def __init__(self, parent: "HoardDir", name: str, file_obj: FileObject):
        self.parent = parent
        self.name = name

        self.file_obj = file_obj

    def reload_props(self):
        logging.error("Useless operation!")


class HoardDir:
    @cached_property
    def fullname(self) -> str:  # fixme replace with FullPosixPath
        parent_path = pathlib.Path(self.parent.fullname) if self.parent is not None else pathlib.Path("/")
        return parent_path.joinpath(self.name).as_posix()

    def __init__(self, parent: Optional["HoardDir"], name: str, node: CompositeObject):
        self.name = name
        self.parent = parent
        assert isinstance(node, CompositeObject)
        self.node = node

    def dirs(self, objects_reader: CachedReader) -> Dict[str, "HoardDir"]:
        result = dict()
        for child_name, child_node_id in self.node.children():
            child_node = CompositeObject(child_node_id, objects_reader)
            if child_node.is_any_tree():
                result[child_name] = HoardDir(self, child_name, child_node)
        return dict(sorted(result.items(), key=lambda kv: kv[0]))

    def files(self, objects_reader: CachedReader) -> Dict[str, HoardFile]:
        result = dict()
        for child_name, child_node_id in self.node.children():
            child_node = CompositeObject(child_node_id, objects_reader)
            if not child_node.is_any_tree():
                presence = read_hoard_file_presence(child_node)
                result[child_name] = HoardFile(self, child_name, presence.file_obj)
        return dict(sorted(result.items(), key=lambda kv: kv[0]))

    def get_dir(self, objects_reader: CachedReader, subname: str) -> Optional["HoardDir"]:
        child_node_id = self.node.get_child(subname)
        child_node = CompositeObject(child_node_id, objects_reader)
        if not child_node.is_any_tree():
            return None

        return HoardDir(self, subname, child_node)

    def walk(self, objects_reader: CachedReader, depth: int) -> Generator[
        Tuple[Optional["HoardDir"], Optional["HoardFile"]], None, None]:
        yield self, None
        if depth <= 0:
            return
        for hoard_file in self.files(objects_reader).values():
            yield None, hoard_file
        for hoard_dir in self.dirs(objects_reader).values():
            yield from hoard_dir.walk(objects_reader, depth - 1)


def composite_from_roots(contents: "HoardContents") -> CompositeNodeID:
    roots = contents.env.roots(write=False)
    result = CompositeNodeID(roots["HOARD"].desired)

    for remote in contents.hoard_config.remotes.all():
        result.set_root_current(remote.uuid, roots[remote.uuid].current)
        result.set_root_desired(remote.uuid, roots[remote.uuid].desired)
    return result


def hoard_tree_root(self: "HoardContents") -> HoardDir:
    root_obj = CompositeObject(composite_from_roots(self), CachedReader(self))
    return HoardDir(None, "", root_obj)
