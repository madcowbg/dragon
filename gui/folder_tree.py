import os
from typing import TypeVar, Union, Dict, Generic, Iterable, Callable

T = TypeVar('T')


class DEPRECATED_FolderNode[T]:
    parent: Union["DEPRECATED_FolderNode[T]", None]

    folders: Dict[str, "DEPRECATED_FolderNode[T]"]
    files: Dict[str, "DEPRECATED_FileNode[T]"]

    def __init__(self, parent: Union["DEPRECATED_FolderNode[T]", None], name: str):
        self.parent = parent
        self.name = name

        self.folders = dict()
        self.files = dict()

    def add_folder(self, name: str) -> "DEPRECATED_FolderNode[T]":
        new_folder = DEPRECATED_FolderNode(self, name)
        self.folders[name] = new_folder
        return new_folder

    def add_file(self, name: str, op: T) -> None:
        self.files[name] = DEPRECATED_FileNode(name, op, self)


class DEPRECATED_FileNode[T]:
    data: T
    parent: DEPRECATED_FolderNode[T]

    def __init__(self, name: str, data: T, parent: DEPRECATED_FolderNode[T]):
        self.name = name
        self.parent = parent
        self.data = data


class DEPRECATED_FolderTree[T]:
    root: DEPRECATED_FolderNode[T]
    nodes: Dict[str, DEPRECATED_FolderNode[T]]

    def __init__(self, data_list: Iterable[T], key: Callable[[T], str]) -> None:
        self.root = DEPRECATED_FolderNode(None, "root")
        self.nodes = {"/": self.root}

        for op in data_list:
            parent = self._ensure_parent_node(key(op))
            parent.add_file(os.path.basename(key(op)), op)

    def _ensure_parent_node(self, path: str) -> DEPRECATED_FolderNode[T]:
        assert isinstance(path, str)
        if path not in self.nodes:
            folder, file = os.path.split(path)
            parent = self._ensure_parent_node(folder)
            node = parent.add_folder(name=file)
            self.nodes[path] = node
        return self.nodes[path]


R = TypeVar('R')


def DEPRECATED_aggregate_on_nodes[T, R](tree: DEPRECATED_FolderTree[T], file_stats: Callable[[T], R], agg: Callable[[R | None, R], R]) \
        -> Dict[DEPRECATED_FolderNode[T] | DEPRECATED_FileNode[T], R]:
    stats = dict()
    DEPRECATED_append_stats_on_children(tree.root, file_stats, agg, stats)
    return stats


def DEPRECATED_append_stats_on_children(
        node: DEPRECATED_FolderNode[T], file_stats: Callable[[T], R], agg: Callable[[R | None, R], R],
        stats: Dict[DEPRECATED_FolderNode[T], R | None]) -> None:

    for folder in node.folders.values():
        DEPRECATED_append_stats_on_children(folder, file_stats, agg, stats)

    new_stat = None
    for folder in node.folders.values():
        new_stat = agg(new_stat, stats[folder])

    for file in node.files.values():
        new_stat = agg(new_stat, file_stats(file))

    stats[node] = new_stat
