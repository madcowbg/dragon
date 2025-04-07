import os
from typing import TypeVar, Union, Dict, Generic, Iterable, Callable

T = TypeVar('T')


class FolderNode[T]:
    parent: Union["FolderNode[T]", None]

    folders: Dict[str, "FolderNode[T]"]
    files: Dict[str, T]

    def __init__(self, parent: Union["FolderNode[T]", None], name: str):
        self.parent = parent
        self.name = name

        self.folders = dict()
        self.files = dict()

    def add_folder(self, name: str) -> "FolderNode[T]":
        new_folder = FolderNode(self, name)
        self.folders[name] = new_folder
        return new_folder

    def add_file(self, name: str, op: T) -> None:
        self.files[name] = FileNode(name, op, self)


class FileNode[T]:
    data: T
    parent: FolderNode[T]

    def __init__(self, name: str, data: T, parent: FolderNode[T]):
        self.name = name
        self.parent = parent
        self.data = data


class FolderTree(Generic[T]):
    root: FolderNode[T]
    nodes: Dict[str, FolderNode[T]]

    def __init__(self, data_list: Iterable[T], key: Callable[[T], str]) -> None:
        self.root = FolderNode(None, "root")
        self.nodes = {"/": self.root}

        for op in data_list:
            parent = self._ensure_parent_node(key(op))
            parent.add_file(os.path.basename(op.hoard_file), op)

    def _ensure_parent_node(self, path: str) -> FolderNode[T]:
        if path not in self.nodes:
            folder, file = os.path.split(path)
            parent = self._ensure_parent_node(folder)
            node = parent.add_folder(name=file)
            self.nodes[path] = node
        return self.nodes[path]


R = TypeVar('R')


def aggregate_on_nodes(tree: FolderTree[T], file_stats: Callable[[T], R], agg: Callable[[R | None, R], R]) \
        -> Dict[FolderNode[T] | FileNode[T], R]:
    stats = dict()
    append_stats_on_children(tree.root, file_stats, agg, stats)
    return stats


def append_stats_on_children(
        node: FolderNode[T], file_stats: Callable[[T], R], agg: Callable[[R | None, R], R],
        stats: Dict[FolderNode[T], R]) -> None:

    for folder in node.folders.values():
        append_stats_on_children(folder, file_stats, agg, stats)

    new_stat = None
    for folder in node.folders.values():
        new_stat = agg(new_stat, stats[folder])

    for file in node.files.values():
        new_stat = agg(new_stat, file_stats(file))

    assert new_stat is not None
    stats[node] = new_stat
