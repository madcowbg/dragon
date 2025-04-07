import asyncio
import logging
import os
import pathlib
import subprocess
from typing import TypeVar, Generic, Union, Callable, List, Iterable, Dict

from rich.text import Text
from textual.app import App, ComposeResult
from textual.css.query import NoMatches
from textual.reactive import reactive, var
from textual.screen import Screen
from textual.widget import Widget
from textual.widgets import Footer, Header, Select, Static, RichLog, Tree
from textual.widgets._tree import TreeNode

from command.hoard import Hoard
from command.pending_file_ops import get_pending_operations, FileOp
from config import HoardRemote
from gui.app_config import config, _write_config
from gui.hoard_explorer_screen import HoardExplorerScreen

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
    append_on_children(tree.root, file_stats, agg, stats)
    return stats


def append_on_children(
        node: FolderNode[T], file_stats: Callable[[T], R], agg: Callable[[R | None, R], R],
        stats: Dict[FolderNode[T], R]) -> None:

    for folder in node.folders.values():
        append_on_children(folder, file_stats, agg, stats)

    new_stat = None
    for folder in node.folders.values():
        new_stat = agg(new_stat, stats[folder])

    for file in node.files.values():
        new_stat = agg(new_stat, file_stats(file))

    stats[node] = new_stat


class HoardContentsPending(Tree[FolderNode[FileOp]]):
    hoard: Hoard | None = reactive(None)
    remote: HoardRemote | None = reactive(None, recompose=True)

    op_tree: FolderTree[FileOp] | None

    def __init__(self, hoard: Hoard, remote: HoardRemote):
        super().__init__('content diffs')
        self.hoard = hoard
        self.remote = remote

        self.op_tree = None
        self.counts = None

    async def on_mount(self):
        async with self.hoard.open_contents(create_missing=False, is_readonly=True) as hoard_contents:
            self.op_tree = FolderTree(
                get_pending_operations(hoard_contents, self.remote.uuid),
                lambda op: op.hoard_file)

        self.root.data = self.op_tree.root
        self.counts = aggregate_on_nodes(
            self.op_tree,
            lambda op: 1,
            lambda old, new: new if old is None else old + new)

        self.root.expand()

    def on_tree_node_expanded(self, event: Tree[FolderNode[FileOp]].NodeExpanded):
        for _, folder in event.node.data.folders.items():
            event.node.add(Text().append(folder.name).append(f" ({self.counts[folder]})", style="green"), data=folder)

        for _, op in event.node.data.files.items():
            event.node.add_leaf(f"{type(op.data)}: {op.data.hoard_file}", data=op)


class CaveInfoWidget(Widget):
    hoard: Hoard | None = reactive(None)
    remote: HoardRemote | None = reactive(None, recompose=True)

    def __init__(self, hoard: Hoard, remote: HoardRemote):
        super().__init__()
        self.hoard = hoard
        self.remote = remote

    def compose(self) -> ComposeResult:
        if self.remote is None:
            yield Static("Please choose a cave.")
        else:
            yield Static(self.remote.name)
            yield Static(self.remote.uuid)
            yield HoardContentsPending(self.hoard, self.remote)


class CaveExplorerScreen(Screen):
    CSS_PATH = "cave_exporer_screen.tcss"

    hoard: Hoard | None = reactive(None, recompose=True)
    remote: HoardRemote | None = var(None)

    def on_mount(self):
        if self.hoard is not None:
            selected_remote = config.get("cave_exporer_selected_repo", None)
            if selected_remote is not None:
                self.remote = self.hoard.config().remotes[selected_remote]

    def watch_hoard(self):
        try:
            self.query_one(CaveInfoWidget).hoard = self.hoard
        except NoMatches:
            pass

    def watch_remote(self):
        if self.remote is not None:
            config["cave_exporer_selected_repo"] = self.remote.uuid
            self.query_one(CaveInfoWidget).remote = self.remote
            _write_config()

    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()

        if self.hoard is not None:
            config = self.hoard.config()
            yield Select(((remote.name, remote) for remote in config.remotes.all()), prompt="Select a cave")
        else:
            yield Select((), prompt="Select a cave", disabled=True)
        yield CaveInfoWidget(self.hoard, self.remote)
        yield RichLog(id="cave_explorer_log")

    def on_select_changed(self, event: Select.Changed):
        self.remote = event.value


class HoardExplorerApp(App):
    BINDINGS = [
        ("d", "toggle_dark", "Toggle dark mode"),
        ("h", "app.push_screen('hoard_explorer')", "Explore hoard"),
        ("c", "app.push_screen('cave_explorer')", "Explore cave"), ]
    CSS_PATH = "hoard_explorer.tcss"
    SCREENS = {
        "hoard_explorer": HoardExplorerScreen,
        "cave_explorer": CaveExplorerScreen}

    def on_mount(self):
        self.push_screen("hoard_explorer")

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.theme = "textual-dark" if self.theme == "textual-light" else "textual-light"

    def on_hoard_explorer_screen_change_hoard_path(self, event: HoardExplorerScreen.ChangeHoardPath):
        self.get_screen("cave_explorer", CaveExplorerScreen).hoard = Hoard(event.new_path.as_posix())

    def action_open_cave_file(self, filepath: str):
        path = pathlib.WindowsPath(filepath)
        if not path.exists():
            self.notify(f"File {filepath} does not exist!", severity="error")
        else:
            self.notify(f"Navigating to {filepath} in Explorer.", severity="information")
            cmd = f"explorer.exe /select,\"{path}\""
            logging.error(cmd)
            subprocess.Popen(cmd)

    def action_open_cave_dir(self, dirpath: str):
        path = pathlib.WindowsPath(dirpath)
        if not path.exists():
            self.notify(f"Folder {dirpath} does not exist!", severity="error")
        else:
            self.notify(f"Opening {dirpath} in Explorer.", severity="information")
            cmd = f"explorer.exe \"{path}\""
            logging.error(cmd)
            subprocess.Popen(cmd)


def start_hoard_explorer_gui(path: str | None = None):
    if path is not None:
        os.chdir(path)

    app = HoardExplorerApp()
    app.run()


if __name__ == "__main__":
    start_hoard_explorer_gui()
