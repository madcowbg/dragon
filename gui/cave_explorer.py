from rich.text import Text
from textual.app import ComposeResult
from textual.css.query import NoMatches
from textual.reactive import reactive, var
from textual.screen import Screen
from textual.widget import Widget
from textual.widgets import Tree, Static, Header, Footer, Select, RichLog

from command.hoard import Hoard
from command.pending_file_ops import FileOp, get_pending_operations
from config import HoardRemote
from gui.app_config import config, _write_config
from gui.folder_tree import FolderNode, FolderTree, aggregate_on_nodes


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
