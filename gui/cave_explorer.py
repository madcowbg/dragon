import logging
from multiprocessing.pool import worker
from sqlite3 import OperationalError
from typing import TypeVar, Dict

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.css.query import NoMatches
from textual.reactive import reactive, var
from textual.screen import Screen
from textual.widget import Widget
from textual.widgets import Tree, Static, Header, Footer, Select, RichLog

from command.contents.comparisons import compare_local_to_hoard
from command.hoard import Hoard
from command.pathing import HoardPathing
from command.pending_file_ops import FileOp, get_pending_operations, CleanupFile, GetFile, CopyFile, MoveFile
from config import HoardRemote
from contents_diff import FileIsSame, DirIsSame, DirMissingInHoard, DirMissingInLocal
from exceptions import RepoOpeningFailed
from gui.app_config import config, _write_config
from gui.folder_tree import FolderNode, FolderTree, aggregate_on_nodes


class HoardContentsPendingToSyncFile(Tree[FolderNode[FileOp]]):
    hoard: Hoard | None = reactive(None)
    remote: HoardRemote | None = reactive(None, recompose=True)

    op_tree: FolderTree[FileOp] | None

    def __init__(self, hoard: Hoard, remote: HoardRemote):
        super().__init__('Pending file sync ')
        self.hoard = hoard
        self.remote = remote

        self.op_tree = None
        self.counts = None
        self.ops_cnt = None

    async def on_mount(self):
        async with self.hoard.open_contents(create_missing=False, is_readonly=True) as hoard_contents:
            self.op_tree = FolderTree(
                get_pending_operations(hoard_contents, self.remote.uuid),
                lambda op: op.hoard_file)

        self.counts = await aggregate_counts(self.op_tree)
        self.ops_cnt = aggregate_on_nodes(
            self.op_tree,
            lambda node: {op_to_str(node.data): 1},
            sum_dicts)

        self.root.data = self.op_tree.root
        self.root.label = self.root.label.append(self._pretty_folder_label_descriptor(self.op_tree.root))
        self.root.expand()


    def on_tree_node_expanded(self, event: Tree[FolderNode[FileOp]].NodeExpanded):
        for _, folder in event.node.data.folders.items():
            folder_label = Text().append(folder.name).append(" ").append(f"({self.counts[folder]})", style="dim")
            cnts_label = self._pretty_folder_label_descriptor(folder)
            event.node.add(folder_label.append(" ").append(cnts_label), data=folder)

        for _, op in event.node.data.files.items():
            event.node.add_leaf(f"{type(op.data)}: {op.data.hoard_file}", data=op)

    def _pretty_folder_label_descriptor(self, folder: FolderNode[FileOp]) -> Text:
        cnts_label = Text().append("{")
        pending = self.ops_cnt[folder]
        if pending is not None:
            for (op_type, order), v in sorted(pending.items(), key=lambda x: x[0][1]):
                cnts_label.append(
                    op_type, style="green" if op_type == "get" else "strike dim" if op_type == "cleanup" else "none") \
                    .append(" ").append(str(v), style="dim").append(",")
        cnts_label.append("}")
        return cnts_label


async def aggregate_counts(op_tree):
    return aggregate_on_nodes(
        op_tree,
        lambda op: 1,
        lambda old, new: new if old is None else old + new)

def op_to_str(op: FileOp):
    if isinstance(op, GetFile):
        return "get", 1
    elif isinstance(op, CopyFile):
        return "copy", 2
    elif isinstance(op, MoveFile):
        return "move", 3
    elif isinstance(op, CleanupFile):
        return "cleanup", 4
    else:
        raise ValueError(f"Unsupported op: {op}")


T = TypeVar('T')


def sum_dicts(old: Dict[T, any], new: Dict[T, any]) -> Dict[T, any]:
    result = old.copy() if old is not None else dict()
    for k, v in new.items():
        result[k] = result.get(k, 0) + v
    return result


PENDING_TO_PULL = 'Hoard vs Repo contents'


class HoardContentsPendingToPull(Tree):
    hoard: Hoard | None = reactive(None)
    remote: HoardRemote | None = reactive(None, recompose=True)
    loaded = reactive(None)

    def __init__(self, hoard: Hoard, remote: HoardRemote):
        super().__init__(PENDING_TO_PULL + " (expand to load)")

        self.hoard = hoard
        self.remote = remote

        self.op_tree = None
        self.counts = None

    @work(exclusive=True)
    async def populate(self):
        self.loaded = True
        try:
            self.root.label = PENDING_TO_PULL + " (loading...)"
            pathing = HoardPathing(self.hoard.config(), self.hoard.paths())
            repo = self.hoard.connect_to_repo(self.remote.uuid, True)
            with repo.open_contents(is_readonly=True) as current_contents:
                async with self.hoard.open_contents(create_missing=False, is_readonly=True) as hoard_contents:
                    # fixme too slow to even load all the is-same cases, should optimize
                    diffs = [
                        diff async for diff in compare_local_to_hoard(current_contents, hoard_contents, pathing)
                        if not isinstance(diff, FileIsSame) and not isinstance(diff, DirIsSame)
                           and not isinstance(diff, DirMissingInLocal) and not isinstance(diff, DirMissingInHoard)]

            self.op_tree = FolderTree(
                diffs,
                lambda diff: diff.hoard_file)

            self.counts = await aggregate_counts(self.op_tree)

            self.root.label = PENDING_TO_PULL + f" ({len(diffs)})"
            self.root.data = self.op_tree.root

            self.post_message(Tree.NodeExpanded(self.root))

        except RepoOpeningFailed as e:
            self.loaded = False
            logging.error(f"Repo opening failed: {e}")
            self.root.label = PENDING_TO_PULL + " (FAILED TO OPEN)"
        except OperationalError as e:
            self.loaded = False
            logging.error(f"Repo opening failed: {e}")
            self.root.label = PENDING_TO_PULL + " (INVALID CONTENTS)"

    async def on_tree_node_expanded(self, event: Tree.NodeExpanded):
        if event.node == self.root and not self.loaded:
            self.populate()

        if self.op_tree is not None:
            for _, folder in event.node.data.folders.items():
                folder_name = Text().append(folder.name).append(" ").append(f"({self.counts[folder]})", style="dim")
                # cnts_label = self._pretty_folder_label_descriptor(folder)
                folder_label = folder_name #.append(" ").append(cnts_label)
                event.node.add(folder_label, data=folder)

            for _, node in event.node.data.files.items():
                event.node.add_leaf(f"{type(node.data)}: {node.data.hoard_file}", data=node.data)


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
            yield Horizontal(
                HoardContentsPendingToSyncFile(self.hoard, self.remote),
                HoardContentsPendingToPull(self.hoard, self.remote),
                id="content_trees")


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
