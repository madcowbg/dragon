import logging
import pathlib
import traceback

from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.reactive import reactive, var
from textual.widget import Widget
from textual.widgets import Label, Tree

from command.hoard import Hoard
from command.pathing import HoardPathing
from contents.hoard import HoardContents
from gui.hoard_tree_widget import HoardTreeWidget
from gui.node_description_widget import NodeDescription


class HoardExplorerScreen(Widget):
    hoard: Hoard | None = reactive(None)
    can_modify: bool = reactive(default=False)

    hoard_contents: HoardContents | None = var(None)

    def __init__(self, hoard_path: pathlib.Path):
        super().__init__()
        self.hoard = Hoard(hoard_path.as_posix())

    def compose(self) -> ComposeResult:
        if self.hoard_contents is not None:
            config = self.hoard.config()
            pathing = HoardPathing(config, self.hoard.paths())
            yield Horizontal(
                HoardTreeWidget(self.hoard_contents, config),
                NodeDescription(self.hoard_contents, config, pathing))
        else:
            yield Label("Please select a valid hoard!")

    def watch_can_modify(self):
        self.close_and_reopen()

    def watch_hoard(self):
        self.close_and_reopen()

    def close_and_reopen(self):
        if self.hoard_contents is not None:
            self.hoard_contents.__exit__(None, None, None)

        try:
            self.notify(f"Loading hoard at {self.hoard.hoardpath}...")
            self.hoard_contents = self.hoard.open_contents(create_missing=False, is_readonly=not self.can_modify)
            self.hoard_contents.__enter__()
        except Exception as e:
            traceback.print_exception(e)
            logging.error(e)
            self.hoard_contents = None

    def on_unmount(self):
        if self.hoard_contents is not None:
            self.hoard_contents.__exit__(None, None, None)

    def on_tree_node_selected(self, event: Tree.NodeSelected):
        self.query_one(NodeDescription).hoard_item = event.node.data
