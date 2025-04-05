import logging
import pathlib
import traceback

from textual import work
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.reactive import reactive, var
from textual.widget import Widget
from textual.widgets import Label, Tree

from command.fast_path import FastPosixPath
from command.hoard import Hoard
from command.pathing import HoardPathing
from contents.hoard import HoardContents
from gui.hoard_tree_widget import HoardTreeWidget
from gui.node_description_widget import NodeDescription


class HoardExplorerWidget(Widget):
    _hoard: Hoard | None = reactive(None)
    can_modify: bool = reactive(default=False)

    hoard_contents: HoardContents | None = var(None)
    hoard_path: pathlib.Path | None = var(None)

    def __init__(self, hoard_path: pathlib.Path, *children: Widget):
        super().__init__(*children)
        self.hoard_path = hoard_path

    def compose(self) -> ComposeResult:
        if self.hoard_contents is not None:
            config = self._hoard.config()
            pathing = HoardPathing(config, self._hoard.paths())
            yield Horizontal(
                HoardTreeWidget(self.hoard_contents, config),
                NodeDescription(self.hoard_contents, config, pathing))
        else:
            yield Label("Please select a valid hoard!")

    async def watch_can_modify(self, new_value: bool, old_value: bool):
        if old_value != new_value:
            self.close_and_reopen()

    async def watch_hoard_path(self, old_hoard_path: FastPosixPath, new_hoard_path: FastPosixPath):
        if old_hoard_path != new_hoard_path:
            self.close_and_reopen()

    @work(exclusive=True)
    async def close_and_reopen(self):
        if self.hoard_contents is not None:
            await self.hoard_contents.__aexit__(None, None, None)

        self._hoard = Hoard(self.hoard_path.as_posix())
        try:
            self.notify(f"Loading hoard at {self._hoard.hoardpath}...")
            self.hoard_contents = self._hoard.open_contents(create_missing=False, is_readonly=not self.can_modify)
            await self.hoard_contents.__aenter__()

            await self.recompose()
        except Exception as e:
            traceback.print_exception(e)
            logging.error(e)
            self.hoard_contents = None

    async def on_unmount(self):
        if self.hoard_contents is not None:
            await self.hoard_contents.__aexit__(None, None, None)

    def on_tree_node_selected(self, event: Tree.NodeSelected):
        self.query_one(NodeDescription).hoard_item = event.node.data
