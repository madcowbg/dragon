import logging
import pathlib
import traceback

from textual import work
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.css.query import NoMatches
from textual.message import Message
from textual.reactive import reactive, var
from textual.screen import Screen
from textual.widget import Widget
from textual.widgets import Label, Tree, Header, Footer, Input, Static, Switch

from command.fast_path import FastPosixPath
from command.hoard import Hoard
from command.pathing import HoardPathing
from contents.hoard import HoardContents
from gui.app_config import config, _write_config
from gui.hoard_tree_widget import HoardTreeWidget
from gui.node_description_widget import NodeDescription, FileAvailabilityPerRepoDataTable


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
                NodeDescription(self.hoard_contents, config, pathing, self.can_modify))
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

    async def on_file_availability_per_repo_data_table_file_status_modified(
            self, event: FileAvailabilityPerRepoDataTable.FileStatusModified):
        logging.info(f"File status modified: {event.hoard_file.fullname}, reloading")
        event.hoard_file.reload_props()
        await self.query_one(NodeDescription).recompose()

        self.query_one(HoardTreeWidget).refresh_file_label(event.hoard_file)


class HoardExplorerScreen(Screen):
    CSS_PATH = "hoard_explorer_screen.tcss"
    AUTO_FOCUS = HoardExplorerWidget

    class ChangeHoardPath(Message):
        def __init__(self, new_path: pathlib.Path):
            super().__init__()
            self.new_path = new_path

    hoard_path: pathlib.Path = reactive(None)
    can_modify: bool = reactive(default=False)

    def watch_hoard_path(self, hoard_path: pathlib.Path):
        if hoard_path is None:
            return

        try:
            screen = self.query_one(HoardExplorerWidget)
            screen.hoard_path = self.hoard_path
        except NoMatches:
            pass

        self.post_message(HoardExplorerScreen.ChangeHoardPath(hoard_path))

    def watch_can_modify(self, new_val: bool, old_val: bool):
        if new_val != old_val:
            screen = self.query_one(HoardExplorerWidget)
            screen.can_modify = self.can_modify

    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()

        self.hoard_path = pathlib.Path(config.get("hoard_path", "."))

        yield Horizontal(
            Label("Hoard:"), Input(value=self.hoard_path.as_posix(), id="hoard_path_input"),
            Static("Can modify?"), Switch(value=self.can_modify, id="switch_can_modify"),
            classes="horizontal_config_line")
        yield HoardExplorerWidget(self.hoard_path)

    def on_switch_changed(self, event: Switch.Changed):
        if event.switch == self.query_one("#switch_can_modify"):
            self.can_modify = not self.can_modify

    def on_input_submitted(self, event: Input.Submitted):
        if event.input == self.query_one("#hoard_path_input", Input):
            config["hoard_path"] = event.value
            _write_config()

            self.hoard_path = pathlib.Path(config["hoard_path"])
            if self.hoard_path.is_dir():
                self.notify(f"New hoard path: {self.hoard_path}")
            else:
                self.notify(f"Hoard path: {self.hoard_path} does not exist!", severity="error")
