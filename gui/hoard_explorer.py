import logging
import os
import pathlib
import traceback
from typing import Dict, Set

import rtoml
from textual import events
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.css.query import NoMatches
from textual.reactive import reactive, var
from textual.widget import Widget
from textual.widgets import Footer, Header, Tree, Label, Input
from textual.widgets._tree import TreeNode

from command.hoard import Hoard
from contents.hoard import HoardContents, HoardFile, HoardDir


class HoardTree(Widget):
    contents: HoardContents = reactive(None)
    loaded: Set[HoardDir | HoardFile] = var(set())

    def __init__(self, contents: HoardContents):
        super().__init__()
        self.contents = contents

    def compose(self):
        yield Tree("Hoard", data=self.contents.fsobjects.tree.root, id="hoard_tree")

    def _expand_hoard_dir(self, widget_node: TreeNode[HoardDir | HoardFile], hoard_dir: HoardDir):
        for folder in hoard_dir.dirs.values():
            widget_node.add(folder.name, allow_expand=True, data=folder)
        for file in hoard_dir.files.values():
            widget_node.add(file.name, allow_expand=False, data=file)

    def on_tree_node_expanded(self, event: Tree[HoardDir | HoardFile].NodeExpanded):
        if event.node.data not in self.loaded:
            self.loaded.add(event.node.data)
            self._expand_hoard_dir(event.node, event.node.data)


class NodeDescription(Widget):
    hoard_item: HoardFile | HoardDir | None = reactive(None, recompose=True)

    def compose(self) -> ComposeResult:
        if self.hoard_item is None:
            yield Label("Please select an item on the left")
        elif isinstance(self.hoard_item, HoardDir):
            hoard_dir = self.hoard_item
            yield Label(f"Folder name: {hoard_dir.name}")
        elif isinstance(self.hoard_item, HoardFile):
            hoard_file = self.hoard_item
            yield Label(f"File name: {hoard_file.name}")
        else:
            raise ValueError(f"unknown hoard item type: {type(self.hoard_item)}")


class HoardExplorerScreen(Widget):
    hoard: Hoard | None = reactive(None)
    hoard_contents: HoardContents | None = var(None)

    def __init__(self, hoard_path: pathlib.Path):
        super().__init__()
        self.hoard = Hoard(hoard_path.as_posix())

    def compose(self) -> ComposeResult:
        if self.hoard_contents is not None:
            yield Horizontal(
                HoardTree(self.hoard_contents),
                NodeDescription())
        else:
            yield Label("Please select a valid hoard!")

    def watch_hoard(self):
        if self.hoard_contents is not None:
            self.hoard_contents.__exit__(None, None, None)

        try:
            self.notify(f"Loading hoard at {self.hoard.hoardpath}...")
            self.hoard_contents = self.hoard.open_contents(create_missing=False)
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


class HoardExplorerApp(App):
    BINDINGS = [("d", "toggle_dark", "Toggle dark mode")]
    CSS_PATH = "hoard_explorer.tcss"

    hoard_path: pathlib.Path = reactive(pathlib.Path("."), recompose=True)
    config: Dict[any, any] = reactive({})

    def on_mount(self):
        if os.path.isfile("hoard_explorer.toml"):
            with open("hoard_explorer.toml", 'r') as f:
                self.config = rtoml.load(f)
        self.hoard_path = pathlib.Path(self.config.get("hoard_path", "."))

    def watch_hoard_path(self):
        try:
            screen = self.query_one(HoardExplorerScreen)
            screen.hoard = Hoard(self.hoard_path.as_posix())
        except NoMatches:
            pass

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        yield Footer()

        yield Horizontal(
            Label("Hoard:"), Input(value=self.hoard_path.as_posix(), id="hoard_path_input"),
            classes="horizontal_config_line")
        yield HoardExplorerScreen(self.hoard_path)

    def on_input_submitted(self, event: Input.Submitted):
        if event.input == self.query_one("#hoard_path_input", Input):
            self.config["hoard_path"] = event.value
            self._write_config()

            self.hoard_path = pathlib.Path(self.config["hoard_path"])
            if self.hoard_path.is_dir():
                self.notify(f"New hoard path: {self.hoard_path}")
            else:
                self.notify(f"Hoard path: {self.hoard_path} does not exist!", severity="error")

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.theme = "textual-dark" if self.theme == "textual-light" else "textual-light"

    def _write_config(self):
        with open("hoard_explorer.toml", 'w') as f:
            rtoml.dump(self.config, f)


if __name__ == "__main__":
    app = HoardExplorerApp()
    app.run()
