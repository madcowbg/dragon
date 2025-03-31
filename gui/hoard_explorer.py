import logging
import os
import pathlib
import subprocess
import traceback
from typing import Dict, Set

import rtoml
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.css.query import NoMatches
from textual.reactive import reactive, var
from textual.widget import Widget
from textual.widgets import Footer, Header, Tree, Label, Input
from textual.widgets._tree import TreeNode

import util
from command.hoard import Hoard
from command.pathing import HoardPathing
from config import HoardConfig
from contents.hoard import HoardContents, HoardFile, HoardDir
from contents.hoard_props import HoardFileProps
from util import format_size


class HoardTree(Tree):
    contents: HoardContents = reactive(None)
    loaded: Set[HoardDir | HoardFile] = var(set())

    def __init__(self, contents: HoardContents):
        super().__init__("Hoard", data=contents.fsobjects.tree.root, id="hoard_tree")
        self.contents = contents

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

    def __init__(self, hoard_contents: HoardContents, hoard_config: HoardConfig, hoard_pathing: HoardPathing):
        super().__init__()
        self.hoard_contents = hoard_contents
        self.hoard_config = hoard_config
        self.hoard_pathing = hoard_pathing

    def compose(self) -> ComposeResult:
        if self.hoard_item is None:
            yield Label("Please select an item on the left")
        elif isinstance(self.hoard_item, HoardDir):
            hoard_dir = self.hoard_item
            yield Label(f"Folder name: {hoard_dir.name}")
            yield Label(f"Hoard path: {hoard_dir.fullname}")
        elif isinstance(self.hoard_item, HoardFile):
            hoard_file = self.hoard_item
            yield Label(f"File name: {hoard_file.name}")
            yield Label(f"Hoard path: {hoard_file.fullname}")

            hoard_props = self.hoard_contents.fsobjects[hoard_file.fullname]
            assert isinstance(hoard_props, HoardFileProps)

            yield Label(f"size = {format_size(hoard_props.size)}", classes="desc_line")
            yield Label(f"fasthash = {hoard_props.fasthash}", classes="desc_line")

            presence = hoard_props.presence
            by_presence = util.group_to_dict(presence.keys(), key=lambda uuid: presence[uuid])

            yield Label("Statuses per repo", classes="desc_section")
            for status, repos in by_presence.items():
                yield Label(f"Repos where status = {status.value.upper()}")
                for repo_uuid in repos:
                    hoard_remote = self.hoard_config.remotes[repo_uuid]
                    full_local_path = self.hoard_pathing.in_hoard(hoard_file.fullname) \
                        .at_local(repo_uuid).on_device_path()
                    yield Horizontal(
                        Label(
                            hoard_remote.name,
                            classes=" ".join([
                                "repo_name",
                                "status_available" if os.path.isfile(full_local_path) else "status_not_available"])),
                        Label(
                            f"[@click=app.open_cave_file('{full_local_path}')]{repo_uuid}[/]",
                            classes="repo_uuid"),
                        classes="desc_status_line")

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
            config = self.hoard.config()
            pathing = HoardPathing(config, self.hoard.paths())
            yield Horizontal(
                HoardTree(self.hoard_contents),
                NodeDescription(self.hoard_contents, config, pathing))
        else:
            yield Label("Please select a valid hoard!")

    def watch_hoard(self):
        if self.hoard_contents is not None:
            self.hoard_contents.__exit__(None, None, None)

        try:
            self.notify(f"Loading hoard at {self.hoard.hoardpath}...")
            self.hoard_contents = self.hoard.open_contents(create_missing=False, is_readonly=True)  # fixme change to editable
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

    def action_open_cave_file(self, filepath: str):
        path = pathlib.WindowsPath(filepath)
        if not path.exists():
            self.notify(f"File {filepath} does not exist!", severity="error")
        else:
            self.notify(f"Opening {filepath} in Explorer.", severity="information")
            cmd = f"explorer.exe /select,\"{pathlib.WindowsPath(filepath)}\""
            logging.error(cmd)
            subprocess.Popen(cmd)

    def _write_config(self):
        with open("hoard_explorer.toml", 'w') as f:
            rtoml.dump(self.config, f)


def start_hoard_explorer_gui(path: str | None = None):
    if path is not None:
        os.chdir(path)

    app = HoardExplorerApp()
    app.run()


if __name__ == "__main__":
    start_hoard_explorer_gui()
