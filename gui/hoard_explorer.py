import logging
import os
import pathlib
import subprocess
import traceback
from typing import Dict, List

import rtoml
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.css.query import NoMatches
from textual.reactive import reactive, var
from textual.widget import Widget
from textual.widgets import Footer, Header, Tree, Label, Input
from textual.widgets._tree import TreeNode

from command.hoard import Hoard
from command.pathing import HoardPathing
from config import HoardConfig, HoardRemote
from contents.hoard import HoardContents, HoardFile, HoardDir
from contents.hoard_props import HoardFileProps
from util import format_size, group_to_dict, format_count


class HoardTree(Tree):
    contents: HoardContents = reactive(None)
    loaded_offset: dict[HoardDir | HoardFile, int] = var(dict())

    def __init__(self, contents: HoardContents, config: HoardConfig):
        super().__init__("Hoard", data=contents.fsobjects.tree.root, id="hoard_tree")
        self.guide_depth = 2
        self.auto_expand = False
        self.contents = contents
        self.select_node(self.root)
        self.root.expand()

        self.mounts: Dict[str, List[HoardRemote]] = group_to_dict(
            config.remotes.all(), key=lambda r: r.mounted_at)
        self.root.set_label(self._create_pretty_folder_label("/", "/", 45))

    def _expand_hoard_dir(self, widget_node: TreeNode[HoardDir | HoardFile], hoard_dir: HoardDir, parent_offset: int):
        label_max_width = 45 - parent_offset * widget_node.tree.guide_depth
        for folder in hoard_dir.dirs.values():
            folder_label = self._create_pretty_folder_label(folder.name, folder.fullname, label_max_width)
            widget_node.add(folder_label, allow_expand=True, data=folder)

        for file in hoard_dir.files.values():
            size = self.contents.fsobjects[file.fullname].size
            file_label = Text().append(file.name.ljust(label_max_width + 2)).append(f"{format_size(size):>13}", "none")
            widget_node.add(file_label, allow_expand=False, data=file)

    def _create_pretty_folder_label(self, name: str, fullname: str, max_width: int, name_style: str = "bold green"):
        count, size = self.contents.fsobjects.stats_in_folder(fullname)
        folder_name = Text().append(name, name_style).append(self._pretty_count_attached(fullname))
        folder_name.align("left", max_width)
        folder_label = folder_name \
            .append(f"{format_count(count):>6}", "dim") \
            .append(f"{format_size(size):>7}", "none")
        return folder_label

    def _pretty_count_attached(self, fullname: str) -> str:
        return f" ✅{len(self.mounts.get(fullname))}" if self.mounts.get(fullname) is not None else ""

    def on_tree_node_expanded(self, event: Tree[HoardDir | HoardFile].NodeExpanded):
        if event.node.data not in self.loaded_offset:
            self.loaded_offset[event.node.data] = 1 + (
                self.loaded_offset[event.node.parent.data] if event.node.parent is not None else 0)
            self._expand_hoard_dir(event.node, event.node.data, self.loaded_offset[event.node.data])


def pretty_truncate(text: str, size: int) -> str:
    assert size >= 5
    if len(text) <= size:
        return text
    left_len = (size - 3) // 2
    return text[:left_len] + "..." + text[-(size - 3 - left_len):]


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

            yield Label(f"Availability on repos", classes="desc_section")
            for hoard_remote in self.hoard_config.remotes.all():
                local_path = self.hoard_pathing.in_hoard(hoard_dir.fullname).at_local(hoard_remote.uuid)
                if local_path is not None:
                    availability_status_class = "status_available" \
                        if os.path.isdir(local_path.on_device_path()) else "status_not_available"
                    yield Horizontal(
                        Label(
                            hoard_remote.name,
                            classes=" ".join([
                                "repo_name",
                                availability_status_class])),
                        Label(
                            f"[@click=app.open_cave_dir('{local_path.on_device_path()}')]{pretty_truncate(hoard_remote.uuid, 15)}[/]",
                            classes="repo_uuid"),
                        Label(Text(self.hoard_pathing.in_local("/", hoard_remote.uuid).on_device_path()), classes=f"remote_location {availability_status_class}"),
                        Label(Text(local_path.as_pure_path.as_posix()), classes="local_path"),
                        classes="desc_status_line")

        elif isinstance(self.hoard_item, HoardFile):
            hoard_file = self.hoard_item
            yield Label(f"File name: {hoard_file.name}")
            yield Label(f"Hoard path: {hoard_file.fullname}")

            hoard_props = self.hoard_contents.fsobjects[hoard_file.fullname]
            assert isinstance(hoard_props, HoardFileProps)

            yield Label(f"size = {format_size(hoard_props.size)}", classes="desc_line")
            yield Label(f"fasthash = {hoard_props.fasthash}", classes="desc_line")

            presence = hoard_props.presence
            by_presence = group_to_dict(presence.keys(), key=lambda uuid: presence[uuid])

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
                HoardTree(self.hoard_contents, config),
                NodeDescription(self.hoard_contents, config, pathing))
        else:
            yield Label("Please select a valid hoard!")

    def watch_hoard(self):
        if self.hoard_contents is not None:
            self.hoard_contents.__exit__(None, None, None)

        try:
            self.notify(f"Loading hoard at {self.hoard.hoardpath}...")
            self.hoard_contents = self.hoard.open_contents(create_missing=False,
                                                           is_readonly=True)  # fixme change to editable
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
