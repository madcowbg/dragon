import logging
import os
import pathlib
import subprocess

from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.css.query import NoMatches
from textual.reactive import reactive
from textual.screen import Screen
from textual.widgets import Footer, Header, Label, Input, Static, Switch

from gui.app_config import config, _write_config
from gui.hoard_explorer_screen import HoardExplorerWidget


class HoardExplorerScreen(Screen):
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

    def watch_can_modify(self, new_val: bool, old_val: bool):
        if new_val != old_val:
            screen = self.query_one(HoardExplorerWidget)
            screen.can_modify = self.can_modify

    def compose(self) -> ComposeResult:
        self.hoard_path = pathlib.Path(config.get("hoard_path", "."))

        """Create child widgets for the app."""
        yield Header()
        yield Footer()

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


class HoardExplorerApp(App):
    BINDINGS = [("d", "toggle_dark", "Toggle dark mode")]
    CSS_PATH = "hoard_explorer.tcss"
    SCREENS = {"hoard_explorer": HoardExplorerScreen}

    def on_mount(self):
        self.push_screen("hoard_explorer")

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.theme = "textual-dark" if self.theme == "textual-light" else "textual-light"

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
