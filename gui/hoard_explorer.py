import logging
import os
import pathlib
import subprocess
from typing import Dict

import rtoml
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.css.query import NoMatches
from textual.reactive import reactive
from textual.widgets import Footer, Header, Label, Input

from command.hoard import Hoard
from gui.hoard_explorer_screen import HoardExplorerScreen


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
