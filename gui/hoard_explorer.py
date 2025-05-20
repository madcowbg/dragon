import logging
import os
import pathlib
import subprocess

from textual import on
from textual.app import App, ComposeResult
from textual.screen import Screen
from textual.widgets import Header, Footer, Label

from command.hoard import Hoard
from gui.app_config import config, _write_config
from gui.cave_explorer_screen import CaveExplorerScreen
from gui.hoard_explorer_screen import HoardExplorerScreen, HoardExplorerSettings


class HoardStateScreen(Screen):

    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()

        yield Label("Hoard state")

class HoardExplorerApp(App):
    BINDINGS = [
        ("d", "toggle_dark", "Toggle dark mode"),
        ("h", "app.push_screen('hoard_explorer')", "Hoard explorer"),
        ("s", "app.push_screen('hoard_state')", "Hoard state"),
        ("c", "app.push_screen('cave_explorer')", "Cave operations"), ]
    CSS_PATH = "hoard_explorer.tcss"
    SCREENS = {
        "hoard_explorer": HoardExplorerScreen,
        "hoard_state": HoardStateScreen,
        "cave_explorer": CaveExplorerScreen}

    def on_mount(self):
        self.get_screen("cave_explorer", CaveExplorerScreen).hoard = Hoard(config.get("hoard_path", "."))
        self.push_screen(config.get("last_screen", "hoard_explorer"))

    def action_push_screen(self, screen: str) -> None:
        config["last_screen"] = screen
        _write_config()

        super().push_screen(screen)

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.theme = "textual-dark" if self.theme == "textual-light" else "textual-light"

    @on(HoardExplorerSettings.ChangeHoardPath)
    def on_change_hoard_path(self, event: HoardExplorerSettings.ChangeHoardPath):
        self.get_screen("cave_explorer", CaveExplorerScreen).hoard = Hoard(event.new_path.as_posix())

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
