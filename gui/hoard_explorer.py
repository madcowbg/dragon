import logging
import os
import pathlib
import subprocess

from textual.app import App

from command.hoard import Hoard
from gui.cave_explorer import CaveExplorerScreen
from gui.hoard_explorer_screen import HoardExplorerScreen


class HoardExplorerApp(App):
    BINDINGS = [
        ("d", "toggle_dark", "Toggle dark mode"),
        ("h", "app.push_screen('hoard_explorer')", "Explore hoard"),
        ("c", "app.push_screen('cave_explorer')", "Explore cave"), ]
    CSS_PATH = "hoard_explorer.tcss"
    SCREENS = {
        "hoard_explorer": HoardExplorerScreen,
        "cave_explorer": CaveExplorerScreen}

    def on_mount(self):
        self.push_screen("hoard_explorer")

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.theme = "textual-dark" if self.theme == "textual-light" else "textual-light"

    def on_hoard_explorer_screen_change_hoard_path(self, event: HoardExplorerScreen.ChangeHoardPath):
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
