from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Grid, Vertical
from textual.screen import Screen
from textual.widgets import Label, Button


class ConfirmActionScreen(Screen[bool]):
    BINDINGS = [Binding('esc', 'handle_no', "No")]
    CSS_PATH = "confirm_action_screen.tcss"

    def __init__(self, question: str) -> None:
        self.question = question
        super().__init__()

    def compose(self) -> ComposeResult:
        yield Grid(
            Vertical(*(Label(line) for line in self.question.splitlines()), id="question"),
            Button("Yes", id="yes", variant="success"),
            Button("No", id="no"),
            id="dialog")

    def on_mount(self):
        self.query_one("#no").focus()

    @on(Button.Pressed, "#yes")
    def handle_yes(self) -> None:
        self.dismiss(True)

    @on(Button.Pressed, "#no")
    def handle_no(self) -> None:
        self.dismiss(False)
