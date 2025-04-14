import dataclasses
import logging
from time import time
from typing import TypeVar, List, Iterable

from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.css.query import NoMatches

from textual.message import Message
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Label, ProgressBar


class StartProgressReporting(Message):
    def __init__(self, id: str, total: float | int | None, title: str):
        super().__init__()
        self.id = id
        self.title = title
        self.total = total


class MarkProgressReporting(Message):
    def __init__(self, id: str, progress: float | int, is_ended: bool):
        super().__init__()
        self.id = id
        self.progress = progress
        self.is_ended = is_ended


def progress_reporting_bar(widget: Widget, id: str, max_frequency: float):
    def alive_bar(total: float, title: str, unit: str):
        progress_report_title = title + (f" ({unit})" if unit else "")
        class Monitor:
            def __init__(self):
                self.past_time = time()
                self.cumulative = 0

            def __enter__(self):
                widget.post_message(StartProgressReporting(id, total, progress_report_title))

                return self.bar

            def __exit__(self, exc_type, exc_val, exc_tb):
                widget.post_message(MarkProgressReporting(id, self.cumulative, True))
                return None

            def bar(self, progress: float | int | None):
                self.cumulative += progress
                new_time = time()
                if new_time > self.past_time + 1 / max_frequency:
                    widget.post_message(MarkProgressReporting(id, self.cumulative, False))
                    self.past_time = new_time

        return Monitor()

    return alive_bar


T = TypeVar('T')


def progress_reporting_it[T](widget: Widget, id: str, max_frequency: float):
    def alive_it(items: List[T] | Iterable[T], *, title: str, total: float | None = None) -> Iterable[T]:
        total = total if total is not None else len(items) if getattr(items, "__len__", None) is not None else None
        widget.post_message(StartProgressReporting(id, total, title))

        past_time = time()
        idx = 0
        for idx, item in enumerate(items):
            yield item

            new_time = time()
            if new_time > past_time + 1 / max_frequency:
                widget.post_message(MarkProgressReporting(id, idx, False))
                past_time = new_time

        widget.post_message(MarkProgressReporting(id, idx, True))

    return alive_it


@dataclasses.dataclass
class ProgressData:
    total: float | None
    id: str
    progress: float
    title: str


class ProgressReporting(Widget):
    progress_bars: List[ProgressData] = reactive([])

    def __init__(self):
        super().__init__()

    def compose(self) -> ComposeResult:
        if len(self.progress_bars) == 0:
            yield Label("No processes.")

        for data in self.progress_bars:
            progress = ProgressBar(data.total, name=data.title, id=data.id)
            progress.update(progress=data.progress)
            yield Horizontal(Label(data.title, id=f"progress-title-{data.id}"), progress)

    async def on_start_progress_reporting(self, event: StartProgressReporting):
        self.notify(
            f"Starting progress for ({event.id}) with title {event.title} with total {event.total}")  # todo remove

        data = self._find_progress_data(event.id)
        if data is None:
            self.progress_bars.append(ProgressData(event.total, event.id, 0, event.title))
            await self.recompose()
        else:
            try:
                pb = self.query_one(f"#{data.id}", ProgressBar)
                label = self.query_one(f"#progress-title-{data.id}", Label)
            except NoMatches:
                return

            assert pb.id == event.id
            label.update(event.title)
            pb.update(total=event.total if event.total is not None else 0, progress=0)

    async def on_mark_progress_reporting(self, event: MarkProgressReporting):
        if event.is_ended:
            self.notify(f"Ending progress for ({event.id})")  # todo remove

        data = self._find_progress_data(event.id)
        if data is None:
            self.notify(f"No progress data for ({event.id})", severity="error")
            return

        try:
            pb = self.query_one(f"#{data.id}", ProgressBar)
        except NoMatches:
            logging.error(f"no bar for {data.id}")
            return

        pb.progress = event.progress
        data.progress = event.progress
        if event.is_ended:
            self.progress_bars.remove(data)
            await self.recompose()

    def _find_progress_data(self, data_id: str) -> ProgressData | None:
        for data in self.progress_bars:
            if data.id == data_id:
                return data
        return None
