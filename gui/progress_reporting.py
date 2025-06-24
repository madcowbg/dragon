import dataclasses
import enum
import logging
from datetime import datetime
from time import time
from typing import TypeVar, List, Iterable, Dict, Tuple, Any, ContextManager, Optional, Collection

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.css.query import NoMatches
from textual.message import Message
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Label, ProgressBar, LoadingIndicator

from task_logging import TaskLogger


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


class TaskStatus(enum.Enum):
    CREATED = "created"
    RUNNING = "started"
    SUCCESS = "success"
    FAILURE = "failure"


class TaskState[ID]:
    def __init__(self, task_id: ID, headline: str):
        self.task_id: ID = task_id

        self.headline: str = headline

        self.status: TaskStatus = TaskStatus.CREATED
        self.started_on: datetime = datetime.now()
        self.logs: List[str] = []


def sort_by_status_and_time(task_state: TaskState) -> Tuple[int, datetime]:
    if task_state.status == TaskStatus.CREATED:
        status_order = 2
    elif task_state.status == TaskStatus.RUNNING:
        status_order = 1
    else:
        assert task_state.status == TaskStatus.SUCCESS or task_state.status == TaskStatus.FAILURE
        status_order = 3

    return status_order, task_state.started_on


_TASK_LISTENERS: List["LongRunningTasks"] = []


class LongRunningTasks(Widget):
    def compose(self) -> ComposeResult:
        if len(_TASKS) == 0:
            yield Label("No tasks.")
            return

        for task_state in sorted(_TASKS.values(), key=sort_by_status_and_time):
            with Horizontal():
                if task_state.status == TaskStatus.CREATED:
                    text_style = "normal"
                elif task_state.status == TaskStatus.RUNNING:
                    text_style = "bold green"
                elif task_state.status == TaskStatus.SUCCESS:
                    text_style = "dim"
                elif task_state.status == TaskStatus.FAILURE:
                    text_style = "bold red"

                yield Label(Text(task_state.headline, style=text_style))
                if task_state.status == TaskStatus.RUNNING:
                    yield LoadingIndicator()
                else:
                    yield Label(" " + task_state.status.value)

    def on_mount(self):
        _TASK_LISTENERS.append(self)

    def on_unmount(self):
        _TASK_LISTENERS.remove(self)

    @work
    async def task_updated(self):
        await self.recompose()  # todo make more targeted as to not refresh the full widget


_TASKS: Dict[Any, TaskState] = {}


def on_long_running_task_receives_update():
    for listener in _TASK_LISTENERS:
        listener.task_updated()


class LongRunningTaskContext(ContextManager, TaskLogger):
    def __init__(self, headline: str):
        self.id = object()
        self.task_state = TaskState(self.id, headline)
        _TASKS[self.id] = self.task_state

    def __enter__(self):
        self.task_state.status = TaskStatus.RUNNING
        on_long_running_task_receives_update()
        return self

    def __exit__(self, exc_type, exc_value, traceback, /):
        if exc_type is None:
            self.task_state.status = TaskStatus.SUCCESS
        else:
            self.task_state.status = TaskStatus.FAILURE

        on_long_running_task_receives_update()
        return None

    def info(self, *args, **kwargs) -> None:
        self.task_state.logs.append(args[0])
        on_long_running_task_receives_update()

    def debug(self, *args, **kwargs) -> None:
        self.task_state.logs.append(args[0])
        on_long_running_task_receives_update()

    def error(self, *args, **kwargs) -> None:
        self.task_state.logs.append(args[0])
        on_long_running_task_receives_update()

    def warning(self, *args, **kwargs) -> None:
        self.task_state.logs.append(args[0])
        on_long_running_task_receives_update()

    def alive_it(self, it: Collection[T], total: Optional[int] = None, **options: Any) -> Iterable[T]:
        on_long_running_task_receives_update()
        return it  # fixme implement progress

    def alive_bar(self, total: Optional[int] = None, **options: Any) -> ContextManager:
        on_long_running_task_receives_update()

        class do_nothing:  # fixme implement progress
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback, /):
                return None

            def __call__(self, *args, **kwargs):
                logging.debug("alive_bar called", *args, **kwargs)

        return do_nothing()
