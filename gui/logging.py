from logging import LogRecord, Formatter
from logging.handlers import QueueHandler
from queue import Queue
from typing import List

from rich.logging import RichHandler
from textual.widgets import RichLog

MAX_LOG_SIZE = 1000
LOGGING_QUEUE: Queue[str] = Queue()

FORMATTER = Formatter(fmt='%(asctime)s - %(funcName)20s() - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


class PythonLoggingWidget(RichLog):
    def __init__(self):
        super().__init__(id="logging-widget")

    def on_mount(self):
        for e in LOGGING_QUEUE.queue:
            self.write(FORMATTER.format(e))

        listeners.append(self)

    def on_unmount(self):
        listeners.remove(self)

listeners: List[PythonLoggingWidget] = list()


class RichPrintHandler(QueueHandler):
    def emit(self, record: LogRecord) -> None:
        while LOGGING_QUEUE.qsize() > MAX_LOG_SIZE:
            LOGGING_QUEUE.get()

        super().emit(record)

        # log_entry = self.format(record)

        for l in listeners:
            l.write(FORMATTER.format(record))


PRINT_HANDLER = RichPrintHandler(LOGGING_QUEUE)
