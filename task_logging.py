import abc
import logging
from typing import Collection, Any, Optional, Iterable, Callable, ContextManager

from alive_progress import alive_it, alive_bar


class TaskLogger(abc.ABC):
    @abc.abstractmethod
    def info(self, *args, **kwargs) -> None: pass

    @abc.abstractmethod
    def debug(self, *args, **kwargs) -> None: pass

    @abc.abstractmethod
    def error(self, *args, **kwargs) -> None: pass

    @abc.abstractmethod
    def warning(self, *args, **kwargs) -> None: pass

    @abc.abstractmethod
    def alive_it[T](self, it: Collection[T], total: Optional[int] = None, **options: Any) -> Iterable[T]: pass

    @abc.abstractmethod
    def alive_bar(self, total: Optional[int] = None, **options: Any) -> ContextManager:
        pass


class PythonLoggingTaskLogger(TaskLogger):
    def info(self, *args, **kwargs) -> None:
        logging.info(*args, **kwargs)

    def debug(self, *args, **kwargs) -> None:
        logging.debug(*args, **kwargs)

    def error(self, *args, **kwargs) -> None:
        logging.error(*args, **kwargs)

    def warning(self, *args, **kwargs) -> None:
        logging.warning(*args, **kwargs)

    def alive_it[T](self, it: Collection[T], total: Optional[int] = None, **options: Any) -> Iterable[T]:
        return alive_it(it, total, **options)

    def alive_bar(self, total: Optional[int] = None, **options: Any) -> ContextManager:
        return alive_bar(total, **options)
