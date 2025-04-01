import asyncio
import os
import threading
from asyncio import Queue, TaskGroup
from itertools import groupby
from sqlite3 import Cursor, Row
from typing import List, Tuple, Any, Callable, Coroutine, Dict, TypeVar, Iterable
import queue

COUNT_KILO, COUNT_MEGA, COUNT_GIGA, COUNT_TERA = 10 ** 3, 10 ** 6, 10 ** 9, 10 ** 12


def format_count(count: int) -> str:
    abs_count = abs(count)
    if abs_count < COUNT_KILO:
        return str(count)
    elif abs_count < COUNT_MEGA:
        return f"{count / COUNT_KILO:.1f}K"
    elif abs_count < COUNT_GIGA:
        return f"{count / COUNT_MEGA:.1f}M"
    elif abs_count < COUNT_TERA:
        return f"{count / COUNT_GIGA:.1f}G"
    else:
        return f"{count / COUNT_TERA:.1f}T"


def format_size(size: int | None) -> str:
    if size < 2 ** 10:
        return f"{size}"
    elif size < 2 ** 20:
        return f"{size / 2 ** 10:.1f}KB"
    elif size < 2 ** 30:
        return f"{size / 2 ** 20:.1f}MB"
    elif size < 2 ** 40:
        return f"{size / 2 ** 30:.1f}GB"
    elif size < 2 ** 50:
        return f"{size / 2 ** 40:.1f}TB"
    else:
        return f"{size / 2 ** 50:.1f}PB"


def to_mb(size: int) -> int:
    return size // (1 << 20)


R = TypeVar('R')


def run_async_in_parallel(
        args: List[Tuple[Any, ...]], fun: Callable[[Any, ...], Coroutine[Any, Any, R]], ntasks: int = 10) -> List[R]:
    q: Queue[Tuple[int, Any]] = asyncio.Queue()
    for i, item in enumerate(args):
        q.put_nowait((i, item))

    result_per_invocation: Dict[int, Any] = dict()

    async def process_item():
        while not q.empty():
            idx, input_tuple = await q.get()
            result = await fun(*input_tuple)
            q.task_done()

            assert idx not in result_per_invocation
            result_per_invocation[idx] = result

    async def run_all():
        async with TaskGroup() as tg:
            for _ in range(ntasks):
                tg.create_task(process_item())

            await q.join()

    asyncio.run(run_all())

    return [v for i, v in sorted(result_per_invocation.items())]


def run_in_parallel_threads(
        args: List[Tuple[Any, ...]], fun: Callable[[Any, ...], R], ntasks: int = 10) -> List[R]:
    q: queue.SimpleQueue[Tuple[int, Any]] = queue.SimpleQueue()
    for i, item in enumerate(args):
        q.put_nowait((i, item))

    result_per_invocation: Dict[int, Any] = dict()

    def process_item():
        while not q.empty():
            idx, input_tuple = q.get_nowait()
            result = fun(*input_tuple)

            assert idx not in result_per_invocation
            result_per_invocation[idx] = result

    threads = [threading.Thread(target=process_item) for _ in range(ntasks)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    return [v for i, v in sorted(result_per_invocation.items())]


FIRST_VALUE: Callable[[Cursor, Row], Any] = lambda cursor, row: row[0] if row is not None else None


def format_percent(num: float): return f"{100 * num:.1f}%"


T = TypeVar('T')
C = TypeVar('C')
U = TypeVar('U')


def group_to_dict(
        objs: Iterable[T], key: Callable[[T], R],
        map_to: Callable[[T], U] = (lambda x: x),
        order_by: Callable[[T], C] = str) -> Dict[R, List[U]]:
    """ Produces map of keys to lists of objects."""
    return dict(
        (obj_key, list(map(map_to, some_objects)))
        for obj_key, some_objects in groupby(sorted(objs, key=lambda o: order_by(key(o))), key=key))


def custom_isabs(folder: str): return folder.startswith("/") or os.path.isabs(folder)  # for 3.13 change in os.path


def pretty_truncate(text: str, size: int) -> str:
    assert size >= 5
    if len(text) <= size:
        return text
    left_len = (size - 3) // 2
    return text[:left_len] + "..." + text[-(size - 3 - left_len):]
