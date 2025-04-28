import asyncio
import binascii
import os
import threading
from asyncio import TaskGroup, QueueShutDown
from itertools import groupby
from sqlite3 import Cursor, Row
from typing import List, Any, Callable, Coroutine, Dict, TypeVar, Iterable

from lmdb_storage.tree_structure import ObjectID

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


def run_in_separate_loop(coro: Coroutine[Any, Any, R]) -> R:
    value = []
    thread = threading.Thread(target=lambda: value.append(asyncio.run(coro)))
    thread.start()
    thread.join()
    return value[0]


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


async def process_async(data: Iterable[T], func: Callable[[T], Coroutine], njobs):
    async with TaskGroup() as tg_walking:
        q = asyncio.Queue(maxsize=njobs)

        async def fill_queue():
            nonlocal q, data
            for p in data:
                await q.put(p)
            await q.join()
            q.shutdown()

        async def process_queue():
            nonlocal q
            while True:
                try:
                    item = await q.get()
                    try:
                        await func(item)
                    finally:
                        q.task_done()
                except QueueShutDown:
                    return

        for _ in range(njobs):
            tg_walking.create_task(process_queue())
        tg_walking.create_task(fill_queue())


def safe_hex(root_id: ObjectID | None) -> str:
    return binascii.hexlify(root_id).decode() if root_id else "None"
