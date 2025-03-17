import asyncio
from asyncio import Queue, TaskGroup
from typing import List, Tuple, Any, Callable, Coroutine, Dict, TypeVar


def format_size(size: int) -> str:
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
    queue: Queue[Tuple[int, Any]] = asyncio.Queue()
    for i, item in enumerate(args):
        queue.put_nowait((i, item))

    result_per_invocation: Dict[int, Any] = dict()

    async def process_item():
        while not queue.empty():
            idx, input_tuple = await queue.get()
            result = await fun(*input_tuple)
            queue.task_done()

            assert idx not in result_per_invocation
            result_per_invocation[idx] = result

    async def run_all():
        async with TaskGroup() as tg:
            for _ in range(ntasks):
                tg.create_task(process_item())

            await queue.join()

    asyncio.run(run_all())

    return [v for i, v in sorted(result_per_invocation.items())]
