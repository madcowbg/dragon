import asyncio
import hashlib
import os
from asyncio import Queue, TaskGroup
from typing import Set, Dict, List

import aiofiles
from alive_progress import alive_bar

def fast_hash(fullpath: str, chunk_size: int = 1 << 16) -> str:
    return asyncio.run(fast_hash_async(fullpath, chunk_size))

async def fast_hash_async(fullpath: str, chunk_size: int = 1 << 16) -> str:
    async with aiofiles.open(fullpath, "rb") as f:
        await f.seek(0, os.SEEK_END)
        size = await f.tell()
        file_data = str(size).encode("utf-8")

        if size <= 3 * chunk_size:
            await f.seek(0)
            file_data += await f.read()
        else:
            await f.seek(0)
            file_data += await f.read(chunk_size)
            await f.seek(size // 2 - chunk_size // 2)
            file_data += await f.read(chunk_size)
            await f.seek(size - chunk_size)
            file_data += await f.read(chunk_size)
    return hashlib.md5(file_data).hexdigest()


async def find_hashes(filenames: List[str]) -> Dict[str, str]:
    file_hashes: Dict[str, str] = dict()

    queue: Queue[str] = asyncio.Queue()
    for f in filenames:
        queue.put_nowait(f)

    with alive_bar(len(filenames)) as bar:
        async def run_queue():
            while not queue.empty():
                fullpath = await queue.get()
                file_hashes[fullpath] = await fast_hash_async(fullpath)
                queue.task_done()
                bar()

        async with TaskGroup() as tg:
            for _ in range(10):
                tg.create_task(run_queue())

            await queue.join()

    return file_hashes


def calc_file_md5(path: str) -> str:
    hasher = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 23), b''):
            hasher.update(chunk)
    return hasher.hexdigest()
