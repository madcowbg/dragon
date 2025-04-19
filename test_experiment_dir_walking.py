import logging
import os
import queue
import unittest
from threading import Thread
from typing import AsyncGenerator
from unittest.async_case import IsolatedAsyncioTestCase

import aiofiles.os as aos
from alive_progress import alive_bar

from command.fast_path import FastPosixPath


def _os_walk(path):
    all_files = list()
    with alive_bar() as bar:
        for dirname, dirs, files in os.walk(path):
            for file in files:
                all_files.append(FastPosixPath(dirname).joinpath(file))
                bar()
    return all_files


async def _aiofiles_os_scandir(path):
    all_files = list()
    with alive_bar() as bar:
        async for file in aiofiles_scandir_recursive(path):
            all_files.append(FastPosixPath(file))
            bar()
    return all_files


async def aiofiles_scandir_recursive(path: str) -> AsyncGenerator[str]:
    with await aos.scandir(path) as iterator:
        for item in iterator:
            if item.is_dir():
                async for sub in aiofiles_scandir_recursive(item.path):
                    yield sub
            elif item.is_file():
                yield item.path


def _os_scandir(root: str):
    all_files = list()
    with alive_bar() as bar:
        q = queue.Queue()
        q.put(root)

        while not q.empty():
            process_item(all_files, bar, q)

    return all_files

def process_item(all_files, bar, q):
    path = q.get(block=True)
    try:
        with os.scandir(path) as iterator:
            for item in iterator:
                if item.is_dir():
                    q.put(item.path)
                elif item.is_file():
                    all_files.append(item.path)
                    bar()
    except OSError:
        logging.error("skipping %s", path)
    finally:
        q.task_done()


def _os_scandir_parallel(root: str, tasks=10):
    all_files = list()
    with alive_bar() as bar:
        q = queue.Queue()
        q.put(root)

        def thread_work():
            while True:
                try:
                    path = q.get(block=True)
                except queue.ShutDown:
                    return
                except Exception as e:
                    q.shutdown()
                    logging.error(e)
                    return
                try:
                    with os.scandir(path) as iterator:
                        for item in iterator:
                            if item.is_dir():
                                q.put_nowait(item.path)
                            elif item.is_file():
                                all_files.append(item.path)
                                bar()
                except OSError:
                    logging.error("skipping %s", item.path)
                finally:
                    q.task_done()

        threads = [Thread(target=thread_work) for _ in range(tasks)]
        for thread in threads:
            thread.start()

        q.join()
        q.shutdown()

        for thread in threads:
            thread.join()

    return all_files


@unittest.skipUnless(os.getenv('MYPROJECT_DEVELOPMENT_TEST'), reason="Lengthy test")
class ExperimentDirWalking(IsolatedAsyncioTestCase):
    def test_list_all_files_local_os_walk(self):
        path = r"C:\Users\Bono\Cloud-Drive"
        all_files = _os_walk(path)

        print(f"os.walk - found {len(all_files)} files in {path}!")

    def test_list_all_files_local_os_scandir(self):
        path = r"C:\Users\Bono\Cloud-Drive"
        all_files = _os_scandir(path)

        print(f"os.scandir - found {len(all_files)} files in {path}!")

    def test_list_all_files_local_os_scandir_parallel(self):
        path = r"C:\Users\Bono\Cloud-Drive"
        all_files = _os_scandir_parallel(path, 30)

        print(f"os.scandir[parallel] - found {len(all_files)} files in {path}!")

    def test_list_all_files_NAS_os_walk(self):
        path = r"X:\Cloud-Drive"
        all_files = _os_walk(path)

        print(f"os.walk - found {len(all_files)} files in {path}!")

    def test_list_all_files_NAS_os_scandir(self):
        path = r"X:\Cloud-Drive"
        all_files = _os_scandir(path)

        print(f"os.scandir - found {len(all_files)} files in {path}!")

    def test_list_all_files_NAS_os_scandir_parallel(self):
        path = r"X:\Cloud-Drive"
        all_files = _os_scandir_parallel(path, 30)

        print(f"os.scandir[parallel] - found {len(all_files)} files in {path}!")

    async def test_list_all_files_local_aiofiles_os_walk(self):
        path = r"C:\Users\Bono\Cloud-Drive"
        all_files = await _aiofiles_os_scandir(path)

        print(f"os.walk - found {len(all_files)} files in {path}!")

