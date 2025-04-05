import asyncio
import logging
import threading
from asyncio import Queue
from io import StringIO
from pathlib import Path
from threading import Thread
from time import sleep
from typing import List

import fire
from watchdog.events import FileSystemEventHandler, FileSystemEvent, DirModifiedEvent
from watchdog.observers import Observer

from command.comparison_repo import find_repo_changes, \
    compute_difference_filtered_by_path, compute_changes_from_diffs, _apply_repo_change_to_contents
from command.fast_path import FastPosixPath
from command.hoard_ignore import HoardIgnore, DEFAULT_IGNORE_GLOBS
from command.repo import ProspectiveRepo, ConnectedRepo
from contents.repo_props import RepoFileStatus


class RepoWatcher(FileSystemEventHandler):
    def __init__(self, hoard_path: FastPosixPath, hoard_ignore: HoardIgnore):
        self.hoard_path = hoard_path
        self.hoard_ignore = hoard_ignore

        self.queue: Queue[str] = Queue()
        self.queue_contents = set[str]()

        self.lock = threading.Lock()

    def on_any_event(self, event: FileSystemEvent):
        if isinstance(event, DirModifiedEvent):
            logging.debug("skipping directory event: %s", event)
            return

        logging.debug("processing event: %s", event)

        self.add_file_or_folder(event.src_path)
        self.add_file_or_folder(event.dest_path)
        logging.debug(f"# queue contents: {len(self.queue_contents)}")

    def add_file_or_folder(self, path):
        if path == '':
            return

        src_path = FastPosixPath(Path(path).absolute())
        with self.lock:
            if src_path in self.queue_contents:
                return

            rel_path = src_path.relative_to(self.hoard_path)
            if self.hoard_ignore.matches(rel_path):
                logging.debug(f"Skipping {src_path} as it is in hoard ignore.")
                return

            logging.info("add %s", src_path)

            self.queue_contents.add(path)
            self.queue.put_nowait(path)


async def updater(
        watcher: RepoWatcher, connected_repo: ConnectedRepo, hoard_ignore: HoardIgnore,
        sleep_interval: int = 10, between_runs_interval: int = 1):
    logging.info("Start updating!")
    while True:
        logging.debug("Getting current queue...")
        allowed_paths: List[str] = []
        while not watcher.queue.empty():
            with watcher.lock:
                item = watcher.queue.get_nowait()
                watcher.queue.task_done()
                allowed_paths.append(item)
                if item in watcher.queue_contents:
                    watcher.queue_contents.remove(item)

        if len(allowed_paths) == 0:
            logging.debug("No items to check, sleeping for %r seconds", sleep_interval)
            sleep(sleep_interval)
            continue

        # now we have a batch, process it
        logging.info(f"Working on {len(allowed_paths)} items")

        with connected_repo.open_contents(is_readonly=False) as contents:
            logging.info("Start updating, setting is_dirty to TRUE")
            contents.config.start_updating()

            logging.info(f"Bumped epoch to {contents.config.epoch}")

            with StringIO() as out:
                diffs = compute_difference_filtered_by_path(contents, connected_repo.path, hoard_ignore, allowed_paths)

                for change in compute_changes_from_diffs(diffs, connected_repo.path, RepoFileStatus.ADDED):
                    _apply_repo_change_to_contents(change, contents, False, out)

                logging.info(out.getvalue())

            logging.info("Ends updating, setting is_dirty to FALSE")
            contents.config.end_updating()
            assert not contents.config.is_dirty

        logging.debug("Sleeping between runs for %r seconds", between_runs_interval)
        sleep(between_runs_interval)

    logging.info("Ending updating!")


def run_daemon(path: str, assume_current: bool = False):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(funcName)20s() - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        force=True)

    repo_path: Path = Path(path).absolute()
    logging.info(f"Starting repo watcher in {repo_path}")
    assert repo_path.is_dir()

    repo = ProspectiveRepo(repo_path.as_posix())
    hoard_ignore: HoardIgnore = HoardIgnore(DEFAULT_IGNORE_GLOBS)
    event_handler = RepoWatcher(FastPosixPath(repo.path), hoard_ignore)

    observer = Observer()
    observer.schedule(event_handler, repo_path, recursive=True)
    observer.start()

    try:
        connected_repo = repo.open_repo().connect(require_contents=True)

        if not assume_current:
            asyncio.run(refresh_all(connected_repo, hoard_ignore))

        def run_updater():
            asyncio.run(updater(event_handler, connected_repo, hoard_ignore))

        updater_thread = Thread(target=run_updater, daemon=True)
        updater_thread.start()

        while observer.is_alive():
            observer.join(1)
    finally:
        observer.stop()
        observer.join()


async def refresh_all(connected_repo, hoard_ignore):
    with connected_repo.open_contents(is_readonly=False) as contents:
        logging.info("Start updating, setting is_dirty to TRUE")
        contents.config.start_updating()

        logging.info(f"Bumped epoch to {contents.config.epoch}")
        with StringIO() as out:
            for change in await find_repo_changes(
                    connected_repo.path, contents, hoard_ignore, RepoFileStatus.ADDED, skip_integrity_checks=False):
                _apply_repo_change_to_contents(change, contents, False, out)
            logging.info(out.getvalue())

        logging.info("Ends updating, setting is_dirty to FALSE")
        contents.config.end_updating()
        assert not contents.config.is_dirty


if __name__ == '__main__':
    fire.Fire(run_daemon)
