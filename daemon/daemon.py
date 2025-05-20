import asyncio
import logging
import threading
from io import StringIO
from pathlib import Path, PurePosixPath

import fire
from watchdog.events import FileSystemEventHandler, DirModifiedEvent, FileModifiedEvent, DirCreatedEvent, \
    FileCreatedEvent, DirDeletedEvent, FileDeletedEvent, DirMovedEvent, FileMovedEvent
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

        self._queue: set[PurePosixPath] = set()

        self.lock = threading.Lock()

    def on_modified(self, event: DirModifiedEvent | FileModifiedEvent):
        if isinstance(event, FileModifiedEvent):
            logging.info("processing modified: %s", event)
            self.add_file_or_folder(event.src_path)

    def on_created(self, event: DirCreatedEvent | FileCreatedEvent) -> None:
        if isinstance(event, FileCreatedEvent):
            logging.info("processing create: %s", event)
            self.add_file_or_folder(event.src_path)

    def on_deleted(self, event: DirDeletedEvent | FileDeletedEvent) -> None:
        if isinstance(event, FileDeletedEvent):
            logging.info("processing delete: %s", event)
            self.add_file_or_folder(event.src_path)

    def on_moved(self, event: DirMovedEvent | FileMovedEvent) -> None:
        if isinstance(event, FileMovedEvent):
            logging.info("processing move: %s", event)
            self.add_file_or_folder(event.src_path)
            self.add_file_or_folder(event.dest_path)

    def add_file_or_folder(self, path: str):
        if path == '':
            return

        src_path = PurePosixPath(Path(path).absolute())
        with self.lock:
            if src_path in self._queue:
                return

            rel_path = src_path.relative_to(self.hoard_path)
            logging.debug(f"Considering relative path {rel_path}...")
            if self.hoard_ignore.matches(rel_path):
                logging.debug(f"Skipping {src_path} as it is in hoard ignore.")
                return

            logging.info("Add %s as touched", src_path)

            self._queue.add(src_path)

    def pop_queue(self) -> set[PurePosixPath]:
        with self.lock:
            current = self._queue
            self._queue = set()
            return current


async def updater(
        watcher: RepoWatcher, connected_repo: ConnectedRepo, hoard_ignore: HoardIgnore,
        sleep_interval: float, between_runs_interval: float):
    logging.info("Start updating!")
    while True:
        logging.debug("Getting current queue...")

        allowed_paths: list[PurePosixPath] = list(watcher.pop_queue())
        if len(allowed_paths) == 0:
            logging.debug("No items to check, sleeping for %r seconds", sleep_interval)
            await asyncio.sleep(sleep_interval)
            continue

        # now we have a batch, process it
        logging.info(f"Working on {len(allowed_paths)} items")

        with connected_repo.open_contents(is_readonly=False) as contents:
            logging.info("Start updating, setting is_dirty to TRUE")
            contents.config.start_updating()

            logging.info(f"Bumped epoch to {contents.config.epoch}")

            with StringIO() as out:
                diffs = compute_difference_filtered_by_path(contents, connected_repo.path, hoard_ignore, allowed_paths)

                async for change in compute_changes_from_diffs(diffs, connected_repo.path, RepoFileStatus.PRESENT):
                    _apply_repo_change_to_contents(change, contents, False, out)

                logging.info(out.getvalue())

            logging.info("Ends updating, setting is_dirty to FALSE")
            contents.config.end_updating()
            assert not contents.config.is_dirty

        logging.debug("Sleeping between runs for %r seconds", between_runs_interval)
        await asyncio.sleep(between_runs_interval)

    logging.info("Ending updating!")


async def run_daemon(path: str, assume_current: bool = False, sleep_interval: float = 10,
                     between_runs_interval: float = 1):
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
            await refresh_all(connected_repo, hoard_ignore)

        updater_task = asyncio.create_task(updater(
            event_handler, connected_repo, hoard_ignore, sleep_interval, between_runs_interval))

        try:
            def wait_for_observer_to_stop():
                while observer.is_alive():
                    observer.join(1)

            await asyncio.get_event_loop().run_in_executor(None, wait_for_observer_to_stop)
        finally:
            updater_task.cancel("Observer has exited!")
    finally:
        observer.stop()
        observer.join()


async def refresh_all(connected_repo, hoard_ignore):
    with connected_repo.open_contents(is_readonly=False) as contents:
        logging.info("Start updating, setting is_dirty to TRUE")
        contents.config.start_updating()

        logging.info(f"Bumped epoch to {contents.config.epoch}")
        with StringIO() as out:
            async for change in find_repo_changes(connected_repo.path, contents, hoard_ignore, RepoFileStatus.PRESENT):
                _apply_repo_change_to_contents(change, contents, False, out)
            logging.info(out.getvalue())

        logging.info("Ends updating, setting is_dirty to FALSE")
        contents.config.end_updating()
        assert not contents.config.is_dirty


if __name__ == '__main__':
    fire.Fire(run_daemon)
