import logging
from io import StringIO
from typing import Optional, List, Dict

from alive_progress import alive_bar

from command.contents.command import dump_remotes
from command.files.file_operations import _fetch_files_in_repo, _cleanup_files_in_repo
from command.hoard import Hoard
from command.pathing import HoardPathing
from command.pending_file_ops import get_pending_operations, FileOpType
from config import HoardConfig
from contents.hoard import MovesAndCopies
from lmdb_storage.deferred_operations import HoardDeferredOperations
from resolve_uuid import resolve_remote_uuid
from task_logging import PythonLoggingTaskLogger, TaskLogger


class HoardCommandFiles:
    def __init__(self, hoard: Hoard):
        self.hoard = hoard

    async def pending(self, repo: Optional[str] = None):
        config = self.hoard.config()

        repo_uuids: List[str] = [resolve_remote_uuid(config, repo)] \
            if repo is not None else [r.uuid for r in config.remotes.all()]

        logging.info(f"Loading hoard contents...")
        with self.hoard.open_contents(create_missing=False) as hoard:
            with StringIO() as out:
                for repo_uuid in repo_uuids:
                    logging.info(f"Iterating over pending ops in {repo_uuid}")
                    out.write(f"{config.remotes[repo_uuid].name}:\n")

                    moves_and_copies = MovesAndCopies(hoard)
                    repos_containing_what_this_one_needs: Dict[str, int] = dict()

                    for op_type, hoard_path, file_obj in get_pending_operations(hoard, repo_uuid, moves_and_copies):
                        num_available = dict(moves_and_copies.get_remote_copies(repo_uuid, file_obj.file_id))
                        if op_type == FileOpType.FETCH:
                            out.write(f"TO_GET (from {len(num_available)}) {hoard_path.as_posix()}\n")
                            for repo in num_available:
                                repos_containing_what_this_one_needs[repo] = \
                                    repos_containing_what_this_one_needs.get(repo, 0) + 1
                        # elif isinstance(op, CopyFile):
                        #     out.write(f"TO_COPY (from {len(num_available)}+?) {hoard_path.as_posix()}\n")
                        #     for repo in num_available:
                        #         repos_containing_what_this_one_needs[repo] = \
                        #             repos_containing_what_this_one_needs.get(repo, 0) + 1
                        # elif isinstance(op, MoveFile):
                        #     out.write(f"TO_MOVE {op.hoard_file.as_posix()} from {op.old_hoard_file}\n")
                        elif op_type == FileOpType.CLEANUP:
                            out.write(f"TO_CLEANUP (is in {len(num_available)}) {hoard_path.as_posix()}\n")
                        elif op_type == FileOpType.RETAIN:
                            needed_locations = dict(moves_and_copies.whereis_needed(file_obj.file_id))
                            out.write(f"TO_RETAIN (needed in {len(needed_locations)} [{', '.join(config.remotes[uuid].name for uuid in needed_locations)}]) {hoard_path.as_posix()}\n")
                        else:
                            raise ValueError(f"Unhandled op type: {op_type}")
                    nc = sorted(map(
                        lambda uc: (config.remotes[uc[0]].name, uc[1]),  # uuid, count -> name, count
                        repos_containing_what_this_one_needs.items()))
                    for name, count in nc:
                        out.write(f" {name} has {count} files\n")
                out.write("DONE")
                return out.getvalue()

    async def push(self, repo: Optional[str] = None, all: bool = False):
        config = self.hoard.config()
        if all:
            if repo is not None:
                return f"Error: can't use --all and --repo={repo} at the same time."
            repo_uuids: List[str] = [r.uuid for r in config.remotes.all()]
        else:
            if repo is None:
                return f"Error: Need either --repo=REPO or --all."
            repo_uuids = [resolve_remote_uuid(config, repo)]

        logging.info(f"Loading hoard contents...")

        with StringIO() as out:
            await execute_files_push(config, self.hoard, repo_uuids, out, PythonLoggingTaskLogger())

            out.write("DONE")
            return out.getvalue()


async def execute_files_push(config: HoardConfig, hoard: Hoard, repo_uuids: List[str], out: StringIO, task_logger: TaskLogger):
    pathing = HoardPathing(config, hoard.paths())
    with hoard.open_contents(False).writeable() as hoard_contents:
        out.write(f"Before push:\n")
        dump_remotes(config, hoard_contents, out)

        moves_and_copies_before_fetching = MovesAndCopies(hoard_contents)
        task_logger.info("try getting all requested files, per repo")

        task_logger.info("Finding files that need copy, for easy lookup")
        for repo_uuid in repo_uuids:
            task_logger.info(f"fetching for {config.remotes[repo_uuid].name}")
            out.write(f"{config.remotes[repo_uuid].name}:\n")

            await _fetch_files_in_repo(moves_and_copies_before_fetching, hoard_contents, repo_uuid, pathing, out, task_logger.alive_bar)

        task_logger.info("Applying deferred operations after potentially getting a lot of files.")
        HoardDeferredOperations(hoard_contents).apply_deferred_queue()

        task_logger.info("Finding files that need copy - will not cleanup them!")
        moves_and_copies_for_cleanup = MovesAndCopies(hoard_contents)
        task_logger.info("try cleaning unneeded files, per repo")
        for repo_uuid in repo_uuids:
            task_logger.info(f"cleaning repo {config.remotes[repo_uuid].name}")
            out.write(f"{config.remotes[repo_uuid].name}:\n")

            _cleanup_files_in_repo(moves_and_copies_for_cleanup, hoard_contents, repo_uuid, pathing, out, task_logger.alive_bar)

        task_logger.info("Applying deferred operations after cleanup.")
        HoardDeferredOperations(hoard_contents).apply_deferred_queue()

        out.write(f"After:\n")
        dump_remotes(config, hoard_contents, out)
