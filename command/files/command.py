import logging
from io import StringIO

from typing import Optional, List, Dict

from alive_progress import alive_bar

from command.contents.command import clean_dangling_files
from command.files.file_operations import _find_files_to_copy, _fetch_files_in_repo, _cleanup_files_in_repo

from command.hoard import Hoard
from command.pathing import HoardPathing
from command.pending_file_ops import get_pending_operations, CopyFile, GetFile, CleanupFile, MoveFile
from config import HoardConfig
from contents.hoard_props import HoardFileStatus
from resolve_uuid import resolve_remote_uuid


class HoardCommandFiles:
    def __init__(self, hoard: Hoard):
        self.hoard = hoard

    async def pending(self, repo: Optional[str] = None):
        config = self.hoard.config()

        repo_uuids: List[str] = [resolve_remote_uuid(config, repo)] \
            if repo is not None else [r.uuid for r in config.remotes.all()]

        logging.info(f"Loading hoard contents...")
        async with self.hoard.open_contents(create_missing=False, is_readonly=True) as hoard:
            with StringIO() as out:
                for repo_uuid in repo_uuids:
                    logging.info(f"Iterating over pending ops in {repo_uuid}")
                    out.write(f"{config.remotes[repo_uuid].name}:\n")

                    repos_containing_what_this_one_needs: Dict[str, int] = dict()
                    for op in get_pending_operations(hoard, repo_uuid):
                        num_available = op.hoard_props.by_status(HoardFileStatus.AVAILABLE)
                        if isinstance(op, GetFile):
                            out.write(f"TO_GET (from {len(num_available)}) {op.hoard_file.as_posix()}\n")
                            for repo in num_available:
                                repos_containing_what_this_one_needs[repo] = \
                                    repos_containing_what_this_one_needs.get(repo, 0) + 1
                        elif isinstance(op, CopyFile):
                            out.write(f"TO_COPY (from {len(num_available)}+?) {op.hoard_file.as_posix()}\n")
                            for repo in num_available:
                                repos_containing_what_this_one_needs[repo] = \
                                    repos_containing_what_this_one_needs.get(repo, 0) + 1
                        elif isinstance(op, MoveFile):
                            out.write(f"TO_MOVE {op.hoard_file.as_posix()} from {op.old_hoard_file}\n")
                        elif isinstance(op, CleanupFile):
                            out.write(f"TO_CLEANUP (is in {len(num_available)}) {op.hoard_file.as_posix()}\n")
                        else:
                            raise ValueError(f"Unhandled op type: {type(op)}")
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
            await execute_files_push(config, self.hoard, repo_uuids, out, progress_bar=alive_bar)

            out.write("DONE")
            return out.getvalue()


async def execute_files_push(config: HoardConfig, hoard: Hoard, repo_uuids: List[str], out: StringIO, progress_bar):
    pathing = HoardPathing(config, hoard.paths())
    async with hoard.open_contents(False, is_readonly=False) as hoard_contents:
        logging.info("try getting all requested files, per repo")

        logging.info("Finding files that need copy, for easy lookup")
        files_to_copy = _find_files_to_copy(hoard_contents)
        for repo_uuid in repo_uuids:
            logging.info(f"fetching for {config.remotes[repo_uuid].name}")
            out.write(f"{config.remotes[repo_uuid].name}:\n")

            await _fetch_files_in_repo(hoard_contents, repo_uuid, pathing, files_to_copy, out, progress_bar)
        logging.info("Finding files that need copy - will not cleanup them!")
        files_to_copy = _find_files_to_copy(hoard_contents)
        logging.info(f"Found {len(files_to_copy)} hashes to copy, won't cleanup them.")
        logging.info("try cleaning unneeded files, per repo")
        for repo_uuid in repo_uuids:
            logging.info(f"cleaning repo {config.remotes[repo_uuid].name}")
            out.write(f"{config.remotes[repo_uuid].name}:\n")

            await _cleanup_files_in_repo(hoard_contents, config, repo_uuid, pathing, files_to_copy, out, progress_bar)
        clean_dangling_files(hoard_contents, out)
