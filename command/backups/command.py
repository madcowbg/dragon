import logging
from io import StringIO
from itertools import groupby
from typing import Dict, Tuple, Callable

from alive_progress import alive_it

from command.content_prefs import BackupSet
from command.hoard import Hoard
from command.pathing import HoardPathing
from contents.hoard import HoardContents
from contents.props import FileStatus, DirProps
from util import format_size


class HoardCommandBackups:
    def __init__(self, hoard: Hoard):
        self.hoard = hoard

    def health(self):
        logging.info("Loading config")
        config = self.hoard.config()
        pathing = HoardPathing(config, self.hoard.paths())

        logging.info(f"Loading hoard...")
        with HoardContents.load(self.hoard.hoard_contents_filename()) as hoard:
            backup_sets = BackupSet.all(config, pathing)
            count_backup_media = sum(len(b.backups) for b in backup_sets)

            with StringIO() as out:
                out.write(f"# backup sets: {len(backup_sets)}\n")
                out.write(f"# backups: {count_backup_media}\n")

                print("Iterating over hoard files")

                file_sizes: Dict[str, int] = dict()
                file_stats_copies: Dict[str, Tuple[int, int, int, int]] = dict()
                for hoard_file, hoard_props in alive_it(hoard.fsobjects):
                    if isinstance(hoard_props, DirProps):
                        continue

                    file_sizes[hoard_file] = hoard_props.size
                    scheduled = 0
                    for backup_set in backup_sets:
                        scheduled += len(backup_set.currently_scheduled_backups(hoard_file, hoard_props))

                    available = len(hoard_props.by_status(FileStatus.AVAILABLE))
                    get_or_copy = len(hoard_props.by_statuses(FileStatus.GET, FileStatus.COPY))
                    cleanup = len(hoard_props.by_status(FileStatus.CLEANUP))

                    file_stats_copies[hoard_file] = (scheduled, available, get_or_copy, cleanup)

                def pivot_stat(stat_idx, fun: Callable[[str], int]) -> Dict[int, int]:
                    return dict(
                        (stat_value, sum(fun(stat_file_tuple[1]) for stat_file_tuple in files))
                        for stat_value, files in groupby(
                            sorted(map(
                                lambda file_fstats: (file_fstats[1][stat_idx], file_fstats[0]),
                                file_stats_copies.items())),
                            lambda stat_file_tuple: stat_file_tuple[0],
                        ))

                for idx, name in [(0, "scheduled"), (1, "available"), (2, "get_or_copy"), (3, "cleanup")]:
                    out.write(f"{name} count:\n")
                    sizes = pivot_stat(idx, lambda f: file_sizes[f])
                    for num_copies, cnt in sorted(pivot_stat(idx, lambda _: 1).items(), key=lambda x: -x[1]):
                        size = sizes[num_copies]
                        out.write(f" {num_copies}: {cnt} files ({format_size(size)})\n")

                out.write("DONE")
                return out.getvalue()
