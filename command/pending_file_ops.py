from typing import Iterable

from contents.hoard import HoardContents
from contents.props import HoardFileProps, FileStatus

type FileOp = GetFile | CopyFile | CleanupFile


class GetFile:  # fixme needs to know if we would fetch or update a current file
    def __init__(self, hoard_file: str, hoard_props: HoardFileProps):
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props


class CopyFile:
    def __init__(self, hoard_file: str, hoard_props: HoardFileProps):
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props


class CleanupFile:
    def __init__(self, hoard_file: str, hoard_props: HoardFileProps):
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props


def get_pending_operations(hoard: HoardContents, repo_uuid: str) -> Iterable[FileOp]:
    for hoard_file, hoard_props in hoard.fsobjects.with_pending(repo_uuid):
        goal_status = hoard_props.get_status(repo_uuid)
        if goal_status == FileStatus.GET:
            yield GetFile(hoard_file, hoard_props)
        elif goal_status == FileStatus.COPY:
            yield CopyFile(hoard_file, hoard_props)
        elif goal_status == FileStatus.CLEANUP:
            yield CleanupFile(hoard_file, hoard_props)
        else:
            raise ValueError(f"File {hoard_file} has no pending ops, yet was selected as one that has.")
