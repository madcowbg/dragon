from pathlib import PurePosixPath
from typing import Iterable

from contents.hoard import HoardContents
from contents.hoard_props import HoardFileStatus, HoardFileProps

type FileOp = GetFile | CopyFile | CleanupFile | MoveFile


class GetFile:  # fixme needs to know if we would fetch or update a current file
    def __init__(self, hoard_file: PurePosixPath, hoard_props: HoardFileProps):
        assert hoard_file.is_absolute()
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props


class CopyFile:
    def __init__(self, hoard_file: PurePosixPath, hoard_props: HoardFileProps):
        assert hoard_file.is_absolute()
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props


class MoveFile:
    def __init__(
            self, hoard_file: PurePosixPath, hoard_props: HoardFileProps, old_hoard_file: str,
            old_hoard_props: HoardFileProps):
        assert hoard_file.is_absolute()
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props
        self.old_hoard_file = old_hoard_file
        self.old_hoard_props = old_hoard_props


class CleanupFile:
    def __init__(self, hoard_file: PurePosixPath, hoard_props: HoardFileProps):
        assert hoard_file.is_absolute()
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props


def get_pending_operations(hoard: HoardContents, repo_uuid: str) -> Iterable[FileOp]:
    for hoard_file, hoard_props in hoard.fsobjects.with_pending(repo_uuid):
        goal_status = hoard_props.get_status(repo_uuid)
        if goal_status == HoardFileStatus.GET:
            yield GetFile(hoard_file, hoard_props)
        elif goal_status == HoardFileStatus.COPY:
            yield CopyFile(hoard_file, hoard_props)
        elif goal_status == HoardFileStatus.MOVE:
            move_file = hoard_props.get_move_file(repo_uuid)
            move_file_props = hoard.fsobjects[PurePosixPath(move_file)]
            yield MoveFile(hoard_file, hoard_props, move_file, move_file_props)
        elif goal_status == HoardFileStatus.CLEANUP:
            yield CleanupFile(hoard_file, hoard_props)
        else:
            raise ValueError(f"File {hoard_file} has no pending ops, yet was selected as one that has.")
