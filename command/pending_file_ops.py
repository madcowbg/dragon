import enum

from command.fast_path import FastPosixPath
from typing import Iterable, List, Tuple

from contents.hoard import HoardContents, MovesAndCopies
from contents.hoard_props import HoardFileStatus, HoardFileProps
from lmdb_storage.file_object import FileObject
from lmdb_storage.tree_iteration import zip_dfs, DiffType
from lmdb_storage.tree_object import ObjectType, StoredObject

type FileOp = GetFile | CopyFile | CleanupFile | MoveFile | RetainFile


class GetFile:  # fixme needs to know if we would fetch or update a current file
    def __init__(self, hoard_file: FastPosixPath, hoard_props: HoardFileProps):
        assert hoard_file.is_absolute()
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props


class CopyFile:
    def __init__(self, hoard_file: FastPosixPath, hoard_props: HoardFileProps):
        assert hoard_file.is_absolute()
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props


class MoveFile:
    def __init__(
            self, hoard_file: FastPosixPath, hoard_props: HoardFileProps, old_hoard_file: str,
            old_hoard_props: HoardFileProps):
        assert hoard_file.is_absolute()
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props
        self.old_hoard_file = old_hoard_file
        self.old_hoard_props = old_hoard_props


class CleanupFile:
    def __init__(self, hoard_file: FastPosixPath, hoard_props: HoardFileProps):
        assert hoard_file.is_absolute()
        self.hoard_file = hoard_file
        self.hoard_props = hoard_props

class RetainFile:
    def __init__(self, hoard_file: FastPosixPath, file_obj: FileObject, needed_locations: List[str]):
        assert hoard_file.is_absolute()
        self.hoard_file = hoard_file
        self.file_obj = file_obj
        self.needed_locations = needed_locations


def DEPRECATED_get_pending_operations(hoard: HoardContents, repo_uuid: str, moves_and_copies: MovesAndCopies) -> Iterable[FileOp]:
    for hoard_file, hoard_props in hoard.fsobjects.with_pending(repo_uuid):
        goal_status = hoard_props.get_status(repo_uuid)
        if goal_status == HoardFileStatus.GET:
            yield GetFile(hoard_file, hoard_props)
        elif goal_status == HoardFileStatus.CLEANUP:
            file_obj = HACK_create_from_hoard_props(hoard_props)
            moves_and_copies_loc = dict(moves_and_copies.whereis_needed(file_obj.file_id))
            if len(moves_and_copies_loc) > 0:
                yield RetainFile(hoard_file, file_obj, list(moves_and_copies_loc.keys()))
            else:
                yield CleanupFile(hoard_file, hoard_props)
        else:
            raise ValueError(f"File {hoard_file} has no pending ops, yet was selected as one that has.")


class FileOpType(enum.Enum):
    FETCH = 1

    CLEANUP = 9
    RETAIN = 10


def get_pending_operations(
        hoard: HoardContents, repo_uuid: str,
        moves_and_copies: MovesAndCopies) -> Iterable[Tuple[FileOpType, FastPosixPath, FileObject]]:
    repo_root = hoard.env.roots(write=False)[repo_uuid]

    with hoard.env.objects(write=False) as objects:
        for hoard_path, diff_type, current_obj_id, desired_obj_id, _ in zip_dfs(
                objects, '', repo_root.current, repo_root.desired, drilldown_same=False):


            if diff_type == DiffType.LEFT_MISSING or diff_type == DiffType.DIFFERENT:
                desired_obj: StoredObject = objects[desired_obj_id]
                if desired_obj.object_type == ObjectType.TREE:
                    continue
                desired_obj: FileObject

                yield FileOpType.FETCH, FastPosixPath(hoard_path), desired_obj
            elif diff_type == DiffType.RIGHT_MISSING:
                current_obj: StoredObject = objects[current_obj_id]
                if current_obj.object_type == ObjectType.TREE:
                    continue
                current_obj: FileObject

                moves_and_copies_loc = dict(moves_and_copies.whereis_needed(current_obj.file_id))
                if len(moves_and_copies_loc) > 0:
                    yield FileOpType.RETAIN, FastPosixPath(hoard_path), current_obj
                else:
                    yield FileOpType.CLEANUP, FastPosixPath(hoard_path), current_obj
            elif diff_type != DiffType.SAME:
                raise ValueError(f"File {hoard_path} has unrecognized diff type {diff_type}")


def HACK_create_from_hoard_props(hoard_props: HoardFileProps) -> FileObject:
    return FileObject.create(hoard_props.fasthash, hoard_props.size, None)
