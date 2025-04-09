import enum

from command.fast_path import FastPosixPath

from contents.repo_props import RepoFileProps
from contents.hoard_props import HoardFileProps

class DiffType(enum.Enum):
    FileOnlyInLocal = enum.auto()
    FileIsSame = enum.auto()
    FileContentsDiffer = enum.auto()
    FileOnlyInHoardLocalDeleted = enum.auto()
    FileOnlyInHoardLocalUnknown = enum.auto()
    FileOnlyInHoardLocalMoved = enum.auto()


class Diff:
    def __init__(self, diff_type: DiffType):
        self.diff_type = diff_type


class FileOnlyInLocal(Diff):
    def __init__(self, local_file: FastPosixPath, curr_file_hoard_path: FastPosixPath, local_props: RepoFileProps, is_added: bool):
        super().__init__(DiffType.FileOnlyInLocal)
        assert not local_file.is_absolute()
        assert curr_file_hoard_path.is_absolute()
        self.local_file = local_file
        self.hoard_file = curr_file_hoard_path
        self.local_props = local_props

        self.is_added = is_added

class FileIsSame(Diff):
    def __init__(self, current_file: FastPosixPath, curr_file_hoard_path: FastPosixPath, local_props: RepoFileProps,
                 hoard_props: HoardFileProps):
        super().__init__(DiffType.FileIsSame)
        assert not current_file.is_absolute()
        assert curr_file_hoard_path.is_absolute()
        self.local_file = current_file
        self.hoard_file = curr_file_hoard_path
        self.local_props = local_props
        self.hoard_props = hoard_props


class FileContentsDiffer(Diff):
    def __init__(
            self, current_file: FastPosixPath, curr_file_hoard_path: FastPosixPath,
            local_props: RepoFileProps, hoard_props: HoardFileProps):
        super().__init__(DiffType.FileContentsDiffer)
        assert not current_file.is_absolute()
        assert curr_file_hoard_path.is_absolute()
        self.local_file = current_file
        self.hoard_file = curr_file_hoard_path
        self.local_props = local_props
        self.hoard_props = hoard_props


class FileOnlyInHoardLocalDeleted(Diff):
    def __init__(
            self, current_file: FastPosixPath, curr_file_hoard_path: FastPosixPath,
            hoard_props: HoardFileProps, local_props: RepoFileProps):
        super().__init__(DiffType.FileOnlyInHoardLocalDeleted)
        assert not current_file.is_absolute()
        assert curr_file_hoard_path.is_absolute()
        self.local_file = current_file
        self.local_props = local_props

        self.hoard_file = curr_file_hoard_path
        self.hoard_props = hoard_props


class FileOnlyInHoardLocalUnknown(Diff):
    def __init__(self, current_file: FastPosixPath, curr_file_hoard_path: FastPosixPath, hoard_props: HoardFileProps):
        super().__init__(DiffType.FileOnlyInHoardLocalUnknown)
        assert not current_file.is_absolute()
        assert curr_file_hoard_path.is_absolute()
        self.local_file = current_file
        self.hoard_file = curr_file_hoard_path
        self.hoard_props = hoard_props


class FileOnlyInHoardLocalMoved(Diff):
    def __init__(
            self, current_file: FastPosixPath, curr_file_hoard_path: FastPosixPath,
            hoard_props: HoardFileProps, local_props: RepoFileProps):
        super().__init__(DiffType.FileOnlyInHoardLocalMoved)
        assert not current_file.is_absolute()
        assert curr_file_hoard_path.is_absolute()
        self.local_file = current_file
        self.local_props = local_props

        self.hoard_file = curr_file_hoard_path
        self.hoard_props = hoard_props
