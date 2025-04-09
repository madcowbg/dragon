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
    def __init__(
            self, diff_type: DiffType, local_file: FastPosixPath,
            curr_file_hoard_path: FastPosixPath, local_props: RepoFileProps | None, hoard_props: HoardFileProps | None,
            is_added: bool | None):
        self.diff_type = diff_type

        assert not local_file.is_absolute()
        assert curr_file_hoard_path.is_absolute()
        self.local_file = local_file
        self.hoard_file = curr_file_hoard_path
        self.local_props = local_props
        self.hoard_props = hoard_props

        self.is_added = is_added


