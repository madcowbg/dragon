from command.fast_path import FastPosixPath

from contents.repo_props import RepoFileProps
from contents.hoard_props import HoardFileProps


class Diff:
    pass


class FileOnlyInLocal(Diff):
    def __init__(self, local_file: FastPosixPath, curr_file_hoard_path: FastPosixPath, local_props: RepoFileProps, is_added: bool):
        assert not local_file.is_absolute()
        assert curr_file_hoard_path.is_absolute()
        self.local_file = local_file
        self.hoard_file = curr_file_hoard_path
        self.local_props = local_props

        self.is_added = is_added

class FileIsSame(Diff):
    def __init__(self, current_file: FastPosixPath, curr_file_hoard_path: FastPosixPath, local_props: RepoFileProps,
                 hoard_props: HoardFileProps):
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
        assert not current_file.is_absolute()
        assert curr_file_hoard_path.is_absolute()
        self.local_file = current_file
        self.local_props = local_props

        self.hoard_file = curr_file_hoard_path
        self.hoard_props = hoard_props


class FileOnlyInHoardLocalUnknown(Diff):
    def __init__(self, current_file: FastPosixPath, curr_file_hoard_path: FastPosixPath, hoard_props: HoardFileProps):
        assert not current_file.is_absolute()
        assert curr_file_hoard_path.is_absolute()
        self.local_file = current_file
        self.hoard_file = curr_file_hoard_path
        self.hoard_props = hoard_props


class FileOnlyInHoardLocalMoved(Diff):
    def __init__(
            self, current_file: FastPosixPath, curr_file_hoard_path: FastPosixPath,
            hoard_props: HoardFileProps, local_props: RepoFileProps):
        assert not current_file.is_absolute()
        assert curr_file_hoard_path.is_absolute()
        self.local_file = current_file
        self.local_props = local_props

        self.hoard_file = curr_file_hoard_path
        self.hoard_props = hoard_props


class DirMissingInHoard(Diff):
    def __init__(self, current_dir: FastPosixPath, curr_dir_hoard_path: FastPosixPath):
        assert not current_dir.is_absolute()
        assert curr_dir_hoard_path.is_absolute()
        self.local_dir = current_dir
        self.hoard_dir = curr_dir_hoard_path


class DirIsSame(Diff):
    def __init__(self, current_dir: FastPosixPath, curr_dir_hoard_path: FastPosixPath):
        assert not current_dir.is_absolute()
        assert curr_dir_hoard_path.is_absolute()
        self.local_dir = current_dir
        self.hoard_dir = curr_dir_hoard_path


class DirMissingInLocal(Diff):
    def __init__(self, current_dir: FastPosixPath, curr_dir_hoard_path: FastPosixPath):
        assert not current_dir.is_absolute()
        assert curr_dir_hoard_path.is_absolute()
        self.local_dir = current_dir
        self.hoard_dir = curr_dir_hoard_path
