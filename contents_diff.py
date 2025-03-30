from contents.repo_props import RepoFileProps
from contents.hoard_props import HoardFileProps


class Diff:
    pass


class FileOnlyInLocal(Diff):
    def __init__(self, local_file: str, curr_file_hoard_path: str, local_props: RepoFileProps):
        self.local_file = local_file
        self.hoard_file = curr_file_hoard_path
        self.local_props = local_props


class FileIsSame(Diff):
    def __init__(self, current_file: str, curr_file_hoard_path: str, local_props: RepoFileProps,
                 hoard_props: HoardFileProps):
        self.local_file = current_file
        self.hoard_file = curr_file_hoard_path
        self.local_props = local_props
        self.hoard_props = hoard_props


class FileContentsDiffer(Diff):
    def __init__(
            self, current_file: str, curr_file_hoard_path: str,
            local_props: RepoFileProps, hoard_props: HoardFileProps):
        self.local_file = current_file
        self.hoard_file = curr_file_hoard_path
        self.local_props = local_props
        self.hoard_props = hoard_props


class FileOnlyInHoardLocalDeleted(Diff):
    def __init__(
            self, current_file: str, curr_file_hoard_path: str,
            hoard_props: HoardFileProps, local_props: RepoFileProps):
        self.local_file = current_file
        self.local_props = local_props

        self.hoard_file = curr_file_hoard_path
        self.hoard_props = hoard_props


class FileOnlyInHoardLocalUnknown(Diff):
    def __init__(self, current_file: str, curr_file_hoard_path: str, hoard_props: HoardFileProps):
        self.local_file = current_file
        self.hoard_file = curr_file_hoard_path
        self.hoard_props = hoard_props


class FileOnlyInHoardLocalMoved(Diff):
    def __init__(
            self, current_file: str, curr_file_hoard_path: str,
            hoard_props: HoardFileProps, local_props: RepoFileProps):
        self.local_file = current_file
        self.local_props = local_props

        self.hoard_file = curr_file_hoard_path
        self.hoard_props = hoard_props


class DirMissingInHoard(Diff):
    def __init__(self, current_dir: str, curr_dir_hoard_path: str):
        self.local_dir = current_dir
        self.hoard_dir = curr_dir_hoard_path


class DirIsSame(Diff):
    def __init__(self, current_dir: str, curr_dir_hoard_path: str):
        self.local_dir = current_dir
        self.hoard_dir = curr_dir_hoard_path


class DirMissingInLocal(Diff):
    def __init__(self, current_dir: str, curr_dir_hoard_path: str):
        self.local_dir = current_dir
        self.hoard_dir = curr_dir_hoard_path
