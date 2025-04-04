import pathlib
from typing import List

import command.fast_path

DEFAULT_IGNORE_GLOBS = [
    r".hoard",
    r".hoard/**",  # current hoard location, but those created recursively, for some reason
    r"**/thumbs.db",
    r"System Volume Information",
    r"$Recycle.Bin",
    r"RECYCLE?",
    r"#recycle"
]


class HoardIgnore:
    def __init__(self, ignore_globs_list: List[str]):
        self.ignore_globs_list = ignore_globs_list

    def matches(self, fullpath: command.fast_path.FastPosixPath) -> bool:
        for glob in self.ignore_globs_list:
            if fullpath.full_match(glob, case_sensitive=False):
                return True
        return False

