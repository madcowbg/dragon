import pathlib
import re
from typing import List

from wcmatch import glob

from command.fast_path import FastPosixPath

DEFAULT_IGNORE_GLOBS = [
    r".hoard",
    r".hoard/**",  # current hoard location, but those created recursively, for some reason
    r"**/thumbs.db",
    r"System Volume Information",
    r"$Recycle.Bin",
    # r"$RECYCLE.BIN"
    r"RECYCLE?",
    r"#recycle"
]


class HoardIgnore:
    def __init__(self, ignore_globs_list: List[str]):
        translated = glob.translate(patterns=ignore_globs_list, flags=glob.IGNORECASE | glob.GLOBSTAR)
        self.regex_globs_list = [re.compile(pattern) for pattern in translated[0]]

    def matches(self, fullpath: pathlib.PurePath) -> bool:
        assert isinstance(fullpath, pathlib.PurePath) or isinstance(fullpath, FastPosixPath)

        posix_path = fullpath.as_posix()

        for regex in self.regex_globs_list:
            if regex.match(posix_path):
                return True
        return False
