import os
from pathlib import Path, PurePosixPath, PurePath
from typing import Union, List


class FastPosixPath(os.PathLike):
    def __init__(self, path: Union[bool, str, Path, "FastPosixPath"], drive: str = None, remainder: List[str] | None = None):
        if isinstance(path, bool):  # supports FastPosixPath(is_absolute, remainder)
            # print((path, drive, remainder))
            assert (drive == '') or (not path)  # paths with drives are absolute
            self._drive = drive
            self._is_absolute = path
            self._rem = remainder
        elif isinstance(path, FastPosixPath):
            # print("other ", path)
            self._drive = path._drive
            self._is_absolute = path._is_absolute
            self._rem = path._rem
        else:
            # print(f"other non-fast {type(path)}", path.__repr__())
            if isinstance(path, PurePath):
                path = path.as_posix()

            if path == '.' or path == '':
                self._drive = ''
                self._is_absolute = False
                self._rem = []
            elif path == '/':
                self._drive = ''
                self._is_absolute = True
                self._rem = []
            else:
                assert isinstance(path, str), repr(path)

                parts = path.split(r"/")

                self._drive = parts[0] if len(parts) > 0 and parts[0].endswith(":") else ''
                self._is_absolute = self._drive != '' or path.startswith("/")
                self._rem = parts if not self._is_absolute else parts[1:]
                assert self._rem.__len__() == 0 or self._rem[0] != '', self._rem
        # print(repr(self))

    def as_posix(self) -> str:
        if not self.is_absolute():
            if len(self._rem) == 0:
                return "."
            else:
                return "/".join(self._rem)
        else:
            return "/" + "/".join(self._rem)

    @property
    def simple(self) -> str:
        return ("/" if self.is_absolute() else "") + "/".join(self._rem)

    def is_absolute(self) -> bool:
        return self._is_absolute

    def __fspath__(self) -> str:
        return self.as_posix()

    def relative_to(self, other_path: Union[str, "FastPosixPath"]):
        if not isinstance(other_path, FastPosixPath):
            other_path = FastPosixPath(other_path)

        assert self.is_relative_to(other_path), f"{self} is not relative to {other_path}"
        return FastPosixPath(False, '', self._rem[len(other_path._rem):])

    def __lt__(self, other: "FastPosixPath"):
        return self.simple.__lt__(other.simple)

    def __eq__(self, other: "FastPosixPath"):
        return self.simple.__eq__(other.simple)

    def __hash__(self) -> int:
        return self.simple.__hash__()

    def is_relative_to(self, other_path: Union[str, "FastPosixPath"]):
        if not isinstance(other_path, FastPosixPath):
            other_path = FastPosixPath(other_path)
        if self._is_absolute != other_path._is_absolute:
            return False
        if self._drive != other_path._drive:
            return False

        # "something/???" vs "something"
        return self._rem[:len(other_path._rem)] == other_path._rem

    def __str__(self):
        return self.as_posix()

    def __repr__(self):
        return f"FastPosixPath({self._is_absolute},{self._drive},{self._rem})"

    def joinpath(self, other_path: Union[str, "FastPosixPath"]):
        if not isinstance(other_path, FastPosixPath):
            other_path = FastPosixPath(other_path)

        assert not other_path._is_absolute, (self, other_path)
        return FastPosixPath(self._is_absolute, self._drive, self._rem + other_path._rem)
