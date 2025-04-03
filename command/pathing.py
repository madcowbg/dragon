from functools import cache
from typing import Optional, Dict

from command.fast_path import FastPosixPath
from config import HoardConfig, HoardPaths, HoardRemote


class HoardPathing:
    def __init__(self, config: HoardConfig, paths: HoardPaths):
        self._config = config
        self._paths = paths

    class HoardPath:
        def __init__(self, path: str, pathing: "HoardPathing"):
            assert path.startswith("/")

            self._path = path
            self._pathing = pathing

        @property
        def as_pure_path(self) -> FastPosixPath:
            return FastPosixPath(self._path)

        def at_local(self, repo_uuid: str) -> Optional["HoardPathing.LocalPath"]:
            mounted_at = self._pathing.smart_mounted_at_str(repo_uuid)
            if not self._path.startswith(mounted_at):
                return None  # is not relative
            return HoardPathing.LocalPath(self._path[len(mounted_at) + 1:], repo_uuid, self._pathing)

        def __str__(self) -> str:
            return self._path

    class LocalPath:
        def __init__(self, path: str, repo_uuid: str, pathing: "HoardPathing"):
            assert not path.startswith("/")

            self._path = path

            self._repo_uuid = repo_uuid
            self._pathing = pathing

        @property
        def as_pure_path(self) -> FastPosixPath:
            return FastPosixPath(self._path)

        def on_device_path(self) -> str:
            return self._pathing._paths[self._repo_uuid].find() + "/" + self._path

        def at_hoard(self) -> "HoardPathing.HoardPath":
            mounted_at = self._pathing.smart_mounted_at_str(self._repo_uuid)
            joined_path = mounted_at + "/" + self._path
            return HoardPathing.HoardPath(joined_path, self._pathing)

        def __str__(self) -> str:
            return self._path

    @cache
    def smart_mounted_at_str(self, repo_uuid) -> str:
        mounted_at = self.mounted_at(repo_uuid).as_posix()
        if mounted_at == "/":
            mounted_at = ""
        return mounted_at

    @cache
    def mounted_at(self, repo_uuid: str) -> FastPosixPath:
        assert self._config.remotes[repo_uuid].mounted_at.is_absolute()
        return self._config.remotes[repo_uuid].mounted_at

    def in_hoard(self, path: FastPosixPath | FastPosixPath) -> HoardPath:
        return HoardPathing.HoardPath(path.as_posix(), self)

    def in_local(self, path: FastPosixPath, repo_uuid: str) -> LocalPath:
        return HoardPathing.LocalPath(path.as_posix(), repo_uuid, self)

    def repos_availability(self, folder: str) -> Dict[HoardRemote, str]:
        paths: Dict[HoardRemote, str] = {}
        for remote in self._config.remotes.all():
            relative_local_path = self.in_hoard(FastPosixPath(folder)).at_local(remote.uuid)
            if relative_local_path is not None:
                paths[remote] = relative_local_path.as_pure_path.as_posix()
        return paths


def is_path_available(pathing: HoardPathing, hoard_file: FastPosixPath, repo: str) -> bool:
    return pathing.in_hoard(hoard_file).at_local(repo) is not None
