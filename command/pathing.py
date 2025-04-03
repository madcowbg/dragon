from functools import cache
from typing import Optional, Dict

from command.fast_path import FastPosixPath
from config import HoardConfig, HoardPaths, HoardRemote


class HoardPathing:
    def __init__(self, config: HoardConfig, paths: HoardPaths):
        self._config = config
        self._paths = paths

    class HoardPath:
        def __init__(self, path: FastPosixPath, pathing: "HoardPathing"):
            assert path._is_absolute

            self._path = path
            self._pathing = pathing

        @property
        def as_pure_path(self) -> FastPosixPath:
            return self._path

        def at_local(self, repo_uuid: str) -> Optional["HoardPathing.LocalPath"]:
            mounted_at = self._pathing.mounted_at(repo_uuid)
            if not self._path.is_relative_to(mounted_at):
                return None  # is not relative
            return HoardPathing.LocalPath(self._path.relative_to(mounted_at), repo_uuid, self._pathing)

        def __str__(self) -> str:
            return self._path.as_posix()

    class LocalPath:
        def __init__(self, path: FastPosixPath, repo_uuid: str, pathing: "HoardPathing"):
            assert not path._is_absolute

            self._path = path

            self._repo_uuid = repo_uuid
            self._pathing = pathing

        @property
        def as_pure_path(self) -> FastPosixPath:
            return FastPosixPath(self._path)

        def on_device_path(self) -> FastPosixPath:
            return self._pathing.cave_found_path(self._repo_uuid).joinpath(self._path.simple)

        def at_hoard(self) -> "HoardPathing.HoardPath":
            joined_path = self._pathing.mounted_at(self._repo_uuid).joinpath(self._path)
            return HoardPathing.HoardPath(joined_path, self._pathing)

        def __str__(self) -> str:
            return self._path.as_posix()

    @cache
    def cave_found_path(self, repo_uuid: str) -> FastPosixPath:
        return FastPosixPath(self._paths[repo_uuid].find())

    @cache
    def mounted_at(self, repo_uuid: str) -> FastPosixPath:
        assert self._config.remotes[repo_uuid].mounted_at.is_absolute()
        return self._config.remotes[repo_uuid].mounted_at

    def in_hoard(self, path: FastPosixPath) -> HoardPath:
        assert isinstance(path, FastPosixPath)
        return HoardPathing.HoardPath(path, self)

    def in_local(self, path: FastPosixPath, repo_uuid: str) -> LocalPath:
        assert isinstance(path, FastPosixPath)
        return HoardPathing.LocalPath(path, repo_uuid, self)

    def repos_availability(self, folder: str) -> Dict[HoardRemote, str]:
        paths: Dict[HoardRemote, str] = {}
        for remote in self._config.remotes.all():
            relative_local_path = self.in_hoard(FastPosixPath(folder)).at_local(remote.uuid)
            if relative_local_path is not None:
                paths[remote] = relative_local_path.as_pure_path.as_posix()
        return paths


def is_path_available(pathing: HoardPathing, hoard_file: FastPosixPath, repo: str) -> bool:
    return pathing.in_hoard(hoard_file).at_local(repo) is not None
