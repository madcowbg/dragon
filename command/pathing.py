import pathlib
from pathlib import PurePosixPath
from typing import Optional, Dict

from config import HoardConfig, HoardPaths, HoardRemote


class HoardPathing:
    def __init__(self, config: HoardConfig, paths: HoardPaths):
        self._config = config
        self._paths = paths

    class HoardPath:
        def __init__(self, path: str | PurePosixPath, pathing: "HoardPathing"):
            self._path = pathlib.PurePosixPath(path)
            assert self._path.is_absolute()

            self._pathing = pathing

        @property
        def as_pure_path(self) -> PurePosixPath:
            return self._path

        def at_local(self, repo_uuid: str) -> Optional["HoardPathing.LocalPath"]:
            mounted_at = self._pathing.mounted_at(repo_uuid)
            if not self._path.is_relative_to(mounted_at):
                return None
            else:
                return HoardPathing.LocalPath(self._path.relative_to(mounted_at).as_posix(), repo_uuid, self._pathing)

        def __str__(self) -> str:
            return self._path.as_posix()

    class LocalPath:
        def __init__(self, path: str, repo_uuid: str, pathing: "HoardPathing"):
            self._path = pathlib.PurePosixPath(path)
            assert not self._path.is_absolute()

            self._repo_uuid = repo_uuid
            self._pathing = pathing

        @property
        def as_pure_path(self) -> PurePosixPath:
            return self._path

        def on_device_path(self) -> str:
            return pathlib.PurePosixPath(self._pathing._paths[self._repo_uuid].find()).joinpath(self._path).as_posix()

        def at_hoard(self) -> "HoardPathing.HoardPath":
            joined_path = pathlib.PurePosixPath(self._pathing.mounted_at(self._repo_uuid)).joinpath(self._path)
            return HoardPathing.HoardPath(joined_path.as_posix(), self._pathing)

        def __str__(self) -> str:
            return self._path.as_posix()

    def mounted_at(self, repo_uuid: str) -> PurePosixPath:
        assert self._config.remotes[repo_uuid].mounted_at.is_absolute()
        return self._config.remotes[repo_uuid].mounted_at

    def in_hoard(self, path: str | PurePosixPath) -> HoardPath:
        return self.HoardPath(path, self)

    def in_local(self, path: str, repo_uuid: str) -> LocalPath:
        return HoardPathing.LocalPath(path, repo_uuid, self)

    def repos_availability(self, folder: str) -> Dict[HoardRemote, str]:
        paths: Dict[HoardRemote, str] = {}
        for remote in self._config.remotes.all():
            relative_local_path = self.in_hoard(folder).at_local(remote.uuid)
            if relative_local_path is not None:
                paths[remote] = relative_local_path.as_pure_path.as_posix()
        return paths


def is_path_available(pathing: HoardPathing, hoard_file: PurePosixPath, repo: str) -> bool:
    return pathing.in_hoard(hoard_file).at_local(repo) is not None
