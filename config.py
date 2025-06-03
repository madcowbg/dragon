import enum
import os
import pathlib
import sys

from command.fast_path import FastPosixPath
from typing import Dict, Any, Optional, List, Generator

import rtoml


class CaveType(enum.Enum):
    INCOMING = "incoming"
    PARTIAL = "partial"
    BACKUP = "backup"


class HoardRemote:
    def __init__(self, uuid: str, doc: Dict[str, Any]):
        self.uuid = uuid
        self.doc = doc

    def __str__(self):
        return f"HoardRemote[{self.name}, uuid={self.uuid}]"

    @property
    def name(self):
        return self.doc["name"] if "name" in self.doc else "INVALID"

    @name.setter
    def name(self, new_name: str):
        assert isinstance(new_name, str)
        self.doc["name"] = new_name

    @property
    def mounted_at(self) -> FastPosixPath | None:
        return FastPosixPath(self.doc["mounted_at"]) if "mounted_at" in self.doc else None

    def mount_at(self, mount_at: FastPosixPath):
        assert mount_at.is_absolute()
        self.doc["mounted_at"] = mount_at.as_posix()

    @property
    def type(self) -> CaveType:
        return CaveType(self.doc["type"])

    @type.setter
    def type(self, value: CaveType):
        self.doc["type"] = value.value

    @property
    def fetch_new(self) -> bool:
        """ Marks whether this repo should auto-fetch all newly-added files."""
        return self.doc.get("fetch_new", False)

    @fetch_new.setter
    def fetch_new(self, value: bool):
        self.doc["fetch_new"] = value

    @property
    def min_copies_before_cleanup(self):
        return self.doc.get("min_copies_before_cleanup", 10)

    @min_copies_before_cleanup.setter
    def min_copies_before_cleanup(self, value: int):
        self.doc["min_copies_before_cleanup"] = value

    def __eq__(self, other):
        return isinstance(other, HoardRemote) and self.uuid == other.uuid and self.doc == other.doc

    def __hash__(self):
        return hash(self.uuid)

class HoardRemotes:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc

    def declare(self, current_uuid: str, name: str, cave_type: CaveType, mount_point: str, fetch_new: bool):
        self.doc[current_uuid] = {
            "name": name,
            "type": cave_type.value,
            "mounted_at": mount_point,
            "fetch_new": fetch_new}

    def names_map(self):
        return dict((props["name"], remote) for remote, props in self.doc.items() if "name" in props)

    def __getitem__(self, remote_uuid: str) -> Optional[HoardRemote]:
        return HoardRemote(remote_uuid, self.doc[remote_uuid]) if remote_uuid in self.doc else None

    def __len__(self):
        return len(self.doc)

    def all(self) -> Generator[HoardRemote, None, None]:
        return (self[uuid] for uuid in self.doc)


class ConnectionSpeed(enum.Enum):
    INTERNAL_DRIVE = "internal"
    EXTERNAL_DRIVE = "attached"
    LOCAL_NETWORK = "nas"
    INTERNET = "internet"


def connection_speed_order(speed: ConnectionSpeed) -> int:
    if speed == ConnectionSpeed.INTERNAL_DRIVE:
        return 1
    elif speed == ConnectionSpeed.EXTERNAL_DRIVE:
        return 2
    elif speed == ConnectionSpeed.LOCAL_NETWORK:
        return 3
    elif speed == ConnectionSpeed.INTERNET:
        return 4
    else:
        raise ValueError(f"Unknown connection speed: {speed}")


class ConnectionLatency(enum.Enum):
    ALWAYS = "milliseconds"
    SECONDS = "seconds"
    MINUTES = "minutes"
    DAYS = "days"


def latency_order(latency: ConnectionLatency) -> int:
    if latency == ConnectionLatency.ALWAYS:
        return 1
    elif latency == ConnectionLatency.SECONDS:
        return 2
    elif latency == ConnectionLatency.MINUTES:
        return 3
    elif latency == ConnectionLatency.DAYS:
        return 4
    else:
        raise ValueError(f"Unknown connection latency: {latency}")


class CavePath:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc

    def find(self) -> str:
        if "exact" in self.doc:
            return self.doc["exact"]

    @property
    def speed(self) -> ConnectionSpeed:
        assert "speed" in self.doc
        return ConnectionSpeed(self.doc["speed"])

    @speed.setter
    def speed(self, speed: ConnectionSpeed):
        assert isinstance(speed, ConnectionSpeed)
        self.doc["speed"] = speed.value

    @property
    def latency(self) -> ConnectionLatency:
        assert "latency" in self.doc
        return ConnectionLatency(self.doc["latency"])

    @latency.setter
    def latency(self, latency: ConnectionLatency):
        assert isinstance(latency, ConnectionLatency)
        self.doc["latency"] = latency.value

    def prioritize_speed_over_latency(self) -> int:
        return connection_speed_order(self.speed) * 100 + latency_order(self.latency)

    def prioritize_latency_over_speed(self) -> int:
        return latency_order(self.latency) * 100 + connection_speed_order(self.speed)

    @classmethod
    def exact(cls, remote_abs_path: str, speed: ConnectionSpeed, latency: ConnectionLatency) -> "CavePath":
        return CavePath({
            "exact": remote_abs_path,
            "latency": latency.value,
            "speed": speed.value})


class HoardPaths:
    def __init__(self, filepath: str, doc: Dict[str, Any]):
        self.filepath = filepath
        self.doc = doc

    @staticmethod
    def load(filename: str) -> "HoardPaths":
        if not os.path.isfile(filename):
            rtoml.dump({}, pathlib.Path(filename))
        with open(filename, "r", encoding="utf-8") as f:
            return HoardPaths(filename, rtoml.load(f))

    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            rtoml.dump(self.doc, f)

    def __contains__(self, uuid: str) -> bool:
        return uuid in self.doc

    def __getitem__(self, uuid: str) -> CavePath:
        return CavePath(self.doc[uuid]) if uuid in self.doc else None

    def __setitem__(self, uuid: str, path: CavePath):
        self.doc[uuid] = path.doc


class HoardConfig:
    @staticmethod
    def load(filename: str, create: bool) -> "HoardConfig":
        if not os.path.isfile(filename):
            if create:
                rtoml.dump({}, pathlib.Path(filename))
            else:
                raise ValueError("Config file does not exist!")
        with open(filename, "r", encoding="utf-8") as f:
            return HoardConfig(filename, rtoml.load(f))

    def __init__(self, filepath: str, contents_doc: Dict[str, Any]):
        self.filepath = filepath
        self.remotes = HoardRemotes(contents_doc["remotes"] if "remotes" in contents_doc else {})

    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            rtoml.dump({
                "remotes": self.remotes.doc
            }, f)
