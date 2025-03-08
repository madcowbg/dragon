import enum
import os
import pathlib
from typing import Dict, Any, Optional

import rtoml


class CaveType(enum.Enum):
    INCOMING = "incoming"
    PARTIAL = "partial"
    FULL = "full"
    BACKUP = "backup"


class HoardRemote:
    def __init__(self, uuid: str, doc: Dict[str, Any]):
        self.uuid = uuid
        self.doc = doc

    @property
    def name(self):
        return self.doc["name"] if "name" in self.doc else "INVALID"

    @property
    def mounted_at(self):
        return self.doc["mounted_at"] if "mounted_at" in self.doc else None

    def mount_at(self, mount_at: str):
        self.doc["mounted_at"] = mount_at

    @property
    def type(self) -> CaveType:
        return CaveType(self.doc["type"])

    @type.setter
    def type(self, value: CaveType):
        self.doc["type"] = value.value


class HoardRemotes:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc

    def declare(self, current_uuid: str, name: str, type: CaveType):
        self.doc[current_uuid] = {
            "name": name,
            "type": type.value}

    def names_map(self):
        return dict((props["name"], remote) for remote, props in self.doc.items() if "name" in props)

    def __getitem__(self, remote_uuid: str) -> Optional[HoardRemote]:
        return HoardRemote(remote_uuid, self.doc[remote_uuid]) if remote_uuid in self.doc else None

    def __len__(self):
        return len(self.doc)

    def all(self):
        return (self[uuid] for uuid in self.doc)


class CavePath:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc

    def find(self) -> str:
        if "exact" in self.doc:
            return self.doc["exact"]

    @classmethod
    def exact(cls, remote_abs_path):
        return CavePath({"exact": remote_abs_path})


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
    def load(filename: str) -> "HoardConfig":
        if not os.path.isfile(filename):
            rtoml.dump({}, pathlib.Path(filename))
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
