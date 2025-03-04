import os
import pathlib
from typing import Dict, Any, Optional

import rtoml


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

    def __setitem__(self, key: str, value: str):  # fixme make key an enum
        if key not in ["uuid", "name", "mounted_at"]:
            raise ValueError(f"Unrecognized param {key}!")
        self.doc[key] = value

    def mount_at(self, mount_at: str):
        self.doc["mounted_at"] = mount_at


class HoardRemotes:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc

    def declare(self, current_uuid: str, name: str):
        self.doc[current_uuid] = {"name": name}

    def names_map(self):
        return dict((props["name"], remote) for remote, props in self.doc.items() if "name" in props)

    def __getitem__(self, remote_uuid: str) -> Optional[HoardRemote]:
        return HoardRemote(remote_uuid, self.doc[remote_uuid]) if remote_uuid in self.doc else None

    def __len__(self):
        return len(self.doc)

    def all(self):
        return (self[uuid] for uuid in self.doc)


class HoardConfig:
    @staticmethod
    def load(filename: str) -> "HoardConfig":
        if not os.path.isfile(filename):
            rtoml.dump({}, pathlib.Path(filename))
        with open(filename, "r", encoding="utf-8") as f:
            return HoardConfig(filename, rtoml.load(f))

    def __init__(self, filepath: str, contents_doc: Dict[str, Any]):
        self.filepath = filepath
        self.paths = contents_doc["paths"] if "paths" in contents_doc else {}
        self.remotes = HoardRemotes(contents_doc["remotes"] if "remotes" in contents_doc else {})

    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            rtoml.dump({
                "paths": self.paths,
                "remotes": self.remotes.doc
            }, f)
