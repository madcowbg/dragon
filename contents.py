import enum
import os
from datetime import datetime
from typing import Dict, Any, List

import rtoml

from config import HoardRemote


class HoardContentsConfig:
    def __init__(self, config_doc: Dict[str, Any]):
        self.doc = config_doc

    def touch_updated(self) -> None:
        self.doc["updated"] = datetime.now().isoformat()

    @property
    def updated(self) -> datetime:
        return datetime.fromisoformat(self.doc["updated"])


class ContentsConfig(HoardContentsConfig):
    def __init__(self, config_doc: Dict[str, Any]):
        super().__init__(config_doc)

    @property
    def uuid(self) -> str:
        return self.doc["uuid"]


class FileProps:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc

    @property
    def size(self):
        return self.doc["size"]

    @property
    def mtime(self):
        return self.doc["mtime"]

    @property
    def fasthash(self):
        return self.doc["fasthash"]


class DirProps:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc


class FSObjects:
    def __init__(self, fsobjects_doc: Dict[str, Any]):
        self.doc = fsobjects_doc
        self.files = dict((f, FileProps(data)) for f, data in self.doc.items() if not data['isdir'])
        self.dirs = dict((f, DirProps(data)) for f, data in self.doc.items() if data['isdir'])

    def add_file(self, filepath: str, size: int, mtime: float, fasthash: str) -> None:
        self.doc[filepath] = {"size": size, "mtime": mtime, "isdir": False, "fasthash": fasthash}
        self.files[filepath] = FileProps(self.doc[filepath])

    def add_dir(self, dirpath):
        self.doc[dirpath] = {"isdir": True}
        self.dirs[dirpath] = DirProps(self.doc[dirpath])


class Contents:
    @staticmethod
    def load(filepath: str):
        with open(filepath, "r", encoding="utf-8") as f:
            return Contents(filepath, rtoml.load(f))

    def __init__(self, filepath: str, contents_doc: Dict[str, Any]):
        self.filepath = filepath
        self.config = ContentsConfig(contents_doc["config"] if "config" in contents_doc else {})
        self.fsobjects = FSObjects(contents_doc["fsobjects"] if "fsobjects" in contents_doc else {})

    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            rtoml.dump({
                "config": self.config.doc,
                "fsobjects": self.fsobjects.doc
            }, f)


class FileStatus(enum.Enum):
    AVAILABLE = "available"
    GET = "get"
    CLEANUP = "cleanup"
    UNKNOWN = "UNKNOWN"


class HoardFileProps:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc

    @property
    def size(self):
        return self.doc["size"]

    @property
    def mtime(self):
        return self.doc["mtime"]

    @property
    def fasthash(self):
        return self.doc["fasthash"]

    def update(self, props: FileProps):
        self.doc["size"] = props.size
        self.doc["mtime"] = props.mtime
        self.doc["fasthash"] = props.fasthash

    def ensure_available(self, remote_uuid: str):
        self.doc["status"][remote_uuid] = FileStatus.AVAILABLE.value

    @property
    def available_at(self) -> List[str]:
        return [uuid for uuid, status in self.doc["status"].items() if status == FileStatus.AVAILABLE.value]

    def mark_for_cleanup(self, repo_uuid: str):
        self.doc["status"][repo_uuid] = FileStatus.CLEANUP.value

    def status(self, repo_uuid: str) -> FileStatus:
        return FileStatus(self.doc["status"][repo_uuid]) if repo_uuid in self.doc["status"] else FileStatus.UNKNOWN

    def mark_to_get(self, repo_uuid: str):

        pass


class HoardFSObjects:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc
        self.files = dict((f, HoardFileProps(data)) for f, data in self.doc.items() if not data['isdir'])
        self.dirs = dict((f, DirProps(data)) for f, data in self.doc.items() if data['isdir'])

    def add_new_file(
            self, curr_file: str, props: FileProps,
            current_uuid: str, repos_to_add_new_files: List[HoardRemote]) -> HoardFileProps:
        self.doc[curr_file] = {
            "isdir": False,
            "size": props.size,
            "mtime": props.mtime,
            "fasthash": props.fasthash,
            "status": dict((r.uuid, FileStatus.GET.value) for r in repos_to_add_new_files)
        }

        # mark as present here
        self.doc[curr_file]["status"][current_uuid] = FileStatus.AVAILABLE.value

        self.files[curr_file] = HoardFileProps(self.doc[curr_file])
        return self.files[curr_file]

    def add_dir(self, curr_dir: str):
        self.doc[curr_dir] = {"isdir": True}
        self.dirs[curr_dir] = DirProps(self.doc[curr_dir])

    def update_file(self, curr_file: str, props: FileProps):
        self.files[curr_file].update(props)

    def delete_file(self, curr_file: str):
        self.files.pop(curr_file)
        self.doc.pop(curr_file)


class HoardContents:
    @staticmethod
    def load(filename: str) -> "HoardContents":
        if not os.path.isfile(filename):
            with open(filename, "w", encoding="utf-8") as f:
                config = {"updated": datetime.now().isoformat()}
                rtoml.dump({
                    "config": config,
                    "fsobjects": {},
                }, f)
        with open(filename, "r", encoding="utf-8") as f:
            return HoardContents(filename, rtoml.load(f))

    def __init__(self, filepath: str, contents_doc: Dict[str, Any]):
        self.filepath = filepath
        self.config = HoardContentsConfig(contents_doc["config"] if "config" in contents_doc else {})
        self.fsobjects = HoardFSObjects(contents_doc["fsobjects"] if "fsobjects" in contents_doc else {})

    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            rtoml.dump({
                "config": self.config.doc,
                "fsobjects": self.fsobjects.doc
            }, f)
