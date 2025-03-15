import enum
import os
import pathlib
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, Generator

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

    @property
    def epoch(self) -> int:
        return int(self.doc["epoch"]) if "epoch" in self.doc else 0

    def bump_epoch(self):
        self.doc["epoch"] = self.epoch + 1


class FSObjectProps:
    pass


class FileProps(FSObjectProps):
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc

    @property
    def size(self) -> float:
        return self.doc["size"]

    @property
    def mtime(self) -> float:
        return self.doc["mtime"]

    @property
    def fasthash(self) -> str:
        return self.doc["fasthash"]


class DirProps(FSObjectProps):
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc


class FSObjects:
    def __init__(self, fsobjects_doc: Dict[str, Any]):
        self._doc = fsobjects_doc
        self._objects = dict(
            (f, FileProps(data) if not data['isdir'] else DirProps(data))
            for f, data in self._doc.items())

    def __len__(self) -> int: return len(self._objects)

    def __getitem__(self, key: str) -> FSObjectProps: return self._objects[key]

    def __iter__(self) -> Generator[Tuple[str, FSObjectProps], None, None]:
        yield from self._objects.copy().items()

    def __contains__(self, item: str) -> bool:
        return item in self._objects

    @property
    def num_files(self):
        return len([f for f, p in self if isinstance(p, FileProps)])

    @property
    def num_dirs(self):
        return len([f for f, p in self if isinstance(p, DirProps)])

    @property
    def total_size(self) -> int:
        return sum(p.size for _, p in self if isinstance(p, FileProps))

    def add_file(self, filepath: str, size: int, mtime: float, fasthash: str) -> None:
        self._doc[filepath] = {"size": size, "mtime": mtime, "isdir": False, "fasthash": fasthash}
        self._objects[filepath] = FileProps(self._doc[filepath])

    def add_dir(self, dirpath):
        self._doc[dirpath] = {"isdir": True}
        self._objects[dirpath] = DirProps(self._doc[dirpath])

    def remove(self, path: str):
        self._doc.pop(path)
        self._objects.pop(path)


class Contents:
    @staticmethod
    def load(filepath: str, create_for_uuid: Optional[str] = None):
        if not os.path.isfile(filepath):
            if create_for_uuid is not None:
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(rtoml.dumps({"config": {"uuid": create_for_uuid}, "epoch": 0}))
            else:
                raise ValueError(f"File {filepath} does not exist, need to pass create=True to create.")
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
                "fsobjects": self.fsobjects._doc
            }, f)


class FileStatus(enum.Enum):
    AVAILABLE = "available"
    GET = "get"
    CLEANUP = "cleanup"
    UNKNOWN = "UNKNOWN"


class HoardFileProps(FSObjectProps):
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc

    @property
    def size(self):
        return self.doc["size"]

    @property
    def fasthash(self):
        return self.doc["fasthash"]

    def replace_file(self, new_props: FileProps, available_uuid: str):
        self.doc["size"] = new_props.size
        self.doc["fasthash"] = new_props.fasthash

        # mark for re-fetching everywhere it is already available, cancel getting it
        for uuid, status in self.doc["status"].copy().items():
            if status == FileStatus.AVAILABLE.value:
                self.mark_to_get(uuid)
            elif status == FileStatus.GET.value or status == FileStatus.CLEANUP.value:
                pass
            else:
                raise ValueError(f"Unknown status: {status}")

        self.doc["status"][available_uuid] = FileStatus.AVAILABLE.value

    def mark_available(self, remote_uuid: str):
        self.doc["status"][remote_uuid] = FileStatus.AVAILABLE.value

    @property
    def available_at(self) -> List[str]:
        return self.by_status(FileStatus.AVAILABLE)

    def by_status(self, selected_status: FileStatus):
        return [uuid for uuid, status in self.doc["status"].items() if status == selected_status.value]

    def mark_for_cleanup(self, repo_uuid: str):
        self.doc["status"][repo_uuid] = FileStatus.CLEANUP.value

    def status(self, repo_uuid: str) -> FileStatus:
        return FileStatus(self.doc["status"][repo_uuid]) if repo_uuid in self.doc["status"] else FileStatus.UNKNOWN

    def mark_to_get(self, repo_uuid: str):
        self.doc["status"][repo_uuid] = FileStatus.GET.value

    def mark_to_delete(self):
        for uuid, status in self.doc["status"].copy().items():
            assert status != FileStatus.UNKNOWN.value

            if status == FileStatus.GET.value:
                self.remove_status(uuid)
            elif status == FileStatus.AVAILABLE.value:
                self.mark_for_cleanup(uuid)
            elif status == FileStatus.CLEANUP.value:
                pass
            else:
                raise ValueError(f"Unknown status: {status}")

    def remove_status(self, remote_uuid: str):
        self.doc["status"].pop(remote_uuid)


class HoardTree:
    def __init__(self, objects: Dict[str, FSObjectProps]):
        self.root = HoardDir(None, "", self)

        for path, props in objects.items():
            if isinstance(props, HoardFileProps):
                filepath = path
                assert os.path.isabs(filepath)
                current = self.root
                parts = pathlib.Path(filepath).parts
                for folder in parts[1:-1]:
                    current = current.get_or_create_dir(folder)
                current.create_file(parts[-1], props)
            elif isinstance(props, DirProps):
                dirpath = path
                assert os.path.isabs(dirpath)
                current = self.root
                for folder in pathlib.Path(dirpath).parts[1:]:
                    current = current.get_or_create_dir(folder)
            else:
                raise ValueError(f"Invalid props type: {type(props)}")

    def walk(self, from_path: str = "/", depth: int = sys.maxsize) -> \
            Generator[Tuple[Optional["HoardDir"], Optional["HoardFile"]], None, None]:
        assert os.path.isabs(from_path)
        current = self.root
        for folder in pathlib.Path(from_path).parts[1:]:
            current = current.get_dir(folder)
            if current is None:
                return

        yield from current.walk(depth)


class HoardFile:
    def __init__(self, parent: "HoardDir", name: str, props: HoardFileProps):
        self.parent = parent
        self.name = name
        self.props = props

        self._fullname: Optional[pathlib.Path] = None

    @property
    def fullname(self):
        if self._fullname is None:
            self._fullname = pathlib.Path(self.parent.fullname).joinpath(self.name)
        return self._fullname.as_posix()


class HoardDir:
    @property
    def fullname(self):
        if self._fullname is None:
            parent_path = pathlib.Path(self.parent.fullname) if self.parent is not None else pathlib.Path("/")
            self._fullname = parent_path.joinpath(self.name)
        return self._fullname.as_posix()

    def __init__(self, parent: Optional["HoardDir"], name: str, tree: HoardTree):
        self.tree = tree
        self.name = name
        self.parent = parent

        self.dirs: Dict[str, HoardDir] = {}
        self.files: Dict[str, HoardFile] = {}

        self._fullname: Optional[str] = None

    def get_or_create_dir(self, subname: str) -> "HoardDir":
        if subname not in self.dirs:
            self.dirs[subname] = HoardDir(self, subname, self.tree)
        return self.dirs[subname]

    def get_dir(self, subname: str) -> Optional["HoardDir"]:
        return self.dirs.get(subname, None)

    def create_file(self, filename: str, props: HoardFileProps):
        assert filename not in self.files
        self.files[filename] = HoardFile(self, filename, props)

    def walk(self, depth: int) -> Generator[Tuple[Optional["HoardDir"], Optional["HoardFile"]], None, None]:
        yield self, None
        if depth <= 0:
            return
        for hoard_file in self.files.values():
            yield None, hoard_file
        for hoard_dir in self.dirs.values():
            yield from hoard_dir.walk(depth - 1)


class HoardFSObjects:
    tree: HoardTree

    def __init__(self, doc: Dict[str, Any]):
        self._doc = doc
        self._objects = dict(
            (f, HoardFileProps(data) if not data['isdir'] else DirProps(data)) for f, data in self._doc.items())
        self.tree = HoardTree(self._objects)

    def __len__(self):
        return len(self._objects)

    def __getitem__(self, key: str) -> FSObjectProps: return self._objects[key]

    def __iter__(self) -> Generator[Tuple[str, FSObjectProps], None, None]: yield from self._objects.copy().items()

    def __contains__(self, item: str) -> bool: return item in self._objects

    def add_new_file(
            self, curr_file: str, props: FileProps,
            current_uuid: str, repos_to_add_new_files: List[HoardRemote]) -> HoardFileProps:
        self._doc[curr_file] = {
            "isdir": False,
            "size": props.size,
            "fasthash": props.fasthash,
            "status": dict((r.uuid, FileStatus.GET.value) for r in repos_to_add_new_files)
        }

        # mark as present here
        self._doc[curr_file]["status"][current_uuid] = FileStatus.AVAILABLE.value

        self._objects[curr_file] = HoardFileProps(self._doc[curr_file])
        return self._objects[curr_file]

    def add_dir(self, curr_dir: str):
        self._doc[curr_dir] = {"isdir": True}
        self._objects[curr_dir] = DirProps(self._doc[curr_dir])

    def delete(self, curr_file: str):
        self._objects.pop(curr_file)
        self._doc.pop(curr_file)

    def move_file(self, orig_file: str, new_path: str, props: HoardFileProps):
        assert orig_file != new_path

        self._doc[new_path] = props.doc
        self._objects[new_path] = props

        self.delete(orig_file)

    def move_dir(self, curr_dir: str, new_path: str, props: DirProps):
        assert curr_dir != new_path

        self._doc[new_path] = props.doc
        self._objects[new_path] = props

        self.delete(curr_dir)

    @property
    def num_files(self):
        return len([f for f, p in self if isinstance(p, HoardFileProps)])

    @property
    def num_dirs(self):
        return len([f for f, p in self if isinstance(p, DirProps)])

    @property
    def total_size(self) -> int:
        return sum(p.size for _, p in self if isinstance(p, HoardFileProps))


class HoardContents:
    @staticmethod
    def load(filename: str) -> "HoardContents":
        if not os.path.isfile(filename):
            with open(filename, "w", encoding="utf-8") as f:
                config = {"updated": datetime.now().isoformat()}
                rtoml.dump({
                    "config": config,
                    "epochs": {},
                    "fsobjects": {},
                }, f)
        with open(filename, "r", encoding="utf-8") as f:
            return HoardContents(filename, rtoml.load(f))

    def __init__(self, filepath: str, contents_doc: Dict[str, Any]):
        self.filepath = filepath
        self.config = HoardContentsConfig(contents_doc["config"] if "config" in contents_doc else {})
        self.fsobjects = HoardFSObjects(contents_doc["fsobjects"] if "fsobjects" in contents_doc else {})
        self.epochs = contents_doc["epochs"] if "epochs" in contents_doc else {}

    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            rtoml.dump({
                "config": self.config.doc,
                "epochs": self.epochs,
                "fsobjects": self.fsobjects._doc
            }, f)

    def epoch(self, remote_uuid: str) -> int:
        return self.epochs.get(remote_uuid, -1)

    def set_epoch(self, remote_uuid: str, epoch: int):
        self.epochs[remote_uuid] = epoch
