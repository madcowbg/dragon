import abc
import enum
import logging
import os
import pathlib
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, Generator

import rtoml


class HoardContentsConfig:
    def __init__(self, config_doc: Dict[str, Any]):
        self.doc = config_doc

    def touch_updated(self) -> None:
        self.doc["updated"] = datetime.now().isoformat()

    @property
    def updated(self) -> datetime:
        return datetime.fromisoformat(self.doc["updated"])


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


class FSObjects(abc.ABC):
    @abc.abstractmethod
    def __len__(self) -> int: pass

    @abc.abstractmethod
    def __getitem__(self, key: str) -> FSObjectProps: pass

    @abc.abstractmethod
    def __iter__(self) -> Generator[Tuple[str, FSObjectProps], None, None]: pass

    @abc.abstractmethod
    def __contains__(self, item: str) -> bool: pass

    num_files: int
    num_dirs: int
    total_size: int

    @abc.abstractmethod
    def add_file(self, filepath: str, size: int, mtime: float, fasthash: str) -> None: pass

    @abc.abstractmethod
    def add_dir(self, dirpath): pass

    @abc.abstractmethod
    def remove(self, path: str): pass


class FileStatus(enum.Enum):
    AVAILABLE = "available"
    GET = "get"
    CLEANUP = "cleanup"
    COPY = "copy"
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

    @property
    def presence(self):
        return dict((repo_uuid, FileStatus(status)) for repo_uuid, status in self.doc["status"].items())

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

    def status_to_copy(self) -> List[str]:
        return [uuid for uuid, status in self.doc["status"].items() if status in STATUSES_TO_COPY]


STATUSES_TO_COPY = [FileStatus.COPY.value, FileStatus.GET.value, FileStatus.AVAILABLE.value]


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

    def __getitem__(self, key: str) -> FSObjectProps:
        return self._objects[key]

    def __iter__(self) -> Generator[Tuple[str, FSObjectProps], None, None]:
        yield from self._objects.copy().items()

    def __contains__(self, item: str) -> bool:
        return item in self._objects

    def add_new_file(
            self, filepath: str, props: FileProps,
            current_uuid: str, repos_to_add_new_files: List[str]) -> HoardFileProps:
        self._doc[filepath] = {
            "isdir": False,
            "size": props.size,
            "fasthash": props.fasthash,
            "status": dict((uuid, FileStatus.GET.value) for uuid in repos_to_add_new_files)
        }

        # mark as present here
        self._doc[filepath]["status"][current_uuid] = FileStatus.AVAILABLE.value

        self._objects[filepath] = HoardFileProps(self._doc[filepath])
        return self._objects[filepath]

    def add_dir(self, curr_dir: str):
        self._doc[curr_dir] = {"isdir": True}
        self._objects[curr_dir] = DirProps(self._doc[curr_dir])

    def delete(self, curr_path: str):
        self._objects.pop(curr_path)
        self._doc.pop(curr_path)

    def move(self, orig_path: str, new_path: str, props: DirProps | HoardFileProps):
        assert orig_path != new_path
        assert isinstance(props, HoardFileProps) or isinstance(props, DirProps)

        self._doc[new_path] = props.doc
        self._objects[new_path] = props

        self.delete(orig_path)

    def copy(self, from_fullpath: str, to_fullpath: str):
        assert from_fullpath != to_fullpath

        props = self._objects[from_fullpath]
        if isinstance(props, HoardFileProps):
            self._doc[to_fullpath] = {
                "isdir": False,
                "size": props.size,
                "fasthash": props.fasthash,
                "status": dict((uuid, FileStatus.COPY.value) for uuid in props.status_to_copy())
            }
            self._objects[to_fullpath] = HoardFileProps(self._doc[to_fullpath])
        elif isinstance(props, DirProps):
            self.add_dir(to_fullpath)
        else:
            raise ValueError(f"props type unrecognized: {type(props)}")

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
    def load(filename: str, write_on_close: bool = True) -> "HoardContents":
        if not os.path.isfile(filename):
            with open(filename, "w", encoding="utf-8") as f:
                config = {"updated": datetime.now().isoformat()}
                rtoml.dump({
                    "config": config,
                    "epochs": {},
                    "fsobjects": {},
                }, f)
        return HoardContents(filename, write_on_close)

    def __init__(self, filepath: str, write_on_close: bool):
        self.filepath = filepath
        self.write_on_close = write_on_close

        self.config = None
        self.fsobjects = None
        self.epochs = None

    def __enter__(self):
        with open(self.filepath, "r", encoding="utf-8") as f:
            contents_doc = rtoml.load(f)

        self.config = HoardContentsConfig(contents_doc["config"] if "config" in contents_doc else {})
        self.fsobjects = HoardFSObjects(contents_doc["fsobjects"] if "fsobjects" in contents_doc else {})
        self.epochs = contents_doc["epochs"] if "epochs" in contents_doc else {}

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.write_on_close:
            self.write()
        self.config = None
        self.fsobjects = None
        self.epochs = None

        return False

    def write(self):
        logging.info(f"Writing contents to file: {self.filepath}")
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
