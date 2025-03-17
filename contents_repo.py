import abc
import os
from datetime import datetime
from typing import Dict, Any, Generator, Tuple, Optional

import rtoml

from contents import FSObjects, FSObjectProps
from contents_props import RepoFileProps, DirProps, TOMLRepoFileProps


class RepoContentsConfig(abc.ABC):
    uuid: str
    epoch: int

    @abc.abstractmethod
    def bump_epoch(self): pass


class TOMLContentsConfig(RepoContentsConfig):
    def __init__(self, config_doc: Dict[str, Any]):
        self.doc = config_doc

    def touch_updated(self) -> None:
        self.doc["updated"] = datetime.now().isoformat()

    @property
    def updated(self) -> datetime:
        return datetime.fromisoformat(self.doc["updated"])

    @property
    def uuid(self) -> str:
        return self.doc["uuid"]

    @property
    def epoch(self) -> int:
        return int(self.doc["epoch"]) if "epoch" in self.doc else 0

    def bump_epoch(self):
        self.doc["epoch"] = self.epoch + 1


class TOMLFSObjects(FSObjects):
    def __init__(self, fsobjects_doc: Dict[str, Any]):
        self._doc = fsobjects_doc
        self._objects = dict(
            (f, TOMLRepoFileProps(data) if not data['isdir'] else DirProps(data))
            for f, data in self._doc.items())

    def __len__(self) -> int: return len(self._objects)

    def __getitem__(self, key: str) -> FSObjectProps: return self._objects[key]

    def __iter__(self) -> Generator[Tuple[str, FSObjectProps], None, None]:
        yield from self._objects.copy().items()

    def __contains__(self, item: str) -> bool:
        return item in self._objects

    @property
    def num_files(self):
        return len([f for f, p in self if isinstance(p, RepoFileProps)])

    @property
    def num_dirs(self):
        return len([f for f, p in self if isinstance(p, DirProps)])

    @property
    def total_size(self) -> int:
        return sum(p.size for _, p in self if isinstance(p, RepoFileProps))

    def add_file(self, filepath: str, size: int, mtime: float, fasthash: str) -> None:
        self._doc[filepath] = {"size": size, "mtime": mtime, "isdir": False, "fasthash": fasthash}
        self._objects[filepath] = TOMLRepoFileProps(self._doc[filepath])

    def add_dir(self, dirpath):
        self._doc[dirpath] = {"isdir": True}
        self._objects[dirpath] = DirProps(self._doc[dirpath])

    def remove(self, path: str):
        self._doc.pop(path)
        self._objects.pop(path)


class RepoContents(abc.ABC):
    fsobjects: FSObjects
    config: RepoContentsConfig

    @abc.abstractmethod
    def __enter__(self) -> "RepoContents": pass

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool: pass

    @staticmethod
    def load(filepath: str, create_for_uuid: Optional[str] = None, write_on_exit: bool = True):
        return TOMLRepoContents.load(filepath, create_for_uuid, write_on_exit)


class TOMLRepoContents(RepoContents):
    @staticmethod
    def load(filepath: str, create_for_uuid: Optional[str] = None, write_on_exit: bool = True):
        if not os.path.isfile(filepath):
            if create_for_uuid is not None:
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(rtoml.dumps({"config": {"uuid": create_for_uuid}, "epoch": 0}))
            else:
                raise ValueError(f"File {filepath} does not exist, need to pass create=True to create.")

        return TOMLRepoContents(filepath, write_on_exit)

    def __init__(self, filepath: str, write_on_exit: bool):
        self.filepath = filepath
        self.write_on_exit = write_on_exit

        self.config = None
        self.fsobjects = None

    def __enter__(self):
        with open(self.filepath, "r", encoding="utf-8") as f:
            contents_doc = rtoml.load(f)

        self.config = TOMLContentsConfig(contents_doc["config"] if "config" in contents_doc else {})
        self.fsobjects = TOMLFSObjects(contents_doc["fsobjects"] if "fsobjects" in contents_doc else {})

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.write_on_exit:
            self.write()
        self.config = None
        self.fsobjects = None

        return False

    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            rtoml.dump({
                "config": self.config.doc,
                "fsobjects": self.fsobjects._doc
            }, f)
