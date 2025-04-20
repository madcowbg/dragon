import abc
import dataclasses
from typing import Iterable

from lmdb_storage.tree_structure import ObjectID, FileObject, Objects


def zip_trees(objects: Objects, root_name: str, left_id: bytes, right_id: bytes):
    assert left_id is not None
    assert right_id is not None

    root_diff = Diff.compute(root_name, left_id, right_id)
    return root_diff.expand(objects)


class Diff:
    path: str

    @staticmethod
    def compute(path: str, left_id: ObjectID, right_id: ObjectID) -> "Diff":
        return AreSame(path, left_id) if left_id == right_id else HaveDifferences(path, left_id, right_id)

    @abc.abstractmethod
    def expand(self, objects: Objects) -> Iterable["Diff"]:
        pass

    def __str__(self):
        return f"{self.__class__.__name__}[{self.path}]"


@dataclasses.dataclass
class AreSame(Diff):
    path: str
    id: ObjectID

    def expand(self, objects: Objects) -> Iterable["Diff"]:
        yield self


@dataclasses.dataclass
class HaveDifferences(Diff):
    path: str
    left_id: ObjectID
    right_id: ObjectID

    def expand(self, objects: Objects) -> Iterable["Diff"]:
        left_obj = objects[self.left_id]
        right_obj = objects[self.right_id]

        yield self

        if isinstance(left_obj, FileObject) or isinstance(right_obj, FileObject):
            return

        # are both dirs, drilldown...
        for left_sub_name, left_file_id in left_obj.children.items():
            if left_sub_name in right_obj.children:
                yield from Diff.compute(
                    self.path + "/" + left_sub_name,
                    left_file_id, right_obj.children[left_sub_name]).expand(objects)
            else:
                yield RemovedInRight(self.path + "/" + left_sub_name, left_file_id)

        for right_sub_name, right_file_id in right_obj.children.items():
            if right_sub_name in left_obj.children:
                pass  # already returned
            else:
                yield AddedInRight(self.path + "/" + right_sub_name, right_file_id)


@dataclasses.dataclass
class AddedInRight(Diff):
    path: str
    left_obj: ObjectID

    def expand(self, objects: Objects) -> Iterable["Diff"]:
        yield self


@dataclasses.dataclass
class RemovedInRight(Diff):
    path: str
    right_obj: ObjectID

    def expand(self, objects: Objects) -> Iterable["Diff"]:
        yield self
