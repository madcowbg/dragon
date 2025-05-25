import abc
from typing import Callable, Dict

from lmdb_storage.file_object import BlobObject
from lmdb_storage.tree_structure import TreeObject, ObjectID, Objects, ObjectType, StoredObject


class ValueCalculator[R](abc.ABC):
    @abc.abstractmethod
    def calculate(self, calculator: "TreeCalculator[R]", objects: Objects, root_obj: StoredObject) -> R:
        pass

    @abc.abstractmethod
    def for_none(self, calculator: "TreeCalculator[R]") -> R:
        pass


class RecursiveSumCalculator(ValueCalculator[int | float]):
    def __init__(self, value_getter: Callable[[BlobObject], int | float]):
        self.value_getter = value_getter

    def calculate(self, calculator: "TreeCalculator[int | float]", objects: Objects, root_obj: StoredObject) -> int | float:
        if root_obj.object_type == ObjectType.TREE:
            return sum(calculator[child_id, objects] for child_id in root_obj.children.values())
        else:
            assert root_obj.object_type == ObjectType.BLOB
            return self.value_getter(root_obj)

    def for_none(self, calculator: "TreeCalculator[int | float]") -> int | float:
        return 0


class TreeCalculator[R]:
    def __init__(self, calculator: ValueCalculator[R]):
        self.calculator = calculator

        self._cache: Dict[ObjectID | None, R] = dict()

    def __getitem__(self, item: (ObjectID | None, Objects)) -> R:
        (root_id, objects) = item
        if root_id not in self._cache:
            if root_id is not None:
                root_obj = objects[root_id]
                self._cache[root_id] = self.calculator.calculate(self, objects, root_obj)
            else:
                self._cache[root_id] = self.calculator.for_none(self)
        return self._cache[root_id]
