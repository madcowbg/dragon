import abc
from typing import Callable, Dict

from lmdb_storage.file_object import BlobObject
from lmdb_storage.tree_structure import ObjectID, Objects
from lmdb_storage.tree_object import ObjectType, StoredObject, TreeObject


class ValueCalculator[T, O, R](abc.ABC):
    @abc.abstractmethod
    def calculate(self, calculator: "TreeCalculator[T, R]", obj: O) -> R:
        pass

    @abc.abstractmethod
    def for_none(self, calculator: "TreeCalculator[T, R]") -> R:
        pass


class CompoundObj[T]:
    children: Dict[str, T]


class RecursiveSumCalculator[K, S, A](ValueCalculator[K, S, int | float]):
    def __init__(self, value_getter: Callable[[A], int | float]):
        self.value_getter = value_getter

    def calculate(self, calculator: "TreeCalculator[S, int | float]", obj: S) -> int | float:
        if self.is_compound(obj):
            obj: CompoundObj[K]
            return sum(calculator[child_id] for _, child_id in obj.children)
        else:
            assert self.is_atom(obj)
            return self.value_getter(obj)

    def for_none(self, calculator: "TreeCalculator[S, int | float]") -> int | float:
        return 0

    @abc.abstractmethod
    def is_compound(self, obj: StoredObject) -> bool:
        pass

    @abc.abstractmethod
    def is_atom(self, obj: StoredObject) -> bool:
        pass


class RecursiveStoredTreeSumCalculator[K, A](RecursiveSumCalculator[K, StoredObject, int | float]):
    def is_compound(self, obj: StoredObject) -> bool:
        return obj.object_type == ObjectType.TREE

    def is_atom(self, obj: StoredObject) -> bool:
        return obj.object_type == ObjectType.BLOB


class TreeCalculator[T, R]:
    def __init__(self, calculator: ValueCalculator[ObjectID, T, R]):
        self.calculator = calculator
        self._cache: Dict[ObjectID | None, R] = dict()

    def __getitem__(self, root_id: ObjectID) -> R:
        if root_id not in self._cache:
            if root_id is not None:
                root_obj = self.compute_object(root_id)
                self._cache[root_id] = self.calculator.calculate(self, root_obj)
            else:
                self._cache[root_id] = self.calculator.for_none(self)
        return self._cache[root_id]

    @abc.abstractmethod
    def compute_object(self, root_id: ObjectID) -> T:
        pass


class StoredTreeCalculator[R](TreeCalculator[StoredObject, R]):
    def __init__(self, objects: Objects, calculator: ValueCalculator[ObjectID, StoredObject, R]):
        super().__init__(calculator)
        self.objects = objects

    def compute_object(self, root_id):
        with self.objects as objects:
            root_obj = objects[root_id]
        return root_obj
