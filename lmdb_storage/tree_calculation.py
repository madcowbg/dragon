import abc
from typing import Callable, Dict, Iterable, Tuple

from lmdb_storage.tree_object import ObjectType, StoredObject, TreeObject
from lmdb_storage.tree_structure import ObjectID, Objects


class ValueCalculator[T, R](abc.ABC):
    @abc.abstractmethod
    def calculate(self, calculator: "CachedCalculator[T, R]", obj: T) -> R:
        pass

    @abc.abstractmethod
    def for_none(self, calculator: "CachedCalculator[T, R]") -> R:
        pass


class RecursiveReader[T, I]:
    @abc.abstractmethod
    def convert(self, obj: T) -> I:
        pass

    @abc.abstractmethod
    def children(self, obj: T) -> Iterable[Tuple[str, T]]:
        pass

    @abc.abstractmethod
    def is_compound(self, obj: T) -> bool:
        pass

    @abc.abstractmethod
    def is_atom(self, obj: T) -> bool:
        pass


class RecursiveCalculator[T, I, R](ValueCalculator[I, R]):
    def __init__(self, value_getter: Callable[[T], R], reader: RecursiveReader[T, I]):
        self.value_getter = value_getter
        self.reader = reader

    def calculate(self, calculator: "CachedCalculator[T, R]", item: T) -> R:
        if self.reader.is_compound(item):
            return self.aggregate(
                (child_name, calculator[child_id]) for child_name, child_id in self.reader.children(item))
        else:
            assert self.reader.is_atom(item)
            return self.value_getter(self.reader.convert(item))

    @abc.abstractmethod
    def aggregate(self, items: Iterable[Tuple[str, R]]) -> R:
        pass


class RecursiveSumCalculator[T, I](RecursiveCalculator[T, I, int | float]):
    def aggregate(self, items: Iterable[Tuple[str, int | float]]) -> int | float:
        return sum(v for _, v in items)

    def for_none(self, calculator: "CachedCalculator[T, int | float]") -> int | float:
        return 0


class TreeReader(RecursiveReader[ObjectID, StoredObject]):
    def __init__(self, objects: Objects):
        self.objects = objects

    def convert(self, obj: ObjectID) -> StoredObject:
        with self.objects as objects:
            return objects[obj]

    def is_compound(self, item: ObjectID) -> bool:
        return self.convert(item).object_type == ObjectType.TREE

    def is_atom(self, item: ObjectID) -> bool:
        return self.convert(item).object_type == ObjectType.BLOB

    def children(self, item: ObjectID) -> Iterable[Tuple[str, ObjectID]]:
        loaded_obj: StoredObject = self.convert(item)
        assert loaded_obj.object_type == ObjectType.TREE
        loaded_obj: TreeObject
        return loaded_obj.children


class CachedCalculator[T, R]:
    def __init__(self, calculator: ValueCalculator[T, R]):
        self.calculator = calculator
        self._cache: Dict[ObjectID | None, R] = dict()

    def __getitem__(self, item: T) -> R:
        if item not in self._cache:
            if item is not None:
                self._cache[item] = self.calculator.calculate(self, item)
            else:
                self._cache[item] = self.calculator.for_none(self)
        return self._cache[item]
