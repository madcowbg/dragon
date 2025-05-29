import abc
from abc import abstractmethod
from typing import Callable, Iterable, Tuple

from lmdb_storage.tree_object import ObjectType, StoredObject, TreeObject
from lmdb_storage.tree_structure import ObjectID, Objects


class StatGetter[T, R]:
    @abstractmethod
    def __getitem__(self, item: T) -> R: pass

class ValueCalculator[T, R](abc.ABC):
    @abc.abstractmethod
    def calculate(self, calculator: StatGetter[T, R], obj: T) -> R:
        pass

    @abc.abstractmethod
    def for_none(self, calculator: StatGetter[T, R]) -> R:
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
    def __init__(self, value_getter: Callable[[I], R], reader: RecursiveReader[T, I]):
        self.value_getter = value_getter
        self.reader = reader

    def calculate(self, calculator: "StatGetter[T, R]", item: T) -> R:
        if self.reader.is_compound(item):
            return self.aggregate(
                (child_name, calculator[child_node_at_path]) for child_name, child_node_at_path in
                self.reader.children(item))
        else:
            assert self.reader.is_atom(item)
            return self.value_getter(self.reader.convert(item))

    @abc.abstractmethod
    def aggregate(self, items: Iterable[Tuple[str, R]]) -> R:
        pass


class RecursiveSumCalculator[T, I](RecursiveCalculator[T, I, int | float]):
    def aggregate(self, items: Iterable[Tuple[str, int | float]]) -> int | float:
        return sum(v for _, v in items)

    def for_none(self, calculator: "StatGetter[T, int | float]") -> int | float:
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

