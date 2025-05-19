import abc
from typing import Collection, Iterable, Tuple, Callable, Type, List, Dict

from lmdb_storage.tree_structure import ObjectID, Objects, TreeObject


class ByRoot[V]:
    def __init__(self, allowed_roots: Collection[str], roots_to_object: Iterable[Tuple[str, V | None]] = ()):
        self.allowed_roots = allowed_roots
        self._roots_to_object = dict((k, v) for k, v in roots_to_object if v is not None)
        for child_name in self._roots_to_object:
            assert child_name in self.allowed_roots, f"Child name '{child_name}' not found in allowed roots list"

    def new(self) -> "ByRoot[V]":
        return ByRoot[V](self.allowed_roots)

    def __len__(self) -> int:  # fixme why do we need to get length? there is no unambiguous answer
        return len(self._roots_to_object.values())

    def get_if_present(self, child_name: str, default: ObjectID | None = None) -> V | None:
        assert child_name in self.allowed_roots, f"Can't get child '{child_name}'!"
        return self._roots_to_object.get(child_name, default)

    def __setitem__(self, child_name: str, value: ObjectID | None):
        assert child_name in self.allowed_roots, f"Can't set child '{child_name}'!"
        if value is None:
            del self._roots_to_object[child_name]  # setting to None deletes the value if set
        else:
            self._roots_to_object[child_name] = value

    def __contains__(self, child_name: str) -> bool:
        assert child_name in self.allowed_roots, f"Can't check if contains a child '{child_name}'!"
        return child_name in self._roots_to_object

    def copy(self) -> "ByRoot[ObjectID]":
        return ByRoot[ObjectID](self.allowed_roots, self._roots_to_object.items())

    def map[R](self, mapper: Callable[[V], R]) -> "ByRoot[R]":
        return ByRoot[R](self.allowed_roots, remap(self._roots_to_object, mapper).items())

    def values(self) -> Collection[V]:
        return self._roots_to_object.values()

    def items(self) -> Collection[Tuple[str, V]]:
        return self._roots_to_object.items()

    def assigned_keys(self) -> Collection[str]:
        return self._roots_to_object.keys()

    def filter_type[T](self, selected_type: Type[T], exclude: bool = False):
        return ByRoot[T](
            self.allowed_roots,
            remap(self._roots_to_object, lambda obj: obj if (exclude ^ (type(obj) is selected_type)) else None).items())

    def __add__(self, other: "ByRoot[V]") -> "ByRoot[V]":
        assert isinstance(other, ByRoot)
        assert set(self.allowed_roots) == set(other.allowed_roots)
        return ByRoot[V](
            self.allowed_roots + other.allowed_roots,
            list(self._roots_to_object.items()) + list(other._roots_to_object.items()))

    def subset_keys(self, subset_roots: Collection[str]) -> List[str]:
        return [r for r in self.assigned_keys() if r in subset_roots]

    def subset(self, subset_roots: Collection[str]) -> "ByRoot[V]":
        return ByRoot[V](subset_roots, [(name, obj) for name, obj in self.items() if name in subset_roots])


def remap[A, B, C](dictionary: Dict[A, B], key: Callable[[B], C]) -> Dict[A, C]:
    return dict((k, key(v)) for k, v in dictionary.items())


class ObjectsByRoot:
    @classmethod
    def singleton(cls, name, file):
        return ByRoot[ObjectID]([name], ((name, file),))

    @classmethod
    def from_map(cls, dictionary: Dict[str, ObjectID]) -> "ByRoot[ObjectID]":
        return ByRoot[ObjectID](list(dictionary), dictionary.items())


class MergeResult[F, R]:
    @abc.abstractmethod
    def add_for_child(self, child_name: str, merged_child_by_roots: R) -> None:
        pass

    @abc.abstractmethod
    def get_value(self) -> R:
        pass


class SeparateRootsMergeResult[F](MergeResult[F, ByRoot[ObjectID]]):
    def __init__(self, allowed_roots: List[str], objects: Objects[F]):
        self.allowed_roots = allowed_roots
        self.objects = objects

        self._merged_children: Dict[str, TreeObject] = dict()

    def add_for_child(self, child_name: str, merged_child_by_roots: ByRoot[ObjectID]) -> None:
        for root_name, obj_id in merged_child_by_roots.items():
            if root_name not in self._merged_children:
                self._merged_children[root_name] = TreeObject({})

            self._merged_children[root_name].children[child_name] = obj_id

    def get_value(self) -> ByRoot[ObjectID]:
        # store potential new objects
        for root_name, child_tree in self._merged_children.items():
            new_child_id = child_tree.id
            self.objects[new_child_id] = child_tree

        result = ByRoot[ObjectID](
            self.allowed_roots,
            ((root_name, child_tree.id) for root_name, child_tree in self._merged_children.items()))

        return result
