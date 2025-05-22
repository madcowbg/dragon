from typing import Tuple, List, Iterable, Callable


class FastAssociation[V]:
    def __init__(self, keys: Tuple[str], values: List[V | None]):
        self._keys: Tuple[str] = keys
        self._values: List[V | None] = values

    def get_if_present(self, root_name: str) -> V | None:
        return self._values[self._keys.index(root_name)]

    def assigned_keys(self) -> Iterable[str]:
        for key, value in zip(self._keys, self._values):
            if value is not None:
                yield key

    def available_items(self) -> Iterable[Tuple[int, V]]:
        for i, value in enumerate(self._values):
            if value is not None:
                yield i, value

    def new[Z](self):
        return FastAssociation[Z](self._keys, [None] * len(self._keys))

    def __getitem__(self, key: int) -> V | None:
        return self._values[key]

    def __setitem__(self, key: int, value: V):
        self._values[key] = value

    def map[R](self, func: Callable[[V], R]) -> "FastAssociation[R]":
        return FastAssociation[R](self._keys, [None if v is None else func(v) for v in self._values])

    def filter(self, func: Callable[[V], bool]) -> "FastAssociation[V]":
        return FastAssociation[V](self._keys, [v if func(v) else None for v in self._values])

    def values(self) -> List[V | None]:
        return [v for v in self._values if v is not None]

    def keyed_items(self)-> Iterable[Tuple[str, V]]:
        for key, value in zip(self._keys, self._values):
            if value is not None:
                yield key, value
