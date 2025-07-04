import logging
import random
from typing import Dict, Any, Callable, Tuple

from lmdb import Transaction
from msgspec import msgpack

from contents.hashable_key import HashableKey
from lmdb_storage.object_store import used_ratio
from lmdb_storage.stats_cache import StatsCache
from lmdb_storage.tree_calculation import StatGetter, ValueCalculator
from lmdb_storage.tree_object import ObjectID


class Calculator[T, R](StatGetter[T, R]):
    def __init__(self, calculator: ValueCalculator[T, R], callback: Callable[[], None] | None = None):
        self.calculator = calculator
        self.callback = callback

    def __getitem__(self, item: T | None) -> R:
        result = self.calculator.calculate(self, item) if item is not None else self.calculator.for_none(self)
        if self.callback:
            self.callback()
        return result


class CachedCalculator[T, R](StatGetter[T, R]):
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


APP_STATS_CACHE: StatsCache | None = None
APP_STATS_CACHE_READER: Transaction | None = None


def app_stats_cache() -> Tuple[StatsCache, Transaction]:
    global APP_STATS_CACHE
    global APP_STATS_CACHE_READER
    if APP_STATS_CACHE is None:
        APP_STATS_CACHE = StatsCache("./app-cache.lmdb")
        APP_STATS_CACHE_READER = APP_STATS_CACHE.begin(write=False)
        APP_STATS_CACHE_READER.__enter__()
    return APP_STATS_CACHE, APP_STATS_CACHE_READER


APP_CACHE_LOG_RATIO = 10000


class AppCachedCalculator[T, R](StatGetter[T, R]):
    def __init__(self, calculator: ValueCalculator[T, R], result_type: type[R]):
        self.calculator = calculator
        assert self.calculator.stat_cache_key is not None

        self._cache: Dict[Any, R] = dict()
        self._result_type = result_type

        self._stats_cache, self._cache_reader = app_stats_cache()

    def __getitem__(self, item: T) -> R:
        if item not in self._cache:
            if item is not None:
                assert isinstance(item, HashableKey)

                item_key = self.calculator.stat_cache_key + item.hashed
                cached_blob = self._cache_reader.get(item_key)

                if random.randint(0, APP_CACHE_LOG_RATIO - 1) == 0:
                    logging.warn(f"calculating stats, used%: {used_ratio(self._stats_cache._env)}\n")

                if cached_blob is not None:
                    self._cache[item] = msgpack.decode(cached_blob, type=self._result_type)

                if item not in self._cache:
                    self._cache[item] = self.calculator.calculate(self, item)

                result = self._cache[item]

                if result.should_store():
                    cached_blob = msgpack.encode(result)
                    with self._stats_cache.begin(write=True) as cache:
                        cache.put(item_key, cached_blob)
            else:
                self._cache[item] = self.calculator.for_none(self)
        return self._cache[item]
