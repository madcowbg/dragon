import logging
import random
import sys
from typing import Dict, Any

from msgspec import msgpack

from contents.recursive_stats_calc import CompositeNodeID, HashableKey
from lmdb_storage.object_store import used_ratio
from lmdb_storage.stats_cache import StatsCache
from lmdb_storage.tree_calculation import StatGetter, ValueCalculator
from lmdb_storage.tree_object import ObjectID


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


def app_stats_cache() -> StatsCache:
    global APP_STATS_CACHE
    if APP_STATS_CACHE is None:
        APP_STATS_CACHE = StatsCache("./app-cache.lmdb")
    return APP_STATS_CACHE


APP_CACHE_LOG_RATIO = 1000


class AppCachedCalculator[T, R](StatGetter[T, R]):
    def __init__(self, calculator: ValueCalculator[T, R], result_type: type[R]):
        self.calculator = calculator
        assert self.calculator.stat_cache_key is not None

        self._cache: Dict[Any, R] = dict()
        self._result_type = result_type

        self._stats_cache: StatsCache = app_stats_cache()

        self._cache_reader = self._stats_cache.begin(write=False)
        self._cache_reader.__enter__()

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
