import hashlib
import timeit
from array import array
from unittest import TestCase


class BloomFilter256:
    """Bloom filter with 256-bit data. Good for up to 32 elements."""

    def __init__(self):
        self.max_size = 256 // 8
        self.data: bytes = bytearray(self.max_size)

    def add(self, item: bytes) -> None:
        assert len(item) == 16, f"item must be a hash of 16 bytes!, is {item.hex()}"
        for b in item:
            assert b % 8 == b & 0x07
            assert b // 8 == b >> 3
            self.data[b >> 3] |= 1 << (b & 0x07)  # efficient way to get lower bytes

    def contains(self, item: bytes) -> bool:
        assert len(item) == 16, f"item must be a hash of 16 bytes!, is {item.hex()}"
        for b in item:
            if ~self.data[b >> 3] & (1 << (b & 0x07)):
                return False
        return True

    def bit_count(self):
        return sum(b.bit_count() for b in self.data)


class BloomFilterM:
    def __init__(self, m: int, compression: int = 0):
        assert m >= 8
        assert not (m & (m - 1)), f"{m} is not a power of 2!"

        self.compression = compression

        self.max_size_bits = m
        self.max_size_l = m >> (5 + compression)

        # address_size is number of bytes to use
        self.hash_size = (((self.max_size_bits - 1).bit_length() - 1) >> 5) + 1
        assert self.hash_size == 1

        self.data: array = array("L", bytearray(self.max_size_l * 4))

        self.k = 4

    def add(self, item: bytes) -> None:
        item_long = array("L", item)

        assert len(item_long) == 4, f"item must be a hash of 16 bytes!, is {item.hex()}"
        hash_mask = self.max_size_l - 1
        mod_mask = len(item_long) - 1
        slow_case = self.hash_size > 1
        byte_addr_plus_offset = 5 + self.compression

        for i in range(4):
            if slow_case:
                b = 0
                to_j = i + self.hash_size
                j = i
                while j < to_j:
                    b = (b << 32) | item_long[j & mod_mask]
                    j += 1
            else:
                b = item_long[i]

            # assert b % 32 == b & 0x1f
            # assert b // 32 == b >> 5
            # assert (b // 32) & hash_mask == (b // 32) % self.max_size_l

            self.data[(b >> byte_addr_plus_offset) & hash_mask] |= 1 << (b & 0x1f)  # efficient way to get lower bytes

    def contains(self, item: bytes) -> bool:
        item_long = array("L", item)

        assert len(item_long) == 4, f"item must be a hash of 16 bytes!, is {item.hex()}"
        hash_mask = self.max_size_l - 1
        mod_mask = len(item_long) - 1
        slow_case = self.hash_size > 1
        byte_addr_plus_offset = 5 + self.compression

        for i in range(4):
            if slow_case:
                b = 0
                to_j = i + self.hash_size
                j = i
                while j < to_j:
                    b = (b << 32) | item_long[j & mod_mask]
                    j += 1
            else:
                b = item_long[i]

            if ~self.data[(b >> byte_addr_plus_offset) & hash_mask] & (1 << (b & 0x1f)):
                return False

        return True

    def bit_count(self):
        return sum(b.bit_count() for b in self.data)

    def compress(self, compression: int):
        assert compression >= 0
        compressed = BloomFilterM(m=self.max_size_bits, compression=compression)
        rate_to_compact = compression - self.compression
        assert rate_to_compact >= 0

        for i in range(len(self.data)):
            compressed.data[i >> rate_to_compact] |= self.data[i]

        return compressed


class StringBloomFilter:
    def __init__(self, m: int = None, compression: int = 0, filter: BloomFilterM | BloomFilter256 | None = None):
        if filter is not None:
            assert m is None
            self.filter = filter
        else:
            self.filter = BloomFilterM(m, compression) if m is not None else BloomFilter256()

    def __iadd__(self, item: str) -> "StringBloomFilter":
        assert isinstance(item, str)
        self.filter.add(self._digest(item))
        return self

    def __ior__(self, item: str) -> "StringBloomFilter":
        return self.__iadd__(item)

    def __contains__(self, item: str) -> bool:
        return self.filter.contains(self._digest(item))

    def _digest(self, item: str):
        return hashlib.md5(item.encode()).digest()

    def bit_count(self) -> int:
        return self.filter.bit_count()

    def compress(self, compression: int):
        assert compression >= 0
        assert isinstance(self.filter, BloomFilterM)
        return StringBloomFilter(filter=self.filter.compress(compression))


class TestBloomFilters(TestCase):
    def setUp(self):
        pass

    def test_create_bloom_filter(self):
        value_hash = hashlib.md5("alpha".encode()).digest()

        f = BloomFilter256()
        self.assertEqual(0, f.bit_count())

        f.add(value_hash)
        self.assertEqual(15, f.bit_count())

        self.assertTrue(f.contains(value_hash))

        self.assertFalse(f.contains(hashlib.md5("alphx".encode()).digest()))

        self.assertTrue(8, f.bit_count())

    def test_create_bloom_filter_m(self):
        value_hash = hashlib.md5("alpha".encode()).digest()

        f = BloomFilterM(256)
        self.assertEqual(0, f.bit_count())

        f.add(value_hash)
        self.assertEqual(4, f.bit_count())

        self.assertTrue(f.contains(value_hash))

        self.assertFalse(f.contains(hashlib.md5("alphx".encode()).digest()))

        self.assertTrue(8, f.bit_count())

    def test_create_string_bloom_filter(self):
        values = ["omkasd", "alshj", "jpa9d 9.", "hkuais", "lhiuas", "alpha", "beta"]
        f = StringBloomFilter()
        self.assertEqual(0, f.bit_count())

        for value in values:
            f += value

        self.assertTrue("alpha" in f)
        for value in values:
            self.assertTrue(value in f)

        self.assertEqual(86, f.bit_count())

        self.assertFalse("adsa" in f)
        self.assertFalse("adsa" in f)
        self.assertFalse("adsa" in f)
        self.assertFalse("adsa" in f)

        f += "alpha"
        self.assertEqual(86, f.bit_count())

        f += "whatevs"
        self.assertEqual(99, f.bit_count())

    def test_fill_filter_to_capacity(self):
        values = [f"{i}" for i in range(32)]
        fillrate = list()

        f = StringBloomFilter()
        for v in values:
            f |= v
            fillrate.append(f.bit_count())

        self.assertEqual([59, 101, 136, 160, 179, 195, 208, 221], fillrate[3::4])

        for v in values:
            self.assertTrue(v in f)

        false_positives = [f"not {i}" in f for i in range(5000)]
        self.assertEqual(470, sum(false_positives))  # about 9.76% false positives is expected

    def test_fill_m_filter_to_capacity(self):
        values = [f"{i}" for i in range(256)]
        fillrate = list()

        f = StringBloomFilter(256 * 8)
        for v in values:
            f |= v
            fillrate.append(f.bit_count())

        self.assertEqual([16, 140, 256, 360, 463, 552, 641, 729], fillrate[3::4 * 8])

        for v in values:
            self.assertTrue(v in f)

        false_positives = [f"not {i}" in f for i in range(5000)]
        self.assertEqual(114, sum(false_positives))  # about 2.55% false positives is expected

    def test_fill_m_filter_to_capacity_larger(self):
        values = [f"{i}" for i in range(500000)]

        start = timeit.default_timer()
        f = StringBloomFilter(8 * (1 << 19))
        for v in values:
            f |= v
        end = timeit.default_timer()
        print(f"creation time: {end - start}s")

        self.assertEqual(1591150, f.bit_count())

        start = timeit.default_timer()
        for v in values:
            self.assertTrue(v in f)
        end = timeit.default_timer()
        print(f"check time: {end - start}s")

        start = timeit.default_timer()
        false_positives = [f"not {i}" in f for i in range(500000)]
        self.assertEqual(10319, sum(false_positives))
        end = timeit.default_timer()
        print(f"check nonexistent positives: {end - start}s")

    def test_fill_with_compression_works_about_the_same(self):
        values = [f"{i}" for i in range(256)]
        fillrate = list()

        f = StringBloomFilter(256 * 8 * 16, compression=4)

        for v in values:
            f |= v
            fillrate.append(f.bit_count())

        self.assertEqual([16, 135, 251, 363, 468, 561, 644, 726], fillrate[3::4 * 8])
        self.assertEqual(256 / 4, len(f.filter.data))

        for v in values:
            self.assertTrue(v in f)

        false_positives = [f"not {i}" in f for i in range(5000)]
        self.assertEqual(125, sum(false_positives))  # about 2.55% false positives is expected

    def test_fill_with_compression_is_same_as_without(self):
        values = [f"{i}" for i in range(256)]

        fillrate = list()
        f = StringBloomFilter(256 * 8 * 16)
        for v in values:
            f |= v
            fillrate.append(f.bit_count())

        compressed_f = f.compress(4)
        self.assertEqual(256 // 4, len(compressed_f.filter.data))

        fillrate = list()
        total_f = StringBloomFilter(256 * 8 * 16, compression=4)
        for v in values:
            total_f |= v
            fillrate.append(total_f.bit_count())

        self.assertEqual([16, 135, 251, 363, 468, 561, 644, 726], fillrate[3::4 * 8])
        self.assertEqual(256 / 4, len(total_f.filter.data))

        self.assertEqual(compressed_f.filter.data, total_f.filter.data)

        false_positives = [f"not {i}" in compressed_f for i in range(5000)]
        self.assertEqual(125, sum(false_positives))  # about 2.55% false positives is expected

        differences_in_unequal = [(f"not {i}" in compressed_f) != (f"not {i}" in total_f) for i in range(5000)]
        self.assertEqual(0, sum(differences_in_unequal))  # they should be identical

    def test_compress_twice_produces_same_filter(self):
        values = [f"{i}" for i in range(256)]

        fillrate = list()
        f = StringBloomFilter(256 * 8 * 16)
        for v in values:
            f |= v
            fillrate.append(f.bit_count())

        compressed_f = f.compress(2)
        self.assertEqual(256 * 4 // 4, len(compressed_f.filter.data))

        compressed_f = compressed_f.compress(4)
        self.assertEqual(256 // 4, len(compressed_f.filter.data))

        fillrate = list()
        total_f = StringBloomFilter(256 * 8 * 16, compression=4)
        for v in values:
            total_f |= v
            fillrate.append(total_f.bit_count())

        self.assertEqual([16, 135, 251, 363, 468, 561, 644, 726], fillrate[3::4 * 8])
        self.assertEqual(256 / 4, len(total_f.filter.data))

        self.assertEqual(compressed_f.filter.data, total_f.filter.data)

        false_positives = [f"not {i}" in compressed_f for i in range(5000)]
        self.assertEqual(125, sum(false_positives))  # about 2.55% false positives is expected

        differences_in_unequal = [(f"not {i}" in compressed_f) != (f"not {i}" in total_f) for i in range(5000)]
        self.assertEqual(0, sum(differences_in_unequal))  # they should be identical

    def test_compression_speed(self):
        values = [f"{i}" for i in range(100)]

        start = timeit.default_timer()
        f = StringBloomFilter(8 * (1 << 20)) # 1m elements
        for v in values:
            f |= v
        end = timeit.default_timer()
        print(f"creation time: {end - start}s")

        start = timeit.default_timer()
        for _ in range(20):
            compressed = f
            for i in range(10):
                compressed = compressed.compress(i)
            for v in values:
                self.assertTrue(v in f)

        end = timeit.default_timer()
        print(f"compression time for 20*10 times: {end - start}s")

        start = timeit.default_timer()
        for _ in range(100):
            compressed = f.compress(9)

        end = timeit.default_timer()
        print(f"compression time for 1000 cycles by 1024 times: {end - start}s")

        start = timeit.default_timer()
        for _ in range(100):
            compressed = f.compress(1)

        end = timeit.default_timer()
        print(f"compression time for 1000 cycles by 2 times: {end - start}s")

        self.assertEqual(388, f.compress(10).bit_count())
