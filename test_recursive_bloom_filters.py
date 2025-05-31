import hashlib
from unittest import TestCase


class BloomFilter256:
    def __init__(self):
        self.max_size = 8
        self.data: bytes = bytearray(self.max_size * 8)

    def add(self, item: bytes) -> None:
        assert len(item) == 16, f"item must be a hash of 16 bytes!, is {item.hex()}"
        for b in item:
            assert b % 8 == b & 0x07
            assert b // 8 == b >> 3
            self.data[b >> 3] |= 1 << (b & 0x07)  # efficient way to get lower bytes

    def contains(self, value_hash: bytes) -> bool:
        for b in value_hash:
            if ~self.data[b >> 3] & (1 << (b & 0x07)):
                return False
        return True

    def bit_count(self):
        return sum(b.bit_count() for b in self.data)


class StringBloomFilter:
    def __init__(self):
        self.filter = BloomFilter256()

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

        self.assertFalse(f.contains("alphx".encode()))

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
        self.assertEqual(470, sum(false_positives))  # aboute 10% false positives is expected