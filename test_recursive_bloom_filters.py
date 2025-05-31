import hashlib
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
    def __init__(self, m):
        assert m >= 8
        assert not (m & (m - 1)), f"{m} is not a power of 2!"

        self.max_size_bits = m
        self.max_size_b = m >> 3

        # address_size is number of bytes to use
        self.hash_size = (((self.max_size_bits - 1).bit_length() - 1) >> 3) + 1

        self.data: bytes = bytearray(self.max_size_b)

    def add(self, item: bytes) -> None:
        assert len(item) == 16, f"item must be a hash of 16 bytes!, is {item.hex()}"
        hash_mask = self.max_size_b - 1
        mod_mask = len(item) - 1
        for i in range(len(item)):
            b = 0
            to_j = i + self.hash_size
            j = i
            while j < to_j:
                b = (b << 8) | item[j & mod_mask]
                j += 1

            # assert b % 8 == b & 0x07
            # assert b // 8 == b >> 3
            # assert (b // 8) & hash_mask == (b // 8) % self.max_size_b
            self.data[(b >> 3) & hash_mask] |= 1 << (b & 0x07)  # efficient way to get lower bytes

    def contains(self, item: bytes) -> bool:
        assert len(item) == 16, f"item must be a hash of 16 bytes!, is {item.hex()}"
        hash_mask = self.max_size_b - 1
        mod_mask = len(item) - 1

        for i in range(len(item)):
            b = 0
            for j in range(self.hash_size):
                b = (b << 8) | item[(i + j) & mod_mask]

            if ~self.data[(b >> 3) & hash_mask] & (1 << (b & 0x07)):
                return False
        return True

    def bit_count(self):
        return sum(b.bit_count() for b in self.data)


class StringBloomFilter:
    def __init__(self, m: int = None):
        self.filter = BloomFilterM(m) if m is not None else BloomFilter256()

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

        self.assertFalse(f.contains(hashlib.md5("alphx".encode()).digest()))

        self.assertTrue(8, f.bit_count())

    def test_create_bloom_filter_m(self):
        value_hash = hashlib.md5("alpha".encode()).digest()

        f = BloomFilterM(256)
        self.assertEqual(0, f.bit_count())

        f.add(value_hash)
        self.assertEqual(15, f.bit_count())

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

        self.assertEqual([64, 500, 840, 1101, 1300, 1463, 1587, 1680], fillrate[3::4 * 8])

        for v in values:
            self.assertTrue(v in f)

        false_positives = [f"not {i}" in f for i in range(5000)]
        self.assertEqual(451, sum(false_positives))  # about 9.76% false positives is expected


    def test_fill_m_filter_to_capacity_larger(self):
        values = [f"{i}" for i in range(500000)]
        fillrate = list()

        f = StringBloomFilter(8 * (1 << 19))
        for v in values:
            f |= v

        self.assertEqual(3571738, f.bit_count())

        for v in values:
            self.assertTrue(v in f)

        false_positives = [f"not {i}" in f for i in range(5000)]
        self.assertEqual(378, sum(false_positives))
