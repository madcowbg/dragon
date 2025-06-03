import unittest

import varint
from varint import decode_buffer


class MyTestCase(unittest.TestCase):
    def test_small_encodes_are_equal_to_byte_representation(self):
        for b in range(128):
            encoded = bytearray()
            encoded.append(b)
            self.assertEqual(encoded, varint.encode(b))

    def test_larger_varints(self):
        many_ints = list(int(i) for i in range(100000))

        packed = bytearray()
        for i in many_ints:
            packed += varint.encode(i)

        self.assertEqual(283488, len(packed))

        idx = 0
        unpacked = []
        while idx < len(packed):
            new_unpacked, idx = decode_buffer(packed, idx)
            unpacked.append(new_unpacked)

        self.assertEqual(100000, len(unpacked))
        self.assertEqual(many_ints, unpacked)


if __name__ == '__main__':
    unittest.main()
