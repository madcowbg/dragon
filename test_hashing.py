import asyncio
import tempfile
import unittest
from os.path import join

from hashing import fast_hash, calc_file_md5
from test_repo_command import write_contents


class TestHashing(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_fast_hash(self):
        test_filename = join(self.tmpdir.name, "test_fasthash")
        data = "".join([str(f * 12311831028 % 23129841) for f in range(1, 100000)])
        write_contents(test_filename, data)

        self.assertEqual("6f3aa4fb14b217b20aed6f98c137cf4c", asyncio.run(fast_hash(test_filename, chunk_size=1 << 16)))

    def test_fast_hash_ignores_some(self):
        test_filename = join(self.tmpdir.name, "test_fasthash")
        data = "".join([str(f * 12311831028 % 23129841) for f in range(1, 100000)])
        ld = list(data)
        print(ld[33000])
        ld[66000] = 'g'  # change one byte
        d2 = "".join(ld)
        write_contents(test_filename, d2)

        self.assertEqual("6f3aa4fb14b217b20aed6f98c137cf4c", asyncio.run(fast_hash(test_filename, chunk_size=1 << 16)))

    def test_md5(self):
        test_filename = join(self.tmpdir.name, "test_fasthash")
        write_contents(test_filename, "".join([str(f * 12311831028 % 23129841) for f in range(1, 100000)]))

        self.assertEqual("26c465fd88266ccb913dabb5606572f9", calc_file_md5(test_filename))
