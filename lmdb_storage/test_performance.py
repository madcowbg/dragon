import hashlib
import logging
import os
import pathlib
import string
import tempfile
import random
import unittest
from typing import Tuple, List
from unittest import TestCase, skipIf

import msgpack

from lmdb_storage.file_object import FileObject
from lmdb_storage.object_store import ObjectStorage

random.seed(42)
vocabulary = [
    ''.join(random.choices(string.ascii_uppercase + string.digits, k=random.randint(5, 20)))
    for _ in range(100)]


def random_append(paths: List[List[str | None]]):
    for _ in range(10):
        for idx in range(len(paths)):
            if random.randint(0, 9) == 0 and (len(paths[idx]) == 0 or paths[idx][-1] is not None):
                paths[idx].append(None)

        cut_points = sorted(random.choices(range(len(paths)), k=random.randint(1, 10)))
        vocab_choice = random.choices(vocabulary, k=len(cut_points))
        cut_point_idx = 0
        for idx in range(len(paths)):
            while cut_point_idx < len(cut_points) and idx > cut_points[cut_point_idx]:
                cut_point_idx += 1
            if cut_point_idx == len(cut_points):
                break

            if (len(paths[idx]) == 0 or paths[idx][-1] is not None) and random.randint(0, 3) > 0:
                paths[idx].append(vocab_choice[cut_point_idx])


def populate_index(seed_value, n_files) -> List[Tuple[str, str, int]]:
    random.seed(seed_value)

    paths = [[] for _ in range(n_files)]
    random_append(paths)

    result = []
    for path in paths:
        size = random.randint(1, 5) * 10
        fasthash = hashlib.sha1("".join(random.choice(vocabulary)).encode()).hexdigest()
        result.append(("/".join(path[:-1]), fasthash, size))
    return result


_random_data = pathlib.Path("tests/random_data.txt")
if not _random_data.is_file():

    first_index = populate_index(42, 200000)
    second_index = populate_index(41, 200000)
    with _random_data.open("wb") as f:
        msgpack.dump([first_index, second_index], f)
else:
    with _random_data.open("rb") as f:
        [first_index, second_index] = msgpack.load(f)


@unittest.skipUnless(os.getenv('RUN_LENGTHY_TESTS'), reason="Lengthy test")
class TestPerformance(TestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s.%(msecs)04d - %(funcName)20s() - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S', force=True)

        self.tmpdir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_create_vocabulary_and_index(self):
        self.assertEqual([
            'AJI0Y6DP',
            'DIVUZ',
            'HV3A3ZMF8MDD4V30T9',
            'CKW5NGCX1945NQ4FM',
            'ZYCWTIQJ7YHL1',
            'IBLJH7',
            'HR5XFF0T0PVN9ER',
            '4FFYVN',
            'T84AZYTJXEPQ85JSG65',
            '4SHNF877VREN93'], vocabulary[:10])

        self.assertEqual([
            ['LN23Y30/9K8YQK94DNMKZA3O/WSQT26Q', '1c685715c99a3a80027b99510544ebbfadc7b2c0', 30],
            ['', 'f68dd39045704e391c39c75a338a2030675761c7', 30],
            ['LN23Y30/9K8YQK94DNMKZA3O/A0L7253J2D54I3QK2I/3D8W3ZP08J3TRP0J43/XPNNGH8/D6357OONNCILDZMFB7/F840982/IOYKL1CQ99CHJ75',
             '57f9658d75c77769af52a4d712e0c5bffb2b5632', 10],
            ['LN23Y30/9K8YQK94DNMKZA3O', 'f4f509e7f8bdb898a342e90a5dae9b4c164e4e91', 30],
            ['LN23Y30/9K8YQK94DNMKZA3O/WSQT26Q/3D8W3ZP08J3TRP0J43', 'c0d4d8e423a4e08fba1dde92d670061c34cf85c1', 10],
            ['LN23Y30/WSQT26Q', 'f8f44bab581a0a658f44a8a46effb9bd1a634f7c', 50],
            ['LN23Y30', '408c02004350088bb9f0015a4b6bfdf65cba8318', 20],
            ['LN23Y30/WSQT26Q/A0L7253J2D54I3QK2I/D6357OONNCILDZMFB7/F840982/IOYKL1CQ99CHJ75',
             'f8f44bab581a0a658f44a8a46effb9bd1a634f7c', 10],
            ['LN23Y30/9K8YQK94DNMKZA3O/WSQT26Q/XPNNGH8/D6357OONNCILDZMFB7/F840982',
             '11c3880297912213e85821d96f96c6a0021936dc', 20],
            ['LN23Y30/A0L7253J2D54I3QK2I/XPNNGH8/D6357OONNCILDZMFB7/IOYKL1CQ99CHJ75',
             '3e88659f07ee8eefd6ce9424936654bd5d713b65', 50]],
            first_index[:10])

    def test_populate_with_fake_data(self):
        logging.info("Opening object storage.")
        env = ObjectStorage(pathlib.Path(self.tmpdir.name).joinpath('fake_data.lmdb').as_posix(), map_size=1 << 30)

        with env.objects(write=True) as objects:
            logging.info("Creating first tree from tuples...")
            first_id = objects.mktree_from_tuples(
                [("/" + path + ".txt", FileObject.create(fasthash, size)) for path, fasthash, size in first_index])
            logging.info("Creating second tree from tuples...")
            second_id = objects.mktree_from_tuples(
                [("/" + path + ".txt", FileObject.create(fasthash, size)) for path, fasthash, size in
                 second_index])
            logging.info("Done creating trees...")
