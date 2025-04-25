import hashlib
import logging
import os
import pathlib
import random
import string
import tempfile
import unittest
from typing import Tuple, List
from unittest import TestCase

import msgpack

from lmdb_storage.file_object import FileObject
from lmdb_storage.object_store import ObjectStorage

random.seed(42)
vocabulary = [
    ''.join(random.choices(string.ascii_uppercase + string.digits, k=random.randint(5, 20)))
    for _ in range(100)]


def random_append(paths: List[List[str | None]]):
    existing = set()
    for _ in range(25):
        cut_points = sorted(random.choices(range(len(paths)), k=random.randint(5, 20)))
        vocab_choice = random.choices(vocabulary, k=len(cut_points) + 1)
        cut_point_idx = 0
        for idx in range(len(paths)):
            while cut_point_idx < len(cut_points) and idx > cut_points[cut_point_idx]:
                cut_point_idx += 1

            if (len(paths[idx]) == 0 or paths[idx][-1] is not None) and random.randint(0, 3) > 0:
                paths[idx].append(vocab_choice[cut_point_idx])

        for idx in range(len(paths)):
            if random.randint(0, 9) == 0 and len(paths[idx]) > 0 and paths[idx][-1] is not None and "|".join(
                    paths[idx]) not in existing:
                existing.add("|".join(paths[idx]))
                paths[idx].append(None)


def populate_index(seed_value, n_files) -> List[Tuple[str, str, int]]:
    random.seed(seed_value)

    paths = [[] for _ in range(n_files)]
    random_append(paths)

    result = []
    for path in paths:
        size = random.randint(1, 5) * 10
        fasthash = hashlib.sha1("".join(random.choice(vocabulary)).encode()).hexdigest()
        result.append(("/".join(path[:-1]), fasthash, size))
    return list(sorted(result))


_random_data = pathlib.Path("tests/random_data.txt")
if not _random_data.is_file():
    _random_data.parent.mkdir(parents=True, exist_ok=True)

    first_index = populate_index(42, 100000)
    second_index = populate_index(41, 100000)
    with _random_data.open("wb") as f:
        msgpack.dump([first_index, second_index], f)
else:
    with _random_data.open("rb") as f:
        [first_index, second_index] = msgpack.load(f, use_list=True)


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
            ['0GET8T63J3R30ME', 'e5fb9288a26f70113cdfa2892fccbe89f21f8ee0', 10],
            ['0GET8T63J3R30ME/3D8W3ZP08J3TRP0J43', '761f7af831fcfe758507ebdebacb5aa2a0919990', 50],
            ['0GET8T63J3R30ME/3D8W3ZP08J3TRP0J43/3KKV9RFTMTTQL/96QE3RZSJ49I',
             '815cbb15215a75af818813caac199e68332ad4a7', 30],
            ['0GET8T63J3R30ME/3D8W3ZP08J3TRP0J43/3KKV9RFTMTTQL/GNZS43C5BA75UUZPEA/96QE3RZSJ49I/HR09SKDGIGA/8OS9XTOG/S0R2SDS8B25SQ8CROY/8DALY8OZCY/ZP6LFEWVZNVKSP2EX5T5/XO9T7E8G8JDP0LVS/8OS9XTOG/A0L7253J2D54I3QK2I/J99T2/XPNNGH8',
             'd87b621583b5d5f4bf22c0942727a8787ad63d31', 50],
            ['0GET8T63J3R30ME/3D8W3ZP08J3TRP0J43/3KKV9RFTMTTQL/GNZS43C5BA75UUZPEA/96QE3RZSJ49I/HR09SKDGIGA/OHP6VZ41NAM148P0TVH/8OS9XTOG/ZP6LFEWVZNVKSP2EX5T5/XO9T7E8G8JDP0LVS/PQKOJTPV60VAIY/DIVUZ/8OS9XTOG',
             '663adba34d0e1ac8c1c6af1486a0f42447f65978', 20],
            ['0GET8T63J3R30ME/3D8W3ZP08J3TRP0J43/3KKV9RFTMTTQL/GNZS43C5BA75UUZPEA/96QE3RZSJ49I/XA11DPG8S/HR09SKDGIGA/8OS9XTOG',
             'a42172401f29761a0a880aefc0adac10b4c4dfaf', 10],
            ['0GET8T63J3R30ME/3D8W3ZP08J3TRP0J43/3KKV9RFTMTTQL/GNZS43C5BA75UUZPEA/96QE3RZSJ49I/XA11DPG8S/HR09SKDGIGA/OHP6VZ41NAM148P0TVH/8OS9XTOG/PQKOJTPV60VAIY/ZP6LFEWVZNVKSP2EX5T5/XO9T7E8G8JDP0LVS/PQKOJTPV60VAIY/DIVUZ/8OS9XTOG/A0L7253J2D54I3QK2I/DP5GJ/J99T2/XPNNGH8',
             '46eca2d71a64b0d0c1e9349d39657713499700c2', 50],
            ['0GET8T63J3R30ME/3D8W3ZP08J3TRP0J43/3KKV9RFTMTTQL/GNZS43C5BA75UUZPEA/96QE3RZSJ49I/XA11DPG8S/OHP6VZ41NAM148P0TVH/8OS9XTOG',
             '148f18cf9507d715a7a3a8f5e9d877fcbddc60fc', 10],
            ['0GET8T63J3R30ME/3D8W3ZP08J3TRP0J43/3KKV9RFTMTTQL/GNZS43C5BA75UUZPEA/S0R2SDS8B25SQ8CROY',
             '03acfc10f8d881bb3c366e0dd726cf4a98111f68', 10],
            ['0GET8T63J3R30ME/3D8W3ZP08J3TRP0J43/8DALY8OZCY', '7c8f0c48649b38522a1894c3c331b88d19278f5d', 40]],
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

        env.roots(write=True)["HEAD"].current = first_id
        logging.info("Run GC...")
        env.gc()
        logging.info("Done GC.")
