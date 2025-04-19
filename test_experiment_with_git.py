import hashlib
import hashlib
import logging
import os
import pathlib
import pickle
import unittest
import zlib
from subprocess import Popen, PIPE
from typing import Dict

from alive_progress import alive_it

from sql_util import sqlite3_standard


def smudge(path: str): return path.replace(".git", "__GIT__")


@unittest.skipUnless(os.getenv('MYPROJECT_DEVELOPMENT_TEST'), reason="Uses total hoard")
class TestWithGit(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = pathlib.Path("test/exported-git")
        self.tmpdir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        pass

    async def test_export_db_to_git(self):
        os.chdir(self.tmpdir)

        process = Popen(["git", "init"], stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = process.communicate()
        process.wait()
        assert process.returncode == 0

        path = r"C:\Users\Bono\hoard\hoard.contents"
        is_readonly = True

        parallelism = 1
        # executor = ThreadPoolExecutor(max_workers=parallelism)
        with sqlite3_standard(f"file:{path}{'?mode=ro' if is_readonly else ''}", uri=True) as conn:
            # files: List[str] = list()
            # for fullpath, fasthash, size in alive_it(islice(conn.execute("SELECT fullpath, fasthash, size FROM fsobject"), 10000)):
            all_data = list(sorted(alive_it(conn.execute("SELECT fullpath, fasthash, size FROM fsobject"))))
            # split_in_batches = batched(all_data, max(10, len(all_data) // parallelism))

            hashed: Dict[str, bytes] = dict()
            # with alive_bar(len(all_data)) as bar:
            #     def create_objects(batch: List[Tuple[str, str, int]]):
            #         for fullpath, fasthash, size in batch:
            #             create_object(fullpath, fasthash, size)
            #             bar()

                # with Pool(processes=parallelism) as pool:
                #     result = pool.starmap(create_object, all_data, chunksize=1000)

            for fullpath, fasthash, size in alive_it(all_data):
                # sha1, _ = create_object(fullpath, fasthash, size)
                sha1, _ = compute_sha1_and_data(fasthash, size)
                hashed[fullpath] = sha1

            # my_env = os.environ.copy()
            # my_env["PATH"] = f"/usr/sbin:/sbin:{my_env['PATH']}"

            process = Popen(["git", "update-index", "--no-ignore-submodules", "--add", "--index-info"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
            (stdout, stderr) = process.communicate(
                "\n".join([f"100644 {sha1}\t{smudge(fullpath.lstrip("/"))}" for fullpath, sha1 in hashed.items()]).encode())

            for err in stderr.decode("utf-8").split("\n"):
                logging.error(err)
            assert process.returncode == 0
            logging.warning(stdout)

                # async with TaskGroup() as tg:
                        # path = pathlib.Path("." + fullpath)
                        # path.parent.mkdir(parents=True, exist_ok=True)
                        # path.write_bytes(pickle.dumps((fasthash, size)))
                        # async def run_batch(batch):
                        #     await asyncio.get_event_loop().run_in_executor(executor, create_objects, batch)
                        #
                        # for batch in split_in_batches:
                        #     tg.create_task(run_batch(batch) )

                        # sha1, compressed_data =

                    # logging.warning(f"Hash {sha1} for {fullpath}")

                    # files.append(path.as_posix())

            # process = Popen(["git", "hash-object", "-w", "--stdin-paths"], stdout=PIPE, stderr=PIPE, stdin=PIPE)
            # (stdout, stderr) = process.communicate("\n".join(files).encode())
            # assert len(stderr) == 0
            # sha1 = stdout.decode().strip()
            # logging.warning(f"Hash {sha1} for {fullpath}")

def create_object(fullpath: str, fasthash: str, size: int):
    sha1, compressed_data = compute_sha1_and_data(fasthash, size)

    obj_path = pathlib.Path(f".git/objects/{sha1[:2]}/{sha1[2:]}")
    os.makedirs(obj_path.parent.as_posix(), exist_ok=True)

    with open(obj_path.as_posix(), 'wb') as f:
        f.write(compressed_data)
    return sha1, compressed_data


    # obj_path.parent.mkdir(parents=True, exist_ok=True)
    # obj_path.write_bytes(compressed_data)


def compute_sha1_and_data(fasthash, size):
    obj_data: bytes = pickle.dumps((fasthash, size))
    packed_data: bytes = f'blob {len(obj_data)}\0'.encode() + obj_data
    sha1 = hashlib.sha1(packed_data).hexdigest()
    compressed_data = zlib.compress(packed_data, level=0)
    return sha1, compressed_data


if __name__ == '__main__':
    unittest.main()
