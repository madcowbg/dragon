import asyncio
import os
import tempfile
import unittest
from os.path import join
from typing import Tuple, List

from contents import HoardContents
from main import TotalCommand
from hashing import fast_hash, calc_file_md5
from test_repo_command import populate, write_contents


def populate_hoard(tmpdir: str):
    populate(tmpdir)

    os.mkdir(join(tmpdir, "hoard"))


class TestRepoCommand(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        populate_hoard(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_create_hoard(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        cave_cmd.refresh()

        repo_uuid = cave_cmd.current_uuid()

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        res = hoard_cmd.remotes()

        self.assertEqual("0 total remotes.", res.strip())

        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local")
        res = hoard_cmd.remotes()
        self.assertEqual(f"1 total remotes.\n  [repo-in-local] {repo_uuid}", res.strip())

    def test_sync_to_hoard(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        cave_cmd.refresh()

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local")

        repo_uuid = cave_cmd.current_uuid()

        res = hoard_cmd.mount_remote("repo-in-local", "/")
        self.assertEqual("set path of repo-in-local to /", res.strip())

        res = hoard_cmd.status("repo-in-local")
        self.assertEqual(
            f"Status of {repo_uuid}:\nA /wat/test.me.different\nA /wat/test.me.once\nA /wat/test.me.twice\nAF /wat\nDONE",
            res.strip())

        res = hoard_cmd.sync("repo-in-local")
        self.assertEqual("Sync'ed repo-in-local to hoard!", res.strip())

        hoard_contents = HoardContents.load(hoard_cmd._hoard_contents_filename())
        self._assert_hoard_contents(
            hoard_contents,
            files_exp=[
                ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),
                ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                ('/wat/test.me.twice', 6, 1, '1881f6f9784fb08bf6690e9763b76ac3')],
            dirs_exp=["/wat"])

        res = hoard_cmd.status("repo-in-local")
        self.assertEqual(f"Status of {repo_uuid}:\nDONE", res.strip())

    def _assert_hoard_contents(
            self, hoard_contents: HoardContents, files_exp: List[Tuple[str, int, int, str]], dirs_exp: List[str]):
        files = sorted(
            (f, prop.size, len(prop.available_at), prop.fasthash) for f, prop in hoard_contents.fsobjects.files.items())
        dirs = sorted(f for f, _ in hoard_contents.fsobjects.dirs.items())
        self.assertEqual(sorted(files_exp), sorted(files))
        self.assertEqual(sorted(dirs_exp), sorted(dirs))

    def test_sync_two_repos(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        cave_cmd.refresh()

        repo_uuid = cave_cmd.current_uuid()

        cave_cmd2 = TotalCommand(path=join(self.tmpdir.name, "repo-2")).cave
        cave_cmd2.init()
        cave_cmd2.refresh()

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local")
        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo-2"), name="repo-in-local-2")

        hoard_cmd.mount_remote("repo-in-local", "/")
        hoard_cmd.sync("repo-in-local")

        self._assert_hoard_contents(
            HoardContents.load(hoard_cmd._hoard_contents_filename()),
            files_exp=[
                ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),
                ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                ('/wat/test.me.twice', 6, 1, '1881f6f9784fb08bf6690e9763b76ac3')],
            dirs_exp=["/wat"])

        hoard_cmd.mount_remote("repo-in-local-2", "/wat")
        res = hoard_cmd.sync("repo-in-local-2")
        self.assertEqual("Sync'ed repo-in-local-2 to hoard!", res.strip())

        self._assert_hoard_contents(
            HoardContents.load(hoard_cmd._hoard_contents_filename()),
            files_exp=[
                ('/wat/test.me.different', 8, 1, 'd6dcdb1bc4677aab619798004537c4e3'),
                ('/wat/test.me.twice', 6, 2, '1881f6f9784fb08bf6690e9763b76ac3')],
            dirs_exp=["/wat"])

        hoard_cmd.sync("repo-in-local")
        self._assert_hoard_contents(
            HoardContents.load(hoard_cmd._hoard_contents_filename()),
            files_exp=[
                ('/wat/test.me.different', 8, 1, 'd6dcdb1bc4677aab619798004537c4e3'),  # retained only from repo-2
                ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                ('/wat/test.me.twice', 6, 2, '1881f6f9784fb08bf6690e9763b76ac3')],
            dirs_exp=["/wat"])

    def test_changing_data(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        cave_cmd.refresh()

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local")

        repo_uuid = cave_cmd.current_uuid()
        hoard_cmd.mount_remote("repo-in-local", "/")
        hoard_cmd.sync("repo-in-local")

        self.assertEqual(f"Status of {repo_uuid}:\nDONE", hoard_cmd.status("repo-in-local").strip())

        os.mkdir(join(self.tmpdir.name, "repo", "newdir"))
        write_contents(join(self.tmpdir.name, "repo", "newdir", "newfile.is"), "lhiWFELHFE")
        os.remove(join(self.tmpdir.name, "repo", "wat", 'test.me.different'))

        # as is not refreshed, no change in status
        self.assertEqual(f"Status of {repo_uuid}:\nDONE", hoard_cmd.status("repo-in-local").strip())

        cave_cmd.refresh()
        hoard_cmd.fetch("repo-in-local")
        self.assertEqual(
            f"Status of {repo_uuid}:\nA /newdir/newfile.is\nD /wat/test.me.different\nAF /newdir\nDONE",
            hoard_cmd.status("repo-in-local").strip())

        res = hoard_cmd.sync("repo-in-local")
        self.assertEqual("Sync'ed repo-in-local to hoard!", res)

        self.assertEqual(f"Status of {repo_uuid}:\nDONE", hoard_cmd.status("repo-in-local").strip())

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
