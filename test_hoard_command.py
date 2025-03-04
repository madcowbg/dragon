import os
import tempfile
import unittest
from os.path import join

from main import TotalCommand
from test_repo_command import populate


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

        res = hoard_cmd.status("repo-in-local")
        self.assertEqual(
            f"Status of {repo_uuid}:\nA wat/test.me.different\nA wat/test.me.once\nA wat/test.me.twice\nAD wat",
            res.strip())

