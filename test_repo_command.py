import os
import pathlib
import tempfile
import unittest
from os.path import join

import fire

from main import TotalCommand


def write_contents(path: str, contents: str) -> None:
    with open(path, 'w') as f:
        f.write(contents)


def populate(tmpdir: str):
    os.mkdir(join(tmpdir, 'repo'))
    os.mkdir(join(tmpdir, 'repo', 'wat'))
    write_contents(join(tmpdir, 'repo', 'wat', 'test.me.twice'), "gsadfs")
    write_contents(join(tmpdir, 'repo', 'wat', 'test.me.once'), "gsadfasd")
    write_contents(join(tmpdir, 'repo', 'wat', 'test.me.different'), "gsadf")

    os.mkdir(join(tmpdir, 'repo-2'))
    write_contents(join(tmpdir, 'repo-2', 'test.me.twice'), "gsadfs")
    write_contents(join(tmpdir, 'repo-2', 'test.me.different'), "gsadf3dq")


class TestRepoCommand(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        populate(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_populate_temp_dir(self):
        self.assertEqual(['repo', 'repo-2'], os.listdir(self.tmpdir.name))
        self.assertEqual(
            ['test.me.different', 'test.me.once', 'test.me.twice'],
            os.listdir(join(self.tmpdir.name, 'repo', 'wat')))

    def test_init_refresh_repo(self):
        res = TotalCommand(path=join(self.tmpdir.name, "repo")).cave.init()

        posix_path = pathlib.Path(self.tmpdir.name).as_posix()
        self.assertEqual(f"Repo initialized at {posix_path}/repo", res)
        self.assertEqual(['current.uuid'], os.listdir(join(self.tmpdir.name, "repo", ".hoard")))

        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        res = cave_cmd.refresh()
        self.assertEqual(f"Refresh done!", res)

        current_uuid = cave_cmd.current_uuid()
        self.assertEqual(
            [f"{current_uuid}.contents", 'current.uuid'],
            os.listdir(join(self.tmpdir.name, "repo", ".hoard")))

    def test_show_repo(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        cave_cmd.refresh()

        current_uuid = cave_cmd.current_uuid()

        res = TotalCommand(path=join(self.tmpdir.name, "repo")).cave.show().split("\n")
        self.assertEqual('Result for local', res[0])
        self.assertEqual(['  # files = 3 of size 19', '  # dirs  = 1', ''], res[3:])
