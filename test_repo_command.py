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

    def test_init_repo(self):
        res = TotalCommand(path=join(self.tmpdir.name, "repo")).cave.init()

        posix_path = pathlib.Path(self.tmpdir.name).as_posix()
        self.assertEqual(f"Repo initialized at {posix_path}/repo", res)
        self.assertEqual(['current.uuid'], os.listdir(join(self.tmpdir.name, "repo", ".hoard")))

        res = TotalCommand(path=join(self.tmpdir.name, "repo")).cave.refresh()
        self.assertEqual(f"Refresh done!", res)
        with open(join(self.tmpdir.name, "repo", ".hoard", "current.uuid")) as f:
            current_uuid = f.readline()  # read uuid
        self.assertEqual(
            [f"{current_uuid}.contents", 'current.uuid'],
            os.listdir(join(self.tmpdir.name, "repo", ".hoard")))

