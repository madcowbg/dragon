import os
import tempfile
import unittest
from os.path import join

from contents import HoardContents
from main import TotalCommand
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
        files = sorted((f, prop.size, len(prop.available_at)) for f, prop in hoard_contents.fsobjects.files.items())
        dirs = sorted(f for f, _ in hoard_contents.fsobjects.dirs.items())
        self.assertEqual(
            [('/wat/test.me.different', 5, 1),
             ('/wat/test.me.once', 8, 1),
             ('/wat/test.me.twice', 6, 1)], files)
        self.assertEqual(["/wat"], dirs)

        res = hoard_cmd.status("repo-in-local")
        self.assertEqual(f"Status of {repo_uuid}:\nDONE", res.strip())

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
