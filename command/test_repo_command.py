import os
import pathlib
import tempfile
import unittest
from os.path import join
from typing import Callable

from dragon import TotalCommand


def write_contents(path: str, contents: str) -> None:
    with open(path, 'w') as f:
        f.write(contents)


def pretty_file_writer(tmpdir: str) -> Callable[[str, str | None], None]:
    def pfw(path: str, contents: str | None):
        if contents is None:
            os.unlink(join(tmpdir, path))
        else:
            folder, file = os.path.split(join(tmpdir, path))
            os.makedirs(folder, exist_ok=True)
            write_contents(join(tmpdir, path), contents)

    return pfw


def populate(tmpdir: str):
    pfw = pretty_file_writer(tmpdir)
    pfw('repo/wat/test.me.twice', "gsadfs")
    pfw('repo/wat/test.me.once', "gsadfasd")
    pfw('repo/wat/test.me.different', "gsadf")

    pfw('repo-2/test.me.twice', "gsadfs")
    pfw('repo-2/test.me.different', "gsadf3dq")


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
        res = cave_cmd.refresh(show_details=False)
        self.assertEqual(f"Refresh done!", res)

        current_uuid = cave_cmd.current_uuid()
        self.assertEqual(
            sorted([f"{current_uuid}.contents", f"{current_uuid}.toml", 'current.uuid']),
            sorted(os.listdir(join(self.tmpdir.name, "repo", ".hoard"))))

    def test_show_repo(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        res = cave_cmd.status()
        self.assertEqual(f"Repo is not initialized at {pathlib.Path(self.tmpdir.name).as_posix()}/repo", res)

        cave_cmd.init()
        res = cave_cmd.status()
        self.assertEqual(f"Repo {cave_cmd.current_uuid()} contents have not been refreshed yet!", res)

        cave_cmd.refresh(show_details=False)

        res = cave_cmd.status_index(show_dates=False)
        self.assertEqual([
            'wat: added @ 1',
            'wat/test.me.different: present @ 1',
            'wat/test.me.once: present @ 1',
            'wat/test.me.twice: present @ 1',
            '--- SUMMARY ---',
            'Result for local',
            'Max size: 3.6TB',
            f'UUID: {cave_cmd.current_uuid()}',
            '  # files = 3 of size 19',
            '  # dirs  = 1',
            ''], res.split("\n"))

        res = cave_cmd.status()
        self.assertEqual(
            f"{cave_cmd.current_uuid()}:\n"
            f"files:\n"
            f"    same: 3 (100.0%)\n"
            f"     mod: 0 (0.0%)\n"
            f"     new: 0 (0.0%)\n"
            f"   moved: 0 (0.0%)\n"
            f" current: 3\n"
            f" in repo: 3\n"
            f" deleted: 0 (0.0%)\n"
            f"dirs:\n"
            f"    same: 1\n"
            f"     new: 0 (0.0%)\n"
            f" current: 1\n"
            f" in repo: 1\n"
            f" deleted: 0 (0.0%)\n", res)

    def test_local_files_lifecycle(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        res = cave_cmd.status()
        self.assertEqual(f"Repo is not initialized at {pathlib.Path(self.tmpdir.name).as_posix()}/repo", res)

        cave_cmd.init()
        res = cave_cmd.status()
        self.assertEqual(f"Repo {cave_cmd.current_uuid()} contents have not been refreshed yet!", res)

        res = cave_cmd.refresh(show_details=False)
        self.assertEqual(f"Refresh done!", res)

        res = cave_cmd.status_index(show_dates=False)
        self.assertEqual([
            'wat: added @ 1',
            'wat/test.me.different: present @ 1',
            'wat/test.me.once: present @ 1',
            'wat/test.me.twice: present @ 1',
            '--- SUMMARY ---',
            'Result for local',
            'Max size: 3.6TB',
            f'UUID: {cave_cmd.current_uuid()}',
            '  # files = 3 of size 19',
            '  # dirs  = 1',
            ''], res.split("\n"))

        pfw = pretty_file_writer(self.tmpdir.name)
        pfw('repo/wat/test.me.once', "lhiuwfelhiufhlu")
        pfw('repo/wat/test.me.anew', "pkosadu")
        pfw('repo/wat/test.me.twice', None)

        res = cave_cmd.refresh(show_details=False)
        self.assertEqual(f"Refresh done!", res)

        res = cave_cmd.status_index(show_dates=False)
        self.assertEqual([
            'wat: added @ 1',
            'wat/test.me.anew: added @ 2',
            'wat/test.me.different: present @ 1',
            'wat/test.me.once: modified @ 2',
            'wat/test.me.twice: deleted @ 2',
            '--- SUMMARY ---',
            'Result for local',
            'Max size: 3.6TB',
            f'UUID: {cave_cmd.current_uuid()}',
            '  # files = 3 of size 27',
            '  # dirs  = 1',
            ''], res.split("\n"))

        res = cave_cmd.status()
        self.assertEqual(
            f"{cave_cmd.current_uuid()}:\n"
            f"files:\n"
            f"    same: 3 (100.0%)\n"
            f"     mod: 0 (0.0%)\n"
            f"     new: 0 (0.0%)\n"
            "   moved: 0 (0.0%)\n"
            f" current: 3\n"
            f" in repo: 3\n"
            f" deleted: 0 (0.0%)\n"
            f"dirs:\n"
            f"    same: 1\n"
            f"     new: 0 (0.0%)\n"
            f" current: 1\n"
            f" in repo: 1\n"
            f" deleted: 0 (0.0%)\n", res)

        pfw('repo/test.me.anew2', "vseer")

        res = cave_cmd.status()
        self.assertEqual(
            f"{cave_cmd.current_uuid()}:\n"
            f"files:\n"
            f"    same: 3 (75.0%)\n"
            f"     mod: 0 (0.0%)\n"
            f"     new: 1 (25.0%)\n"
            f"   moved: 0 (0.0%)\n"
            f" current: 4\n"
            f" in repo: 3\n"
            f" deleted: 0 (0.0%)\n"
            f"dirs:\n"
            f"    same: 1\n"
            f"     new: 0 (0.0%)\n"
            f" current: 1\n"
            f" in repo: 1\n"
            f" deleted: 0 (0.0%)\n", res)

        res = cave_cmd.refresh(show_details=False)
        self.assertEqual(f"Refresh done!", res)

        res = cave_cmd.status_index(show_dates=False)
        self.assertEqual([
            'test.me.anew2: added @ 3',
            'wat: added @ 1',
            'wat/test.me.anew: added @ 2',
            'wat/test.me.different: present @ 1',
            'wat/test.me.once: modified @ 2',
            'wat/test.me.twice: deleted @ 2',
            '--- SUMMARY ---',
            'Result for local',
            'Max size: 3.6TB',
            f'UUID: {cave_cmd.current_uuid()}',
            '  # files = 4 of size 32',
            '  # dirs  = 1',
            ''], res.split("\n"))
