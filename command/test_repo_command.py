import os
import pathlib
import tempfile
from os.path import join
from typing import Callable
from unittest import IsolatedAsyncioTestCase

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


class TestRepoCommand(IsolatedAsyncioTestCase):
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

    async def test_init_refresh_repo(self):
        res = TotalCommand(path=join(self.tmpdir.name, "repo")).cave.init()

        posix_path = pathlib.Path(self.tmpdir.name).as_posix()
        self.assertEqual(f"Repo initialized at {posix_path}/repo", res)
        self.assertEqual(['current.uuid'], os.listdir(join(self.tmpdir.name, "repo", ".hoard")))

        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        res = await cave_cmd.refresh()
        self.assertEqual([
            'PRESENT_FILE wat/test.me.different',
            'PRESENT_FILE wat/test.me.once',
            'PRESENT_FILE wat/test.me.twice',
            'old: None',
            'current: 72174f950289a454493d243bb72bdb76982e5f62',
            'Refresh done!'], res.splitlines())

        current_uuid = cave_cmd.current_uuid()
        self.assertEqual(
            sorted([
                f"{current_uuid}.contents.lmdb",
                f'{current_uuid}.contents.lmdb-BAK',
                f"{current_uuid}.contents.lmdb-lock",
                f"{current_uuid}.toml",
                'current.uuid',
                'filesystem-index.rtoml']),
            sorted(os.listdir(join(self.tmpdir.name, "repo", ".hoard"))))

    async def test_show_repo(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        res = await cave_cmd.status()
        self.assertEqual(f"Repo is not initialized at {pathlib.Path(self.tmpdir.name).as_posix()}/repo", res)

        cave_cmd.init()
        res = await cave_cmd.status()
        self.assertEqual(f"Repo {cave_cmd.current_uuid()} contents have not been refreshed yet!", res)

        await cave_cmd.refresh()

        res = cave_cmd.status_index(show_dates=False)
        self.assertEqual([
            'wat/test.me.different: present @ -1',
            'wat/test.me.once: present @ -1',
            'wat/test.me.twice: present @ -1',
            '--- SUMMARY ---',
            'Result for local [72174f950289a454493d243bb72bdb76982e5f62]:',
            'Max size: 3.5TB',
            f'UUID: {cave_cmd.current_uuid()}',
            '  # files = 3 of size 19',
            ''], res.split("\n"))

        res = await cave_cmd.status()
        self.assertEqual(
            f"{cave_cmd.current_uuid()} [72174f950289a454493d243bb72bdb76982e5f62]:\n"
            f"files:\n"
            f"    same: 3 (100.0%)\n"
            f"     mod: 0 (0.0%)\n"
            f"     new: 0 (0.0%)\n"
            f"   moved: 0 (0.0%)\n"
            f" current: 3\n"
            f" in repo: 3\n"
            f" deleted: 0 (0.0%)\n", res)

    async def test_local_files_lifecycle(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        res = await cave_cmd.status()
        self.assertEqual(f"Repo is not initialized at {pathlib.Path(self.tmpdir.name).as_posix()}/repo", res)

        cave_cmd.init()
        res = await cave_cmd.status()
        self.assertEqual(f"Repo {cave_cmd.current_uuid()} contents have not been refreshed yet!", res)

        res = await cave_cmd.refresh()
        self.assertEqual([
            'PRESENT_FILE wat/test.me.different',
            'PRESENT_FILE wat/test.me.once',
            'PRESENT_FILE wat/test.me.twice',
            'old: None',
            'current: 72174f950289a454493d243bb72bdb76982e5f62',
            'Refresh done!'], res.splitlines())

        res = cave_cmd.status_index(show_dates=False)
        self.assertEqual([
            'wat/test.me.different: present @ -1',
            'wat/test.me.once: present @ -1',
            'wat/test.me.twice: present @ -1',
            '--- SUMMARY ---',
            'Result for local [72174f950289a454493d243bb72bdb76982e5f62]:',
            'Max size: 3.5TB',
            f'UUID: {cave_cmd.current_uuid()}',
            '  # files = 3 of size 19',
            ''], res.split("\n"))

        pfw = pretty_file_writer(self.tmpdir.name)
        pfw('repo/wat/test.me.once', "lhiuwfelhiufhlu")
        pfw('repo/wat/test.me.anew', "pkosadu")
        pfw('repo/wat/test.me.twice', None)

        res = await cave_cmd.refresh()
        self.assertEqual([
            'DELETED_NO_COPY wat/test.me.twice',
            'PRESENT_FILE wat/test.me.anew',
            'MODIFIED_FILE wat/test.me.once',
            'old: 72174f950289a454493d243bb72bdb76982e5f62',
            'current: 2f4fced348f1d2ef3a9ab45d3beb06a29c634bb4',
            'Refresh done!'], res.splitlines())

        res = cave_cmd.status_index(show_dates=False)
        self.assertEqual([
            'wat/test.me.anew: present @ -1',
            'wat/test.me.different: present @ -1',
            'wat/test.me.once: present @ -1',
            '--- SUMMARY ---',
            'Result for local [2f4fced348f1d2ef3a9ab45d3beb06a29c634bb4]:',
            'Max size: 3.5TB',
            f'UUID: {cave_cmd.current_uuid()}',
            '  # files = 3 of size 27',
            ''], res.split("\n"))

        res = await cave_cmd.status()
        self.assertEqual(
            f"{cave_cmd.current_uuid()} [2f4fced348f1d2ef3a9ab45d3beb06a29c634bb4]:\n"
            f"files:\n"
            f"    same: 3 (100.0%)\n"
            f"     mod: 0 (0.0%)\n"
            f"     new: 0 (0.0%)\n"
            "   moved: 0 (0.0%)\n"
            f" current: 3\n"
            f" in repo: 3\n"
            f" deleted: 0 (0.0%)\n", res)

        pfw('repo/test.me.anew2', "vseer")

        res = await cave_cmd.status()
        self.assertEqual(
            f"{cave_cmd.current_uuid()} [2f4fced348f1d2ef3a9ab45d3beb06a29c634bb4]:\n"
            f"files:\n"
            f"    same: 3 (75.0%)\n"
            f"     mod: 0 (0.0%)\n"
            f"     new: 1 (25.0%)\n"
            f"   moved: 0 (0.0%)\n"
            f" current: 4\n"
            f" in repo: 3\n"
            f" deleted: 0 (0.0%)\n", res)

        res = await cave_cmd.refresh()
        self.assertEqual([
            'PRESENT_FILE test.me.anew2',
            'old: 2f4fced348f1d2ef3a9ab45d3beb06a29c634bb4',
            'current: bb53a6b65807a929d01672df634348493f34bb48',
            'Refresh done!'], res.splitlines())

        res = cave_cmd.status_index(show_dates=False)
        self.assertEqual([
            'test.me.anew2: present @ -1',
            'wat/test.me.anew: present @ -1',
            'wat/test.me.different: present @ -1',
            'wat/test.me.once: present @ -1',
            '--- SUMMARY ---',
            'Result for local [bb53a6b65807a929d01672df634348493f34bb48]:',
            'Max size: 3.5TB',
            f'UUID: {cave_cmd.current_uuid()}',
            '  # files = 4 of size 32',
            ''], res.split("\n"))
