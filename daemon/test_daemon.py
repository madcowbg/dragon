import asyncio
import pathlib
import shutil
import tempfile
from os.path import join
from unittest.async_case import IsolatedAsyncioTestCase

from command.test_repo_command import populate, pretty_file_writer
from daemon.daemon import run_daemon
from dragon import TotalCommand


class TestDaemon(IsolatedAsyncioTestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.pfw = pretty_file_writer(self.tmpdir.name)
        populate(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    async def test_run_daemon(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_path = pathlib.Path(self.tmpdir.name).joinpath("repo").as_posix()
        res = cave_cmd.init()
        self.assertEqual(f"Repo initialized at {cave_path}", res)

        res = await cave_cmd.refresh()
        self.assertEqual([
            'PRESENT_FILE wat/test.me.different',
            'PRESENT_FILE wat/test.me.once',
            'PRESENT_FILE wat/test.me.twice',
            'Refresh done!'], res.splitlines())

        daemon_task = asyncio.create_task(run_daemon(cave_path, False, 0.01, 0.01))
        await asyncio.sleep(0.1)
        daemon_task.cancel()

        res = cave_cmd.status_index(show_dates=False)
        self.assertEqual([
            'wat/test.me.different: present @ 1',
            'wat/test.me.once: present @ 1',
            'wat/test.me.twice: present @ 1',
            '--- SUMMARY ---',
            'Result for local',
            'Max size: 3.6TB',
            f'UUID: {cave_cmd.current_uuid()}',
            '  # files = 3 of size 19'], res.splitlines())

        daemon_task = asyncio.create_task(run_daemon(cave_path, False, 0.01, 0.01))

        # add a new file
        self.pfw('repo/wat/test.add.with.daemon', "somenewdataaaaa")
        # fixme weird and random-prone to fix the life of the daemon - maybe monitor it another way?
        await asyncio.sleep(0.1)

        res = cave_cmd.status_index(show_dates=False)
        self.assertEqual([
            'wat/test.add.with.daemon: added @ 3',
            'wat/test.me.different: present @ 1',
            'wat/test.me.once: present @ 1',
            'wat/test.me.twice: present @ 1',
            '--- SUMMARY ---',
            'Result for local',
            'Max size: 3.6TB',
            f'UUID: {cave_cmd.current_uuid()}',
            '  # files = 4 of size 34'], res.splitlines())

        self.pfw('repo/wat/test.me.once', None)
        await asyncio.sleep(0.1)
        shutil.move(
            join(self.tmpdir.name, "repo/wat/test.me.different"),
            join(self.tmpdir.name, "repo/test.me.different"))
        await asyncio.sleep(0.1)

        res = cave_cmd.status_index(show_dates=False)
        self.assertEqual([
            'test.me.different: added @ 6',
            'wat/test.add.with.daemon: added @ 3',
            'wat/test.me.different: moved_from @ 6',
            'wat/test.me.once: deleted @ 5',
            'wat/test.me.twice: present @ 1',
            '--- SUMMARY ---',
            'Result for local',
            'Max size: 3.6TB',
            f'UUID: {cave_cmd.current_uuid()}',
            '  # files = 3 of size 26'], res.splitlines())

        daemon_task.cancel()
