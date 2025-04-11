import os
import shutil
import tempfile
from os.path import join
from pathlib import Path
from unittest.async_case import IsolatedAsyncioTestCase

from command.test_hoard_command import populate_repotypes, init_complex_hoard


class TestIncomingRepos(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        os.mkdir(join(self.tmpdir.name, "hoard"))
        populate_repotypes(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    async def test_adding_full_then_adding_partial(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir.name)

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual([
            'ADD_NEW_TO_HOARD /test.me.1',
            'ADD_NEW_TO_HOARD /wat/test.me.2',
            "Sync'ed repo-partial-name to hoard!",
            '=/test.me.1',
            '=/wat/test.me.2',
            'ADD_NEW_TO_HOARD /test.me.4',
            'ADD_NEW_TO_HOARD /wat/test.me.3',
            "Sync'ed repo-full-name to hoard!",
            '=/test.me.1',
            '=/wat/test.me.3',
            "Sync'ed repo-backup-name to hoard!",
            'CLEANUP_SAME /test.me.4',
            'INCOMING_TO_HOARD /test.me.5',
            'INCOMING_TO_HOARD /wat/test.me.6',
            'CLEANUP_DIFFERENT /wat/test.me.3',
            "Sync'ed repo-incoming-name to hoard!",
            'DONE'], res.splitlines())

        res = await partial_cave_cmd.refresh()
        self.assertEqual(['NO CHANGES', 'Refresh done!'], res.splitlines())

        res = await full_cave_cmd.refresh()
        self.assertEqual(['NO CHANGES', 'Refresh done!'], res.splitlines())

        res = await backup_cave_cmd.refresh()
        self.assertEqual(['NO CHANGES', 'Refresh done!'], res.splitlines())

        res = await incoming_cave_cmd.refresh()
        self.assertEqual(['NO CHANGES', 'Refresh done!'], res.splitlines())

        # should do nothing
        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual([
            "Sync'ed repo-partial-name to hoard!",
            "Sync'ed repo-full-name to hoard!",
            "Sync'ed repo-backup-name to hoard!",
            "Sync'ed repo-incoming-name to hoard!",
            'DONE'], res.splitlines())

        # move a file that is already in repo-full
        Path(join(self.tmpdir.name, "repo-full/wat2")).mkdir(parents=True, exist_ok=True)
        shutil.move(join(self.tmpdir.name, "repo-full/wat/test.me.3"),
                    join(self.tmpdir.name, "repo-full/wat2/test.me.3"))

        res = await full_cave_cmd.refresh()
        self.assertEqual([
            'MOVED wat/test.me.3 TO wat2/test.me.3',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'ADD_NEW_TO_HOARD /wat2/test.me.3',
            'MOVE repo-backup-name: /wat/test.me.3 to /wat2/test.me.3',
            'CLEANUP_MOVED /wat/test.me.3',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())

        # try re-doing it
        res = await full_cave_cmd.refresh()
        self.assertEqual(['NO CHANGES', 'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(["Sync'ed repo-full-name to hoard!", 'DONE'], res.splitlines())

        res = await hoard_cmd.contents.pending_pull(incoming_cave_cmd.current_uuid())
        self.assertEqual(['Status of repo-incoming-name:', ], res.splitlines())

        res = await incoming_cave_cmd.refresh()
        self.assertEqual(['NO CHANGES', 'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            "Sync'ed repo-incoming-name to hoard!",
            'DONE'], res.splitlines())
