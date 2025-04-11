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
        shutil.move(
            join(self.tmpdir.name, "repo-full/wat/test.me.3"),
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

        # move another file that is already in repo-full
        Path(join(self.tmpdir.name, "repo-full/wat2")).mkdir(parents=True, exist_ok=True)
        shutil.move(
            join(self.tmpdir.name, "repo-full/test.me.4"),
            join(self.tmpdir.name, "repo-full/wat2/test.me.4"))

        res = await full_cave_cmd.refresh()
        self.assertEqual([
            'MOVED test.me.4 TO wat2/test.me.4',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'ADD_NEW_TO_HOARD /wat2/test.me.4',
            'CLEANUP_MOVED /test.me.4',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.pending_pull(incoming_cave_cmd.current_uuid())
        self.assertEqual(['Status of repo-incoming-name:', ], res.splitlines())

        res = await incoming_cave_cmd.refresh()
        self.assertEqual(['NO CHANGES', 'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            "Sync'ed repo-incoming-name to hoard!",
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual([
            '/test.me.1 = a:3',
            '/test.me.4 = c:2',
            '/test.me.5 = g:2 c:1',
            '/wat/test.me.2 = a:2 g:1',
            '/wat/test.me.3 = c:2',
            '/wat/test.me.6 = g:2 c:1',
            '/wat2/test.me.3 = a:1',
            '/wat2/test.me.4 = a:1 g:1',
            'DONE'], res.splitlines())

        res = await hoard_cmd.files.push(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            'repo-incoming-name:',
            'repo-incoming-name:',
            'd test.me.4',
            "NEEDS_COPY ['repo-backup-name', 'repo-full-name'] test.me.5",
            'd wat/test.me.3',
            "NEEDS_COPY ['repo-backup-name', 'repo-full-name'] wat/test.me.6",
            'DONE'], res.splitlines())

        res = await hoard_cmd.files.push(backup_cave_cmd.current_uuid())
        self.assertEqual([
            'repo-backup-name:',
            '+ test.me.5',
            '+ wat/test.me.2',
            '+ wat/test.me.6',
            'MOVED /wat/test.me.3 to /wat2/test.me.3',
            '+ wat2/test.me.4',
            'repo-backup-name:',
            'd test.me.4',
            'd wat/test.me.3',
            'remove dangling /test.me.4',
            'remove dangling /wat/test.me.3',
            'DONE'], res.splitlines())

        res = await hoard_cmd.files.push(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            'repo-incoming-name:',
            'repo-incoming-name:',
            "NEEDS_COPY ['repo-full-name'] test.me.5",
            "NEEDS_COPY ['repo-full-name'] wat/test.me.6",
            'DONE'], res.splitlines())

        res = await hoard_cmd.files.push(full_cave_cmd.current_uuid())
        self.assertEqual([
            'repo-full-name:',
            '+ test.me.5',
            '+ wat/test.me.6',
            'repo-full-name:',
            'DONE'], res.splitlines())

        res = await hoard_cmd.files.push(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            'repo-incoming-name:',
            'repo-incoming-name:',
            'd test.me.5',
            'd wat/test.me.6',
            'DONE'], res.splitlines())
