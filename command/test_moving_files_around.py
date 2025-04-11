import os
import tempfile
from os.path import join
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
            '+/test.me.1',
            '+/wat/test.me.2',
            "Sync'ed repo-partial-name to hoard!",
            '=/test.me.1',
            '=/wat/test.me.2',
            '+/test.me.4',
            '+/wat/test.me.3',
            "Sync'ed repo-full-name to hoard!",
            '=/test.me.1',
            '=/wat/test.me.3',
            "Sync'ed repo-backup-name to hoard!",
            'ADD_TO_HOARD_AND_CLEANUP /test.me.4',
            '<+/test.me.5',
            '<+/wat/test.me.6',
            'u/wat/test.me.3',
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
            'ALREADY_MARKED_GET /wat/test.me.3',
            "Sync'ed repo-full-name to hoard!",
            'ALREADY_MARKED_GET /wat/test.me.3',
            "Sync'ed repo-backup-name to hoard!",
            'ADD_TO_HOARD_AND_CLEANUP /test.me.4',  # FIXME bad, I think - should not try to re-add them
            'ADD_TO_HOARD_AND_CLEANUP /test.me.5',
            'ADD_TO_HOARD_AND_CLEANUP /wat/test.me.3',
            'ADD_TO_HOARD_AND_CLEANUP /wat/test.me.6',
            "Sync'ed repo-incoming-name to hoard!",
            'DONE'], res.splitlines())
