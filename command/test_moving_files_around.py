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
            'Before: Hoard [a80f91] <- repo [curr: a80f91, stg: f6a740, des: a80f91]',
            'ADD_NEW_TO_HOARD /test.me.1',
            'ADD_NEW_TO_HOARD /wat/test.me.2',
            'After: Hoard [f6a740], repo [curr: f6a740, stg: f6a740, des: f6a740]',
            "Sync'ed repo-partial-name to hoard!",
            'Before: Hoard [f6a740] <- repo [curr: a80f91, stg: d99580, des: f6a740]',
            '=/test.me.1',
            '=/wat/test.me.2',
            'ADD_NEW_TO_HOARD /test.me.4',
            'ADD_NEW_TO_HOARD /wat/test.me.3',
            'After: Hoard [d99580], repo [curr: d99580, stg: d99580, des: d99580]',
            "Sync'ed repo-full-name to hoard!",
            'Before: Hoard [d99580] <- repo [curr: a80f91, stg: 9fbdcf, des: d99580]',
            '=/test.me.1',
            '=/wat/test.me.3',
            'After: Hoard [d99580], repo [curr: 9fbdcf, stg: 9fbdcf, des: d99580]',
            "Sync'ed repo-backup-name to hoard!",
            'Before: Hoard [d99580] <- repo [curr: a80f91, stg: e9ce07, des: a80f91]',
            'CLEANUP_SAME /test.me.4',
            'INCOMING_TO_HOARD /test.me.5',
            'INCOMING_TO_HOARD /wat/test.me.6',
            'CLEANUP_DIFFERENT /wat/test.me.3',
            'After: Hoard [89527b], repo [curr: 843a75, stg: e9ce07, des: a80f91]',
            "Sync'ed repo-incoming-name to hoard!",
            'DONE'], res.splitlines())

        res = await partial_cave_cmd.refresh()
        self.assertEqual([
            'NO CHANGES',
            'old: f6a74030fa0a826b18e424d44f8aca9be8c657f3',
            'current: f6a74030fa0a826b18e424d44f8aca9be8c657f3',
            'Refresh done!'], res.splitlines())

        res = await full_cave_cmd.refresh()
        self.assertEqual([
            'NO CHANGES',
            'old: d995800c80add686a027bac8628ca610418c64b6',
            'current: d995800c80add686a027bac8628ca610418c64b6',
            'Refresh done!'], res.splitlines())

        res = await backup_cave_cmd.refresh()
        self.assertEqual(['NO CHANGES',
                          'old: 9fbdcfe094f258f954ba6f65c4a3641d25b32e06',
                          'current: 9fbdcfe094f258f954ba6f65c4a3641d25b32e06',
                          'Refresh done!'], res.splitlines())

        res = await incoming_cave_cmd.refresh()
        self.assertEqual(['NO CHANGES',
                          'old: e9ce073b9d61e12d35bbb0fa537581065083c886',
                          'current: e9ce073b9d61e12d35bbb0fa537581065083c886',
                          'Refresh done!'], res.splitlines())

        # should do nothing
        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual([
            'Before: Hoard [89527b] <- repo [curr: f6a740, stg: f6a740, des: f6a740]',
            'After: Hoard [89527b], repo [curr: f6a740, stg: f6a740, des: f6a740]',
            "Sync'ed repo-partial-name to hoard!",
            'Before: Hoard [89527b] <- repo [curr: d99580, stg: d99580, des: 89527b]',
            'After: Hoard [89527b], repo [curr: d99580, stg: d99580, des: 89527b]',
            "Sync'ed repo-full-name to hoard!",
            'Before: Hoard [89527b] <- repo [curr: 9fbdcf, stg: 9fbdcf, des: 89527b]',
            'After: Hoard [89527b], repo [curr: 9fbdcf, stg: 9fbdcf, des: 89527b]',
            "Sync'ed repo-backup-name to hoard!",
            'Before: Hoard [89527b] <- repo [curr: 843a75, stg: e9ce07, des: a80f91]',
            'After: Hoard [89527b], repo [curr: 843a75, stg: e9ce07, des: a80f91]',
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
            'old: d995800c80add686a027bac8628ca610418c64b6',
            'current: 74285a5ab9a1566b524560f0a873bb877148424f',
            'Refresh done!'], res.splitlines())

        res = full_cave_cmd.status_index(show_files=True, show_dates=False)
        self.assertEqual((
            'test.me.1: present @ -1\n'
            'test.me.4: present @ -1\n'
            'wat/test.me.2: present @ -1\n'
            'wat2/test.me.3: present @ -1\n'
            '--- SUMMARY ---\n'
            'Result for local [74285a5ab9a1566b524560f0a873bb877148424f]:\n'
            'Max size: 3.5TB\n'
            f'UUID: {full_cave_cmd.current_uuid()}\n'
            '  # files = 4 of size 35\n'), res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Before: Hoard [89527b] <- repo [curr: d99580, stg: 74285a, des: 89527b]',
            'ADD_NEW_TO_HOARD /wat2/test.me.3',  # fixme maybe should be MOVED instead
            'DELETE_FROM_HOARD /wat/test.me.3',
            'After: Hoard [894baf], repo [curr: 74285a, stg: 74285a, des: 894baf]',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())

        # try re-doing it
        res = await full_cave_cmd.refresh()
        self.assertEqual([
            'NO CHANGES',
            'old: 74285a5ab9a1566b524560f0a873bb877148424f',
            'current: 74285a5ab9a1566b524560f0a873bb877148424f',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Before: Hoard [894baf] <- repo [curr: 74285a, stg: 74285a, des: 894baf]',
            'After: Hoard [894baf], repo [curr: 74285a, stg: 74285a, des: 894baf]',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.pending_pull(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            'Status of repo-incoming-name:',
            'Hoard root: 894baf9ffef6c3bdc358c61ad4c157b8903bd409:',
            'Repo root: e9ce073b9d61e12d35bbb0fa537581065083c886:'], res.splitlines())

        res = await incoming_cave_cmd.refresh()
        self.assertEqual([
            'NO CHANGES',
            'old: e9ce073b9d61e12d35bbb0fa537581065083c886',
            'current: e9ce073b9d61e12d35bbb0fa537581065083c886',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            'Before: Hoard [894baf] <- repo [curr: b36f83, stg: e9ce07, des: a80f91]',
            'After: Hoard [894baf], repo [curr: b36f83, stg: e9ce07, des: a80f91]',
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
            'old: 74285a5ab9a1566b524560f0a873bb877148424f',
            'current: 9f4d64f79603af68e200303ed6c46498fe69fc5c',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Before: Hoard [894baf] <- repo [curr: 74285a, stg: 9f4d64, des: 894baf]',
            'ADD_NEW_TO_HOARD /wat2/test.me.4',
            'DELETE_FROM_HOARD /test.me.4',  # fixme should be CLEANUP_MOVED maybe
            'After: Hoard [0f6ca5], repo [curr: 9f4d64, stg: 9f4d64, des: 0f6ca5]',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.pending_pull(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            'Status of repo-incoming-name:',
            'Hoard root: 0f6ca590b141ab71406b9f2a09b9e1a78223cc8d:',
            'Repo root: e9ce073b9d61e12d35bbb0fa537581065083c886:'], res.splitlines())

        res = await incoming_cave_cmd.refresh()
        self.assertEqual([
            'NO CHANGES',
            'old: e9ce073b9d61e12d35bbb0fa537581065083c886',
            'current: e9ce073b9d61e12d35bbb0fa537581065083c886',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            'Before: Hoard [0f6ca5] <- repo [curr: 4745b9, stg: e9ce07, des: a80f91]',
            'After: Hoard [0f6ca5], repo [curr: 4745b9, stg: e9ce07, des: a80f91]',
            "Sync'ed repo-incoming-name to hoard!",
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual([
            'Root: 0f6ca590b141ab71406b9f2a09b9e1a78223cc8d',
            '/test.me.1 = a:3',
            '/test.me.4 = c:1',  # should be c:2 if moved
            '/test.me.5 = g:2 c:1',
            '/wat/test.me.2 = a:2 g:1',
            '/wat/test.me.3 = c:2',
            '/wat/test.me.6 = g:2 c:1',
            '/wat2/test.me.3 = a:1 g:1',  # should be no g:1 if moved
            '/wat2/test.me.4 = a:1 g:1',
            'DONE'], res.splitlines())

        res = await hoard_cmd.files.push(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            'repo-incoming-name:',
            'repo-incoming-name:',
            'd test.me.4',
            "NEEDS_MORE_COPIES (0) ['repo-backup-name', 'repo-full-name'] test.me.5",
            'd wat/test.me.3',
            "NEEDS_MORE_COPIES (0) ['repo-backup-name', 'repo-full-name'] wat/test.me.6",
            'remove dangling /test.me.4',
            'DONE'], res.splitlines())

        res = await hoard_cmd.files.push(backup_cave_cmd.current_uuid())
        self.assertEqual([
            'repo-backup-name:',
            '+ test.me.5',
            '+ wat/test.me.2',
            '+ wat/test.me.6',
            '+ wat2/test.me.3',  # fixme should be MOVED
            '+ wat2/test.me.4',
            'repo-backup-name:',
            # 'd test.me.4',
            'd wat/test.me.3',
            # 'remove dangling /test.me.4',
            'remove dangling /wat/test.me.3',
            'DONE'], res.splitlines())

        res = await hoard_cmd.files.push(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            'repo-incoming-name:',
            'repo-incoming-name:',
            "NEEDS_MORE_COPIES (1) ['repo-full-name'] test.me.5",
            "NEEDS_MORE_COPIES (1) ['repo-full-name'] wat/test.me.6",
            'DONE'], res.splitlines())

        # change setting to allow cleanup earlier
        config = hoard_cmd.hoard.config()
        config.remotes[incoming_cave_cmd.current_uuid()].min_copies_before_cleanup = 1
        config.write()

        res = await hoard_cmd.files.push(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            'repo-incoming-name:',
            'repo-incoming-name:',
            'd test.me.5',
            'd wat/test.me.6',
            'DONE'], res.splitlines())

        res = await hoard_cmd.files.push(full_cave_cmd.current_uuid())
        self.assertEqual([
            'repo-full-name:',
            '+ test.me.5',
            '+ wat/test.me.6',
            'repo-full-name:',
            'DONE'], res.splitlines())

    async def test_moving_files_do_not_add_themselves_back(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir.name)

        await hoard_cmd.contents.pull(all=True)

        # move a file that is already in repo-full
        Path(join(self.tmpdir.name, "repo-full/wat2")).mkdir(parents=True, exist_ok=True)
        shutil.move(
            join(self.tmpdir.name, "repo-full/wat/test.me.3"),
            join(self.tmpdir.name, "repo-full/wat2/test.me.3"))

        res = await full_cave_cmd.refresh()
        self.assertEqual([
            'MOVED wat/test.me.3 TO wat2/test.me.3',
            'old: d995800c80add686a027bac8628ca610418c64b6',
            'current: 74285a5ab9a1566b524560f0a873bb877148424f',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Before: Hoard [89527b] <- repo [curr: d99580, stg: 74285a, des: 89527b]',
            'ADD_NEW_TO_HOARD /wat2/test.me.3',  # fixme maybe should be MOVED
            'DELETE_FROM_HOARD /wat/test.me.3',
            'After: Hoard [894baf], repo [curr: 74285a, stg: 74285a, des: 894baf]',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.get(full_cave_cmd.current_uuid(), path="")
        self.assertEqual([
            'Considered 6 files.',
            'DONE'], res.splitlines())

        res = await hoard_cmd.backups.assign(available_only=False)
        self.assertEqual([
            'set: / with 1/1 media',
            'DONE'], res.splitlines())

    async def test_moving_files_twice_fallbacks_to_get(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir.name)

        await hoard_cmd.contents.pull(all=True)

        # move a file that is already in repo-full
        Path(join(self.tmpdir.name, "repo-full/wat2")).mkdir(parents=True, exist_ok=True)
        shutil.move(
            join(self.tmpdir.name, "repo-full/wat/test.me.3"),
            join(self.tmpdir.name, "repo-full/wat2/test.me.3"))

        res = await full_cave_cmd.refresh()
        self.assertEqual([
            'MOVED wat/test.me.3 TO wat2/test.me.3',
            'old: d995800c80add686a027bac8628ca610418c64b6',
            'current: 74285a5ab9a1566b524560f0a873bb877148424f',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Before: Hoard [89527b] <- repo [curr: d99580, stg: 74285a, des: 89527b]',
            'ADD_NEW_TO_HOARD /wat2/test.me.3',  # fixme maybe MOVE instead of add/delete
            'DELETE_FROM_HOARD /wat/test.me.3',
            'After: Hoard [894baf], repo [curr: 74285a, stg: 74285a, des: 894baf]',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())

        Path(join(self.tmpdir.name, "repo-full/wat3")).mkdir(parents=True, exist_ok=True)
        shutil.move(
            join(self.tmpdir.name, "repo-full/wat2/test.me.3"),
            join(self.tmpdir.name, "repo-full/wat3/test.me.3"))

        res = await full_cave_cmd.refresh()
        self.assertEqual([
            'MOVED wat2/test.me.3 TO wat3/test.me.3',
            'old: 74285a5ab9a1566b524560f0a873bb877148424f',
            'current: dfd71dfc5fa36785b360536595c42d598149a795',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Before: Hoard [894baf] <- repo [curr: 74285a, stg: dfd71d, des: 894baf]',
            'ADD_NEW_TO_HOARD /wat3/test.me.3',
            'DELETE_FROM_HOARD /wat2/test.me.3',  # fixme should be cleanup for moved maybe
            'After: Hoard [b2cf90], repo [curr: dfd71d, stg: dfd71d, des: b2cf90]',
            'remove dangling /wat2/test.me.3',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())

        res = await hoard_cmd.files.push(full_cave_cmd.current_uuid())
        self.assertEqual([
            'repo-full-name:',
            '+ test.me.5',
            '+ wat/test.me.6',
            'repo-full-name:',
            'DONE'], res.splitlines())

        res = await hoard_cmd.files.push(backup_cave_cmd.current_uuid())
        self.assertEqual([
            'repo-backup-name:',
            '+ test.me.4',
            '+ test.me.5',
            '+ wat/test.me.2',
            '+ wat/test.me.6',
            '+ wat3/test.me.3',
            'repo-backup-name:',
            'd wat/test.me.3',
            # 'd wat2/test.me.3',  # fixme should be cleaned up, actually
            # 'remove dangling /wat2/test.me.3',
            'DONE'], res.splitlines())
