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
            'Pulling repo-partial-name...',
            'Before: Hoard [a80f91] <- repo [curr: a80f91, stg: f9bfc2, des: a80f91]',
            'REPO_MARK_FILE_AVAILABLE /test.me.1',
            'HOARD_FILE_ADDED /test.me.1',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2',
            'HOARD_FILE_ADDED /wat/test.me.2',
            'updated repo-partial-name from a80f91 to f9bfc2',
            'updated repo-full-name from a80f91 to f9bfc2',
            'updated repo-backup-name from a80f91 to f9bfc2',
            'After: Hoard [f9bfc2], repo [curr: f9bfc2, stg: f9bfc2, des: f9bfc2]',
            "Sync'ed repo-partial-name to hoard!",
            'Pulling repo-full-name...',
            'Before: Hoard [f9bfc2] <- repo [curr: a80f91, stg: 1ad9e0, des: f9bfc2]',
            'REPO_MARK_FILE_AVAILABLE /test.me.1',
            'REPO_MARK_FILE_AVAILABLE /test.me.4',
            'HOARD_FILE_ADDED /test.me.4',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3',
            'HOARD_FILE_ADDED /wat/test.me.3',
            'updated repo-full-name from f9bfc2 to 1ad9e0',
            'updated repo-backup-name from f9bfc2 to 1ad9e0',
            'After: Hoard [1ad9e0], repo [curr: 1ad9e0, stg: 1ad9e0, des: 1ad9e0]',
            "Sync'ed repo-full-name to hoard!",
            'Pulling repo-backup-name...',
            'Before: Hoard [1ad9e0] <- repo [curr: a80f91, stg: 3a0889, des: 1ad9e0]',
            'REPO_MARK_FILE_AVAILABLE /test.me.1',
            'REPO_DESIRED_FILE_TO_GET /test.me.4',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.2',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3',
            'After: Hoard [1ad9e0], repo [curr: 3a0889, stg: 3a0889, des: 1ad9e0]',
            "Sync'ed repo-backup-name to hoard!",
            'Pulling repo-incoming-name...',
            'Before: Hoard [1ad9e0] <- repo [curr: a80f91, stg: 3d1726, des: a80f91]',
            'REPO_MARK_FILE_AVAILABLE /test.me.4',
            'REPO_MARK_FILE_AVAILABLE /test.me.5',
            'HOARD_FILE_ADDED /test.me.5',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.6',
            'HOARD_FILE_ADDED /wat/test.me.6',
            'updated repo-full-name from 1ad9e0 to 8da760',
            'updated repo-backup-name from 1ad9e0 to 8da760',
            'After: Hoard [8da760], repo [curr: 3d1726, stg: 3d1726, des: a80f91]',
            "Sync'ed repo-incoming-name to hoard!",
            'DONE'], res.splitlines())

        res = await partial_cave_cmd.refresh()
        self.assertEqual([
            'NO CHANGES',
            'old: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe',
            'current: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe',
            'Refresh done!'], res.splitlines())

        res = await full_cave_cmd.refresh()
        self.assertEqual([
            'NO CHANGES',
            'old: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad',
            'current: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad',
            'Refresh done!'], res.splitlines())

        res = await backup_cave_cmd.refresh()
        self.assertEqual([
            'NO CHANGES',
            'old: 3a0889e00c0c4ace24843be76d59b3baefb16d77',
            'current: 3a0889e00c0c4ace24843be76d59b3baefb16d77',
            'Refresh done!'], res.splitlines())

        res = await incoming_cave_cmd.refresh()
        self.assertEqual([
            'NO CHANGES',
            'old: 3d1726bd296f20d36cb9df60a0da4d4feae29248',
            'current: 3d1726bd296f20d36cb9df60a0da4d4feae29248',
            'Refresh done!'], res.splitlines())

        # should do nothing
        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual([
            'Pulling repo-partial-name...',
            'Before: Hoard [8da760] <- repo [curr: f9bfc2, stg: f9bfc2, des: f9bfc2]',
            'After: Hoard [8da760], repo [curr: f9bfc2, stg: f9bfc2, des: f9bfc2]',
            "Sync'ed repo-partial-name to hoard!",
            'Pulling repo-full-name...',
            'Before: Hoard [8da760] <- repo [curr: 1ad9e0, stg: 1ad9e0, des: 8da760]',
            'REPO_DESIRED_FILE_TO_GET /test.me.5',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.6',
            'After: Hoard [8da760], repo [curr: 1ad9e0, stg: 1ad9e0, des: 8da760]',
            "Sync'ed repo-full-name to hoard!",
            'Pulling repo-backup-name...',
            'Before: Hoard [8da760] <- repo [curr: 3a0889, stg: 3a0889, des: 8da760]',
            'REPO_DESIRED_FILE_TO_GET /test.me.4',
            'REPO_DESIRED_FILE_TO_GET /test.me.5',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.2',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.6',
            'After: Hoard [8da760], repo [curr: 3a0889, stg: 3a0889, des: 8da760]',
            "Sync'ed repo-backup-name to hoard!",
            'Pulling repo-incoming-name...',
            'Before: Hoard [8da760] <- repo [curr: 3d1726, stg: 3d1726, des: a80f91]',
            'REPO_FILE_TO_DELETE /test.me.4',
            'REPO_FILE_TO_DELETE /test.me.5',
            'REPO_FILE_TO_DELETE /wat/test.me.3',
            'REPO_FILE_TO_DELETE /wat/test.me.6',
            'After: Hoard [8da760], repo [curr: 3d1726, stg: 3d1726, des: a80f91]',
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
            'old: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad',
            'current: ba46648582193b0f2ab9a4894708a6106f7479ab',
            'Refresh done!'], res.splitlines())

        res = full_cave_cmd.status_index(show_files=True, show_dates=False)
        self.assertEqual((
            'test.me.1: present @ -1\n'
            'test.me.4: present @ -1\n'
            'wat/test.me.2: present @ -1\n'
            'wat2/test.me.3: present @ -1\n'
            '--- SUMMARY ---\n'
            'Result for local [ba46648582193b0f2ab9a4894708a6106f7479ab]:\n'
            'Max size: 3.5TB\n'
            f'UUID: {full_cave_cmd.current_uuid()}\n'
            '  # files = 4 of size 35\n'), res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Pulling repo-full-name...',
            'Before: Hoard [8da760] <- repo [curr: 1ad9e0, stg: ba4664, des: 8da760]',
            'REPO_DESIRED_FILE_TO_GET /test.me.5',
            'REPO_FILE_TO_DELETE /wat/test.me.3',
            'HOARD_FILE_DELETED /wat/test.me.3',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.6',
            'REPO_MARK_FILE_AVAILABLE /wat2/test.me.3',
            'HOARD_FILE_ADDED /wat2/test.me.3',
            'updated repo-full-name from 8da760 to d16f79',
            'updated repo-backup-name from 8da760 to d16f79',
            'After: Hoard [d16f79], repo [curr: ba4664, stg: ba4664, des: d16f79]',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())

        # try re-doing it
        res = await full_cave_cmd.refresh()
        self.assertEqual([
            'NO CHANGES',
            'old: ba46648582193b0f2ab9a4894708a6106f7479ab',
            'current: ba46648582193b0f2ab9a4894708a6106f7479ab',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Pulling repo-full-name...',
            'Before: Hoard [d16f79] <- repo [curr: ba4664, stg: ba4664, des: d16f79]',
            'REPO_DESIRED_FILE_TO_GET /test.me.5',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.6',
            'After: Hoard [d16f79], repo [curr: ba4664, stg: ba4664, des: d16f79]',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.pending_pull(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            'Status of repo-incoming-name:',
            'Hoard root: d16f79c5de630091a8b88890d3659da01e0be07f:',
            'Repo current=3d1726 staging=3d1726 desired=a80f91',
            'Repo root: 3d1726bd296f20d36cb9df60a0da4d4feae29248:',
            'REPO_FILE_TO_DELETE /test.me.4',
            'REPO_FILE_TO_DELETE /test.me.5',
            'REPO_FILE_TO_DELETE /wat/test.me.3',
            'REPO_FILE_TO_DELETE /wat/test.me.6'], res.splitlines())

        res = await incoming_cave_cmd.refresh()
        self.assertEqual([
            'NO CHANGES',
            'old: 3d1726bd296f20d36cb9df60a0da4d4feae29248',
            'current: 3d1726bd296f20d36cb9df60a0da4d4feae29248',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            'Pulling repo-incoming-name...',
            'Before: Hoard [d16f79] <- repo [curr: 3d1726, stg: 3d1726, des: a80f91]',
            'REPO_FILE_TO_DELETE /test.me.4',
            'REPO_FILE_TO_DELETE /test.me.5',
            'REPO_FILE_TO_DELETE /wat/test.me.3',
            'REPO_FILE_TO_DELETE /wat/test.me.6',
            'After: Hoard [d16f79], repo [curr: 3d1726, stg: 3d1726, des: a80f91]',
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
            'old: ba46648582193b0f2ab9a4894708a6106f7479ab',
            'current: 1ae17cebfa11804367069a737056b9d2c3b0fc06',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Pulling repo-full-name...',
            'Before: Hoard [d16f79] <- repo [curr: ba4664, stg: 1ae17c, des: d16f79]',
            'REPO_FILE_TO_DELETE /test.me.4',  # fixme should be CLEANUP_MOVED maybe
            'HOARD_FILE_DELETED /test.me.4',
            'REPO_DESIRED_FILE_TO_GET /test.me.5',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.6',
            'REPO_MARK_FILE_AVAILABLE /wat2/test.me.4',
            'HOARD_FILE_ADDED /wat2/test.me.4',
            'updated repo-full-name from d16f79 to ea749c',
            'updated repo-backup-name from d16f79 to ea749c',
            'After: Hoard [ea749c], repo [curr: 1ae17c, stg: 1ae17c, des: ea749c]',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.pending_pull(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            'Status of repo-incoming-name:',
            'Hoard root: ea749cf79b306a27fab649f8c8234e3a968c1529:',
            'Repo current=3d1726 staging=3d1726 desired=a80f91',
            'Repo root: 3d1726bd296f20d36cb9df60a0da4d4feae29248:',
            'REPO_FILE_TO_DELETE /test.me.4',
            'REPO_FILE_TO_DELETE /test.me.5',
            'REPO_FILE_TO_DELETE /wat/test.me.3',
            'REPO_FILE_TO_DELETE /wat/test.me.6'], res.splitlines())

        res = await incoming_cave_cmd.refresh()
        self.assertEqual([
            'NO CHANGES',
            'old: 3d1726bd296f20d36cb9df60a0da4d4feae29248',
            'current: 3d1726bd296f20d36cb9df60a0da4d4feae29248',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(incoming_cave_cmd.current_uuid())
        self.assertEqual([
            'Pulling repo-incoming-name...',
            'Before: Hoard [ea749c] <- repo [curr: 3d1726, stg: 3d1726, des: a80f91]',
            'REPO_FILE_TO_DELETE /test.me.4',
            'REPO_FILE_TO_DELETE /test.me.5',
            'REPO_FILE_TO_DELETE /wat/test.me.3',
            'REPO_FILE_TO_DELETE /wat/test.me.6',
            'After: Hoard [ea749c], repo [curr: 3d1726, stg: 3d1726, des: a80f91]',
            "Sync'ed repo-incoming-name to hoard!",
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual([
            'Root: ea749cf79b306a27fab649f8c8234e3a968c1529',
            'Remote repo-backup-name current=3a0889 staging=3a0889 desired=ea749c',
            'Remote repo-full-name current=1ae17c staging=1ae17c desired=ea749c',
            'Remote repo-incoming-name current=3d1726 staging=3d1726 desired=a80f91',
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2',
            '/test.me.1 = a:3',
            '/test.me.4 = c:1',
            '/test.me.5 = g:2 c:1',
            '/wat/test.me.2 = a:2 g:1',
            '/wat/test.me.3 = c:2',
            '/wat/test.me.6 = g:2 c:1',
            '/wat2/test.me.3 = a:1 g:1',
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
            'old: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad',
            'current: ba46648582193b0f2ab9a4894708a6106f7479ab',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Pulling repo-full-name...',
            'Before: Hoard [8da760] <- repo [curr: 1ad9e0, stg: ba4664, des: 8da760]',
            'REPO_DESIRED_FILE_TO_GET /test.me.5',
            'REPO_FILE_TO_DELETE /wat/test.me.3',
            'HOARD_FILE_DELETED /wat/test.me.3',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.6',
            'REPO_MARK_FILE_AVAILABLE /wat2/test.me.3',
            'HOARD_FILE_ADDED /wat2/test.me.3',
            'updated repo-full-name from 8da760 to d16f79',
            'updated repo-backup-name from 8da760 to d16f79',
            'After: Hoard [d16f79], repo [curr: ba4664, stg: ba4664, des: d16f79]',
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
            'old: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad',
            'current: ba46648582193b0f2ab9a4894708a6106f7479ab',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Pulling repo-full-name...',
            'Before: Hoard [8da760] <- repo [curr: 1ad9e0, stg: ba4664, des: 8da760]',
            'REPO_DESIRED_FILE_TO_GET /test.me.5',
            'REPO_FILE_TO_DELETE /wat/test.me.3',
            'HOARD_FILE_DELETED /wat/test.me.3',  # fixme maybe MOVE instead of add/delete
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.6',
            'REPO_MARK_FILE_AVAILABLE /wat2/test.me.3',
            'HOARD_FILE_ADDED /wat2/test.me.3',
            'updated repo-full-name from 8da760 to d16f79',
            'updated repo-backup-name from 8da760 to d16f79',
            'After: Hoard [d16f79], repo [curr: ba4664, stg: ba4664, des: d16f79]',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())

        Path(join(self.tmpdir.name, "repo-full/wat3")).mkdir(parents=True, exist_ok=True)
        shutil.move(
            join(self.tmpdir.name, "repo-full/wat2/test.me.3"),
            join(self.tmpdir.name, "repo-full/wat3/test.me.3"))

        res = await full_cave_cmd.refresh()
        self.assertEqual([
            'MOVED wat2/test.me.3 TO wat3/test.me.3',
            'old: ba46648582193b0f2ab9a4894708a6106f7479ab',
            'current: fa1f813dc78eba785566f303591042e6d080dba4',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Pulling repo-full-name...',
            'Before: Hoard [d16f79] <- repo [curr: ba4664, stg: fa1f81, des: d16f79]',
            'REPO_DESIRED_FILE_TO_GET /test.me.5',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.6',
            'REPO_FILE_TO_DELETE /wat2/test.me.3',
            'HOARD_FILE_DELETED /wat2/test.me.3',
            'REPO_MARK_FILE_AVAILABLE /wat3/test.me.3',
            'HOARD_FILE_ADDED /wat3/test.me.3',
            'updated repo-full-name from d16f79 to e13afb',
            'updated repo-backup-name from d16f79 to e13afb',
            'After: Hoard [e13afb], repo [curr: fa1f81, stg: fa1f81, des: e13afb]',
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
