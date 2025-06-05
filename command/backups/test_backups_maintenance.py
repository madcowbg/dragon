import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import IsolatedAsyncioTestCase

from command.test_hoard_command import dump_file_list
from dragon import TotalCommand
from lmdb_storage.test_performance_with_fake_data import populate_index, vocabulary_short


def populate_random_data(tmp_path: str):
    partial_repos_data = dict(
        (f"work-repo-{i}", populate_index(100 + i, 5, vocabulary=vocabulary_short, chance_pct=85)) for i in range(3))

    for partial_name, partial_index in partial_repos_data.items():
        repo_path = Path(tmp_path).joinpath(partial_name)
        repo_path.mkdir(parents=True, exist_ok=False)
        for fpath, rnddata, size in partial_index:
            file_path = repo_path.joinpath(fpath)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            file_path.with_suffix(".file").write_text(rnddata * size)

    partial_repo_cmds = dict(
        (partial_name, TotalCommand(path=Path(tmp_path).joinpath(partial_name).as_posix()).cave)
        for partial_name in partial_repos_data.keys())

    full_repo_path = Path(tmp_path).joinpath("work-repo-full")
    full_repo_path.mkdir(parents=True, exist_ok=True)
    full_repo_cmd = TotalCommand(path=full_repo_path.as_posix()).cave

    hoard_path = Path(tmp_path).joinpath("hoard")
    hoard_path.mkdir(parents=True, exist_ok=True)
    hoard_cmd = TotalCommand(path=hoard_path.as_posix()).hoard

    return partial_repo_cmds, full_repo_cmd, hoard_cmd


async def add_remotes(full_repo_cmd, hoard_cmd, partial_repo_cmds):
    await hoard_cmd.init()
    for name, cmd in partial_repo_cmds.items():
        cmd.init()
        await cmd.refresh()
        hoard_cmd.add_remote(remote_path=cmd.repo.path, name=name, mount_point="/")

    full_repo_cmd.init()
    await full_repo_cmd.refresh()
    hoard_cmd.add_remote(remote_path=full_repo_cmd.repo.path, name="repo-full", mount_point="/", fetch_new=True)


class TestBackupMaintenance(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmpdir.cleanup()

    async def test_file_structure_is_as_expected(self):
        partial_repo_cmds, full_repo_cmd, hoard_cmd = populate_random_data(self.tmpdir.name)

        self.assertEqual([
            'work-repo-0/FPWI.file',
            'work-repo-0/FPWI/W6A.file',
            'work-repo-0/WU.file',
            'work-repo-0/WU/BP.file',
            'work-repo-0/ZEZ.file',
            'work-repo-1/FPWI/WU/QD6V.file',
            'work-repo-1/GYS.file',
            'work-repo-1/QD6V.file',
            'work-repo-1/QD6V/WU/WU/BP.file',
            'work-repo-2/FPWI/GYS.file',
            'work-repo-2/W6A.file',
            'work-repo-2/W6A/QD6V/W6A/4T/FPWI.file',
            'work-repo-2/WU/FPWI/FPWI/FPWI/W6A/QD6V.file',
            'work-repo-2/ZEZ.file'],
            dump_file_list(self.tmpdir.name, ""))

        res = await hoard_cmd.init()
        self.assertEqual('DONE', res)

        for name, cmd in partial_repo_cmds.items():
            cmd.init()
            res = await cmd.refresh()
            self.assertEqual('Refresh done!', res.splitlines()[-1])

            res = hoard_cmd.add_remote(remote_path=cmd.repo.path, name=name, mount_point="/")
            self.assertTrue(res.startswith("Added work-repo-"))

        full_repo_cmd.init()
        await full_repo_cmd.refresh()
        res = hoard_cmd.add_remote(remote_path=full_repo_cmd.repo.path, name="repo-full", mount_point="/")
        self.assertTrue(res.startswith("Added repo-full"))

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True, show_empty=True)
        self.assertEqual((
            'Root: None\n'
            '|Num Files           |total |\n'
            '|repo-full           |      |\n'
            '|work-repo-0         |      |\n'
            '|work-repo-1         |      |\n'
            '|work-repo-2         |      |\n'
            '\n'
            '|Size                |total |\n'
            '|repo-full           |      |\n'
            '|work-repo-0         |      |\n'
            '|work-repo-1         |      |\n'
            '|work-repo-2         |      |\n'), res)

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual((
            'Pulling work-repo-0...\n'
            'Before: Hoard [None] <- repo [curr: None, stg: 978c01, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /FPWI/W6A.file\n'
            'HOARD_FILE_ADDED /FPWI/W6A.file\n'
            'REPO_MARK_FILE_AVAILABLE /FPWI.file\n'
            'HOARD_FILE_ADDED /FPWI.file\n'
            'REPO_MARK_FILE_AVAILABLE /WU/BP.file\n'
            'HOARD_FILE_ADDED /WU/BP.file\n'
            'REPO_MARK_FILE_AVAILABLE /WU.file\n'
            'HOARD_FILE_ADDED /WU.file\n'
            'REPO_MARK_FILE_AVAILABLE /ZEZ.file\n'
            'HOARD_FILE_ADDED /ZEZ.file\n'
            'updated work-repo-0 from None to 978c01\n'
            'After: Hoard [978c01], repo [curr: 978c01, stg: 978c01, des: 978c01]\n'
            "Sync'ed work-repo-0 to hoard!\n"
            'Pulling work-repo-1...\n'
            'Before: Hoard [978c01] <- repo [curr: None, stg: e37b4c, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /FPWI/WU/QD6V.file\n'
            'HOARD_FILE_ADDED /FPWI/WU/QD6V.file\n'
            'REPO_MARK_FILE_AVAILABLE /GYS.file\n'
            'HOARD_FILE_ADDED /GYS.file\n'
            'REPO_MARK_FILE_AVAILABLE /QD6V/WU/WU/BP.file\n'
            'HOARD_FILE_ADDED /QD6V/WU/WU/BP.file\n'
            'REPO_MARK_FILE_AVAILABLE /QD6V.file\n'
            'HOARD_FILE_ADDED /QD6V.file\n'
            'updated work-repo-1 from None to e37b4c\n'
            'After: Hoard [e36549], repo [curr: e37b4c, stg: e37b4c, des: e37b4c]\n'
            "Sync'ed work-repo-1 to hoard!\n"
            'Pulling work-repo-2...\n'
            'Before: Hoard [e36549] <- repo [curr: None, stg: 191bd3, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /FPWI/GYS.file\n'
            'HOARD_FILE_ADDED /FPWI/GYS.file\n'
            'REPO_MARK_FILE_AVAILABLE /W6A/QD6V/W6A/4T/FPWI.file\n'
            'HOARD_FILE_ADDED /W6A/QD6V/W6A/4T/FPWI.file\n'
            'REPO_MARK_FILE_AVAILABLE /W6A.file\n'
            'HOARD_FILE_ADDED /W6A.file\n'
            'REPO_MARK_FILE_AVAILABLE /WU/FPWI/FPWI/FPWI/W6A/QD6V.file\n'
            'HOARD_FILE_ADDED /WU/FPWI/FPWI/FPWI/W6A/QD6V.file\n'
            'REPO_MARK_FILE_AVAILABLE /ZEZ.file\n'
            'HOARD_FILE_CHANGED /ZEZ.file\n'
            'updated work-repo-0 from 978c01 to fd3ef9\n'
            'updated work-repo-2 from None to 191bd3\n'
            'After: Hoard [3f807f], repo [curr: 191bd3, stg: 191bd3, des: 191bd3]\n'
            "Sync'ed work-repo-2 to hoard!\n"
            'Pulling repo-full...\n'
            'Before: Hoard [3f807f] <- repo [curr: None, stg: a80f91, des: None]\n'
            'After: Hoard [3f807f], repo [curr: a80f91, stg: a80f91, des: None]\n'
            "Sync'ed repo-full to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True, show_empty=True)
        self.assertEqual((
            'Root: 3f807f22b0eabb5a44fca944acd489882afc09cb\n'
            '|Num Files           |total |availa|get   |copy  |\n'
            '|repo-full           |      |      |      |      |\n'
            '|work-repo-0         |     5|     4|     1|     1|\n'
            '|work-repo-1         |     4|     4|      |      |\n'
            '|work-repo-2         |     5|     5|      |      |\n'
            '\n'
            '|Size                |total |availa|get   |copy  |\n'
            '|repo-full           |      |      |      |      |\n'
            '|work-repo-0         | 6.2KB| 4.3KB| 2.0KB| 2.0KB|\n'
            '|work-repo-1         | 5.9KB| 5.9KB|      |      |\n'
            '|work-repo-2         | 6.6KB| 6.6KB|      |      |\n'), res)

    async def test_populate_with_pull(self):
        partial_repo_cmds, full_repo_cmd, hoard_cmd = populate_random_data(self.tmpdir.name)

        await add_remotes(full_repo_cmd, hoard_cmd, partial_repo_cmds)

        await hoard_cmd.contents.pull(all=True)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True, show_empty=True)
        self.assertEqual((
            'Root: 3f807f22b0eabb5a44fca944acd489882afc09cb\n'
            '|Num Files           |total |availa|get   |copy  |\n'
            '|repo-full           |    13|      |    13|    13|\n'
            '|work-repo-0         |     5|     4|     1|     1|\n'
            '|work-repo-1         |     4|     4|      |      |\n'
            '|work-repo-2         |     5|     5|      |      |\n'
            '\n'
            '|Size                |total |availa|get   |copy  |\n'
            '|repo-full           |16.8KB|      |16.8KB|16.8KB|\n'
            '|work-repo-0         | 6.2KB| 4.3KB| 2.0KB| 2.0KB|\n'
            '|work-repo-1         | 5.9KB| 5.9KB|      |      |\n'
            '|work-repo-2         | 6.6KB| 6.6KB|      |      |\n'), res)

    async def test_create_backups_from_start(self):
        partial_repo_cmds, full_repo_cmd, hoard_cmd = populate_random_data(self.tmpdir.name)

        await add_remotes(full_repo_cmd, hoard_cmd, partial_repo_cmds)
        backup_repo_cmds = await self.init_backup_repos(hoard_cmd)

        await hoard_cmd.contents.pull(all=True)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True, show_empty=True)
        self.assertEqual((
            'Root: 3f807f22b0eabb5a44fca944acd489882afc09cb\n'
            '|Num Files           |total |availa|get   |copy  |\n'
            '|backup-repo-0       |     4|      |     4|     4|\n'
            '|backup-repo-1       |     4|      |     4|     4|\n'
            '|backup-repo-2       |     3|      |     3|     3|\n'
            '|backup-repo-3       |     3|      |     3|     3|\n'
            '|repo-full           |    13|      |    13|    13|\n'
            '|work-repo-0         |     5|     4|     1|     1|\n'
            '|work-repo-1         |     4|     4|      |      |\n'
            '|work-repo-2         |     5|     5|      |      |\n'
            '\n'
            '|Size                |total |availa|get   |copy  |\n'
            '|backup-repo-0       | 5.5KB|      | 5.5KB| 5.5KB|\n'
            '|backup-repo-1       | 4.7KB|      | 4.7KB| 4.7KB|\n'
            '|backup-repo-2       | 5.1KB|      | 5.1KB| 5.1KB|\n'
            '|backup-repo-3       | 3.5KB|      | 3.5KB| 3.5KB|\n'
            '|repo-full           |16.8KB|      |16.8KB|16.8KB|\n'
            '|work-repo-0         | 6.2KB| 4.3KB| 2.0KB| 2.0KB|\n'
            '|work-repo-1         | 5.9KB| 5.9KB|      |      |\n'
            '|work-repo-2         | 6.6KB| 6.6KB|      |      |\n'), res)

    async def test_assign_backups(self):
        partial_repo_cmds, full_repo_cmd, hoard_cmd = populate_random_data(self.tmpdir.name)

        await add_remotes(full_repo_cmd, hoard_cmd, partial_repo_cmds)

        await hoard_cmd.contents.pull(all=True)

        backup_repo_cmds = await self.init_backup_repos(hoard_cmd)
        self.assertEqual([
            'backup-repo-0', 'backup-repo-1', 'backup-repo-2', 'backup-repo-3'], list(sorted(backup_repo_cmds.keys())))

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual((
            'Skipping update as work-repo-0.staging has not changed: 978c01\n'
            'Skipping update as work-repo-1.staging has not changed: e37b4c\n'
            'Skipping update as work-repo-2.staging has not changed: 191bd3\n'
            'Skipping update as repo-full.staging has not changed: a80f91\n'
            'Pulling backup-repo-0...\n'
            'Before: Hoard [3f807f] <- repo [curr: None, stg: a80f91, des: None]\n'
            'After: Hoard [3f807f], repo [curr: a80f91, stg: a80f91, des: None]\n'
            "Sync'ed backup-repo-0 to hoard!\n"
            'Pulling backup-repo-1...\n'
            'Before: Hoard [3f807f] <- repo [curr: None, stg: a80f91, des: None]\n'
            'After: Hoard [3f807f], repo [curr: a80f91, stg: a80f91, des: None]\n'
            "Sync'ed backup-repo-1 to hoard!\n"
            'Pulling backup-repo-2...\n'
            'Before: Hoard [3f807f] <- repo [curr: None, stg: a80f91, des: None]\n'
            'After: Hoard [3f807f], repo [curr: a80f91, stg: a80f91, des: None]\n'
            "Sync'ed backup-repo-2 to hoard!\n"
            'Pulling backup-repo-3...\n'
            'Before: Hoard [3f807f] <- repo [curr: None, stg: a80f91, des: None]\n'
            'After: Hoard [3f807f], repo [curr: a80f91, stg: a80f91, des: None]\n'
            "Sync'ed backup-repo-3 to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True, show_empty=True)
        self.assertEqual((
            'Root: 3f807f22b0eabb5a44fca944acd489882afc09cb\n'
            '|Num Files           |total |availa|get   |copy  |\n'
            '|backup-repo-0       |      |      |      |      |\n'
            '|backup-repo-1       |      |      |      |      |\n'
            '|backup-repo-2       |      |      |      |      |\n'
            '|backup-repo-3       |      |      |      |      |\n'
            '|repo-full           |    13|      |    13|    13|\n'
            '|work-repo-0         |     5|     4|     1|     1|\n'
            '|work-repo-1         |     4|     4|      |      |\n'
            '|work-repo-2         |     5|     5|      |      |\n'
            '\n'
            '|Size                |total |availa|get   |copy  |\n'
            '|backup-repo-0       |      |      |      |      |\n'
            '|backup-repo-1       |      |      |      |      |\n'
            '|backup-repo-2       |      |      |      |      |\n'
            '|backup-repo-3       |      |      |      |      |\n'
            '|repo-full           |16.8KB|      |16.8KB|16.8KB|\n'
            '|work-repo-0         | 6.2KB| 4.3KB| 2.0KB| 2.0KB|\n'
            '|work-repo-1         | 5.9KB| 5.9KB|      |      |\n'
            '|work-repo-2         | 6.6KB| 6.6KB|      |      |\n'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True, show_empty=True)
        self.assertEqual((
            'Root: 3f807f22b0eabb5a44fca944acd489882afc09cb\n'
            '|Num Files           |total |availa|get   |copy  |\n'
            '|backup-repo-0       |      |      |      |      |\n'
            '|backup-repo-1       |      |      |      |      |\n'
            '|backup-repo-2       |      |      |      |      |\n'
            '|backup-repo-3       |      |      |      |      |\n'
            '|repo-full           |    13|      |    13|    13|\n'
            '|work-repo-0         |     5|     4|     1|     1|\n'
            '|work-repo-1         |     4|     4|      |      |\n'
            '|work-repo-2         |     5|     5|      |      |\n'
            '\n'
            '|Size                |total |availa|get   |copy  |\n'
            '|backup-repo-0       |      |      |      |      |\n'
            '|backup-repo-1       |      |      |      |      |\n'
            '|backup-repo-2       |      |      |      |      |\n'
            '|backup-repo-3       |      |      |      |      |\n'
            '|repo-full           |16.8KB|      |16.8KB|16.8KB|\n'
            '|work-repo-0         | 6.2KB| 4.3KB| 2.0KB| 2.0KB|\n'
            '|work-repo-1         | 5.9KB| 5.9KB|      |      |\n'
            '|work-repo-2         | 6.6KB| 6.6KB|      |      |\n'), res)

        res = await hoard_cmd.backups.assign(available_only=False)
        self.assertEqual((
            'set: / with 4/4 media\n'
            'BACKUP [backup-repo-0]/FPWI/GYS.file\n'
            'BACKUP [backup-repo-1]/FPWI/W6A.file\n'
            'BACKUP [backup-repo-2]/FPWI/WU/QD6V.file\n'
            'BACKUP [backup-repo-3]/FPWI.file\n'
            'BACKUP [backup-repo-1]/GYS.file\n'
            'BACKUP [backup-repo-2]/QD6V/WU/WU/BP.file\n'
            'BACKUP [backup-repo-3]/QD6V.file\n'
            'BACKUP [backup-repo-0]/W6A/QD6V/W6A/4T/FPWI.file\n'
            'BACKUP [backup-repo-0]/W6A.file\n'
            'BACKUP [backup-repo-1]/WU/BP.file\n'
            'BACKUP [backup-repo-3]/WU/FPWI/FPWI/FPWI/W6A/QD6V.file\n'
            'BACKUP [backup-repo-2]/WU.file\n'
            'BACKUP [backup-repo-0]/ZEZ.file\n'
            ' backup-repo-0 <- 4 files (5.1KB)\n'
            ' backup-repo-1 <- 3 files (4.3KB)\n'
            ' backup-repo-2 <- 3 files (3.5KB)\n'
            ' backup-repo-3 <- 3 files (3.9KB)\n'
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True, show_empty=True)
        self.assertEqual((
            'Root: 3f807f22b0eabb5a44fca944acd489882afc09cb\n'
            '|Num Files           |total |availa|get   |copy  |\n'
            '|backup-repo-0       |     4|      |     4|     4|\n'
            '|backup-repo-1       |     3|      |     3|     3|\n'
            '|backup-repo-2       |     3|      |     3|     3|\n'
            '|backup-repo-3       |     3|      |     3|     3|\n'
            '|repo-full           |    13|      |    13|    13|\n'
            '|work-repo-0         |     5|     4|     1|     1|\n'
            '|work-repo-1         |     4|     4|      |      |\n'
            '|work-repo-2         |     5|     5|      |      |\n'
            '\n'
            '|Size                |total |availa|get   |copy  |\n'
            '|backup-repo-0       | 5.1KB|      | 5.1KB| 5.1KB|\n'
            '|backup-repo-1       | 4.3KB|      | 4.3KB| 4.3KB|\n'
            '|backup-repo-2       | 3.5KB|      | 3.5KB| 3.5KB|\n'
            '|backup-repo-3       | 3.9KB|      | 3.9KB| 3.9KB|\n'
            '|repo-full           |16.8KB|      |16.8KB|16.8KB|\n'
            '|work-repo-0         | 6.2KB| 4.3KB| 2.0KB| 2.0KB|\n'
            '|work-repo-1         | 5.9KB| 5.9KB|      |      |\n'
            '|work-repo-2         | 6.6KB| 6.6KB|      |      |\n'),
            res)

    async def test_move_files_after_backup(self):
        partial_repo_cmds, full_repo_cmd, hoard_cmd = populate_random_data(self.tmpdir.name)

        await add_remotes(full_repo_cmd, hoard_cmd, partial_repo_cmds)
        backup_repo_cmds = await self.init_backup_repos(hoard_cmd)

        await hoard_cmd.contents.pull(all=True)

        res = await hoard_cmd.contents.ls()
        self.assertEqual((
            'Root: 3f807f22b0eabb5a44fca944acd489882afc09cb\n'
            'Remote backup-repo-0 current=a80f91 staging=a80f91 desired=ae5d47\n'
            'Remote backup-repo-1 current=a80f91 staging=a80f91 desired=7a5957\n'
            'Remote backup-repo-2 current=a80f91 staging=a80f91 desired=d870e1\n'
            'Remote backup-repo-3 current=a80f91 staging=a80f91 desired=40432b\n'
            'Remote repo-full current=a80f91 staging=a80f91 desired=3f807f\n'
            'Remote work-repo-0 current=978c01 staging=978c01 desired=fd3ef9\n'
            'Remote work-repo-1 current=e37b4c staging=e37b4c desired=e37b4c\n'
            'Remote work-repo-2 current=191bd3 staging=191bd3 desired=191bd3\n'
            '/\n'
            '/FPWI.file = a:1 g:2\n'
            '/GYS.file = a:1 g:2\n'
            '/QD6V.file = a:1 g:2\n'
            '/W6A.file = a:1 g:2\n'
            '/WU.file = a:1 g:2\n'
            '/ZEZ.file = a:1 g:4\n'
            '/FPWI\n'
            '/FPWI/GYS.file = a:1 g:2\n'
            '/FPWI/W6A.file = a:1 g:2\n'
            '/FPWI/WU\n'
            '/FPWI/WU/QD6V.file = a:1 g:2\n'
            '/QD6V\n'
            '/QD6V/WU\n'
            '/QD6V/WU/WU\n'
            '/QD6V/WU/WU/BP.file = a:1 g:2\n'
            '/W6A\n'
            '/W6A/QD6V\n'
            '/W6A/QD6V/W6A\n'
            '/W6A/QD6V/W6A/4T\n'
            '/W6A/QD6V/W6A/4T/FPWI.file = a:1 g:2\n'
            '/WU\n'
            '/WU/BP.file = a:1 g:2\n'
            '/WU/FPWI\n'
            '/WU/FPWI/FPWI\n'
            '/WU/FPWI/FPWI/FPWI\n'
            '/WU/FPWI/FPWI/FPWI/W6A\n'
            '/WU/FPWI/FPWI/FPWI/W6A/QD6V.file = a:1 g:2\n'
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True, show_empty=True)
        self.assertEqual((
            'Root: 3f807f22b0eabb5a44fca944acd489882afc09cb\n'
            '|Num Files           |total |availa|get   |copy  |\n'
            '|backup-repo-0       |     4|      |     4|     4|\n'
            '|backup-repo-1       |     4|      |     4|     4|\n'
            '|backup-repo-2       |     3|      |     3|     3|\n'
            '|backup-repo-3       |     3|      |     3|     3|\n'
            '|repo-full           |    13|      |    13|    13|\n'
            '|work-repo-0         |     5|     4|     1|     1|\n'
            '|work-repo-1         |     4|     4|      |      |\n'
            '|work-repo-2         |     5|     5|      |      |\n'
            '\n'
            '|Size                |total |availa|get   |copy  |\n'
            '|backup-repo-0       | 5.5KB|      | 5.5KB| 5.5KB|\n'
            '|backup-repo-1       | 4.7KB|      | 4.7KB| 4.7KB|\n'
            '|backup-repo-2       | 5.1KB|      | 5.1KB| 5.1KB|\n'
            '|backup-repo-3       | 3.5KB|      | 3.5KB| 3.5KB|\n'
            '|repo-full           |16.8KB|      |16.8KB|16.8KB|\n'
            '|work-repo-0         | 6.2KB| 4.3KB| 2.0KB| 2.0KB|\n'
            '|work-repo-1         | 5.9KB| 5.9KB|      |      |\n'
            '|work-repo-2         | 6.6KB| 6.6KB|      |      |\n'),
            res)

        res = await hoard_cmd.files.push(all=True)
        self.assertEqual((
            'Before push:\n'
            'Remote backup-repo-0 current=a80f91 staging=a80f91 desired=ae5d47\n'
            'Remote backup-repo-1 current=a80f91 staging=a80f91 desired=7a5957\n'
            'Remote backup-repo-2 current=a80f91 staging=a80f91 desired=d870e1\n'
            'Remote backup-repo-3 current=a80f91 staging=a80f91 desired=40432b\n'
            'Remote repo-full current=a80f91 staging=a80f91 desired=3f807f\n'
            'Remote work-repo-0 current=978c01 staging=978c01 desired=fd3ef9\n'
            'Remote work-repo-1 current=e37b4c staging=e37b4c desired=e37b4c\n'
            'Remote work-repo-2 current=191bd3 staging=191bd3 desired=191bd3\n'
            'work-repo-0:\n'
            'REMOTE_COPY [work-repo-2] ZEZ.file\n'
            'work-repo-1:\n'
            'work-repo-2:\n'
            'repo-full:\n'
            'REMOTE_COPY [work-repo-0] FPWI.file\n'
            'REMOTE_COPY [work-repo-2] FPWI/GYS.file\n'
            'REMOTE_COPY [work-repo-0] FPWI/W6A.file\n'
            'REMOTE_COPY [work-repo-1] FPWI/WU/QD6V.file\n'
            'REMOTE_COPY [work-repo-0] GYS.file\n'
            'REMOTE_COPY [work-repo-1] QD6V.file\n'
            'REMOTE_COPY [work-repo-1] QD6V/WU/WU/BP.file\n'
            'REMOTE_COPY [work-repo-2] W6A.file\n'
            'REMOTE_COPY [work-repo-2] W6A/QD6V/W6A/4T/FPWI.file\n'
            'REMOTE_COPY [work-repo-0] WU.file\n'
            'REMOTE_COPY [work-repo-0] WU/BP.file\n'
            'REMOTE_COPY [work-repo-2] WU/FPWI/FPWI/FPWI/W6A/QD6V.file\n'
            'REMOTE_COPY [work-repo-2] ZEZ.file\n'
            'backup-repo-0:\n'
            'REMOTE_COPY [work-repo-0] FPWI/W6A.file\n'
            'REMOTE_COPY [work-repo-0] GYS.file\n'
            'REMOTE_COPY [work-repo-2] W6A.file\n'
            'REMOTE_COPY [work-repo-2] ZEZ.file\n'
            'backup-repo-1:\n'
            'REMOTE_COPY [work-repo-0] FPWI.file\n'
            'REMOTE_COPY [work-repo-1] QD6V/WU/WU/BP.file\n'
            'REMOTE_COPY [work-repo-2] W6A/QD6V/W6A/4T/FPWI.file\n'
            'REMOTE_COPY [work-repo-2] WU/FPWI/FPWI/FPWI/W6A/QD6V.file\n'
            'backup-repo-2:\n'
            'REMOTE_COPY [work-repo-1] QD6V.file\n'
            'REMOTE_COPY [work-repo-0] WU/BP.file\n'
            'REMOTE_COPY [work-repo-2] ZEZ.file\n'
            'backup-repo-3:\n'
            'REMOTE_COPY [work-repo-2] FPWI/GYS.file\n'
            'REMOTE_COPY [work-repo-1] FPWI/WU/QD6V.file\n'
            'REMOTE_COPY [work-repo-0] WU.file\n'
            'work-repo-0:\n'
            'work-repo-1:\n'
            'work-repo-2:\n'
            'repo-full:\n'
            'backup-repo-0:\n'
            'backup-repo-1:\n'
            'backup-repo-2:\n'
            'backup-repo-3:\n'
            'After:\n'
            'Remote backup-repo-0 current=ae5d47 staging=a80f91 desired=ae5d47\n'
            'Remote backup-repo-1 current=7a5957 staging=a80f91 desired=7a5957\n'
            'Remote backup-repo-2 current=d870e1 staging=a80f91 desired=d870e1\n'
            'Remote backup-repo-3 current=40432b staging=a80f91 desired=40432b\n'
            'Remote repo-full current=3f807f staging=a80f91 desired=3f807f\n'
            'Remote work-repo-0 current=fd3ef9 staging=978c01 desired=fd3ef9\n'
            'Remote work-repo-1 current=e37b4c staging=e37b4c desired=e37b4c\n'
            'Remote work-repo-2 current=191bd3 staging=191bd3 desired=191bd3\n'
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True, show_empty=True)
        self.assertEqual((
            'Root: 3f807f22b0eabb5a44fca944acd489882afc09cb\n'
            '|Num Files           |total |availa|\n'
            '|backup-repo-0       |     4|     4|\n'
            '|backup-repo-1       |     4|     4|\n'
            '|backup-repo-2       |     3|     3|\n'
            '|backup-repo-3       |     3|     3|\n'
            '|repo-full           |    13|    13|\n'
            '|work-repo-0         |     5|     5|\n'
            '|work-repo-1         |     4|     4|\n'
            '|work-repo-2         |     5|     5|\n'
            '\n'
            '|Size                |total |availa|\n'
            '|backup-repo-0       | 5.5KB| 5.5KB|\n'
            '|backup-repo-1       | 4.7KB| 4.7KB|\n'
            '|backup-repo-2       | 5.1KB| 5.1KB|\n'
            '|backup-repo-3       | 3.5KB| 3.5KB|\n'
            '|repo-full           |16.8KB|16.8KB|\n'
            '|work-repo-0         | 6.2KB| 6.2KB|\n'
            '|work-repo-1         | 5.9KB| 5.9KB|\n'
            '|work-repo-2         | 6.6KB| 6.6KB|\n'), res)

        # moving some files and folders
        shutil.move(
            Path(self.tmpdir.name).joinpath('work-repo-1/QD6V.file'),
            Path(self.tmpdir.name).joinpath('work-repo-1/QD6V-new.file'))
        shutil.move(
            Path(self.tmpdir.name).joinpath('work-repo-1/QD6V/WU/'),
            Path(self.tmpdir.name).joinpath('work-repo-1/QD6V/wololo/'))
        shutil.move(
            Path(self.tmpdir.name).joinpath('work-repo-2/FPWI/GYS.file'),
            Path(self.tmpdir.name).joinpath('work-repo-0/FPWI/GYS.file'))

        Path(self.tmpdir.name).joinpath('work-repo-2/z/W6A-a copy.file').parent.mkdir(parents=True)
        shutil.copy(
            Path(self.tmpdir.name).joinpath('work-repo-2/W6A.file'),
            Path(self.tmpdir.name).joinpath('work-repo-2/z/W6A-a copy.file'))

        Path(self.tmpdir.name).joinpath('work-repo-2/b/W6A-another copy.file').parent.mkdir(parents=True)
        shutil.move(
            Path(self.tmpdir.name).joinpath('work-repo-2/W6A.file'),
            Path(self.tmpdir.name).joinpath('work-repo-2/b/W6A-another copy.file'))

        for name, cmd in partial_repo_cmds.items():
            await cmd.refresh()

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual((
            'Pulling work-repo-0...\n'
            'Before: Hoard [3f807f] <- repo [curr: fd3ef9, stg: f1a4a0, des: fd3ef9]\n'
            'REPO_MARK_FILE_AVAILABLE /FPWI/GYS.file\n'
            'updated work-repo-0 from fd3ef9 to f1a4a0\n'
            'After: Hoard [3f807f], repo [curr: f1a4a0, stg: f1a4a0, des: f1a4a0]\n'
            "Sync'ed work-repo-0 to hoard!\n"
            'Pulling work-repo-1...\n'
            'Before: Hoard [3f807f] <- repo [curr: e37b4c, stg: e550f6, des: e37b4c]\n'
            'REPO_FILE_TO_DELETE /QD6V/WU/WU/BP.file\n'
            'HOARD_FILE_DELETED /QD6V/WU/WU/BP.file\n'
            'REPO_MARK_FILE_AVAILABLE /QD6V/wololo/WU/BP.file\n'
            'HOARD_FILE_ADDED /QD6V/wololo/WU/BP.file\n'
            'REPO_MARK_FILE_AVAILABLE /QD6V-new.file\n'
            'HOARD_FILE_ADDED /QD6V-new.file\n'
            'REPO_FILE_TO_DELETE /QD6V.file\n'
            'HOARD_FILE_DELETED /QD6V.file\n'
            'updated work-repo-1 from e37b4c to e550f6\n'
            'updated repo-full from 3f807f to 4c3353\n'
            'updated backup-repo-1 from 7a5957 to 9b3019\n'
            'updated backup-repo-2 from d870e1 to 6a15b3\n'
            'updated backup-repo-3 from 40432b to 14d484\n'
            'After: Hoard [4c3353], repo [curr: e550f6, stg: e550f6, des: e550f6]\n'
            "Sync'ed work-repo-1 to hoard!\n"
            'Pulling work-repo-2...\n'
            'Before: Hoard [4c3353] <- repo [curr: 191bd3, stg: 80ea2e, des: 191bd3]\n'
            'REPO_FILE_TO_DELETE /FPWI/GYS.file\n'
            'HOARD_FILE_DELETED /FPWI/GYS.file\n'
            'REPO_FILE_TO_DELETE /W6A.file\n'
            'HOARD_FILE_DELETED /W6A.file\n'
            'REPO_MARK_FILE_AVAILABLE /b/W6A-another copy.file\n'
            'HOARD_FILE_ADDED /b/W6A-another copy.file\n'
            'REPO_MARK_FILE_AVAILABLE /z/W6A-a copy.file\n'
            'HOARD_FILE_ADDED /z/W6A-a copy.file\n'
            'updated work-repo-0 from f1a4a0 to fd3ef9\n'
            'updated work-repo-2 from 191bd3 to 80ea2e\n'
            'updated repo-full from 4c3353 to 8a4837\n'
            'updated backup-repo-0 from ae5d47 to e39dde\n'
            'updated backup-repo-2 from 6a15b3 to 16d028\n'
            'updated backup-repo-3 from 14d484 to 0b1f89\n'
            'After: Hoard [8a4837], repo [curr: 80ea2e, stg: 80ea2e, des: 80ea2e]\n'
            "Sync'ed work-repo-2 to hoard!\n"
            'Skipping update as repo-full.staging has not changed: a80f91\n'
            'Skipping update as backup-repo-0.staging has not changed: a80f91\n'
            'Skipping update as backup-repo-1.staging has not changed: a80f91\n'
            'Skipping update as backup-repo-2.staging has not changed: a80f91\n'
            'Skipping update as backup-repo-3.staging has not changed: a80f91\n'
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True, show_empty=True)
        self.assertEqual((
            'Root: 8a4837fd80ab9a8991eaa3ec407b25c2e405c6c6\n'
            '|Num Files           |total |availa|get   |copy  |move  |cleanu|reserv|\n'
            '|backup-repo-0       |     4|     3|      |      |      |     1|     1|\n'
            '|backup-repo-1       |     5|     3|     1|     1|      |     1|     1|\n'
            '|backup-repo-2       |     4|     2|     1|     1|      |     1|     1|\n'
            '|backup-repo-3       |     5|     2|     2|     2|      |     1|      |\n'
            '|repo-full           |    17|     9|     4|     4|     4|     4|     3|\n'
            '|work-repo-0         |     6|     5|      |      |      |     1|      |\n'
            '|work-repo-1         |     4|     4|      |      |      |      |      |\n'
            '|work-repo-2         |     5|     5|      |      |      |      |      |\n'
            '\n'
            '|Size                |total |availa|get   |copy  |move  |cleanu|reserv|\n'
            '|backup-repo-0       | 5.5KB| 4.3KB|      |      |      | 1.2KB| 1.2KB|\n'
            '|backup-repo-1       | 5.9KB| 3.1KB| 1.2KB| 1.2KB|      | 1.6KB| 1.6KB|\n'
            '|backup-repo-2       | 6.2KB| 3.9KB| 1.2KB| 1.2KB|      | 1.2KB| 1.2KB|\n'
            '|backup-repo-3       | 6.2KB| 2.0KB| 2.7KB| 2.7KB|      | 1.6KB|      |\n'
            '|repo-full           |21.9KB|11.3KB| 5.1KB| 5.1KB| 5.1KB| 5.5KB| 3.9KB|\n'
            '|work-repo-0         | 7.8KB| 6.2KB|      |      |      | 1.6KB|      |\n'
            '|work-repo-1         | 5.9KB| 5.9KB|      |      |      |      |      |\n'
            '|work-repo-2         | 6.2KB| 6.2KB|      |      |      |      |      |\n'), res)

        res = await hoard_cmd.contents.ls()
        self.assertEqual((
            'Root: 8a4837fd80ab9a8991eaa3ec407b25c2e405c6c6\n'
            'Remote backup-repo-0 current=ae5d47 staging=a80f91 desired=e39dde\n'
            'Remote backup-repo-1 current=7a5957 staging=a80f91 desired=9b3019\n'
            'Remote backup-repo-2 current=d870e1 staging=a80f91 desired=16d028\n'
            'Remote backup-repo-3 current=40432b staging=a80f91 desired=0b1f89\n'
            'Remote repo-full current=3f807f staging=a80f91 desired=8a4837\n'
            'Remote work-repo-0 current=f1a4a0 staging=f1a4a0 desired=fd3ef9\n'
            'Remote work-repo-1 current=e550f6 staging=e550f6 desired=e550f6\n'
            'Remote work-repo-2 current=80ea2e staging=80ea2e desired=80ea2e\n'
            '/\n'
            '/FPWI.file = a:3\n'
            '/GYS.file = a:3\n'
            '/QD6V-new.file = a:1 g:2\n'
            '/WU.file = a:3\n'
            '/ZEZ.file = a:5\n'
            '/FPWI\n'
            '/FPWI/W6A.file = a:3\n'
            '/FPWI/WU\n'
            '/FPWI/WU/QD6V.file = a:3\n'
            '/QD6V\n'
            '/QD6V/wololo\n'
            '/QD6V/wololo/WU\n'
            '/QD6V/wololo/WU/BP.file = a:1 g:2\n'
            '/W6A\n'
            '/W6A/QD6V\n'
            '/W6A/QD6V/W6A\n'
            '/W6A/QD6V/W6A/4T\n'
            '/W6A/QD6V/W6A/4T/FPWI.file = a:3\n'
            '/WU\n'
            '/WU/BP.file = a:3\n'
            '/WU/FPWI\n'
            '/WU/FPWI/FPWI\n'
            '/WU/FPWI/FPWI/FPWI\n'
            '/WU/FPWI/FPWI/FPWI/W6A\n'
            '/WU/FPWI/FPWI/FPWI/W6A/QD6V.file = a:3\n'
            '/b\n'
            '/b/W6A-another copy.file = a:1 g:2\n'
            '/z\n'
            '/z/W6A-a copy.file = a:1 g:2\n'
            'DONE'), res)

        res = await hoard_cmd.backups.unassign(all=True)
        self.assertEqual((
            'Unassigning from backup-repo-0 [e39dde]:\n'
            'Desired root for backup-repo-0 is e39dde <- e39dde\n'
            'Unassigning from backup-repo-1 [9b3019]:\n'
            'WONT_GET /QD6V-new.file\n'
            'Desired root for backup-repo-1 is 6ef094 <- 9b3019\n'
            'Unassigning from backup-repo-2 [16d028]:\n'
            'WONT_GET /b/W6A-another copy.file\n'
            'Desired root for backup-repo-2 is 6a15b3 <- 16d028\n'
            'Unassigning from backup-repo-3 [0b1f89]:\n'
            'WONT_GET /QD6V/wololo/WU/BP.file\n'
            'WONT_GET /z/W6A-a copy.file\n'
            'Desired root for backup-repo-3 is 89a270 <- 0b1f89\n'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True, show_empty=True)
        self.assertEqual((
            'Root: 8a4837fd80ab9a8991eaa3ec407b25c2e405c6c6\n'
            '|Num Files           |total |availa|get   |copy  |move  |cleanu|reserv|\n'
            '|backup-repo-0       |     4|     3|      |      |      |     1|     1|\n'
            '|backup-repo-1       |     4|     3|      |      |      |     1|     1|\n'
            '|backup-repo-2       |     3|     2|      |      |      |     1|     1|\n'
            '|backup-repo-3       |     3|     2|      |      |      |     1|      |\n'
            '|repo-full           |    17|     9|     4|     4|     4|     4|     3|\n'
            '|work-repo-0         |     6|     5|      |      |      |     1|      |\n'
            '|work-repo-1         |     4|     4|      |      |      |      |      |\n'
            '|work-repo-2         |     5|     5|      |      |      |      |      |\n'
            '\n'
            '|Size                |total |availa|get   |copy  |move  |cleanu|reserv|\n'
            '|backup-repo-0       | 5.5KB| 4.3KB|      |      |      | 1.2KB| 1.2KB|\n'
            '|backup-repo-1       | 4.7KB| 3.1KB|      |      |      | 1.6KB| 1.6KB|\n'
            '|backup-repo-2       | 5.1KB| 3.9KB|      |      |      | 1.2KB| 1.2KB|\n'
            '|backup-repo-3       | 3.5KB| 2.0KB|      |      |      | 1.6KB|      |\n'
            '|repo-full           |21.9KB|11.3KB| 5.1KB| 5.1KB| 5.1KB| 5.5KB| 3.9KB|\n'
            '|work-repo-0         | 7.8KB| 6.2KB|      |      |      | 1.6KB|      |\n'
            '|work-repo-1         | 5.9KB| 5.9KB|      |      |      |      |      |\n'
            '|work-repo-2         | 6.2KB| 6.2KB|      |      |      |      |      |\n'), res)

        res = await hoard_cmd.backups.assign(available_only=False)
        self.assertEqual((
            'set: / with 4/4 media\n'
            'REASSIGN [backup-repo-0] /W6A.file to /b/W6A-another copy.file\n'
            'REASSIGN [backup-repo-1] /QD6V/WU/WU/BP.file to /QD6V/wololo/WU/BP.file\n'
            'REASSIGN [backup-repo-2] /QD6V.file to /QD6V-new.file\n'
            'BACKUP [backup-repo-3]/z/W6A-a copy.file\n'
            ' backup-repo-3 <- 1 files (1.2KB)\n'
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True, show_empty=True)
        self.assertEqual((
            'Root: 8a4837fd80ab9a8991eaa3ec407b25c2e405c6c6\n'
            '|Num Files           |total |availa|get   |copy  |move  |cleanu|reserv|\n'
            '|backup-repo-0       |     5|     3|     1|     1|     1|     1|     1|\n'  # a fixme bit too much for effective "move" ops
            '|backup-repo-1       |     5|     3|     1|     1|     1|     1|     1|\n'
            '|backup-repo-2       |     4|     2|     1|     1|     1|     1|     1|\n'
            '|backup-repo-3       |     4|     2|     1|     1|      |     1|      |\n'
            '|repo-full           |    17|     9|     4|     4|     4|     4|     3|\n'
            '|work-repo-0         |     6|     5|      |      |      |     1|      |\n'
            '|work-repo-1         |     4|     4|      |      |      |      |      |\n'
            '|work-repo-2         |     5|     5|      |      |      |      |      |\n'
            '\n'
            '|Size                |total |availa|get   |copy  |move  |cleanu|reserv|\n'
            '|backup-repo-0       | 6.6KB| 4.3KB| 1.2KB| 1.2KB| 1.2KB| 1.2KB| 1.2KB|\n'
            '|backup-repo-1       | 6.2KB| 3.1KB| 1.6KB| 1.6KB| 1.6KB| 1.6KB| 1.6KB|\n'
            '|backup-repo-2       | 6.2KB| 3.9KB| 1.2KB| 1.2KB| 1.2KB| 1.2KB| 1.2KB|\n'
            '|backup-repo-3       | 4.7KB| 2.0KB| 1.2KB| 1.2KB|      | 1.6KB|      |\n'
            '|repo-full           |21.9KB|11.3KB| 5.1KB| 5.1KB| 5.1KB| 5.5KB| 3.9KB|\n'
            '|work-repo-0         | 7.8KB| 6.2KB|      |      |      | 1.6KB|      |\n'
            '|work-repo-1         | 5.9KB| 5.9KB|      |      |      |      |      |\n'
            '|work-repo-2         | 6.2KB| 6.2KB|      |      |      |      |      |\n'), res)

        res = await hoard_cmd.files.push(all=True)
        self.assertEqual((
            'Before push:\n'
            'Remote backup-repo-0 current=ae5d47 staging=a80f91 desired=27597a\n'
            'Remote backup-repo-1 current=7a5957 staging=a80f91 desired=f3f7e5\n'
            'Remote backup-repo-2 current=d870e1 staging=a80f91 desired=0fdb3f\n'
            'Remote backup-repo-3 current=40432b staging=a80f91 desired=178937\n'
            'Remote repo-full current=3f807f staging=a80f91 desired=8a4837\n'
            'Remote work-repo-0 current=f1a4a0 staging=f1a4a0 desired=fd3ef9\n'
            'Remote work-repo-1 current=e550f6 staging=e550f6 desired=e550f6\n'
            'Remote work-repo-2 current=80ea2e staging=80ea2e desired=80ea2e\n'
            'work-repo-0:\n'
            'work-repo-1:\n'
            'work-repo-2:\n'
            'repo-full:\n'
            'LOCAL_MOVE QD6V-new.file\n'
            'LOCAL_MOVE QD6V/wololo/WU/BP.file\n'
            'LOCAL_MOVE b/W6A-another copy.file\n'
            'REMOTE_COPY [work-repo-2] z/W6A-a copy.file\n'  # this could be a copy, but for the deferred add
            'backup-repo-0:\n'
            'LOCAL_MOVE b/W6A-another copy.file\n'
            'backup-repo-1:\n'
            'LOCAL_MOVE QD6V/wololo/WU/BP.file\n'
            'backup-repo-2:\n'
            'LOCAL_MOVE QD6V-new.file\n'
            'backup-repo-3:\n'
            'REMOTE_COPY [work-repo-2] z/W6A-a copy.file\n'
            'work-repo-0:\n'
            'd FPWI/GYS.file\n'
            'work-repo-1:\n'
            'work-repo-2:\n'
            'repo-full:\n'
            'd FPWI/GYS.file\n'
            'backup-repo-0:\n'
            'backup-repo-1:\n'
            'backup-repo-2:\n'
            'backup-repo-3:\n'
            'd FPWI/GYS.file\n'
            'After:\n'
            'Remote backup-repo-0 current=27597a staging=a80f91 desired=27597a\n'
            'Remote backup-repo-1 current=f3f7e5 staging=a80f91 desired=f3f7e5\n'
            'Remote backup-repo-2 current=0fdb3f staging=a80f91 desired=0fdb3f\n'
            'Remote backup-repo-3 current=178937 staging=a80f91 desired=178937\n'
            'Remote repo-full current=8a4837 staging=a80f91 desired=8a4837\n'
            'Remote work-repo-0 current=fd3ef9 staging=f1a4a0 desired=fd3ef9\n'
            'Remote work-repo-1 current=e550f6 staging=e550f6 desired=e550f6\n'
            'Remote work-repo-2 current=80ea2e staging=80ea2e desired=80ea2e\n'
            'DONE'), res)

    async def init_backup_repos(self, hoard_cmd):
        backup_repos_names = [f"backup-repo-{i}" for i in range(4)]
        backup_repo_cmds = dict(
            (backup_name, TotalCommand(path=Path(self.tmpdir.name).joinpath(backup_name).as_posix()).cave)
            for backup_name in backup_repos_names)

        for name, cmd in backup_repo_cmds.items():
            Path(cmd.repo.path).mkdir(parents=True, exist_ok=True)
            cmd.init()
            await cmd.refresh()
            hoard_cmd.add_remote(remote_path=cmd.repo.path, name=name, mount_point="/", type="backup")

        return backup_repo_cmds


if __name__ == '__main__':
    unittest.main()
