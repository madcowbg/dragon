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
    hoard_cmd.add_remote(remote_path=full_repo_cmd.repo.path, name="repo-full", mount_point="/")
    await hoard_cmd.contents.pull(all=True)


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

    async def test_assign_backups(self):
        partial_repo_cmds, full_repo_cmd, hoard_cmd = populate_random_data(self.tmpdir.name)

        await add_remotes(full_repo_cmd, hoard_cmd, partial_repo_cmds)

        backup_repo_cmds = await self.init_backup_repos(hoard_cmd)
        self.assertEqual([
            'backup-repo-0', 'backup-repo-1', 'backup-repo-2', 'backup-repo-3'], list(sorted(backup_repo_cmds.keys())))

    async def init_backup_repos(self, hoard_cmd):
        backup_repos_names = [f"backup-repo-{i}" for i in range(4)]
        backup_repo_cmds = dict(
            (backup_name, TotalCommand(path=Path(self.tmpdir.name).joinpath(backup_name).as_posix()).cave)
            for backup_name in backup_repos_names)
        for name, cmd in backup_repo_cmds.items():
            Path(cmd.repo.path).mkdir(parents=True, exist_ok=True)
            cmd.init()
            await cmd.refresh()
            hoard_cmd.add_remote(remote_path=cmd.repo.path, name=name, mount_point="/")
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
            '|repo-full           |      |      |      |      |\n'
            '|work-repo-0         |     5|     4|     1|     1|\n'
            '|work-repo-1         |     4|     4|      |      |\n'
            '|work-repo-2         |     5|     5|      |      |\n'
            '\n'
            '|Size                |total |availa|get   |copy  |\n'
            '|backup-repo-0       |      |      |      |      |\n'
            '|backup-repo-1       |      |      |      |      |\n'
            '|backup-repo-2       |      |      |      |      |\n'
            '|backup-repo-3       |      |      |      |      |\n'
            '|repo-full           |      |      |      |      |\n'
            '|work-repo-0         | 6.2KB| 4.3KB| 2.0KB| 2.0KB|\n'
            '|work-repo-1         | 5.9KB| 5.9KB|      |      |\n'
            '|work-repo-2         | 6.6KB| 6.6KB|      |      |\n'), res)

        return backup_repo_cmds


if __name__ == '__main__':
    unittest.main()
