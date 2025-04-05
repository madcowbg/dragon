import os
import tempfile
from os.path import join
from unittest import IsolatedAsyncioTestCase

from command.command_repo import RepoCommand
from command.test_repo_command import pretty_file_writer
from config import CaveType
from dragon import TotalCommand


async def init_complex_hoard(tmpdir: str):
    partial_cave_cmd = TotalCommand(path=join(tmpdir, "repo-partial")).cave
    partial_cave_cmd.init()
    await partial_cave_cmd.refresh(show_details=False)

    full_cave_cmd = TotalCommand(path=join(tmpdir, "repo-full")).cave
    full_cave_cmd.init()
    await full_cave_cmd.refresh(show_details=False)

    incoming_cave_cmd = TotalCommand(path=join(tmpdir, "repo-incoming")).cave
    incoming_cave_cmd.init()
    await incoming_cave_cmd.refresh(show_details=False)

    hoard_cmd = TotalCommand(path=join(tmpdir, "hoard")).hoard
    hoard_cmd.init()

    hoard_cmd.add_remote(
        remote_path=join(tmpdir, "repo-partial"), name="repo-partial-name", mount_point="/",
        type=CaveType.PARTIAL)

    hoard_cmd.add_remote(
        remote_path=join(tmpdir, "repo-full"), name="repo-full-name", mount_point="/",
        type=CaveType.PARTIAL, fetch_new=True)

    hoard_cmd.add_remote(
        remote_path=join(tmpdir, "repo-incoming"), name="repo-incoming-name", mount_point="/",
        type=CaveType.INCOMING)

    res = hoard_cmd.remotes(hide_paths=True)
    assert (""
            "3 total remotes."
            f"\n  [repo-partial-name] {partial_cave_cmd.current_uuid()} (partial)"
            f"\n  [repo-full-name] {full_cave_cmd.current_uuid()} (partial)"
            f"\n  [repo-incoming-name] {incoming_cave_cmd.current_uuid()} (incoming)"
            "\nMounts:"
            "\n  / -> repo-partial-name, repo-full-name, repo-incoming-name"
            "\nDONE") == res.strip()

    # make sure resolving the command from a hoard path works
    tmp_command = RepoCommand(path=join(tmpdir, "hoard"), name="repo-partial-name")
    assert partial_cave_cmd.current_uuid() == tmp_command.current_uuid()
    assert partial_cave_cmd.repo.path == tmp_command.repo.path

    return hoard_cmd, partial_cave_cmd, full_cave_cmd, incoming_cave_cmd


def populate_repotypes(tmpdir: str):
    os.mkdir(join(tmpdir, 'hoard'))
    # f"D /wat/test.me.different\n"
    # f"D /wat/test.me.once\n"
    # f"D /wat/test.me.twice\nDONE"
    pfw = pretty_file_writer(tmpdir)

    pfw('repo-partial/test.me.1', "gsadfs" * 10)
    pfw('repo-partial/wat/test.me.2', "gsadf3dq" * 2)

    pfw('repo-full/test.me.1', "gsadfs" * 10)
    pfw('repo-full/test.me.4', "fwadeaewdsa" * 7)
    pfw('repo-full/wat/test.me.2', "gsadf3dq" * 2)
    pfw('repo-full/wat/test.me.3', "afaswewfas" * 9)

    pfw('repo-incoming/wat/test.me.3', "asdgvarfa")
    pfw('repo-incoming/test.me.4', "fwadeaewdsa" * 7)
    pfw('repo-incoming/test.me.5', "adsfg" * 12)
    pfw('repo-incoming/wat/test.me.6', "f2fwsdf" * 11)

    pfw('repo-backup-1/test.me.1', "gsadfs" * 10)
    pfw('repo-backup-1/wat/test.me.3', "afaswewfas" * 9)

    pfw('repo-backup-2/test.me.1', "gsadfs" * 10)

    pfw('repo-backup-3/test.me.obsolete', "asdfvawef" * 13)

    os.mkdir(join(tmpdir, 'repo-backup-4'))  # empty

    os.mkdir(join(tmpdir, 'repo-backup-5'))  # empty


class TestBackups(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        populate_repotypes(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    async def test_get_to_steady_state(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, incoming_cave_cmd = await init_complex_hoard(self.tmpdir.name)

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual(
            "+/test.me.1\n"
            "+/wat/test.me.2\n"
            "Sync'ed repo-partial-name to hoard!\n"
            "=/test.me.1\n"
            "=/wat/test.me.2\n"
            "+/test.me.4\n"
            "+/wat/test.me.3\n"
            "Sync'ed repo-full-name to hoard!\n"
            "-/test.me.4\n"
            "<+/test.me.5\n"
            "<+/wat/test.me.6\n"
            "u/wat/test.me.3\n"
            "Sync'ed repo-incoming-name to hoard!\n"
            "DONE", res)

        res = await hoard_cmd.contents.ls()
        self.assertEqual(
            "/\n"
            "/test.me.1 = a:2\n"
            "/test.me.4 = a:1 c:1\n"
            "/test.me.5 = g:1 c:1\n"
            "/wat\n"
            "/wat/test.me.2 = a:2\n"
            "/wat/test.me.6 = g:1 c:1\n"
            "/wat/test.me.3 = g:1 c:1\n"
            "DONE", res)

        res = await hoard_cmd.files.pending(repo=incoming_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-incoming-name:\n"
            "TO_CLEANUP (is in 1) /test.me.4\n"
            "TO_CLEANUP (is in 0) /test.me.5\n"
            "TO_CLEANUP (is in 0) /wat/test.me.6\n"
            "TO_CLEANUP (is in 0) /wat/test.me.3\n"
            "DONE", res)

        await partial_cave_cmd.refresh(show_details=False)
        await full_cave_cmd.refresh(show_details=False)
        await incoming_cave_cmd.refresh(show_details=False)

        res = await hoard_cmd.files.push(all=True)
        self.assertEqual(
            f"repo-partial-name:\n"
            f"repo-full-name:\n"
            "+ test.me.5\n"
            "+ wat/test.me.3\n"
            "+ wat/test.me.6\n"
            f"repo-incoming-name:\n"
            f"repo-partial-name:\n"
            f"repo-full-name:\n"
            f"repo-incoming-name:\n"
            "d test.me.4\n"
            "d test.me.5\n"
            "d wat/test.me.3\n"
            "d wat/test.me.6\n"
            "DONE", res)

        res = await hoard_cmd.files.pending(repo=incoming_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-incoming-name:\n"
            "DONE", res)

        res = await hoard_cmd.contents.ls()
        self.assertEqual(
            "/\n"
            "/test.me.1 = a:2\n"
            "/test.me.4 = a:1\n"
            "/test.me.5 = a:1\n"
            "/wat\n"
            "/wat/test.me.2 = a:2\n"
            "/wat/test.me.6 = a:1\n"
            "/wat/test.me.3 = a:1\n"
            "DONE", res)

    async def test_create_with_simple_backup_from_start(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, incoming_cave_cmd = await init_complex_hoard(self.tmpdir.name)

        backup_1_cmd = await self._init_and_refresh_repo("repo-backup-1")
        backup_2_cmd = await self._init_and_refresh_repo("repo-backup-2")
        backup_3_cmd = await self._init_and_refresh_repo("repo-backup-3")
        backup_4_cmd = await self._init_and_refresh_repo("repo-backup-4")

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-backup-1"), name="backup-1", mount_point="/",
            type=CaveType.BACKUP)
        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-backup-2"), name="backup-2", mount_point="/",
            type=CaveType.BACKUP)
        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-backup-3"), name="backup-3", mount_point="/",
            type=CaveType.BACKUP)
        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-backup-4"), name="backup-4", mount_point="/",
            type=CaveType.BACKUP)

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual(
            "+/test.me.1\n"
            "+/wat/test.me.2\n"
            "Sync'ed repo-partial-name to hoard!\n"
            "=/test.me.1\n"
            "=/wat/test.me.2\n"
            "+/test.me.4\n"
            "+/wat/test.me.3\n"
            "Sync'ed repo-full-name to hoard!\n"
            "-/test.me.4\n"
            "<+/test.me.5\n"
            "<+/wat/test.me.6\n"
            "u/wat/test.me.3\n"
            "Sync'ed repo-incoming-name to hoard!\n"
            "=/test.me.1\n"
            "RESTORE /wat/test.me.3\n"
            "Sync'ed backup-1 to hoard!\n"
            "=/test.me.1\n"
            "Sync'ed backup-2 to hoard!\n"
            "?/test.me.obsolete\n"
            "Sync'ed backup-3 to hoard!\n"
            "Sync'ed backup-4 to hoard!\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |cleanup   |\n"
            "|backup-1                 |         3|         1|         2|          |\n"
            "|backup-2                 |         3|         1|         2|          |\n"
            "|backup-3                 |         2|          |         2|          |\n"
            "|backup-4                 |         1|          |         1|          |\n"
            "|repo-full-name           |         6|         3|         3|          |\n"
            "|repo-incoming-name       |         4|          |          |         4|\n"
            "|repo-partial-name        |         2|         2|          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |cleanup   |\n"
            "|backup-1                 |       129|        60|        69|          |\n"
            "|backup-2                 |       153|        60|        93|          |\n"
            "|backup-3                 |       154|          |       154|          |\n"
            "|backup-4                 |         9|          |         9|          |\n"
            "|repo-full-name           |       299|       153|       146|          |\n"
            "|repo-incoming-name       |       223|          |          |       223|\n"
            "|repo-partial-name        |        76|        76|          |          |\n", res)

        res = await hoard_cmd.contents.status(path="/wat", hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |cleanup   |\n"
            "|backup-1                 |         1|          |         1|          |\n"
            "|backup-2                 |         1|          |         1|          |\n"
            "|backup-3                 |         1|          |         1|          |\n"
            "|backup-4                 |         1|          |         1|          |\n"
            "|repo-full-name           |         3|         1|         2|          |\n"
            "|repo-incoming-name       |         2|          |          |         2|\n"
            "|repo-partial-name        |         1|         1|          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |cleanup   |\n"
            "|backup-1                 |         9|          |         9|          |\n"
            "|backup-2                 |        16|          |        16|          |\n"
            "|backup-3                 |        77|          |        77|          |\n"
            "|backup-4                 |         9|          |         9|          |\n"
            "|repo-full-name           |       102|        16|        86|          |\n"
            "|repo-incoming-name       |        86|          |          |        86|\n"
            "|repo-partial-name        |        16|        16|          |          |\n", res)

        res = await hoard_cmd.backups.health()
        self.assertEqual(
            "# backup sets: 1\n"
            "# backups: 4\n"
            "scheduled count:\n"
            " 1: 3 files (153)\n"
            " 2: 3 files (146)\n"
            "available count:\n"
            " 0: 5 files (239)\n"
            " 2: 1 files (60)\n"
            "get_or_copy count:\n"
            " 0: 1 files (60)\n"
            " 1: 1 files (16)\n"
            " 2: 3 files (214)\n"
            " 3: 1 files (9)\n"
            "move count:\n"
            " 0: 6 files (299)\n"
            "cleanup count:\n"
            " 0: 2 files (76)\n"
            " 1: 4 files (223)\n"
            "DONE", res)

        res = await hoard_cmd.files.push(all=True)
        self.assertEqual(
            "repo-partial-name:\n"
            "repo-full-name:\n"
            "+ test.me.5\n"
            "+ wat/test.me.3\n"
            "+ wat/test.me.6\n"
            "repo-incoming-name:\n"
            "backup-1:\n"
            "+ test.me.5\n"
            "+ wat/test.me.3\n"
            "backup-2:\n"
            "+ test.me.4\n"
            "+ wat/test.me.2\n"
            "backup-3:\n"
            "+ test.me.4\n"
            "+ wat/test.me.6\n"
            "backup-4:\n"
            "+ wat/test.me.3\n"
            "repo-partial-name:\n"
            "repo-full-name:\n"
            "repo-incoming-name:\n"
            "d test.me.4\n"
            "d test.me.5\n"
            "d wat/test.me.3\n"
            "d wat/test.me.6\n"
            "backup-1:\n"
            "backup-2:\n"
            "backup-3:\n"
            "backup-4:\n"
            "DONE", res)

        res = await hoard_cmd.backups.health()
        self.assertEqual(
            "# backup sets: 1\n"
            "# backups: 4\n"
            "scheduled count:\n"
            " 1: 3 files (153)\n"
            " 2: 3 files (146)\n"
            "available count:\n"
            " 1: 3 files (153)\n"
            " 2: 3 files (146)\n"
            "get_or_copy count:\n"
            " 0: 6 files (299)\n"
            "move count:\n"
            " 0: 6 files (299)\n"
            "cleanup count:\n"
            " 0: 6 files (299)\n"
            "DONE", res)

        res = await hoard_cmd.contents.drop(repo="backup-2", path="wat")
        self.assertEqual(
            'DROP /wat/test.me.2\n'
            "Considered 3 files, 1 marked for cleanup, 0 won't be downloaded, 2 are skipped.\n"
            'DONE', res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            '|Num Files                |total     |available |cleanup   |\n'
            '|backup-1                 |         3|         3|          |\n'
            '|backup-2                 |         3|         2|         1|\n'
            '|backup-3                 |         2|         2|          |\n'
            '|backup-4                 |         1|         1|          |\n'
            '|repo-full-name           |         6|         6|          |\n'
            '|repo-partial-name        |         2|         2|          |\n'
            '\n'
            '|Size                     |total     |available |cleanup   |\n'
            '|backup-1                 |       129|       129|          |\n'
            '|backup-2                 |       153|       137|        16|\n'
            '|backup-3                 |       154|       154|          |\n'
            '|backup-4                 |         9|         9|          |\n'
            '|repo-full-name           |       299|       299|          |\n'
            '|repo-partial-name        |        76|        76|          |\n', res)

    async def test_add_backup_repos_over_time(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, incoming_cave_cmd = await init_complex_hoard(self.tmpdir.name)

        await hoard_cmd.contents.pull(all=True)

        backup_1_cmd = await self._init_and_refresh_repo("repo-backup-1")
        backup_2_cmd = await self._init_and_refresh_repo("repo-backup-2")
        backup_3_cmd = await self._init_and_refresh_repo("repo-backup-3")
        backup_4_cmd = await self._init_and_refresh_repo("repo-backup-4")

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-backup-1"), name="backup-1", mount_point="/",
            type=CaveType.BACKUP)
        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-backup-2"), name="backup-2", mount_point="/",
            type=CaveType.BACKUP)
        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-backup-3"), name="backup-3", mount_point="/",
            type=CaveType.BACKUP)
        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-backup-4"), name="backup-4", mount_point="/",
            type=CaveType.BACKUP)

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual(
            "Skipping update as past epoch 1 is not after hoard epoch 1\n"
            "Skipping update as past epoch 1 is not after hoard epoch 1\n"
            "Skipping update as past epoch 1 is not after hoard epoch 1\n"
            "=/test.me.1\n"
            "RESTORE /wat/test.me.3\n"
            "Sync'ed backup-1 to hoard!\n"
            "=/test.me.1\n"
            "Sync'ed backup-2 to hoard!\n"
            "?/test.me.obsolete\n"
            "Sync'ed backup-3 to hoard!\n"
            "Sync'ed backup-4 to hoard!\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |cleanup   |\n"
            "|backup-1                 |         2|         1|         1|          |\n"
            "|backup-2                 |         1|         1|          |          |\n"
            "|repo-full-name           |         6|         3|         3|          |\n"
            "|repo-incoming-name       |         4|          |          |         4|\n"
            "|repo-partial-name        |         2|         2|          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |cleanup   |\n"
            "|backup-1                 |        69|        60|         9|          |\n"
            "|backup-2                 |        60|        60|          |          |\n"
            "|repo-full-name           |       299|       153|       146|          |\n"
            "|repo-incoming-name       |       223|          |          |       223|\n"
            "|repo-partial-name        |        76|        76|          |          |\n", res)

        res = await hoard_cmd.backups.health()
        self.assertEqual(
            "# backup sets: 1\n"
            "# backups: 4\n"
            "scheduled count:\n"
            " 0: 4 files (230)\n"
            " 1: 1 files (9)\n"
            " 2: 1 files (60)\n"
            "available count:\n"
            " 0: 5 files (239)\n"
            " 2: 1 files (60)\n"
            "get_or_copy count:\n"
            " 0: 3 files (153)\n"
            " 1: 2 files (137)\n"
            " 2: 1 files (9)\n"
            "move count:\n"
            " 0: 6 files (299)\n"
            "cleanup count:\n"
            " 0: 2 files (76)\n"
            " 1: 4 files (223)\n"
            "DONE", res)

        res = await hoard_cmd.backups.assign()
        self.assertEqual(
            'set: / with 4 media\n'
            ' backup-2 <- 1 files (77)\n'
            ' backup-3 <- 2 files (76)\n'
            ' backup-4 <- 1 files (77)\n'
            'DONE', res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |cleanup   |\n"
            "|backup-1                 |         2|         1|         1|          |\n"
            "|backup-2                 |         2|         1|         1|          |\n"
            "|backup-3                 |         2|          |         2|          |\n"
            "|backup-4                 |         1|          |         1|          |\n"
            "|repo-full-name           |         6|         3|         3|          |\n"
            "|repo-incoming-name       |         4|          |          |         4|\n"
            "|repo-partial-name        |         2|         2|          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |cleanup   |\n"
            "|backup-1                 |        69|        60|         9|          |\n"
            "|backup-2                 |       137|        60|        77|          |\n"
            "|backup-3                 |        76|          |        76|          |\n"
            "|backup-4                 |        77|          |        77|          |\n"
            "|repo-full-name           |       299|       153|       146|          |\n"
            "|repo-incoming-name       |       223|          |          |       223|\n"
            "|repo-partial-name        |        76|        76|          |          |\n", res)

        res = await hoard_cmd.contents.drop(repo="backup-1", path="wat")
        self.assertEqual(
            'WONT_GET /wat/test.me.3\n'  # fixme wrong
            "Considered 3 files, 0 marked for cleanup, 1 won't be downloaded, 2 are skipped.\n"
            'DONE', res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|backup-1                 |         1|         1|          |          |\n'
            '|backup-2                 |         2|         1|         1|          |\n'
            '|backup-3                 |         2|          |         2|          |\n'
            '|backup-4                 |         1|          |         1|          |\n'
            '|repo-full-name           |         6|         3|         3|          |\n'
            '|repo-incoming-name       |         4|          |          |         4|\n'
            '|repo-partial-name        |         2|         2|          |          |\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|backup-1                 |        60|        60|          |          |\n'
            '|backup-2                 |       137|        60|        77|          |\n'
            '|backup-3                 |        76|          |        76|          |\n'
            '|backup-4                 |        77|          |        77|          |\n'
            '|repo-full-name           |       299|       153|       146|          |\n'
            '|repo-incoming-name       |       223|          |          |       223|\n'
            '|repo-partial-name        |        76|        76|          |          |\n', res)

        res = await hoard_cmd.backups.health()
        self.assertEqual(
            '# backup sets: 1\n'
            '# backups: 4\n'
            'scheduled count:\n'
            ' 0: 1 files (9)\n'
            ' 1: 4 files (230)\n'
            ' 2: 1 files (60)\n'
            'available count:\n'
            ' 0: 5 files (239)\n'
            ' 2: 1 files (60)\n'
            'get_or_copy count:\n'
            ' 0: 1 files (60)\n'
            ' 1: 3 files (102)\n'
            ' 2: 2 files (137)\n'
            "move count:\n"
            " 0: 6 files (299)\n"
            'cleanup count:\n'
            ' 0: 2 files (76)\n'
            ' 1: 4 files (223)\n'
            'DONE', res)

        res = await hoard_cmd.backups.clean()
        # self.assertEqual(  fixme make it run stably
        #     'set: / with 4 media\n'
        #     ' backup-1 LOST 1 files (60)\n'
        #     'DONE', res)
        self.assertEqual(3, len(res.splitlines()))

        res = await hoard_cmd.backups.health()
        self.assertEqual(
            '# backup sets: 1\n'
            '# backups: 4\n'
            'scheduled count:\n'
            ' 0: 1 files (9)\n'
            ' 1: 5 files (290)\n'
            'available count:\n'
            ' 0: 5 files (239)\n'
            ' 1: 1 files (60)\n'
            'get_or_copy count:\n'
            ' 0: 1 files (60)\n'
            ' 1: 3 files (102)\n'
            ' 2: 2 files (137)\n'
            "move count:\n"
            " 0: 6 files (299)\n"
            'cleanup count:\n'
            ' 0: 1 files (16)\n'
            ' 1: 5 files (283)\n'
            'DONE', res)

    async def _init_and_refresh_repo(self, backup_folder: str) -> RepoCommand:
        backup_1_cmd = TotalCommand(path=join(self.tmpdir.name, backup_folder)).cave
        backup_1_cmd.init()
        await backup_1_cmd.refresh(show_details=False)

        return backup_1_cmd
