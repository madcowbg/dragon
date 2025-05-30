import os
import shutil
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
    await hoard_cmd.init()

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
        self.assertEqual([
            'Pulling repo-partial-name...',
            'Before: Hoard [None] <- repo [curr: None, stg: 418ef1, des: None]',
            'REPO_MARK_FILE_AVAILABLE /test.me.1',
            'HOARD_FILE_ADDED /test.me.1',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2',
            'HOARD_FILE_ADDED /wat/test.me.2',
            'updated repo-partial-name from None to 418ef1',
            'updated repo-full-name from None to 418ef1',
            'After: Hoard [418ef1], repo [curr: 418ef1, stg: 418ef1, des: 418ef1]',
            "Sync'ed repo-partial-name to hoard!",
            'Pulling repo-full-name...',
            'Before: Hoard [418ef1] <- repo [curr: None, stg: 94524f, des: 418ef1]',
            'REPO_MARK_FILE_AVAILABLE /test.me.1',
            'REPO_MARK_FILE_AVAILABLE /test.me.4',
            'HOARD_FILE_ADDED /test.me.4',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3',
            'HOARD_FILE_ADDED /wat/test.me.3',
            'updated repo-full-name from 418ef1 to 94524f',
            'After: Hoard [94524f], repo [curr: 94524f, stg: 94524f, des: 94524f]',
            "Sync'ed repo-full-name to hoard!",
            'Pulling repo-incoming-name...',
            'Before: Hoard [94524f] <- repo [curr: None, stg: a513af, des: None]',
            'REPO_MARK_FILE_AVAILABLE /test.me.4',
            'REPO_MARK_FILE_AVAILABLE /test.me.5',
            'HOARD_FILE_ADDED /test.me.5',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.6',
            'HOARD_FILE_ADDED /wat/test.me.6',
            'updated repo-full-name from 94524f to c8405b',
            'After: Hoard [c8405b], repo [curr: a513af, stg: a513af, des: None]',
            "Sync'ed repo-incoming-name to hoard!",
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.ls()
        self.assertEqual(
            'Root: c8405b542a1e9b691f8e2be70d1bd387e54d530d\n'
            'Remote repo-full-name current=94524f staging=94524f desired=c8405b\n'
            'Remote repo-incoming-name current=a513af staging=a513af desired=None\n'
            'Remote repo-partial-name current=418ef1 staging=418ef1 desired=418ef1\n'
            "/\n"
            "/test.me.1 = a:2\n"
            "/test.me.4 = a:1 c:1\n"
            "/test.me.5 = g:1 c:1\n"
            "/wat\n"
            "/wat/test.me.2 = a:2\n"
            "/wat/test.me.3 = a:1 c:1\n"
            "/wat/test.me.6 = g:1 c:1\n"
            "DONE", res)

        res = await hoard_cmd.files.pending(repo=incoming_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-incoming-name:\n"
            "TO_CLEANUP (is in 1) /test.me.4\n"
            "TO_CLEANUP (is in 0) /test.me.5\n"
            "TO_CLEANUP (is in 1) /wat/test.me.3\n"
            "TO_CLEANUP (is in 0) /wat/test.me.6\n"
            "DONE", res)

        await partial_cave_cmd.refresh(show_details=False)
        await full_cave_cmd.refresh(show_details=False)
        await incoming_cave_cmd.refresh(show_details=False)

        res = await hoard_cmd.files.push(all=True)
        self.assertEqual((
            'Before push:\n'
            'Remote repo-full-name current=94524f staging=94524f desired=c8405b\n'
            'Remote repo-incoming-name current=a513af staging=a513af desired=None\n'
            'Remote repo-partial-name current=418ef1 staging=418ef1 desired=418ef1\n'
            'repo-partial-name:\n'
            'repo-full-name:\n'
            '+ test.me.5\n'
            '+ wat/test.me.6\n'
            'repo-incoming-name:\n'
            'repo-partial-name:\n'
            'repo-full-name:\n'
            'repo-incoming-name:\n'
            'd test.me.4\n'
            'd test.me.5\n'
            'd wat/test.me.3\n'
            'd wat/test.me.6\n'
            'After:\n'
            'Remote repo-full-name current=c8405b staging=94524f desired=c8405b\n'
            'Remote repo-incoming-name current=None staging=a513af desired=None\n'
            'Remote repo-partial-name current=418ef1 staging=418ef1 desired=418ef1\n'
            'DONE'), res)

        res = await hoard_cmd.files.pending(repo=incoming_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-incoming-name:\n"
            "DONE", res)

        res = await hoard_cmd.contents.ls()
        self.assertEqual((
            'Root: c8405b542a1e9b691f8e2be70d1bd387e54d530d\n'
            'Remote repo-full-name current=c8405b staging=94524f desired=c8405b\n'
            'Remote repo-incoming-name current=None staging=a513af desired=None\n'
            'Remote repo-partial-name current=418ef1 staging=418ef1 desired=418ef1\n'
            '/\n'
            '/test.me.1 = a:2\n'
            '/test.me.4 = a:1\n'
            '/test.me.5 = a:1\n'
            '/wat\n'
            '/wat/test.me.2 = a:2\n'
            '/wat/test.me.3 = a:1\n'
            '/wat/test.me.6 = a:1\n'
            'DONE'), res)

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
        self.assertEqual([
            'Pulling repo-partial-name...',
            'Before: Hoard [None] <- repo [curr: None, stg: 418ef1, des: None]',
            'REPO_MARK_FILE_AVAILABLE /test.me.1',
            'HOARD_FILE_ADDED /test.me.1',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2',
            'HOARD_FILE_ADDED /wat/test.me.2',
            'updated repo-partial-name from None to 418ef1',
            'updated repo-full-name from None to 418ef1',
            'updated backup-1 from None to 6ef88c',
            'updated backup-2 from None to cf736d',
            'After: Hoard [418ef1], repo [curr: 418ef1, stg: 418ef1, des: 418ef1]',
            "Sync'ed repo-partial-name to hoard!",
            'Pulling repo-full-name...',
            'Before: Hoard [418ef1] <- repo [curr: None, stg: 94524f, des: 418ef1]',
            'REPO_MARK_FILE_AVAILABLE /test.me.1',
            'REPO_MARK_FILE_AVAILABLE /test.me.4',
            'HOARD_FILE_ADDED /test.me.4',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3',
            'HOARD_FILE_ADDED /wat/test.me.3',
            'updated repo-full-name from 418ef1 to 94524f',
            'updated backup-3 from None to cf33f9',
            'updated backup-4 from None to b7d919',
            'After: Hoard [94524f], repo [curr: 94524f, stg: 94524f, des: 94524f]',
            "Sync'ed repo-full-name to hoard!",
            'Pulling repo-incoming-name...',
            'Before: Hoard [94524f] <- repo [curr: None, stg: a513af, des: None]',
            'REPO_MARK_FILE_AVAILABLE /test.me.4',
            'REPO_MARK_FILE_AVAILABLE /test.me.5',
            'HOARD_FILE_ADDED /test.me.5',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.6',
            'HOARD_FILE_ADDED /wat/test.me.6',
            'updated repo-full-name from 94524f to c8405b',
            'updated backup-1 from 6ef88c to 9b5eb2',
            'updated backup-2 from cf736d to 261ad1',
            'After: Hoard [c8405b], repo [curr: a513af, stg: a513af, des: None]',
            "Sync'ed repo-incoming-name to hoard!",
            'Pulling backup-1...',
            'Before: Hoard [c8405b] <- repo [curr: None, stg: 1388f4, des: 9b5eb2]',
            'REPO_MARK_FILE_AVAILABLE /test.me.1',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.6',
            'updated backup-1 from 9b5eb2 to 4bff8f',
            'After: Hoard [c8405b], repo [curr: 1388f4, stg: 1388f4, des: 4bff8f]',
            "Sync'ed backup-1 to hoard!",
            'Pulling backup-2...',
            'Before: Hoard [c8405b] <- repo [curr: None, stg: 6ef88c, des: 261ad1]',
            'REPO_MARK_FILE_AVAILABLE /test.me.1',
            'REPO_DESIRED_FILE_TO_GET /test.me.5',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.2',
            'updated backup-2 from 261ad1 to ea34fb',
            'After: Hoard [c8405b], repo [curr: 6ef88c, stg: 6ef88c, des: ea34fb]',
            "Sync'ed backup-2 to hoard!",
            'Pulling backup-3...',
            'Before: Hoard [c8405b] <- repo [curr: None, stg: fe2e3c, des: cf33f9]',
            'REPO_DESIRED_FILE_TO_GET /test.me.4',
            'After: Hoard [c8405b], repo [curr: fe2e3c, stg: fe2e3c, des: cf33f9]',
            "Sync'ed backup-3 to hoard!",
            'Pulling backup-4...',
            'Before: Hoard [c8405b] <- repo [curr: None, stg: a80f91, des: b7d919]',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.3',
            'After: Hoard [c8405b], repo [curr: a80f91, stg: a80f91, des: b7d919]',
            "Sync'ed backup-4 to hoard!",
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual([
            'Root: c8405b542a1e9b691f8e2be70d1bd387e54d530d',
            '|Num Files                |total     |available |get       |cleanup   |',
            '|backup-1                 |         3|         2|         1|          |',
            '|backup-2                 |         3|         1|         2|          |',
            '|backup-3                 |         2|          |         1|         1|',
            '|backup-4                 |         1|          |         1|          |',
            '|repo-full-name           |         6|         4|         2|          |',
            '|repo-incoming-name       |         4|          |          |         4|',
            '|repo-partial-name        |         2|         2|          |          |',
            '',
            '|Size                     |total     |available |get       |cleanup   |',
            '|backup-1                 |       227|       150|        77|          |',
            '|backup-2                 |       136|        60|        76|          |',
            '|backup-3                 |       194|          |        77|       117|',
            '|backup-4                 |        90|          |        90|          |',
            '|repo-full-name           |       380|       243|       137|          |',
            '|repo-incoming-name       |       304|          |          |       304|',
            '|repo-partial-name        |        76|        76|          |          |'], res.splitlines())

        res = await hoard_cmd.contents.status(path="/wat", hide_time=True, hide_disk_sizes=True)
        self.assertEqual([
            'Root: c8405b542a1e9b691f8e2be70d1bd387e54d530d',
            '|Num Files                |total     |available |get       |cleanup   |',
            '|backup-1                 |         2|         1|         1|          |',
            '|backup-2                 |         1|          |         1|          |',
            '|backup-4                 |         1|          |         1|          |',
            '|repo-full-name           |         3|         2|         1|          |',
            '|repo-incoming-name       |         2|          |          |         2|',
            '|repo-partial-name        |         1|         1|          |          |',
            '',
            '|Size                     |total     |available |get       |cleanup   |',
            '|backup-1                 |       167|        90|        77|          |',
            '|backup-2                 |        16|          |        16|          |',
            '|backup-4                 |        90|          |        90|          |',
            '|repo-full-name           |       183|       106|        77|          |',
            '|repo-incoming-name       |       167|          |          |       167|',
            '|repo-partial-name        |        16|        16|          |          |'], res.splitlines())

        res = await hoard_cmd.backups.health()
        self.assertEqual([
            '# backup sets: 1',
            '# backups: 4',
            'scheduled count:',
            ' 0: 1 files (117)',
            ' 1: 4 files (230)',
            ' 2: 2 files (150)',
            'available count:',
            ' 0: 5 files (347)',
            ' 1: 1 files (90)',
            ' 2: 1 files (60)',
            'get_or_copy count:',
            ' 0: 2 files (177)',
            ' 1: 3 files (183)',
            ' 2: 2 files (137)',
            'move count:',
            ' 0: 7 files (497)',
            'cleanup count:',
            ' 0: 2 files (76)',
            ' 1: 5 files (421)',
            'DONE'], res.splitlines())

        res = await hoard_cmd.files.push(all=True)
        self.assertEqual([
            'Before push:',
            'Remote backup-1 current=1388f4 staging=1388f4 desired=4bff8f',
            'Remote backup-2 current=6ef88c staging=6ef88c desired=ea34fb',
            'Remote backup-3 current=fe2e3c staging=fe2e3c desired=cf33f9',
            'Remote backup-4 current=a80f91 staging=a80f91 desired=b7d919',
            'Remote repo-full-name current=94524f staging=94524f desired=c8405b',
            'Remote repo-incoming-name current=a513af staging=a513af desired=None',
            'Remote repo-partial-name current=418ef1 staging=418ef1 desired=418ef1',
            'repo-partial-name:',
            'repo-full-name:',
            '+ test.me.5',
            '+ wat/test.me.6',
            'repo-incoming-name:',
            'backup-1:',
            '+ wat/test.me.6',
            'backup-2:',
            '+ test.me.5',
            '+ wat/test.me.2',
            'backup-3:',
            '+ test.me.4',
            'backup-4:',
            '+ wat/test.me.3',
            'repo-partial-name:',
            'repo-full-name:',
            'repo-incoming-name:',
            'd test.me.4',
            'd test.me.5',
            'd wat/test.me.3',
            'd wat/test.me.6',
            'backup-1:',
            'backup-2:',
            'backup-3:',
            'd test.me.obsolete',
            'backup-4:',
            'After:',
            'Remote backup-1 current=4bff8f staging=1388f4 desired=4bff8f',
            'Remote backup-2 current=ea34fb staging=6ef88c desired=ea34fb',
            'Remote backup-3 current=cf33f9 staging=fe2e3c desired=cf33f9',
            'Remote backup-4 current=b7d919 staging=a80f91 desired=b7d919',
            'Remote repo-full-name current=c8405b staging=94524f desired=c8405b',
            'Remote repo-incoming-name current=None staging=a513af desired=None',
            'Remote repo-partial-name current=418ef1 staging=418ef1 desired=418ef1',
            'DONE'], res.splitlines())

        res = await hoard_cmd.backups.health()
        self.assertEqual([
            '# backup sets: 1',
            '# backups: 4',
            'scheduled count:',
            ' 1: 4 files (230)',
            ' 2: 2 files (150)',
            'available count:',
            ' 1: 4 files (230)',
            ' 2: 2 files (150)',
            'get_or_copy count:',
            ' 0: 6 files (380)',
            'move count:',
            ' 0: 6 files (380)',
            'cleanup count:',
            ' 0: 6 files (380)',
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.drop(repo="backup-2", path="wat")
        self.assertEqual([
            'DROP /wat/test.me.2',
            '1 marked for cleanup.',
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual([
            'Root: c8405b542a1e9b691f8e2be70d1bd387e54d530d',
            '|Num Files                |total     |available |cleanup   |',
            '|backup-1                 |         3|         3|          |',
            '|backup-2                 |         3|         2|         1|',
            '|backup-3                 |         1|         1|          |',
            '|backup-4                 |         1|         1|          |',
            '|repo-full-name           |         6|         6|          |',
            '|repo-partial-name        |         2|         2|          |',
            '',
            '|Size                     |total     |available |cleanup   |',
            '|backup-1                 |       227|       227|          |',
            '|backup-2                 |       136|       120|        16|',
            '|backup-3                 |        77|        77|          |',
            '|backup-4                 |        90|        90|          |',
            '|repo-full-name           |       380|       380|          |',
            '|repo-partial-name        |        76|        76|          |'], res.splitlines())

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
        self.assertEqual((
            'Skipping update as repo-partial-name.staging has not changed: 418ef1\n'
            'Skipping update as repo-full-name.staging has not changed: 94524f\n'
            'Skipping update as repo-incoming-name.staging has not changed: a513af\n'
            'Pulling backup-1...\n'
            'Before: Hoard [c8405b] <- repo [curr: None, stg: 1388f4, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3\n'
            'updated backup-1 from None to 1388f4\n'
            'After: Hoard [c8405b], repo [curr: 1388f4, stg: 1388f4, des: 1388f4]\n'
            "Sync'ed backup-1 to hoard!\n"
            'Pulling backup-2...\n'
            'Before: Hoard [c8405b] <- repo [curr: None, stg: 6ef88c, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'updated backup-2 from None to 6ef88c\n'
            'After: Hoard [c8405b], repo [curr: 6ef88c, stg: 6ef88c, des: 6ef88c]\n'
            "Sync'ed backup-2 to hoard!\n"
            'Pulling backup-3...\n'
            'Before: Hoard [c8405b] <- repo [curr: None, stg: fe2e3c, des: None]\n'
            'After: Hoard [c8405b], repo [curr: fe2e3c, stg: fe2e3c, des: None]\n'
            "Sync'ed backup-3 to hoard!\n"
            'Pulling backup-4...\n'
            'Before: Hoard [c8405b] <- repo [curr: None, stg: a80f91, des: None]\n'
            'After: Hoard [c8405b], repo [curr: a80f91, stg: a80f91, des: None]\n'
            "Sync'ed backup-4 to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual([
            'Root: c8405b542a1e9b691f8e2be70d1bd387e54d530d',
            '|Num Files                |total     |available |get       |cleanup   |',
            '|backup-1                 |         2|         2|          |          |',
            '|backup-2                 |         1|         1|          |          |',
            '|backup-3                 |         1|          |          |         1|',
            '|repo-full-name           |         6|         4|         2|          |',
            '|repo-incoming-name       |         4|          |          |         4|',
            '|repo-partial-name        |         2|         2|          |          |',
            '',
            '|Size                     |total     |available |get       |cleanup   |',
            '|backup-1                 |       150|       150|          |          |',
            '|backup-2                 |        60|        60|          |          |',
            '|backup-3                 |       117|          |          |       117|',
            '|repo-full-name           |       380|       243|       137|          |',
            '|repo-incoming-name       |       304|          |          |       304|',
            '|repo-partial-name        |        76|        76|          |          |'], res.splitlines())

        res = await hoard_cmd.backups.health()
        self.assertEqual([
            '# backup sets: 1',
            '# backups: 4',
            'scheduled count:',
            ' 0: 5 files (347)',
            ' 1: 1 files (90)',
            ' 2: 1 files (60)',
            'available count:',
            ' 0: 5 files (347)',
            ' 1: 1 files (90)',
            ' 2: 1 files (60)',
            'get_or_copy count:',
            ' 0: 5 files (360)',
            ' 1: 2 files (137)',
            'move count:',
            ' 0: 7 files (497)',
            'cleanup count:',
            ' 0: 2 files (76)',
            ' 1: 5 files (421)',
            'DONE'], res.splitlines())

        res = await hoard_cmd.backups.assign(available_only=False)
        self.assertEqual((
            'set: / with 4/4 media\n'
            ' backup-2 <- 1 files (60)\n'
            ' backup-4 <- 3 files (170)\n'
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual([
            'Root: c8405b542a1e9b691f8e2be70d1bd387e54d530d',
            '|Num Files                |total     |available |get       |cleanup   |',
            '|backup-1                 |         2|         2|          |          |',
            '|backup-2                 |         2|         1|         1|          |',
            '|backup-3                 |         1|          |          |         1|',
            '|backup-4                 |         3|          |         3|          |',
            '|repo-full-name           |         6|         4|         2|          |',
            '|repo-incoming-name       |         4|          |          |         4|',
            '|repo-partial-name        |         2|         2|          |          |',
            '',
            '|Size                     |total     |available |get       |cleanup   |',
            '|backup-1                 |       150|       150|          |          |',
            '|backup-2                 |       120|        60|        60|          |',
            '|backup-3                 |       117|          |          |       117|',
            '|backup-4                 |       170|          |       170|          |',
            '|repo-full-name           |       380|       243|       137|          |',
            '|repo-incoming-name       |       304|          |          |       304|',
            '|repo-partial-name        |        76|        76|          |          |'], res.splitlines())

        res = await hoard_cmd.contents.drop(repo="backup-1", path="wat")
        self.assertEqual(
            'DROP /wat/test.me.3\n'
            '1 marked for cleanup.\n'
            'DONE', res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual([
            'Root: c8405b542a1e9b691f8e2be70d1bd387e54d530d',
            '|Num Files                |total     |available |get       |cleanup   |',
            '|backup-1                 |         2|         1|          |         1|',
            '|backup-2                 |         2|         1|         1|          |',
            '|backup-3                 |         1|          |          |         1|',
            '|backup-4                 |         3|          |         3|          |',
            '|repo-full-name           |         6|         4|         2|          |',
            '|repo-incoming-name       |         4|          |          |         4|',
            '|repo-partial-name        |         2|         2|          |          |',
            '',
            '|Size                     |total     |available |get       |cleanup   |',
            '|backup-1                 |       150|        60|          |        90|',
            '|backup-2                 |       120|        60|        60|          |',
            '|backup-3                 |       117|          |          |       117|',
            '|backup-4                 |       170|          |       170|          |',
            '|repo-full-name           |       380|       243|       137|          |',
            '|repo-incoming-name       |       304|          |          |       304|',
            '|repo-partial-name        |        76|        76|          |          |'], res.splitlines())

        res = await hoard_cmd.backups.health()
        self.assertEqual([
            '# backup sets: 1',
            '# backups: 4',
            'scheduled count:',
            ' 0: 2 files (207)',
            ' 1: 4 files (230)',
            ' 2: 1 files (60)',
            'available count:',
            ' 0: 6 files (437)',
            ' 2: 1 files (60)',
            'get_or_copy count:',
            ' 0: 3 files (267)',
            ' 1: 2 files (93)',
            ' 2: 2 files (137)',
            'move count:',
            ' 0: 7 files (497)',
            'cleanup count:',
            ' 0: 2 files (76)',
            ' 1: 4 files (331)',
            ' 2: 1 files (90)',
            'DONE'], res.splitlines())

        res = await hoard_cmd.backups.clean()
        # self.assertEqual(  fixme make it run stably
        #     'set: / with 4 media\n'
        #     ' backup-1 LOST 1 files (60)\n'
        #     'DONE', res)
        self.assertEqual(3, len(res.splitlines()))

        res = await hoard_cmd.backups.health()
        self.assertEqual([
            '# backup sets: 1',
            '# backups: 4',
            'scheduled count:',
            ' 0: 2 files (207)',
            ' 1: 5 files (290)',
            'available count:',
            ' 0: 6 files (437)',
            ' 1: 1 files (60)',
            'get_or_copy count:',
            ' 0: 3 files (267)',
            ' 1: 2 files (93)',
            ' 2: 2 files (137)',
            'move count:',
            ' 0: 7 files (497)',
            'cleanup count:',
            ' 0: 1 files (16)',
            ' 1: 5 files (391)',
            ' 2: 1 files (90)',
            'DONE'], res.splitlines())

    async def _init_and_refresh_repo(self, backup_folder: str) -> RepoCommand:
        backup_1_cmd = TotalCommand(path=join(self.tmpdir.name, backup_folder)).cave
        backup_1_cmd.init()
        await backup_1_cmd.refresh(show_details=False)

        return backup_1_cmd

    async def test_reassign_backup_when_repo_is_unavailable(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, incoming_cave_cmd = await init_complex_hoard(self.tmpdir.name)

        await hoard_cmd.contents.pull(all=True)

        backup_1_cmd = await self._init_and_refresh_repo("repo-backup-1")
        backup_2_cmd = await self._init_and_refresh_repo("repo-backup-2")

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-backup-1"), name="backup-1", mount_point="/",
            type=CaveType.BACKUP)
        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-backup-2"), name="backup-2", mount_point="/",
            type=CaveType.BACKUP)

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual([
            'Skipping update as repo-partial-name.staging has not changed: 418ef1',
            'Skipping update as repo-full-name.staging has not changed: 94524f',
            'Skipping update as repo-incoming-name.staging has not changed: a513af',
            'Pulling backup-1...',
            'Before: Hoard [c8405b] <- repo [curr: None, stg: 1388f4, des: None]',
            'REPO_MARK_FILE_AVAILABLE /test.me.1',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3',
            'updated backup-1 from None to 1388f4',
            'After: Hoard [c8405b], repo [curr: 1388f4, stg: 1388f4, des: 1388f4]',
            "Sync'ed backup-1 to hoard!",
            'Pulling backup-2...',
            'Before: Hoard [c8405b] <- repo [curr: None, stg: 6ef88c, des: None]',
            'REPO_MARK_FILE_AVAILABLE /test.me.1',
            'updated backup-2 from None to 6ef88c',
            'After: Hoard [c8405b], repo [curr: 6ef88c, stg: 6ef88c, des: 6ef88c]',
            "Sync'ed backup-2 to hoard!",
            'DONE'], res.splitlines())

        res = await hoard_cmd.backups.assign(available_only=False)
        self.assertEqual([
            'set: / with 2/2 media',
            ' backup-1 <- 2 files (93)',
            ' backup-2 <- 2 files (137)',
            'DONE'], res.splitlines())

        res = await hoard_cmd.backups.health()
        self.assertEqual([
            '# backup sets: 1',
            '# backups: 2',
            'scheduled count:',
            ' 1: 5 files (320)',
            ' 2: 1 files (60)',
            'available count:',
            ' 0: 4 files (230)',
            ' 1: 1 files (90)',
            ' 2: 1 files (60)',
            'get_or_copy count:',
            ' 0: 2 files (150)',
            ' 1: 2 files (93)',
            ' 2: 2 files (137)',
            'move count:',
            ' 0: 6 files (380)',
            'cleanup count:',
            ' 0: 2 files (76)',
            ' 1: 4 files (304)',
            'DONE'], res.splitlines())

        shutil.move(join(self.tmpdir.name, "repo-backup-2"), join(self.tmpdir.name, "repo-backup-2-removed"))

        res = await hoard_cmd.backups.unassign(all_unavailable=True)
        self.assertEqual([
            'Remote backup-1 is available, will not unassign',
            'Unassigning from backup-2 [e4eef2]:',
            'WONT_GET /test.me.4',
            'WONT_GET /test.me.5',
            'Desired root for backup-2 is 6ef88c <- e4eef2'], res.splitlines())

        res = await hoard_cmd.backups.assign(available_only=True)
        self.assertEqual([
            'set: / with 1/2 media',
            ' backup-1 <- 2 files (137)',
            'DONE'], res.splitlines())

    async def test_reassign_backup_when_unassigning_custom_repo(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, incoming_cave_cmd = await init_complex_hoard(self.tmpdir.name)

        await hoard_cmd.contents.pull(all=True)

        backup_1_cmd = await self._init_and_refresh_repo("repo-backup-1")
        backup_2_cmd = await self._init_and_refresh_repo("repo-backup-2")

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-backup-1"), name="backup-1", mount_point="/",
            type=CaveType.BACKUP)
        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-backup-2"), name="backup-2", mount_point="/",
            type=CaveType.BACKUP)

        await hoard_cmd.contents.pull(all=True)
        res = await hoard_cmd.backups.assign(available_only=False)
        self.assertEqual([
            'set: / with 2/2 media',
            ' backup-1 <- 2 files (93)',
            ' backup-2 <- 2 files (137)',
            'DONE'], res.splitlines())

        res = await hoard_cmd.backups.unassign(repo="backup-1")
        self.assertEqual([
            'Unassigning from backup-1 [3bd084]:',
            'WONT_GET /wat/test.me.2',
            'WONT_GET /wat/test.me.6',
            'Desired root for backup-1 is 1388f4 <- 3bd084',
            'Skipping backup-2!'], res.splitlines())

        res = await hoard_cmd.backups.assign(available_only=False)
        self.assertEqual([
            'set: / with 2/2 media',
            ' backup-1 <- 2 files (93)',
            'DONE'], res.splitlines())
