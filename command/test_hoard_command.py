import os
import pathlib
import tempfile
from os.path import join
from typing import Tuple, List, Dict
from unittest import IsolatedAsyncioTestCase

from command.command_repo import RepoCommand
from command.test_repo_command import populate, write_contents, pretty_file_writer
from config import CaveType
from contents.hoard import HoardContents
from contents.hoard_props import HoardFileProps
from dragon import TotalCommand
from resolve_uuid import resolve_remote_uuid


def populate_hoard(tmpdir: str):
    populate(tmpdir)

    os.mkdir(join(tmpdir, "hoard"))


class TestHoardCommand(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        populate_hoard(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    async def test_create_hoard(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        await cave_cmd.refresh(show_details=False)

        repo_uuid = cave_cmd.current_uuid()

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()
        res = hoard_cmd.remotes(hide_paths=True)

        self.assertEqual("0 total remotes.\nMounts:\nDONE", res.strip())

        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local", mount_point="/")
        res = hoard_cmd.remotes(hide_paths=True)
        self.assertEqual(
            f"1 total remotes.\n"
            f"  [repo-in-local] {repo_uuid} (partial)\n"
            f"Mounts:\n"
            f"  / -> repo-in-local\n"
            f"DONE", res.strip())

    async def test_sync_to_hoard(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        await cave_cmd.refresh(show_details=False)

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()
        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local", mount_point="/")

        repo_uuid = cave_cmd.current_uuid()

        res = await hoard_cmd.contents.differences("repo-in-local")
        self.assertEqual(
            'Root: None\n'
            f"Status of repo-in-local:\n"
            f"PRESENT /wat/test.me.different\n"
            f"PRESENT /wat/test.me.once\n"
            f"PRESENT /wat/test.me.twice\n"
            f"DONE",
            res.strip())

        res = await hoard_cmd.contents.pending_pull("repo-in-local")
        self.assertEqual([
            'Status of repo-in-local:',
            'Hoard root: a80f91bc48850a1fb3459bb76b9f6308d4d35710:',
            'Repo root: b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5:',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.different',
            'HOARD_FILE_ADDED /wat/test.me.different',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.once',
            'HOARD_FILE_ADDED /wat/test.me.once',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.twice',
            'HOARD_FILE_ADDED /wat/test.me.twice'], res.splitlines())

        res = await hoard_cmd.contents.pull("repo-in-local")
        self.assertEqual((
            'Pulling repo-in-local...\n'
            'Before: Hoard [a80f91] <- repo [curr: a80f91, stg: b09bd5, des: a80f91]\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.different\n'
            'HOARD_FILE_ADDED /wat/test.me.different\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.once\n'
            'HOARD_FILE_ADDED /wat/test.me.once\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.twice\n'
            'HOARD_FILE_ADDED /wat/test.me.twice\n'
            'updated repo-in-local from a80f91 to b09bd5\n'
            'After: Hoard [b09bd5], repo [curr: b09bd5, stg: b09bd5, des: b09bd5]\n'
            "Sync'ed repo-in-local to hoard!\n"
            'DONE'), res.strip())

        async with hoard_cmd.hoard.open_contents(False) as hoard_contents:
            self._assert_hoard_contents(
                hoard_contents,
                files_exp=[
                    ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),
                    ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                    ('/wat/test.me.twice', 6, 1, '1881f6f9784fb08bf6690e9763b76ac3')])

        res = await hoard_cmd.contents.differences("repo-in-local")
        self.assertEqual('Root: b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5\nStatus of repo-in-local:\nDONE', res.strip())

        res = await hoard_cmd.contents.pending_pull("repo-in-local")
        self.assertEqual([
            'Status of repo-in-local:',
            'Hoard root: b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5:',
            'Repo root: b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5:'], res.splitlines())

    def _assert_hoard_contents(
            self, hoard_contents: HoardContents, files_exp: List[Tuple[str, int, int, str]]):
        files = sorted(
            (f.as_posix(), prop.size, len(prop.available_at), prop.fasthash)
            for f, prop in hoard_contents.fsobjects if isinstance(prop, HoardFileProps))
        self.assertEqual(sorted(files_exp), sorted(files))

    async def test_sync_two_repos(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        await cave_cmd.refresh(show_details=False)

        repo_uuid = cave_cmd.current_uuid()

        cave_cmd2 = TotalCommand(path=join(self.tmpdir.name, "repo-2")).cave
        cave_cmd2.init()
        await cave_cmd2.refresh(show_details=False)
        repo_uuid2 = cave_cmd2.current_uuid()

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()
        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local", mount_point="/")
        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-2"), name="repo-in-local-2", type=CaveType.BACKUP,
            mount_point="/wat")

        await hoard_cmd.contents.pull("repo-in-local")

        async with hoard_cmd.hoard.open_contents(False) as hc:
            self._assert_hoard_contents(
                hc,
                files_exp=[
                    ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),
                    ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                    ('/wat/test.me.twice', 6, 1, '1881f6f9784fb08bf6690e9763b76ac3')])

        res = await hoard_cmd.contents.pull("repo-in-local")
        self.assertEqual("Skipping update as past epoch 1 is not after hoard epoch 1\nDONE", res)

        res = await hoard_cmd.contents.pull("repo-in-local-2")
        self.assertEqual((
            'Pulling repo-in-local-2...\n'
            'Before: Hoard [b09bd5] <- repo [curr: a80f91, stg: 9bf6ad, des: b09bd5]\n'
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.different\n'
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.once\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.twice\n'
            'After: Hoard [b09bd5], repo [curr: 9bf6ad, stg: 9bf6ad, des: b09bd5]\n'
            "Sync'ed repo-in-local-2 to hoard!\n"
            'DONE'),
            res.strip())

        async with hoard_cmd.hoard.open_contents(False) as hc:
            self._assert_hoard_contents(
                hc,
                files_exp=[
                    ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),
                    ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                    ('/wat/test.me.twice', 6, 2, '1881f6f9784fb08bf6690e9763b76ac3')])

        res = await hoard_cmd.contents.pull("repo-in-local", ignore_epoch=True)
        self.assertEqual((
            'Pulling repo-in-local...\n'
            'Before: Hoard [b09bd5] <- repo [curr: b09bd5, stg: b09bd5, des: b09bd5]\n'
            'After: Hoard [b09bd5], repo [curr: b09bd5, stg: b09bd5, des: b09bd5]\n'
            "Sync'ed repo-in-local to hoard!\n"
            'DONE'), res)

        async with hoard_cmd.hoard.open_contents(False) as hc:
            self._assert_hoard_contents(
                hc,
                files_exp=[
                    ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),  # retained only from repo
                    ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                    ('/wat/test.me.twice', 6, 2, '1881f6f9784fb08bf6690e9763b76ac3')])

        res = await hoard_cmd.contents.differences("repo-in-local-2")
        self.assertEqual(
            'Root: b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5\n'
            f"Status of repo-in-local-2:\n"
            f"MODIFIED /wat/test.me.different\n"
            f"MISSING /wat/test.me.once\n"
            f"DONE", res.strip())

        res = await hoard_cmd.contents.pending_pull("repo-in-local-2")
        self.assertEqual([
            'Status of repo-in-local-2:',
            'Hoard root: b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5:',
            'Repo root: c17cdfe722957d756b35f362cfd1113fc90097e0:',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.different',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.once'], res.splitlines())

        res = await hoard_cmd.contents.differences("repo-in-local")
        self.assertEqual('Root: b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5\nStatus of repo-in-local:\nDONE', res.strip())

        res = hoard_cmd.remotes(hide_paths=True)
        self.assertEqual(
            f"2 total remotes.\n"
            f"  [repo-in-local] {repo_uuid} (partial)\n"
            f"  [repo-in-local-2] {repo_uuid2} (backup)\n"
            f"Mounts:\n"
            f"  / -> repo-in-local\n"
            f"  /wat -> repo-in-local-2\n"
            f"DONE",
            res.strip())

        res = await hoard_cmd.health()
        self.assertEqual(
            "Health stats:\n2 total remotes.\n"
            f"  [repo-in-local] {repo_uuid}: 2 with no other copy\n"
            f"  [repo-in-local-2] {repo_uuid2}: 0 with no other copy\n"
            "Hoard health stats:\n"
            "  1 copies: 2 files\n"
            "  2 copies: 1 files\nDONE", res)

    async def test_changing_data(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        await cave_cmd.refresh(show_details=False)

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()
        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local", mount_point="/")

        repo_uuid = cave_cmd.current_uuid()
        await hoard_cmd.contents.pull("repo-in-local")

        self.assertEqual(
            f"Root: b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5\nStatus of repo-in-local:\nDONE",
            (await hoard_cmd.contents.differences("repo-in-local")).strip())

        os.mkdir(join(self.tmpdir.name, "repo", "newdir"))
        write_contents(join(self.tmpdir.name, "repo", "newdir", "newfile.is"), "lhiWFELHFE")
        os.remove(join(self.tmpdir.name, "repo", "wat", 'test.me.different'))

        res = await cave_cmd.status()
        self.assertEqual(
            f"{cave_cmd.current_uuid()} [b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5]:\n"
            "files:\n"
            "    same: 2 (66.7%)\n"
            "     mod: 0 (0.0%)\n"
            "     new: 1 (33.3%)\n"
            "   moved: 0 (0.0%)\n"
            " current: 3\n"
            " in repo: 3\n"
            " deleted: 1 (33.3%)\n", res)

        # touch file without changing contents, no difference
        pathlib.Path(join(self.tmpdir.name, "repo/wat/test.me.once")).touch()
        res = await cave_cmd.status()
        self.assertEqual(
            f"{cave_cmd.current_uuid()} [b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5]:\n"
            "files:\n"
            "    same: 2 (66.7%)\n"
            "     mod: 0 (0.0%)\n"
            "     new: 1 (33.3%)\n"
            "   moved: 0 (0.0%)\n"
            " current: 3\n"
            " in repo: 3\n"
            " deleted: 1 (33.3%)\n", res)

        # as is not refreshed, no change in status
        self.assertEqual(
            'Root: b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5\nStatus of repo-in-local:\nDONE',
            (await hoard_cmd.contents.differences("repo-in-local")).strip())

        await cave_cmd.refresh(show_details=False)
        self.assertEqual((
            'Root: b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5\n'
            'Status of repo-in-local:\n'
            'PRESENT /newdir/newfile.is\n'
            'DELETED /wat/test.me.different\n'
            'DONE'),
            (await hoard_cmd.contents.differences("repo-in-local")).strip())

        res = await hoard_cmd.contents.pull("repo-in-local")
        self.assertEqual((
            'Pulling repo-in-local...\n'
            'Before: Hoard [b09bd5] <- repo [curr: b09bd5, stg: 7393db, des: b09bd5]\n'
            'REPO_MARK_FILE_AVAILABLE /newdir/newfile.is\n'
            'HOARD_FILE_ADDED /newdir/newfile.is\n'
            'REPO_FILE_TO_DELETE /wat/test.me.different\n'
            'HOARD_FILE_DELETED /wat/test.me.different\n'
            'updated repo-in-local from b09bd5 to 7393db\n'
            'After: Hoard [7393db], repo [curr: 7393db, stg: 7393db, des: 7393db]\n'
            "Sync'ed repo-in-local to hoard!\n"
            'DONE'), res)

        self.assertEqual(
            'Root: 7393dbdffb99e9a77571725899dfacb75b63b460\nStatus of repo-in-local:\nDONE',
            (await hoard_cmd.contents.differences("repo-in-local")).strip())

    async def test_clone(self):
        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()

        new_repo_path = join(self.tmpdir.name, "cloned-repo")
        os.mkdir(new_repo_path)

        res = await hoard_cmd.clone(to_path=new_repo_path, mount_at="/wat", name="cloned-repo")
        self.assertEqual("DONE", res)

        new_uuid = resolve_remote_uuid(hoard_cmd.hoard.config(), "cloned-repo")

        res = await hoard_cmd.health()
        self.assertEqual(
            "Health stats:\n"
            "1 total remotes.\n"
            f"  [cloned-repo] {new_uuid}: 0 with no other copy\n"
            "Hoard health stats:\n"
            "DONE", res)

        res = await hoard_cmd.contents.differences(new_uuid)
        self.assertEqual(f"Root: None\nStatus of cloned-repo:\nDONE", res)

    async def test_populate_one_repo_from_other_repo(self):
        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()

        new_repo_path = join(self.tmpdir.name, "cloned-repo")
        os.mkdir(new_repo_path)

        await hoard_cmd.clone(to_path=new_repo_path, mount_at="/wat", name="cloned-repo", fetch_new=True)

        cloned_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "cloned-repo")).cave

        orig_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        orig_cave_cmd.init()
        await orig_cave_cmd.refresh(show_details=False)

        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local", mount_point="/")

        # status should be still empty hoard
        new_uuid = resolve_remote_uuid(hoard_cmd.hoard.config(), "cloned-repo")
        res = await hoard_cmd.contents.differences(new_uuid)
        self.assertEqual('Root: None\nStatus of cloned-repo:\nDONE', res)

        await hoard_cmd.contents.pull("repo-in-local")

        # after population by other repo, it is now lacking files
        res = await hoard_cmd.contents.differences(new_uuid)
        self.assertEqual(
            'Root: b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5\n'
            f"Status of cloned-repo:\n"
            "MISSING /wat/test.me.different\n"
            "MISSING /wat/test.me.once\n"
            "MISSING /wat/test.me.twice\n"
            f"DONE", res)

        res = await hoard_cmd.contents.pending_pull(new_uuid)
        self.assertEqual([
            'Status of cloned-repo:',
            'Hoard root: b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5:',
            'Repo root: a80f91bc48850a1fb3459bb76b9f6308d4d35710:',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.different',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.once',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.twice'], res.splitlines())

        res = await hoard_cmd.files.push(repo="cloned-repo")
        self.assertEqual(
            f"cloned-repo:\n"
            "+ test.me.different\n"
            "+ test.me.once\n"
            "+ test.me.twice\n"
            f"cloned-repo:\n"
            "DONE", res.strip())

        res = await cloned_cave_cmd.refresh(show_details=False)
        self.assertEqual((
            'old: a80f91bc48850a1fb3459bb76b9f6308d4d35710\n'
            'current: 98265a8981105d55ce844f15a660760c0634d07c\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.differences(new_uuid)
        self.assertEqual('Root: b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5\nStatus of cloned-repo:\nDONE', res.strip())

        res = await hoard_cmd.contents.pending_pull(new_uuid)
        self.assertEqual([
            'Status of cloned-repo:',
            'Hoard root: b09bd5b9c68780abde8a55aa5c7a4a70d66e78b5:',
            'Repo root: 98265a8981105d55ce844f15a660760c0634d07c:'], res.splitlines())

        res = await hoard_cmd.files.push(repo="cloned-repo")
        self.assertEqual(
            f"cloned-repo:\n"
            f"cloned-repo:\n"
            "DONE", res.strip())

        self.assertEqual([
            'cloned-repo/test.me.different',
            'cloned-repo/test.me.once',
            'cloned-repo/test.me.twice'], dump_file_list(self.tmpdir.name, "cloned-repo"))

        res = hoard_cmd.remotes()
        self.assertEqual(
            "2 total remotes.\n"
            f"  [cloned-repo] {cloned_cave_cmd.current_uuid()} (partial) "
            f"in {pathlib.Path(self.tmpdir.name).joinpath('cloned-repo').as_posix()} [internal: milliseconds]\n"
            f"  [repo-in-local] {orig_cave_cmd.current_uuid()} (partial) "
            f"in {pathlib.Path(self.tmpdir.name).joinpath('repo').as_posix()} [internal: milliseconds]\n"
            "Mounts:\n"
            "  / -> repo-in-local\n"
            "  /wat -> cloned-repo\n"
            "DONE", res.strip())

    async def test_create_repo_types(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = await init_complex_hoard(
            self.tmpdir.name)

    async def test_sync_hoard_definitions(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = await init_complex_hoard(
            self.tmpdir.name)

        res = await hoard_cmd.contents.differences("repo-partial-name")
        self.assertEqual((
            'Root: None\n'
            'Status of repo-partial-name:\n'
            'PRESENT /test.me.1\n'
            'PRESENT /wat/test.me.2\n'
            'DONE'), res.strip())

        res = await hoard_cmd.contents.pull("repo-partial-name")
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [a80f91] <- repo [curr: a80f91, stg: f6a740, des: a80f91]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'HOARD_FILE_ADDED /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'HOARD_FILE_ADDED /wat/test.me.2\n'
            'updated repo-partial-name from a80f91 to f6a740\n'
            'updated repo-full-name from a80f91 to f6a740\n'
            'updated repo-backup-name from a80f91 to f6a740\n'
            'After: Hoard [f6a740], repo [curr: f6a740, stg: f6a740, des: f6a740]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res.strip())

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual((
            'Root: f6a74030fa0a826b18e424d44f8aca9be8c657f3\n'
            '/test.me.1 = a:1 g:2\n'
            '/wat/test.me.2 = a:1 g:2\n'
            'DONE'), res)

        res = await hoard_cmd.contents.pull("repo-partial-name", ignore_epoch=True)  # does noting...
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [f6a740] <- repo [curr: f6a740, stg: f6a740, des: f6a740]\n'
            'After: Hoard [f6a740], repo [curr: f6a740, stg: f6a740, des: f6a740]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res.strip())

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual((
            'Root: f6a74030fa0a826b18e424d44f8aca9be8c657f3\n'
            '/test.me.1 = a:1 g:2\n'
            '/wat/test.me.2 = a:1 g:2\n'
            'DONE'), res)

        res = await hoard_cmd.contents.pull("repo-full-name")
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [f6a740] <- repo [curr: a80f91, stg: d99580, des: f6a740]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.4\n'
            'HOARD_FILE_ADDED /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3\n'
            'HOARD_FILE_ADDED /wat/test.me.3\n'
            'updated repo-full-name from f6a740 to d99580\n'
            'updated repo-backup-name from f6a740 to d99580\n'
            'After: Hoard [d99580], repo [curr: d99580, stg: d99580, des: d99580]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res.strip())

        res = await hoard_cmd.contents.pull("repo-full-name", ignore_epoch=True)  # does nothing ...
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [d99580] <- repo [curr: d99580, stg: d99580, des: d99580]\n'
            'After: Hoard [d99580], repo [curr: d99580, stg: d99580, des: d99580]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res.strip())

        res = await hoard_cmd.contents.pull("repo-backup-name")  # just registers the files already in backup
        self.assertEqual((
            'Pulling repo-backup-name...\n'
            'Before: Hoard [d99580] <- repo [curr: a80f91, stg: 9fbdcf, des: d99580]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_DESIRED_FILE_TO_GET /test.me.4\n'
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.2\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3\n'
            'After: Hoard [d99580], repo [curr: 9fbdcf, stg: 9fbdcf, des: d99580]\n'
            "Sync'ed repo-backup-name to hoard!\n"
            'DONE'), res.strip())

        res = await hoard_cmd.contents.pull("repo-backup-name")  # does nothing
        self.assertEqual("Skipping update as past epoch 1 is not after hoard epoch 1\nDONE", res.strip())

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual(
            'Root: d995800c80add686a027bac8628ca610418c64b6\n'
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:1 g:1\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.3 = a:2\n"
            "DONE", res)

        res = await hoard_cmd.contents.pull("repo-incoming-name")
        self.assertEqual((
            'Pulling repo-incoming-name...\n'
            'Before: Hoard [d99580] <- repo [curr: a80f91, stg: e9ce07, des: a80f91]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.5\n'
            'HOARD_FILE_ADDED /test.me.5\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.6\n'
            'HOARD_FILE_ADDED /wat/test.me.6\n'
            'updated repo-full-name from d99580 to 89527b\n'
            'updated repo-backup-name from d99580 to 89527b\n'
            'After: Hoard [89527b], repo [curr: e9ce07, stg: e9ce07, des: a80f91]\n'
            "Sync'ed repo-incoming-name to hoard!\n"
            'DONE'), res.strip())

        res = await incoming_cave_cmd.refresh(show_details=False)
        self.assertEqual((
            'old: e9ce073b9d61e12d35bbb0fa537581065083c886\n'
            'current: e9ce073b9d61e12d35bbb0fa537581065083c886\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull("repo-incoming-name")
        self.assertEqual((
            'Pulling repo-incoming-name...\n'
            'Before: Hoard [89527b] <- repo [curr: 843a75, stg: e9ce07, des: a80f91]\n'
            'REPO_FILE_TO_DELETE /test.me.4\n'
            'REPO_FILE_TO_DELETE /test.me.5\n'
            'REPO_FILE_TO_DELETE /wat/test.me.3\n'
            'REPO_FILE_TO_DELETE /wat/test.me.6\n'
            'After: Hoard [89527b], repo [curr: e9ce07, stg: e9ce07, des: a80f91]\n'
            "Sync'ed repo-incoming-name to hoard!\n"
            'DONE'), res.strip())

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual(
            'Root: 89527b0fa576e127d04089d9cb5aab0e5619696d\n'
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:1 g:1 c:1\n"
            "/test.me.5 = g:2 c:1\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.3 = a:2 c:1\n"
            "/wat/test.me.6 = g:2 c:1\n"
            "DONE", res)

    async def test_sync_hoard_file_contents_one(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = await init_complex_hoard(
            self.tmpdir.name)

        await hoard_cmd.contents.pull("repo-partial-name")
        await hoard_cmd.contents.pull("repo-full-name")
        await hoard_cmd.contents.pull("repo-backup-name")  # just registers the files already in backup
        res = await hoard_cmd.contents.pull("repo-incoming-name")
        self.assertEqual((
            'Pulling repo-incoming-name...\n'
            'Before: Hoard [d99580] <- repo [curr: a80f91, stg: e9ce07, des: a80f91]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.5\n'
            'HOARD_FILE_ADDED /test.me.5\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.6\n'
            'HOARD_FILE_ADDED /wat/test.me.6\n'
            'updated repo-full-name from d99580 to 89527b\n'
            'updated repo-backup-name from d99580 to 89527b\n'
            'After: Hoard [89527b], repo [curr: e9ce07, stg: e9ce07, des: a80f91]\n'
            "Sync'ed repo-incoming-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual(
            'Root: 89527b0fa576e127d04089d9cb5aab0e5619696d\n'
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:1 g:1 c:1\n"
            "/test.me.5 = g:2 c:1\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.3 = a:2 c:1\n"
            "/wat/test.me.6 = g:2 c:1\n"
            "DONE", res)

        self.assertEqual([
            'repo-partial/test.me.1',
            'repo-partial/wat/test.me.2', ], dump_file_list(self.tmpdir.name, 'repo-partial'))

        res = await hoard_cmd.files.push("repo-full-name")
        self.assertEqual(
            f"repo-full-name:\n"
            "+ test.me.5\n"
            "+ wat/test.me.6\n"
            f"repo-full-name:\n"
            "DONE", res)

        self.assertEqual([
            'repo-full/test.me.1',
            'repo-full/test.me.4',
            'repo-full/test.me.5',
            'repo-full/wat/test.me.2',
            'repo-full/wat/test.me.3',
            'repo-full/wat/test.me.6'], dump_file_list(self.tmpdir.name, 'repo-full'))

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual(
            'Root: 89527b0fa576e127d04089d9cb5aab0e5619696d\n'
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:1 g:1 c:1\n"
            "/test.me.5 = a:1 g:1 c:1\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.3 = a:2 c:1\n"
            "/wat/test.me.6 = a:1 g:1 c:1\n"
            "DONE", res)

        res = await full_cave_cmd.refresh(show_details=False)
        self.assertEqual((
            'old: d995800c80add686a027bac8628ca610418c64b6\n'
            'current: 89527b0fa576e127d04089d9cb5aab0e5619696d\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.differences("repo-full-name")
        self.assertEqual((
            'Root: 89527b0fa576e127d04089d9cb5aab0e5619696d\n'
            'Status of repo-full-name:\n'
            'DONE'), res)

    async def test_pull_all(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = await init_complex_hoard(
            self.tmpdir.name)

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [a80f91] <- repo [curr: a80f91, stg: f6a740, des: a80f91]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'HOARD_FILE_ADDED /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'HOARD_FILE_ADDED /wat/test.me.2\n'
            'updated repo-partial-name from a80f91 to f6a740\n'
            'updated repo-full-name from a80f91 to f6a740\n'
            'updated repo-backup-name from a80f91 to f6a740\n'
            'After: Hoard [f6a740], repo [curr: f6a740, stg: f6a740, des: f6a740]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'Pulling repo-full-name...\n'
            'Before: Hoard [f6a740] <- repo [curr: a80f91, stg: d99580, des: f6a740]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.4\n'
            'HOARD_FILE_ADDED /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3\n'
            'HOARD_FILE_ADDED /wat/test.me.3\n'
            'updated repo-full-name from f6a740 to d99580\n'
            'updated repo-backup-name from f6a740 to d99580\n'
            'After: Hoard [d99580], repo [curr: d99580, stg: d99580, des: d99580]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'Pulling repo-backup-name...\n'
            'Before: Hoard [d99580] <- repo [curr: a80f91, stg: 9fbdcf, des: d99580]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_DESIRED_FILE_TO_GET /test.me.4\n'
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.2\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3\n'
            'After: Hoard [d99580], repo [curr: 9fbdcf, stg: 9fbdcf, des: d99580]\n'
            "Sync'ed repo-backup-name to hoard!\n"
            'Pulling repo-incoming-name...\n'
            'Before: Hoard [d99580] <- repo [curr: a80f91, stg: e9ce07, des: a80f91]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.5\n'
            'HOARD_FILE_ADDED /test.me.5\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.6\n'
            'HOARD_FILE_ADDED /wat/test.me.6\n'
            'updated repo-full-name from d99580 to 89527b\n'
            'updated repo-backup-name from d99580 to 89527b\n'
            'After: Hoard [89527b], repo [curr: e9ce07, stg: e9ce07, des: a80f91]\n'
            "Sync'ed repo-incoming-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_disk_sizes=True)
        self.assertEqual([
            'Root: 89527b0fa576e127d04089d9cb5aab0e5619696d',
            '|Num Files                |             updated|total     |available |get       |cleanup   |',
            '|repo-backup-name         |                 now|         6|         2|         4|          |',
            '|repo-full-name           |                 now|         6|         4|         2|          |',
            '|repo-incoming-name       |                 now|         4|          |          |         4|',
            '|repo-partial-name        |                 now|         2|         2|          |          |',
            '',
            '|Size                     |             updated|total     |available |get       |cleanup   |',
            '|repo-backup-name         |                 now|        47|        16|        31|          |',
            '|repo-full-name           |                 now|        47|        35|        12|          |',
            '|repo-incoming-name       |                 now|        33|          |          |        33|',
            '|repo-partial-name        |                 now|        14|        14|          |          |'],
            res.splitlines())

    async def test_sync_hoard_file_contents_all(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = await init_complex_hoard(
            self.tmpdir.name)

        await hoard_cmd.contents.pull("repo-partial-name")
        await hoard_cmd.contents.pull("repo-full-name")
        await hoard_cmd.contents.pull("repo-backup-name")  # just registers the files already in backup
        await hoard_cmd.contents.pull("repo-incoming-name")

        res = await hoard_cmd.contents.status(hide_disk_sizes=True, hide_time=True)
        self.assertEqual([
            'Root: 89527b0fa576e127d04089d9cb5aab0e5619696d',
            '|Num Files                |total     |available |get       |cleanup   |',
            '|repo-backup-name         |         6|         2|         4|          |',
            '|repo-full-name           |         6|         4|         2|          |',
            '|repo-incoming-name       |         4|          |          |         4|',
            '|repo-partial-name        |         2|         2|          |          |',
            '',
            '|Size                     |total     |available |get       |cleanup   |',
            '|repo-backup-name         |        47|        16|        31|          |',
            '|repo-full-name           |        47|        35|        12|          |',
            '|repo-incoming-name       |        33|          |          |        33|',
            '|repo-partial-name        |        14|        14|          |          |'],
            res.splitlines())

        res = await hoard_cmd.files.push(all=True)
        self.assertEqual([
            'repo-partial-name:',
            'repo-full-name:',
            '+ test.me.5',
            '+ wat/test.me.6',
            'repo-backup-name:',
            '+ test.me.4',
            '+ test.me.5',
            '+ wat/test.me.2',
            '+ wat/test.me.6',
            'repo-incoming-name:',
            'repo-partial-name:',
            'repo-full-name:',
            'repo-backup-name:',
            'repo-incoming-name:',
            'd test.me.4',
            'd test.me.5',
            'd wat/test.me.3',
            'd wat/test.me.6',
            'DONE'], res.splitlines())

        self.assertEqual([
            'repo-partial/test.me.1',
            'repo-partial/wat/test.me.2'],
            dump_file_list(self.tmpdir.name, 'repo-partial'))

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual(
            'Root: 89527b0fa576e127d04089d9cb5aab0e5619696d\n'
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:2\n"
            "/test.me.5 = a:2\n"
            "/wat/test.me.2 = a:3\n"
            "/wat/test.me.3 = a:2\n"
            "/wat/test.me.6 = a:2\n"
            "DONE", res)

        self.assertEqual([
            'repo-full/test.me.1',
            'repo-full/test.me.4',
            'repo-full/test.me.5',
            'repo-full/wat/test.me.2',
            'repo-full/wat/test.me.3',
            'repo-full/wat/test.me.6'], dump_file_list(self.tmpdir.name, 'repo-full'))

        self.assertEqual([
            'repo-backup/test.me.1',
            'repo-backup/test.me.4',
            'repo-backup/test.me.5',
            'repo-backup/wat/test.me.2',
            'repo-backup/wat/test.me.3',
            'repo-backup/wat/test.me.6'], dump_file_list(self.tmpdir.name, 'repo-backup'))

        self.assertEqual([], dump_file_list(self.tmpdir.name, 'repo-incoming'))

    async def test_partial_cloning(self):
        populate_repotypes(self.tmpdir.name)
        pfw = pretty_file_writer(self.tmpdir.name)
        pfw("repo-full/wat/inner/another.file", "asdafaqw")

        full_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo-full")).cave
        full_cave_cmd.init()
        await full_cave_cmd.refresh(show_details=False)

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-full"), name="repo-full-name", mount_point="/",
            type=CaveType.PARTIAL, fetch_new=True)

        res = await hoard_cmd.contents.pull("repo-full-name")
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [a80f91] <- repo [curr: a80f91, stg: 3f8d62, des: a80f91]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'HOARD_FILE_ADDED /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.4\n'
            'HOARD_FILE_ADDED /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /wat/inner/another.file\n'
            'HOARD_FILE_ADDED /wat/inner/another.file\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'HOARD_FILE_ADDED /wat/test.me.2\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3\n'
            'HOARD_FILE_ADDED /wat/test.me.3\n'
            'updated repo-full-name from a80f91 to 3f8d62\n'
            'After: Hoard [3f8d62], repo [curr: 3f8d62, stg: 3f8d62, des: 3f8d62]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        os.mkdir(join(self.tmpdir.name, "repo-cloned-wat"))
        res = await hoard_cmd.clone(
            to_path=join(self.tmpdir.name, "repo-cloned-wat"), mount_at="/wat", name="repo-cloned-wat", fetch_new=True)
        self.assertEqual("DONE", res)

        cloned_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo-cloned-wat")).cave

        res = await hoard_cmd.contents.get(repo="repo-cloned-wat", path="inner")
        self.assertEqual("+/wat/inner/another.file\nConsidered 1 files.\nDONE", res)

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            'Root: 3f8d62ebedc2db018bc4527719e668d97a425cb0\n'
            "/ => (repo-full-name:.)\n"
            "/test.me.1 = a:1\n"
            "/test.me.4 = a:1\n"
            "/wat => (repo-cloned-wat:.), (repo-full-name:wat)\n"
            "/wat/test.me.2 = a:1\n"
            "/wat/test.me.3 = a:1\n"
            "/wat/inner => (repo-cloned-wat:inner), (repo-full-name:wat/inner)\n"
            "/wat/inner/another.file = a:1 g:1\n"
            "DONE", res)

        self.assertEqual([], dump_file_list(self.tmpdir.name, "repo-cloned-wat/"))  # no files yet

        res = await hoard_cmd.files.push("repo-cloned-wat")
        self.assertEqual(
            f"repo-cloned-wat:\n"
            "+ inner/another.file\n"
            f"repo-cloned-wat:\n"
            "DONE", res)

        self.assertEqual(
            ['repo-cloned-wat/inner/another.file'],
            dump_file_list(self.tmpdir.name, "repo-cloned-wat/"))

        res = await cloned_cave_cmd.refresh(show_details=False)
        self.assertEqual((
            'old: a80f91bc48850a1fb3459bb76b9f6308d4d35710\n'
            'current: ecaebc37e813c8babcf72589d044d96fe1b3318f\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.get(repo="repo-cloned-wat", path="")
        self.assertEqual(
            "+/wat/test.me.2\n"
            "+/wat/test.me.3\n"
            "Considered 3 files.\nDONE", res)

        res = await hoard_cmd.files.push("repo-cloned-wat")
        self.assertEqual(
            f"repo-cloned-wat:\n"
            "+ test.me.2\n"
            "+ test.me.3\n"
            f"repo-cloned-wat:\n"
            "DONE", res)

        self.assertEqual([
            'repo-cloned-wat/inner/another.file',
            'repo-cloned-wat/test.me.2',
            'repo-cloned-wat/test.me.3'],
            dump_file_list(self.tmpdir.name, "repo-cloned-wat/"))

    async def test_moving_locations_no_files(self):
        populate_repotypes(self.tmpdir.name)
        partial_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo-partial")).cave
        partial_cave_cmd.init()
        await partial_cave_cmd.refresh(show_details=False)

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-partial"), name="repo-partial-name", mount_point="/first-point",
            type=CaveType.PARTIAL, fetch_new=True)

        res = await hoard_cmd.contents.pull("repo-partial-name")
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [a80f91] <- repo [curr: a80f91, stg: 9eb9bc, des: a80f91]\n'
            'REPO_MARK_FILE_AVAILABLE /first-point/test.me.1\n'
            'HOARD_FILE_ADDED /first-point/test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /first-point/wat/test.me.2\n'
            'HOARD_FILE_ADDED /first-point/wat/test.me.2\n'
            'updated repo-partial-name from a80f91 to 9eb9bc\n'
            'After: Hoard [9eb9bc], repo [curr: 9eb9bc, stg: 9eb9bc, des: 9eb9bc]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res.strip())

        res = hoard_cmd.remotes(hide_paths=True)
        self.assertEqual(
            f"1 total remotes.\n"
            f"  [repo-partial-name] {partial_cave_cmd.current_uuid()} (partial)\n"
            "Mounts:\n"
            "  /first-point -> repo-partial-name\n"
            "DONE", res.strip())

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            'Root: 9eb9bcf80f37d52571b8a1262c483405c22390d1\n'
            "/\n"
            "/first-point => (repo-partial-name:.)\n"
            "/first-point/test.me.1 = a:1\n"
            "/first-point/wat => (repo-partial-name:wat)\n"
            "/first-point/wat/test.me.2 = a:1\n"
            "DONE", res)

        res = await hoard_cmd.move_mounts(from_path="/first-point/inner", to_path="/cant-move-files")
        self.assertEqual(
            "Can't move /first-point/inner to /cant-move-files, requires moving files in repo-partial-name:inner.",
            res.strip())

        res = await hoard_cmd.move_mounts(from_path="/", to_path="/move-all-inside")
        self.assertEqual(
            "Moving files and folders:\n"
            "/first-point/test.me.1=>/move-all-inside/first-point/test.me.1\n"
            "/first-point/wat/test.me.2=>/move-all-inside/first-point/wat/test.me.2\n"
            "Moving 1 repos:\n"
            "[repo-partial-name] /first-point => /move-all-inside/first-point\n"
            "DONE", res)

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            'Root: 9eb9bcf80f37d52571b8a1262c483405c22390d1\n'
            "/\n"
            "/move-all-inside\n"
            "/move-all-inside/first-point => (repo-partial-name:.)\n"
            "/move-all-inside/first-point/test.me.1 = a:1\n"
            "/move-all-inside/first-point/wat => (repo-partial-name:wat)\n"
            "/move-all-inside/first-point/wat/test.me.2 = a:1\n"
            "DONE", res)

        res = hoard_cmd.remotes(hide_paths=True)
        self.assertEqual(
            f"1 total remotes.\n"
            f"  [repo-partial-name] {partial_cave_cmd.current_uuid()} (partial)\n"
            "Mounts:\n"
            "  /move-all-inside/first-point -> repo-partial-name\n"
            "DONE", res.strip())

        res = await hoard_cmd.move_mounts(from_path="/first-point", to_path="/moved-data")
        self.assertEqual("No repos to move!", res.strip())

        res = await hoard_cmd.move_mounts(from_path="/move-all-inside/first-point", to_path="/moved-data")
        self.assertEqual(
            "Moving files and folders:\n"
            "/move-all-inside/first-point/test.me.1=>/moved-data/test.me.1\n"
            "/move-all-inside/first-point/wat/test.me.2=>/moved-data/wat/test.me.2\n"
            "Moving 1 repos:\n"
            "[repo-partial-name] /move-all-inside/first-point => /moved-data\n"
            "DONE", res)

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            'Root: 9eb9bcf80f37d52571b8a1262c483405c22390d1\n'
            "/\n"
            "/moved-data => (repo-partial-name:.)\n"
            "/moved-data/test.me.1 = a:1\n"
            "/moved-data/wat => (repo-partial-name:wat)\n"
            "/moved-data/wat/test.me.2 = a:1\n"
            "DONE", res)

        res = await hoard_cmd.move_mounts(from_path="/moved-data", to_path="/")
        self.assertEqual(
            "Moving files and folders:\n"
            "/moved-data/test.me.1=>/test.me.1\n"
            "/moved-data/wat/test.me.2=>/wat/test.me.2\n"
            "Moving 1 repos:\n"
            "[repo-partial-name] /moved-data => /\n"
            "DONE", res)

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            'Root: 9eb9bcf80f37d52571b8a1262c483405c22390d1\n'
            "/ => (repo-partial-name:.)\n"
            "/test.me.1 = a:1\n"
            "/wat => (repo-partial-name:wat)\n"
            "/wat/test.me.2 = a:1\n"
            "DONE", res)

        await partial_cave_cmd.refresh(show_details=False)

        res = await hoard_cmd.contents.pull("repo-partial-name")  # needs to do nothing
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [f6a740] <- repo [curr: f6a740, stg: f6a740, des: f6a740]\n'
            'After: Hoard [f6a740], repo [curr: f6a740, stg: f6a740, des: f6a740]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res.strip())

    async def test_copy_locations_of_files(self):
        populate_repotypes(self.tmpdir.name)
        partial_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo-partial")).cave
        partial_cave_cmd.init()
        await partial_cave_cmd.refresh(show_details=False)

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-partial"), name="repo-partial-name",
            mount_point="/first-point",
            type=CaveType.PARTIAL, fetch_new=True)

        # "ADD_NEW_TO_HOARD /first-point/test.me.1\n"
        # "ADD_NEW_TO_HOARD /first-point/wat/test.me.2\n"
        await hoard_cmd.contents.pull("repo-partial-name")

        res = await hoard_cmd.move_mounts(from_path="/first-point", to_path="/moved-data")
        self.assertEqual(
            "Moving files and folders:\n"
            "/first-point/test.me.1=>/moved-data/test.me.1\n"
            "/first-point/wat/test.me.2=>/moved-data/wat/test.me.2\n"
            "Moving 1 repos:\n"
            "[repo-partial-name] /first-point => /moved-data\n"
            "DONE", res)

        res = await hoard_cmd.contents.copy(from_path="/moved-data/wat", to_path="/moved-data/zed")
        self.assertEqual(
            "c+ /moved-data/zed/test.me.2\n"
            "DONE", res)

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            'Root: 9eb9bcf80f37d52571b8a1262c483405c22390d1\n'
            "/\n"
            "/moved-data => (repo-partial-name:.)\n"
            "/moved-data/test.me.1 = a:1\n"
            "/moved-data/wat => (repo-partial-name:wat)\n"
            "/moved-data/wat/test.me.2 = a:1\n"
            "/moved-data/zed => (repo-partial-name:zed)\n"
            "/moved-data/zed/test.me.2 = x:1\n"
            "DONE", res)

        res = await hoard_cmd.files.push("repo-partial-name")
        self.assertEqual(
            f"repo-partial-name:\n"
            "c+ zed/test.me.2\n"
            f"repo-partial-name:\n"
            "DONE", res)

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            'Root: 9eb9bcf80f37d52571b8a1262c483405c22390d1\n'
            "/\n"
            "/moved-data => (repo-partial-name:.)\n"
            "/moved-data/test.me.1 = a:1\n"
            "/moved-data/wat => (repo-partial-name:wat)\n"
            "/moved-data/wat/test.me.2 = a:1\n"
            "/moved-data/zed => (repo-partial-name:zed)\n"
            "/moved-data/zed/test.me.2 = a:1\n"
            "DONE", res)

        res = dump_file_list(self.tmpdir.name, "repo-partial")
        self.assertEqual(
            ['repo-partial/test.me.1',
             'repo-partial/wat/test.me.2',
             'repo-partial/zed/test.me.2'], res)

    async def test_restore_missing_local_file_on_refresh(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = await init_complex_hoard(
            self.tmpdir.name)

        res = await hoard_cmd.contents.pull("repo-partial-name")
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [a80f91] <- repo [curr: a80f91, stg: f6a740, des: a80f91]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'HOARD_FILE_ADDED /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'HOARD_FILE_ADDED /wat/test.me.2\n'
            'updated repo-partial-name from a80f91 to f6a740\n'
            'updated repo-full-name from a80f91 to f6a740\n'
            'updated repo-backup-name from a80f91 to f6a740\n'
            'After: Hoard [f6a740], repo [curr: f6a740, stg: f6a740, des: f6a740]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.pull("repo-backup-name")
        self.assertEqual((
            'Pulling repo-backup-name...\n'
            'Before: Hoard [f6a740] <- repo [curr: a80f91, stg: 9fbdcf, des: f6a740]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.2\n'
            'After: Hoard [f6a740], repo [curr: 9fbdcf, stg: 9fbdcf, des: f6a740]\n'
            "Sync'ed repo-backup-name to hoard!\n"
            'DONE'), res)

        self.assertEqual(
            'Root: f6a74030fa0a826b18e424d44f8aca9be8c657f3\n'
            "/\n"
            "/test.me.1 = a:2 g:1\n"
            "/wat\n"
            "/wat/test.me.2 = a:1 g:2\n"
            '/wat/test.me.3 = c:1\n'
            "DONE", await hoard_cmd.contents.ls(depth=2))

        res = await hoard_cmd.files.push("repo-backup-name")
        self.assertEqual(
            'repo-backup-name:\n'
            '+ wat/test.me.2\n'
            'repo-backup-name:\n'
            'd wat/test.me.3\n'
            'remove dangling /wat/test.me.3\n'
            'DONE', res.strip())

        self.assertEqual([
            'repo-backup/test.me.1',
            'repo-backup/wat/test.me.2'], dump_file_list(self.tmpdir.name, 'repo-backup'))

        self.assertEqual(
            'Root: f6a74030fa0a826b18e424d44f8aca9be8c657f3\n'
            "/\n"
            "/test.me.1 = a:2 g:1\n"
            "/wat\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "DONE", await hoard_cmd.contents.ls(depth=2))

        self.assertEqual([
            'repo-partial/test.me.1',
            'repo-partial/wat/test.me.2'],
            dump_file_list(self.tmpdir.name, 'repo-partial'))

        os.remove(join(self.tmpdir.name, 'repo-partial/wat/test.me.2'))

        res = await partial_cave_cmd.status()
        self.assertEqual(
            f"{partial_cave_cmd.current_uuid()} [f6a74030fa0a826b18e424d44f8aca9be8c657f3]:\n"
            "files:\n"
            "    same: 1 (100.0%)\n"
            "     mod: 0 (0.0%)\n"
            "     new: 0 (0.0%)\n"
            "   moved: 0 (0.0%)\n"
            " current: 1\n"
            " in repo: 2\n"
            " deleted: 1 (50.0%)\n", res)

        res = await partial_cave_cmd.refresh(show_details=False)
        self.assertEqual((
            'old: f6a74030fa0a826b18e424d44f8aca9be8c657f3\n'
            'current: 10a305855f7cefebf03059a3688e771b8c0dfc26\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull("repo-partial-name", force_fetch_local_missing=True)
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [f6a740] <- repo [curr: f6a740, stg: 10a305, des: f6a740]\n'
            'After: Hoard [f6a740], repo [curr: 10a305, stg: 10a305, des: f6a740]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        self.assertEqual(
            'Root: f6a74030fa0a826b18e424d44f8aca9be8c657f3\n'
            "/\n"
            "/test.me.1 = a:2 g:1\n"
            "/wat\n"
            "/wat/test.me.2 = a:1 g:2\n"
            "DONE", await hoard_cmd.contents.ls(depth=2))

        res = await hoard_cmd.files.push("repo-partial-name")
        self.assertEqual(
            f"repo-partial-name:\n"
            f"+ wat/test.me.2\n"
            f"repo-partial-name:\n"
            f"DONE", res)

        self.assertEqual(
            ['repo-partial/test.me.1', 'repo-partial/wat/test.me.2'],
            dump_file_list(self.tmpdir.name, 'repo-partial'))

    async def test_pull_on_missing_contents_are_skipped(self):
        populate_repotypes(self.tmpdir.name)
        tmpdir = self.tmpdir.name

        hoard_cmd = TotalCommand(path=join(tmpdir, "hoard")).hoard
        hoard_cmd.init()

        res = hoard_cmd.add_remote(
            remote_path=join(tmpdir, "repo-partial"), name="repo-partial-name", mount_point="/",
            type=CaveType.PARTIAL)
        self.assertEqual(f"Repo not initialized at {join(tmpdir, 'repo-partial')}!", res)

        partial_cave_cmd = TotalCommand(path=join(tmpdir, "repo-partial")).cave
        partial_cave_cmd.init()

        res = hoard_cmd.add_remote(
            remote_path=join(tmpdir, "repo-partial"), name="repo-partial-name", mount_point="/",
            type=CaveType.PARTIAL)
        self.assertEqual(fr"Added repo-partial-name[{partial_cave_cmd.current_uuid()}] at {tmpdir}\repo-partial!", res)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual(f"Repo {partial_cave_cmd.current_uuid()} has no current contents available!\nDONE", res)

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual(f"Repo {partial_cave_cmd.current_uuid()} has no current contents available!\nDONE", res)

        res = await partial_cave_cmd.refresh(show_details=False)
        self.assertEqual('old: None\ncurrent: f6a74030fa0a826b18e424d44f8aca9be8c657f3\nRefresh done!', res)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [a80f91] <- repo [curr: a80f91, stg: f6a740, des: a80f91]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'HOARD_FILE_ADDED /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'HOARD_FILE_ADDED /wat/test.me.2\n'
            'updated repo-partial-name from a80f91 to f6a740\n'
            'After: Hoard [f6a740], repo [curr: f6a740, stg: f6a740, des: f6a740]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        full_cave_cmd = TotalCommand(path=join(tmpdir, "repo-full")).cave
        full_cave_cmd.init()
        await full_cave_cmd.refresh(show_details=False)

        res = hoard_cmd.add_remote(
            remote_path=join(tmpdir, "repo-full"), name="repo-full-name", mount_point="/",
            type=CaveType.PARTIAL, fetch_new=True)
        self.assertEqual(f"Added repo-full-name[{full_cave_cmd.current_uuid()}] at {join(tmpdir, 'repo-full')}!", res)

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual((
            'Skipping update as past epoch 1 is not after hoard epoch 1\n'
            'Pulling repo-full-name...\n'
            'Before: Hoard [f6a740] <- repo [curr: a80f91, stg: d99580, des: a80f91]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.4\n'
            'HOARD_FILE_ADDED /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3\n'
            'HOARD_FILE_ADDED /wat/test.me.3\n'
            'updated repo-full-name from a80f91 to d99580\n'
            'After: Hoard [d99580], repo [curr: d99580, stg: d99580, des: d99580]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)


def dump_file_list(tmpdir: str, path: str, data: bool = False) -> List[str] | Dict[str, str]:
    files = sorted([
        pathlib.Path(join(dirpath, filename)).relative_to(tmpdir).as_posix()
        for dirpath, dirnames, filenames in os.walk(join(tmpdir, path), topdown=True)
        for filename in filenames if dirpath.find(".hoard") == -1])
    if not data:
        return files
    else:
        def read(f):
            with open(join(tmpdir, f)) as fo:
                return fo.read()

        return dict((f, read(f)) for f in files)


def populate_repotypes(tmpdir: str):
    # f"D /wat/test.me.different\n"
    # f"D /wat/test.me.once\n"
    # f"D /wat/test.me.twice\nDONE"
    pfw = pretty_file_writer(tmpdir)

    pfw('repo-partial/test.me.1', "gsadfs")
    pfw('repo-partial/wat/test.me.2', "gsadf3dq")

    pfw('repo-full/test.me.1', "gsadfs")
    pfw('repo-full/test.me.4', "fwadeaewdsa")
    pfw('repo-full/wat/test.me.2', "gsadf3dq")
    pfw('repo-full/wat/test.me.3', "afaswewfas")

    pfw('repo-backup/test.me.1', "gsadfs")
    pfw('repo-backup/wat/test.me.3', "afaswewfas")

    pfw('repo-incoming/wat/test.me.3', "asdgvarfa")
    pfw('repo-incoming/test.me.4', "fwadeaewdsa")
    pfw('repo-incoming/test.me.5', "adsfg")
    pfw('repo-incoming/wat/test.me.6', "f2fwsdf")


async def init_complex_hoard(tmpdir: str):
    partial_cave_cmd = TotalCommand(path=join(tmpdir, "repo-partial")).cave
    partial_cave_cmd.init()
    await partial_cave_cmd.refresh(show_details=False)

    full_cave_cmd = TotalCommand(path=join(tmpdir, "repo-full")).cave
    full_cave_cmd.init()
    await full_cave_cmd.refresh(show_details=False)

    backup_cave_cmd = TotalCommand(path=join(tmpdir, "repo-backup")).cave
    backup_cave_cmd.init()
    await backup_cave_cmd.refresh(show_details=False)

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
        remote_path=join(tmpdir, "repo-backup"), name="repo-backup-name", mount_point="/",
        type=CaveType.BACKUP)

    hoard_cmd.add_remote(
        remote_path=join(tmpdir, "repo-incoming"), name="repo-incoming-name", mount_point="/",
        type=CaveType.INCOMING)

    res = hoard_cmd.remotes(hide_paths=True)
    assert (""
            "4 total remotes."
            f"\n  [repo-partial-name] {partial_cave_cmd.current_uuid()} (partial)"
            f"\n  [repo-full-name] {full_cave_cmd.current_uuid()} (partial)"
            f"\n  [repo-backup-name] {backup_cave_cmd.current_uuid()} (backup)"
            f"\n  [repo-incoming-name] {incoming_cave_cmd.current_uuid()} (incoming)"
            "\nMounts:"
            "\n  / -> repo-partial-name, repo-full-name, repo-backup-name, repo-incoming-name"
            "\nDONE") == res.strip()

    # make sure resolving the command from a hoard path works
    tmp_command = RepoCommand(path=join(tmpdir, "hoard"), name="repo-partial-name")
    assert partial_cave_cmd.current_uuid() == tmp_command.current_uuid()
    assert partial_cave_cmd.repo.path == tmp_command.repo.path

    return hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd
