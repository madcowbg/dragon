import logging
import os
import pathlib
import sys
import tempfile
from os.path import join
from typing import Tuple, List, Dict, Iterable
from unittest import IsolatedAsyncioTestCase

from command.command_repo import RepoCommand
from command.fast_path import FastPosixPath
from command.test_repo_command import populate, write_contents, pretty_file_writer
from config import CaveType
from contents.hoard import HoardContents
from contents.hoard_props import HoardFileProps
from dragon import TotalCommand
from lmdb_storage.file_object import BlobObject, FileObject
from lmdb_storage.operations.fast_association import FastAssociation
from lmdb_storage.operations.generator import TreeGenerator
from lmdb_storage.operations.util import ByRoot
from lmdb_storage.tree_object import StoredObject, ObjectType, TreeObject, MaybeObjectID
from lmdb_storage.tree_structure import Objects
from resolve_uuid import resolve_remote_uuid


def populate_hoard(tmpdir: str):
    populate(tmpdir)

    os.mkdir(join(tmpdir, "hoard"))


class TestHoardCommand(IsolatedAsyncioTestCase):
    def setUp(self):
        # logging.basicConfig(level=logging.DEBUG)
        # logging.getLogger().setLevel(logging.DEBUG)
        # logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

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
        await hoard_cmd.init()
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
        await hoard_cmd.init()
        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local", mount_point="/")

        repo_uuid = cave_cmd.current_uuid()

        res = await hoard_cmd.contents.differences("repo-in-local")
        self.assertEqual((
            'Root: None\n'
            'Remote repo-in-local current=None staging=None desired=None\n'
            'Status of repo-in-local:\n'
            'PRESENT /wat/test.me.different\n'
            'PRESENT /wat/test.me.once\n'
            'PRESENT /wat/test.me.twice\n'
            'DONE'),
            res.strip())

        res = await hoard_cmd.contents.pending_pull("repo-in-local")
        self.assertEqual([
            'Status of repo-in-local:',
            'Hoard root: None:',
            'Repo current=None staging=72174f desired=None',
            'Repo root: 72174f950289a454493d243bb72bdb76982e5f62:',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.different',
            'HOARD_FILE_ADDED /wat/test.me.different',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.once',
            'HOARD_FILE_ADDED /wat/test.me.once',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.twice',
            'HOARD_FILE_ADDED /wat/test.me.twice'], res.splitlines())

        res = await hoard_cmd.contents.tree_differences("repo-in-local")
        self.assertEqual(['Tree Differences up to level 3:', 'DONE'], res.splitlines())

        res = await hoard_cmd.contents.pull("repo-in-local")
        self.assertEqual((
            'Pulling repo-in-local...\n'
            'Before: Hoard [None] <- repo [curr: None, stg: 72174f, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.different\n'
            'HOARD_FILE_ADDED /wat/test.me.different\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.once\n'
            'HOARD_FILE_ADDED /wat/test.me.once\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.twice\n'
            'HOARD_FILE_ADDED /wat/test.me.twice\n'
            'updated repo-in-local from None to 72174f\n'
            'After: Hoard [72174f], repo [curr: 72174f, stg: 72174f, des: 72174f]\n'
            "Sync'ed repo-in-local to hoard!\n"
            'DONE'), res.strip())

        with hoard_cmd.hoard.open_contents(False) as hoard_contents:
            self._assert_hoard_contents(
                hoard_contents,
                files_exp=[
                    ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),
                    ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                    ('/wat/test.me.twice', 6, 1, '1881f6f9784fb08bf6690e9763b76ac3')])

        res = await hoard_cmd.contents.differences("repo-in-local")
        self.assertEqual((
            'Root: 72174f950289a454493d243bb72bdb76982e5f62\n'
            'Remote repo-in-local current=72174f staging=72174f desired=72174f\n'
            'Status of repo-in-local:\n'
            'DONE'), res.strip())

        res = await hoard_cmd.contents.tree_differences("repo-in-local")
        self.assertEqual(['Tree Differences up to level 3:', 'DONE'], res.splitlines())

        res = await hoard_cmd.contents.pending_pull("repo-in-local")
        self.assertEqual([
            'Status of repo-in-local:',
            'Hoard root: 72174f950289a454493d243bb72bdb76982e5f62:',
            'Repo current=72174f staging=72174f desired=72174f',
            'Repo root: 72174f950289a454493d243bb72bdb76982e5f62:'], res.splitlines())

    def _assert_hoard_contents(
            self, hoard_contents: HoardContents, files_exp: List[Tuple[str, int, int, str]]):
        files = sorted(
            (f.as_posix(), prop.size, len(prop.available_at), prop.fasthash)
            for f, prop in HoardFilesIterator.DEPRECATED_all(hoard_contents) if isinstance(prop, HoardFileProps))
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
        await hoard_cmd.init()
        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local", mount_point="/")
        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-2"), name="repo-in-local-2", type=CaveType.BACKUP,
            mount_point="/wat")

        await hoard_cmd.contents.pull("repo-in-local")

        with hoard_cmd.hoard.open_contents(False) as hc:
            self._assert_hoard_contents(
                hc,
                files_exp=[
                    ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),
                    ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                    ('/wat/test.me.twice', 6, 1, '1881f6f9784fb08bf6690e9763b76ac3')])

        res = await hoard_cmd.contents.pull("repo-in-local")
        self.assertEqual('Skipping update as repo-in-local.staging has not changed: 72174f\nDONE', res)

        res = await hoard_cmd.contents.pull("repo-in-local-2")
        self.assertEqual((
            'Pulling repo-in-local-2...\n'
            'Before: Hoard [72174f] <- repo [curr: None, stg: 966d51, des: 72174f]\n'
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.different\n'
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.once\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.twice\n'
            'After: Hoard [72174f], repo [curr: 966d51, stg: 966d51, des: 72174f]\n'
            "Sync'ed repo-in-local-2 to hoard!\n"
            'DONE'),
            res.strip())

        with hoard_cmd.hoard.open_contents(False) as hc:
            self._assert_hoard_contents(
                hc,
                files_exp=[
                    ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),
                    ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                    ('/wat/test.me.twice', 6, 2, '1881f6f9784fb08bf6690e9763b76ac3')])

        res = await hoard_cmd.contents.pull("repo-in-local", ignore_epoch=True)
        self.assertEqual((
            'Pulling repo-in-local...\n'
            'Before: Hoard [72174f] <- repo [curr: 72174f, stg: 72174f, des: 72174f]\n'
            'After: Hoard [72174f], repo [curr: 72174f, stg: 72174f, des: 72174f]\n'
            "Sync'ed repo-in-local to hoard!\n"
            'DONE'), res)

        with hoard_cmd.hoard.open_contents(False) as hc:
            self._assert_hoard_contents(
                hc,
                files_exp=[
                    ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),  # retained only from repo
                    ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                    ('/wat/test.me.twice', 6, 2, '1881f6f9784fb08bf6690e9763b76ac3')])

        res = await hoard_cmd.contents.differences("repo-in-local-2")
        self.assertEqual((
            'Root: 72174f950289a454493d243bb72bdb76982e5f62\n'
            'Remote repo-in-local current=72174f staging=72174f desired=72174f\n'
            'Remote repo-in-local-2 current=966d51 staging=966d51 desired=72174f\n'
            'Status of repo-in-local-2:\n'
            'MODIFIED /wat/test.me.different\n'
            'MISSING /wat/test.me.once\n'
            'DONE'), res.strip())

        res = await hoard_cmd.contents.tree_differences("repo-in-local-2")
        self.assertEqual([
            'Tree Differences up to level 3:',
            '/[D]: GET: 1, CHANGE: 1',
            ' wat[D]: GET: 1, CHANGE: 1',
            '  test.me.different: CHANGE: 1',
            '  test.me.once: GET: 1',
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.pending_pull("repo-in-local-2")
        self.assertEqual([
            'Status of repo-in-local-2:',
            'Hoard root: 72174f950289a454493d243bb72bdb76982e5f62:',
            'Repo current=966d51 staging=966d51 desired=72174f',
            'Repo root: 1b736d16c16bdacf49df7cd5aa66e7b5479ad4b7:',
            'REPO_DESIRED_FILE_CHANGED /wat/test.me.different',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.once'], res.splitlines())

        res = await hoard_cmd.contents.differences("repo-in-local")
        self.assertEqual((
            'Root: 72174f950289a454493d243bb72bdb76982e5f62\n'
            'Remote repo-in-local current=72174f staging=72174f desired=72174f\n'
            'Remote repo-in-local-2 current=966d51 staging=966d51 desired=72174f\n'
            'Status of repo-in-local:\n'
            'DONE'), res.strip())

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
            f"  [repo-in-local]: 2 with no other copy\n"
            f"  [repo-in-local-2]: 0 with no other copy\n"
            "Hoard health stats:\n"
            "  1 copies: 2 files\n"
            "  2 copies: 1 files\n"
            'Fasthash health stats:\n'
            ' #existing fasthashes = 4\n'
            '  len 32 -> 8\n'
            ' #hoard fasthashes = 3\n'
            '  len 32 -> 6\n'
            ' #existing but not in hoard: 1\n'
            ' #hoard but not existing: 0\n'
            '  1 copies - 2 hashes, space est: 13 = 2 x 1 x (5 ~ 8)\n'
            '  2 copies - 1 hashes, space est: 12 = 1 x 2 x 6\n'
            "DONE", res)

        res = await cave_cmd2.refresh()
        self.assertEqual((
            'NO CHANGES\n'
            'old: 1b736d16c16bdacf49df7cd5aa66e7b5479ad4b7\n'
            'current: 1b736d16c16bdacf49df7cd5aa66e7b5479ad4b7\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull("repo-in-local-2", ignore_epoch=True)
        self.assertEqual([
            'Pulling repo-in-local-2...',
            'Before: Hoard [72174f] <- repo [curr: 966d51, stg: 966d51, des: 72174f]',
            'REPO_DESIRED_FILE_CHANGED /wat/test.me.different',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.once',
            'After: Hoard [72174f], repo [curr: 966d51, stg: 966d51, des: 72174f]',
            "Sync'ed repo-in-local-2 to hoard!",
            'DONE'], res.splitlines())

    async def test_changing_data(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        await cave_cmd.refresh(show_details=False)

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        await hoard_cmd.init()
        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local", mount_point="/")

        repo_uuid = cave_cmd.current_uuid()
        await hoard_cmd.contents.pull("repo-in-local")

        self.assertEqual((
            'Root: 72174f950289a454493d243bb72bdb76982e5f62\n'
            'Remote repo-in-local current=72174f staging=72174f desired=72174f\n'
            'Status of repo-in-local:\n'
            'DONE'),
            (await hoard_cmd.contents.differences("repo-in-local")).strip())

        os.mkdir(join(self.tmpdir.name, "repo", "newdir"))
        write_contents(join(self.tmpdir.name, "repo", "newdir", "newfile.is"), "lhiWFELHFE")
        os.remove(join(self.tmpdir.name, "repo", "wat", 'test.me.different'))

        res = await cave_cmd.status()
        self.assertEqual(
            f"{cave_cmd.current_uuid()} [72174f950289a454493d243bb72bdb76982e5f62]:\n"
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
            f"{cave_cmd.current_uuid()} [72174f950289a454493d243bb72bdb76982e5f62]:\n"
            "files:\n"
            "    same: 2 (66.7%)\n"
            "     mod: 0 (0.0%)\n"
            "     new: 1 (33.3%)\n"
            "   moved: 0 (0.0%)\n"
            " current: 3\n"
            " in repo: 3\n"
            " deleted: 1 (33.3%)\n", res)

        # as is not refreshed, no change in status
        self.assertEqual((
            'Root: 72174f950289a454493d243bb72bdb76982e5f62\n'
            'Remote repo-in-local current=72174f staging=72174f desired=72174f\n'
            'Status of repo-in-local:\n'
            'DONE'),
            (await hoard_cmd.contents.differences("repo-in-local")).strip())

        await cave_cmd.refresh(show_details=False)
        self.assertEqual((
            'Root: 72174f950289a454493d243bb72bdb76982e5f62\n'
            'Remote repo-in-local current=72174f staging=72174f desired=72174f\n'
            'Status of repo-in-local:\n'
            'PRESENT /newdir/newfile.is\n'
            'DELETED /wat/test.me.different\n'
            'DONE'),
            (await hoard_cmd.contents.differences("repo-in-local")).strip())

        res = await hoard_cmd.contents.pull("repo-in-local")
        self.assertEqual((
            'Pulling repo-in-local...\n'
            'Before: Hoard [72174f] <- repo [curr: 72174f, stg: 4504c1, des: 72174f]\n'
            'REPO_MARK_FILE_AVAILABLE /newdir/newfile.is\n'
            'HOARD_FILE_ADDED /newdir/newfile.is\n'
            'REPO_FILE_TO_DELETE /wat/test.me.different\n'
            'HOARD_FILE_DELETED /wat/test.me.different\n'
            'updated repo-in-local from 72174f to 4504c1\n'
            'After: Hoard [4504c1], repo [curr: 4504c1, stg: 4504c1, des: 4504c1]\n'
            "Sync'ed repo-in-local to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.tree_differences("repo-in-local")
        self.assertEqual(['Tree Differences up to level 3:', 'DONE'], res.splitlines())

        self.assertEqual((
            'Root: 4504c1f3941271cfc96da6bcf8d5f4198c2f4132\n'
            'Remote repo-in-local current=4504c1 staging=4504c1 desired=4504c1\n'
            'Status of repo-in-local:\n'
            'DONE'),
            (await hoard_cmd.contents.differences("repo-in-local")).strip())

    async def test_clone(self):
        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        await hoard_cmd.init()

        new_repo_path = join(self.tmpdir.name, "cloned-repo")
        os.mkdir(new_repo_path)

        res = await hoard_cmd.clone(to_path=new_repo_path, mount_at="/wat", name="cloned-repo")
        self.assertEqual("DONE", res)

        new_uuid = resolve_remote_uuid(hoard_cmd.hoard.config(), "cloned-repo")

        res = await hoard_cmd.health()
        self.assertEqual(
            "Health stats:\n"
            "1 total remotes.\n"
            f"  [cloned-repo]: 0 with no other copy\n"
            "Hoard health stats:\n"
            'Fasthash health stats:\n'
            ' #existing fasthashes = 0\n'
            ' #hoard fasthashes = 0\n'
            ' #existing but not in hoard: 0\n'
            ' #hoard but not existing: 0\n'
            "DONE", res)

        res = await hoard_cmd.contents.differences(new_uuid)
        self.assertEqual((
            'Root: None\n'
            'Remote cloned-repo current=None staging=None desired=None\n'
            'Status of cloned-repo:\n'
            'DONE'), res)

    async def test_populate_one_repo_from_other_repo(self):
        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        await hoard_cmd.init()

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
        self.assertEqual((
            'Root: None\n'
            'Remote cloned-repo current=None staging=None desired=None\n'
            'Remote repo-in-local current=None staging=None desired=None\n'
            'Status of cloned-repo:\n'
            'DONE'), res)

        await hoard_cmd.contents.pull("repo-in-local")

        # after population by other repo, it is now lacking files
        res = await hoard_cmd.contents.differences(new_uuid)
        self.assertEqual((
            'Root: 72174f950289a454493d243bb72bdb76982e5f62\n'
            'Remote cloned-repo current=None staging=None desired=72174f\n'
            'Remote repo-in-local current=72174f staging=72174f desired=72174f\n'
            'Status of cloned-repo:\n'
            'MISSING /wat/test.me.different\n'
            'MISSING /wat/test.me.once\n'
            'MISSING /wat/test.me.twice\n'
            'DONE'), res)

        res = await hoard_cmd.contents.tree_differences(new_uuid)
        self.assertEqual([
            'Tree Differences up to level 3:',
            '/[D]: GET: 3',
            ' wat[D]: GET: 3',
            '  test.me.different: GET: 1',
            '  test.me.once: GET: 1',
            '  test.me.twice: GET: 1',
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.pending_pull(new_uuid)
        self.assertEqual([
            'Status of cloned-repo:',
            'Hoard root: 72174f950289a454493d243bb72bdb76982e5f62:',
            'Repo current=None staging=654b8b desired=72174f',
            'Repo root: a80f91bc48850a1fb3459bb76b9f6308d4d35710:',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.different',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.once',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.twice'], res.splitlines())

        res = await hoard_cmd.files.push(repo="cloned-repo")
        self.assertEqual((
            'Before push:\n'
            'Remote cloned-repo current=None staging=None desired=72174f\n'
            'Remote repo-in-local current=72174f staging=72174f desired=72174f\n'
            'cloned-repo:\n'
            'REMOTE_COPY [repo-in-local] test.me.different\n'
            'REMOTE_COPY [repo-in-local] test.me.once\n'
            'REMOTE_COPY [repo-in-local] test.me.twice\n'
            'cloned-repo:\n'
            'After:\n'
            'Remote cloned-repo current=72174f staging=None desired=72174f\n'
            'Remote repo-in-local current=72174f staging=72174f desired=72174f\n'
            'DONE'), res.strip())

        res = await cloned_cave_cmd.refresh(show_details=False)
        self.assertEqual((
            'old: a80f91bc48850a1fb3459bb76b9f6308d4d35710\n'
            'current: ff1444e1c29844fddb93d7745ea1348ead80d0b6\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.differences(new_uuid)
        self.assertEqual((
            'Root: 72174f950289a454493d243bb72bdb76982e5f62\n'
            'Remote cloned-repo current=72174f staging=None desired=72174f\n'
            'Remote repo-in-local current=72174f staging=72174f desired=72174f\n'
            'Status of cloned-repo:\n'
            'DONE'), res.strip())

        res = await hoard_cmd.contents.pending_pull(new_uuid)
        self.assertEqual([
            'Status of cloned-repo:',
            'Hoard root: 72174f950289a454493d243bb72bdb76982e5f62:',
            'Repo current=72174f staging=72174f desired=72174f',
            'Repo root: ff1444e1c29844fddb93d7745ea1348ead80d0b6:'], res.splitlines())

        res = await hoard_cmd.files.push(repo="cloned-repo")
        self.assertEqual((
            'Before push:\n'
            'Remote cloned-repo current=72174f staging=None desired=72174f\n'
            'Remote repo-in-local current=72174f staging=72174f desired=72174f\n'
            'cloned-repo:\n'
            'cloned-repo:\n'
            'After:\n'
            'Remote cloned-repo current=72174f staging=None desired=72174f\n'
            'Remote repo-in-local current=72174f staging=72174f desired=72174f\n'
            'DONE'), res.strip())

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
            'Remote repo-backup-name current=None staging=None desired=None\n'
            'Remote repo-full-name current=None staging=None desired=None\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=None staging=None desired=None\n'
            'Status of repo-partial-name:\n'
            'PRESENT /test.me.1\n'
            'PRESENT /wat/test.me.2\n'
            'DONE'), res.strip())

        res = await hoard_cmd.contents.pull("repo-partial-name")
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [None] <- repo [curr: None, stg: f9bfc2, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'HOARD_FILE_ADDED /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'HOARD_FILE_ADDED /wat/test.me.2\n'
            'updated repo-partial-name from None to f9bfc2\n'
            'updated repo-full-name from None to f9bfc2\n'
            'updated repo-backup-name from None to f9bfc2\n'
            'After: Hoard [f9bfc2], repo [curr: f9bfc2, stg: f9bfc2, des: f9bfc2]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res.strip())

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual((
            'Root: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe\n'
            'Remote repo-backup-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-full-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            '/test.me.1 = a:1 g:2\n'
            '/wat/test.me.2 = a:1 g:2\n'
            'DONE'), res)

        res = await hoard_cmd.contents.pull("repo-partial-name", ignore_epoch=True)  # does noting...
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [f9bfc2] <- repo [curr: f9bfc2, stg: f9bfc2, des: f9bfc2]\n'
            'After: Hoard [f9bfc2], repo [curr: f9bfc2, stg: f9bfc2, des: f9bfc2]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res.strip())

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual((
            'Root: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe\n'
            'Remote repo-backup-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-full-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            '/test.me.1 = a:1 g:2\n'
            '/wat/test.me.2 = a:1 g:2\n'
            'DONE'), res)

        res = await hoard_cmd.contents.pull("repo-full-name")
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [f9bfc2] <- repo [curr: None, stg: 1ad9e0, des: f9bfc2]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.4\n'
            'HOARD_FILE_ADDED /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3\n'
            'HOARD_FILE_ADDED /wat/test.me.3\n'
            'updated repo-full-name from f9bfc2 to 1ad9e0\n'
            'updated repo-backup-name from f9bfc2 to 1ad9e0\n'
            'After: Hoard [1ad9e0], repo [curr: 1ad9e0, stg: 1ad9e0, des: 1ad9e0]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res.strip())

        res = await hoard_cmd.contents.tree_differences("repo-full-name")
        self.assertEqual(['Tree Differences up to level 3:', 'DONE'], res.splitlines())

        res = await hoard_cmd.contents.pull("repo-full-name", ignore_epoch=True)  # does nothing ...
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [1ad9e0] <- repo [curr: 1ad9e0, stg: 1ad9e0, des: 1ad9e0]\n'
            'After: Hoard [1ad9e0], repo [curr: 1ad9e0, stg: 1ad9e0, des: 1ad9e0]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res.strip())

        res = await hoard_cmd.contents.pull("repo-backup-name")  # just registers the files already in backup
        self.assertEqual((
            'Pulling repo-backup-name...\n'
            'Before: Hoard [1ad9e0] <- repo [curr: None, stg: 3a0889, des: 1ad9e0]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_DESIRED_FILE_TO_GET /test.me.4\n'
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.2\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3\n'
            'After: Hoard [1ad9e0], repo [curr: 3a0889, stg: 3a0889, des: 1ad9e0]\n'
            "Sync'ed repo-backup-name to hoard!\n"
            'DONE'), res.strip())

        res = await hoard_cmd.contents.pull("repo-backup-name")  # does nothing
        self.assertEqual('Skipping update as repo-backup-name.staging has not changed: 3a0889\nDONE', res.strip())

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual((
            'Root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'Remote repo-backup-name current=3a0889 staging=3a0889 desired=1ad9e0\n'
            'Remote repo-full-name current=1ad9e0 staging=1ad9e0 desired=1ad9e0\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            '/test.me.1 = a:3\n'
            '/test.me.4 = a:1 g:1\n'
            '/wat/test.me.2 = a:2 g:1\n'
            '/wat/test.me.3 = a:2\n'
            'DONE'), res)

        res = await hoard_cmd.contents.pull("repo-incoming-name")
        self.assertEqual((
            'Pulling repo-incoming-name...\n'
            'Before: Hoard [1ad9e0] <- repo [curr: None, stg: 3d1726, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.5\n'
            'HOARD_FILE_ADDED /test.me.5\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.6\n'
            'HOARD_FILE_ADDED /wat/test.me.6\n'
            'updated repo-full-name from 1ad9e0 to 8da760\n'
            'updated repo-backup-name from 1ad9e0 to 8da760\n'
            'After: Hoard [8da760], repo [curr: 3d1726, stg: 3d1726, des: None]\n'
            "Sync'ed repo-incoming-name to hoard!\n"
            'DONE'), res.strip())

        res = await hoard_cmd.contents.tree_differences("repo-incoming-name")
        self.assertEqual([
            'Tree Differences up to level 3:',
            '/[D]: DELETE: 4',
            ' test.me.4: DELETE: 1',
            ' test.me.5: DELETE: 1',
            ' wat[D]: DELETE: 2',
            '  test.me.3: DELETE: 1',
            '  test.me.6: DELETE: 1',
            'DONE'], res.splitlines())

        res = await incoming_cave_cmd.refresh(show_details=False)
        self.assertEqual((
            'old: 3d1726bd296f20d36cb9df60a0da4d4feae29248\n'
            'current: 3d1726bd296f20d36cb9df60a0da4d4feae29248\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull("repo-incoming-name", ignore_epoch=True)
        self.assertEqual((
            'Pulling repo-incoming-name...\n'
            'Before: Hoard [8da760] <- repo [curr: 3d1726, stg: 3d1726, des: None]\n'
            'REPO_FILE_TO_DELETE /test.me.4\n'
            'REPO_FILE_TO_DELETE /test.me.5\n'
            'REPO_FILE_TO_DELETE /wat/test.me.3\n'
            'REPO_FILE_TO_DELETE /wat/test.me.6\n'
            'After: Hoard [8da760], repo [curr: 3d1726, stg: 3d1726, des: None]\n'
            "Sync'ed repo-incoming-name to hoard!\n"
            'DONE'), res.strip())

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual((
            'Root: 8da76083b9eab9f49945d8f2487df38ab909b7df\n'
            'Remote repo-backup-name current=3a0889 staging=3a0889 desired=8da760\n'
            'Remote repo-full-name current=1ad9e0 staging=1ad9e0 desired=8da760\n'
            'Remote repo-incoming-name current=3d1726 staging=3d1726 desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            '/test.me.1 = a:3\n'
            '/test.me.4 = a:1 g:1 c:1\n'
            '/test.me.5 = g:2 c:1\n'
            '/wat/test.me.2 = a:2 g:1\n'
            '/wat/test.me.3 = a:2 c:1\n'
            '/wat/test.me.6 = g:2 c:1\n'
            'DONE'), res)

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
            'Before: Hoard [1ad9e0] <- repo [curr: None, stg: 3d1726, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.5\n'
            'HOARD_FILE_ADDED /test.me.5\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.6\n'
            'HOARD_FILE_ADDED /wat/test.me.6\n'
            'updated repo-full-name from 1ad9e0 to 8da760\n'
            'updated repo-backup-name from 1ad9e0 to 8da760\n'
            'After: Hoard [8da760], repo [curr: 3d1726, stg: 3d1726, des: None]\n'
            "Sync'ed repo-incoming-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual((
            'Root: 8da76083b9eab9f49945d8f2487df38ab909b7df\n'
            'Remote repo-backup-name current=3a0889 staging=3a0889 desired=8da760\n'
            'Remote repo-full-name current=1ad9e0 staging=1ad9e0 desired=8da760\n'
            'Remote repo-incoming-name current=3d1726 staging=3d1726 desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            '/test.me.1 = a:3\n'
            '/test.me.4 = a:1 g:1 c:1\n'
            '/test.me.5 = g:2 c:1\n'
            '/wat/test.me.2 = a:2 g:1\n'
            '/wat/test.me.3 = a:2 c:1\n'
            '/wat/test.me.6 = g:2 c:1\n'
            'DONE'), res)

        self.assertEqual([
            'repo-partial/test.me.1',
            'repo-partial/wat/test.me.2', ], dump_file_list(self.tmpdir.name, 'repo-partial'))

        res = await hoard_cmd.files.push("repo-full-name")
        self.assertEqual((
            'Before push:\n'
            'Remote repo-backup-name current=3a0889 staging=3a0889 desired=8da760\n'
            'Remote repo-full-name current=1ad9e0 staging=1ad9e0 desired=8da760\n'
            'Remote repo-incoming-name current=3d1726 staging=3d1726 desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            'repo-full-name:\n'
            'REMOTE_COPY [repo-incoming-name] test.me.5\n'
            'REMOTE_COPY [repo-incoming-name] wat/test.me.6\n'
            'repo-full-name:\n'
            'After:\n'
            'Remote repo-backup-name current=3a0889 staging=3a0889 desired=8da760\n'
            'Remote repo-full-name current=8da760 staging=1ad9e0 desired=8da760\n'
            'Remote repo-incoming-name current=3d1726 staging=3d1726 desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            'DONE'), res)

        self.assertEqual([
            'repo-full/test.me.1',
            'repo-full/test.me.4',
            'repo-full/test.me.5',
            'repo-full/wat/test.me.2',
            'repo-full/wat/test.me.3',
            'repo-full/wat/test.me.6'], dump_file_list(self.tmpdir.name, 'repo-full'))

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual((
            'Root: 8da76083b9eab9f49945d8f2487df38ab909b7df\n'
            'Remote repo-backup-name current=3a0889 staging=3a0889 desired=8da760\n'
            'Remote repo-full-name current=8da760 staging=1ad9e0 desired=8da760\n'
            'Remote repo-incoming-name current=3d1726 staging=3d1726 desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            '/test.me.1 = a:3\n'
            '/test.me.4 = a:1 g:1 c:1\n'
            '/test.me.5 = a:1 g:1 c:1\n'
            '/wat/test.me.2 = a:2 g:1\n'
            '/wat/test.me.3 = a:2 c:1\n'
            '/wat/test.me.6 = a:1 g:1 c:1\n'
            'DONE'), res)

        res = await full_cave_cmd.refresh(show_details=False)
        self.assertEqual((
            'old: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'current: 8da76083b9eab9f49945d8f2487df38ab909b7df\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.differences("repo-full-name")
        self.assertEqual((
            'Root: 8da76083b9eab9f49945d8f2487df38ab909b7df\n'
            'Remote repo-backup-name current=3a0889 staging=3a0889 desired=8da760\n'
            'Remote repo-full-name current=8da760 staging=1ad9e0 desired=8da760\n'
            'Remote repo-incoming-name current=3d1726 staging=3d1726 desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            'Status of repo-full-name:\n'
            'DONE'), res)

    async def test_pull_all(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = await init_complex_hoard(
            self.tmpdir.name)

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [None] <- repo [curr: None, stg: f9bfc2, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'HOARD_FILE_ADDED /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'HOARD_FILE_ADDED /wat/test.me.2\n'
            'updated repo-partial-name from None to f9bfc2\n'
            'updated repo-full-name from None to f9bfc2\n'
            'updated repo-backup-name from None to f9bfc2\n'
            'After: Hoard [f9bfc2], repo [curr: f9bfc2, stg: f9bfc2, des: f9bfc2]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'Pulling repo-full-name...\n'
            'Before: Hoard [f9bfc2] <- repo [curr: None, stg: 1ad9e0, des: f9bfc2]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.4\n'
            'HOARD_FILE_ADDED /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3\n'
            'HOARD_FILE_ADDED /wat/test.me.3\n'
            'updated repo-full-name from f9bfc2 to 1ad9e0\n'
            'updated repo-backup-name from f9bfc2 to 1ad9e0\n'
            'After: Hoard [1ad9e0], repo [curr: 1ad9e0, stg: 1ad9e0, des: 1ad9e0]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'Pulling repo-backup-name...\n'
            'Before: Hoard [1ad9e0] <- repo [curr: None, stg: 3a0889, des: 1ad9e0]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_DESIRED_FILE_TO_GET /test.me.4\n'
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.2\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3\n'
            'After: Hoard [1ad9e0], repo [curr: 3a0889, stg: 3a0889, des: 1ad9e0]\n'
            "Sync'ed repo-backup-name to hoard!\n"
            'Pulling repo-incoming-name...\n'
            'Before: Hoard [1ad9e0] <- repo [curr: None, stg: 3d1726, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.5\n'
            'HOARD_FILE_ADDED /test.me.5\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.6\n'
            'HOARD_FILE_ADDED /wat/test.me.6\n'
            'updated repo-full-name from 1ad9e0 to 8da760\n'
            'updated repo-backup-name from 1ad9e0 to 8da760\n'
            'After: Hoard [8da760], repo [curr: 3d1726, stg: 3d1726, des: None]\n'
            "Sync'ed repo-incoming-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.tree_differences("repo-partial-name")
        self.assertEqual(['Tree Differences up to level 3:', 'DONE'], res.splitlines())

        res = await hoard_cmd.contents.tree_differences("repo-full-name")
        self.assertEqual([
            'Tree Differences up to level 3:',
            '/[D]: GET: 2',
            ' test.me.5: GET: 1',
            ' wat[D]: GET: 1',
            '  test.me.6: GET: 1',
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.status(hide_disk_sizes=True)
        self.assertEqual([
            'Root: 8da76083b9eab9f49945d8f2487df38ab909b7df',
            '|Num Files           |             updated|total  |availab|get    |copy   '
            '|cleanup|reserve|',
            '|repo-backup-name    |                 now|      6|      2|      4|      '
            '4|       |       |',
            '|repo-full-name      |                 now|      6|      4|      2|      '
            '2|       |       |',
            '|repo-incoming-name  |                 now|      4|       |       |       '
            '|      4|      3|',
            '|repo-partial-name   |                 now|      2|      2|       |       '
            '|       |       |',
            '',
            '|Size                |             updated|total  |availab|get    |copy   '
            '|cleanup|reserve|',
            '|repo-backup-name    |                 now|     47|     16|     31|     '
            '31|       |       |',
            '|repo-full-name      |                 now|     47|     35|     12|     '
            '12|       |       |',
            '|repo-incoming-name  |                 now|     33|       |       |       '
            '|     33|     23|',
            '|repo-partial-name   |                 now|     14|     14|       |       '
            '|       |       |'],
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
            'Root: 8da76083b9eab9f49945d8f2487df38ab909b7df',
            '|Num Files           |total  |availab|get    |copy   |cleanup|reserve|',
            '|repo-backup-name    |      6|      2|      4|      4|       |       |',
            '|repo-full-name      |      6|      4|      2|      2|       |       |',
            '|repo-incoming-name  |      4|       |       |       |      4|      3|',
            '|repo-partial-name   |      2|      2|       |       |       |       |',
            '',
            '|Size                |total  |availab|get    |copy   |cleanup|reserve|',
            '|repo-backup-name    |     47|     16|     31|     31|       |       |',
            '|repo-full-name      |     47|     35|     12|     12|       |       |',
            '|repo-incoming-name  |     33|       |       |       |     33|     23|',
            '|repo-partial-name   |     14|     14|       |       |       |       |'],
            res.splitlines())

        res = await hoard_cmd.files.push(all=True)
        self.assertEqual([
            'Before push:',
            'Remote repo-backup-name current=3a0889 staging=3a0889 desired=8da760',
            'Remote repo-full-name current=1ad9e0 staging=1ad9e0 desired=8da760',
            'Remote repo-incoming-name current=3d1726 staging=3d1726 desired=None',
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2',
            'repo-partial-name:',
            'repo-full-name:',
            'REMOTE_COPY [repo-incoming-name] test.me.5',
            'REMOTE_COPY [repo-incoming-name] wat/test.me.6',
            'repo-backup-name:',
            'REMOTE_COPY [repo-full-name] test.me.4',
            'REMOTE_COPY [repo-incoming-name] test.me.5',
            'REMOTE_COPY [repo-partial-name] wat/test.me.2',
            'REMOTE_COPY [repo-incoming-name] wat/test.me.6',
            'repo-incoming-name:',
            'repo-partial-name:',
            'repo-full-name:',
            'repo-backup-name:',
            'repo-incoming-name:',
            'd test.me.4',
            'd test.me.5',
            'd wat/test.me.3',
            'd wat/test.me.6',
            'After:',
            'Remote repo-backup-name current=8da760 staging=3a0889 desired=8da760',
            'Remote repo-full-name current=8da760 staging=1ad9e0 desired=8da760',
            'Remote repo-incoming-name current=None staging=3d1726 desired=None',
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2',
            'DONE'], res.splitlines())

        self.assertEqual([
            'repo-partial/test.me.1',
            'repo-partial/wat/test.me.2'],
            dump_file_list(self.tmpdir.name, 'repo-partial'))

        res = await hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual((
            'Root: 8da76083b9eab9f49945d8f2487df38ab909b7df\n'
            'Remote repo-backup-name current=8da760 staging=3a0889 desired=8da760\n'
            'Remote repo-full-name current=8da760 staging=1ad9e0 desired=8da760\n'
            'Remote repo-incoming-name current=None staging=3d1726 desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            '/test.me.1 = a:3\n'
            '/test.me.4 = a:2\n'
            '/test.me.5 = a:2\n'
            '/wat/test.me.2 = a:3\n'
            '/wat/test.me.3 = a:2\n'
            '/wat/test.me.6 = a:2\n'
            'DONE'), res)

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

        res = await hoard_cmd.health()
        self.assertEqual((
            'Health stats:\n'
            '4 total remotes.\n'
            '  [repo-partial-name]: 0 with no other copy\n'
            '  [repo-full-name]: 0 with no other copy\n'
            '  [repo-backup-name]: 0 with no other copy\n'
            '  [repo-incoming-name]: 0 with no other copy\n'
            'Hoard health stats:\n'
            '  2 copies: 4 files\n'
            '  3 copies: 2 files\n'
            'Fasthash health stats:\n'
            ' #existing fasthashes = 6\n'
            '  len 32 -> 12\n'
            ' #hoard fasthashes = 6\n'
            '  len 32 -> 12\n'
            ' #existing but not in hoard: 0\n'
            ' #hoard but not existing: 0\n'
            '  2 copies - 4 hashes, space est: 66 = 4 x 2 x (5 ~ 11)\n'
            '  3 copies - 2 hashes, space est: 42 = 2 x 3 x (6 ~ 8)\n'
            'DONE'), res)

    async def test_partial_cloning(self):
        populate_repotypes(self.tmpdir.name)
        pfw = pretty_file_writer(self.tmpdir.name)
        pfw("repo-full/wat/inner/another.file", "asdafaqw")

        full_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo-full")).cave
        full_cave_cmd.init()
        await full_cave_cmd.refresh(show_details=False)

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        await hoard_cmd.init()

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-full"), name="repo-full-name", mount_point="/",
            type=CaveType.PARTIAL, fetch_new=True)

        res = await hoard_cmd.contents.pull("repo-full-name")
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [None] <- repo [curr: None, stg: d48f4e, des: None]\n'
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
            'updated repo-full-name from None to d48f4e\n'
            'After: Hoard [d48f4e], repo [curr: d48f4e, stg: d48f4e, des: d48f4e]\n'
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
        self.assertEqual((
            'Root: d48f4edfef726ccfebd58e8c7061e5f18c815734\n'
            'Remote repo-cloned-wat current=None staging=None desired=31a330\n'
            'Remote repo-full-name current=d48f4e staging=d48f4e desired=d48f4e\n'
            '/ => (repo-full-name:.)\n'
            '/test.me.1 = a:1\n'
            '/test.me.4 = a:1\n'
            '/wat => (repo-cloned-wat:.), (repo-full-name:wat)\n'
            '/wat/test.me.2 = a:1\n'
            '/wat/test.me.3 = a:1\n'
            '/wat/inner => (repo-cloned-wat:inner), (repo-full-name:wat/inner)\n'
            '/wat/inner/another.file = a:1 g:1\n'
            'DONE'), res)

        self.assertEqual([], dump_file_list(self.tmpdir.name, "repo-cloned-wat/"))  # no files yet

        res = await hoard_cmd.files.push("repo-cloned-wat")
        self.assertEqual((
            'Before push:\n'
            'Remote repo-cloned-wat current=None staging=None desired=31a330\n'
            'Remote repo-full-name current=d48f4e staging=d48f4e desired=d48f4e\n'
            'repo-cloned-wat:\n'
            'REMOTE_COPY [repo-full-name] inner/another.file\n'
            'repo-cloned-wat:\n'
            'After:\n'
            'Remote repo-cloned-wat current=31a330 staging=None desired=31a330\n'
            'Remote repo-full-name current=d48f4e staging=d48f4e desired=d48f4e\n'
            'DONE'), res)

        self.assertEqual(
            ['repo-cloned-wat/inner/another.file'],
            dump_file_list(self.tmpdir.name, "repo-cloned-wat/"))

        res = await cloned_cave_cmd.refresh(show_details=False)
        self.assertEqual((
            'old: a80f91bc48850a1fb3459bb76b9f6308d4d35710\n'
            'current: 844f930bec57c669128c27975d86a5d81bd51f47\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.get(repo="repo-cloned-wat", path="")
        self.assertEqual(
            "+/wat/test.me.2\n"
            "+/wat/test.me.3\n"
            "Considered 3 files.\nDONE", res)

        res = await hoard_cmd.files.push("repo-cloned-wat")
        self.assertEqual((
            'Before push:\n'
            'Remote repo-cloned-wat current=31a330 staging=None desired=8ab884\n'
            'Remote repo-full-name current=d48f4e staging=d48f4e desired=d48f4e\n'
            'repo-cloned-wat:\n'
            'REMOTE_COPY [repo-full-name] test.me.2\n'
            'REMOTE_COPY [repo-full-name] test.me.3\n'
            'repo-cloned-wat:\n'
            'After:\n'
            'Remote repo-cloned-wat current=8ab884 staging=None desired=8ab884\n'
            'Remote repo-full-name current=d48f4e staging=d48f4e desired=d48f4e\n'
            'DONE'), res)

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
        await hoard_cmd.init()

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-partial"), name="repo-partial-name", mount_point="/first-point",
            type=CaveType.PARTIAL, fetch_new=True)

        res = await hoard_cmd.contents.pull("repo-partial-name")
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [None] <- repo [curr: None, stg: cfe6f4, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /first-point/test.me.1\n'
            'HOARD_FILE_ADDED /first-point/test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /first-point/wat/test.me.2\n'
            'HOARD_FILE_ADDED /first-point/wat/test.me.2\n'
            'updated repo-partial-name from None to cfe6f4\n'
            'After: Hoard [cfe6f4], repo [curr: cfe6f4, stg: cfe6f4, des: cfe6f4]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res.strip())

        res = await hoard_cmd.contents.tree_differences("repo-partial-name")
        self.assertEqual(['Tree Differences up to level 3:', 'DONE'], res.splitlines())

        res = hoard_cmd.remotes(hide_paths=True)
        self.assertEqual(
            f"1 total remotes.\n"
            f"  [repo-partial-name] {partial_cave_cmd.current_uuid()} (partial)\n"
            "Mounts:\n"
            "  /first-point -> repo-partial-name\n"
            "DONE", res.strip())

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual((
            'Root: cfe6f4f9842fe5cd83051d49aa307bc40ed10084\n'
            'Remote repo-partial-name current=cfe6f4 staging=cfe6f4 desired=cfe6f4\n'
            '/\n'
            '/first-point => (repo-partial-name:.)\n'
            '/first-point/test.me.1 = a:1\n'
            '/first-point/wat => (repo-partial-name:wat)\n'
            '/first-point/wat/test.me.2 = a:1\n'
            'DONE'), res)

        res = await hoard_cmd.move_mounts(from_path="/first-point/inner", to_path="/cant-move-files")
        self.assertEqual(
            "Can't move /first-point/inner to /cant-move-files, requires moving files in repo-partial-name:inner.",
            res.strip())

        res = await hoard_cmd.move_mounts(from_path="/", to_path="/move-all-inside")
        self.assertEqual((
            'Moving files and folders:\n'
            'repo-partial-name.current: cfe6f4 => f38587\n'
            'repo-partial-name.staging: cfe6f4 => f38587\n'
            'repo-partial-name.desired: cfe6f4 => f38587\n'
            'HOARD.desired: cfe6f4 => f38587\n'
            '/first-point/test.me.1=>/move-all-inside/first-point/test.me.1\n'
            '/first-point/wat/test.me.2=>/move-all-inside/first-point/wat/test.me.2\n'
            'Moving 1 repos:\n'
            '[repo-partial-name] /first-point => /move-all-inside/first-point\n'
            'DONE'), res)

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual((
            'Root: f3858768bf4e1794b1a4dbffb40acd9d8044bab3\n'
            'Remote repo-partial-name current=f38587 staging=f38587 desired=f38587\n'
            '/\n'
            '/move-all-inside\n'
            '/move-all-inside/first-point => (repo-partial-name:.)\n'
            '/move-all-inside/first-point/test.me.1 = a:1\n'
            '/move-all-inside/first-point/wat => (repo-partial-name:wat)\n'
            '/move-all-inside/first-point/wat/test.me.2 = a:1\n'
            'DONE'), res)

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
        self.assertEqual((
            'Moving files and folders:\n'
            'repo-partial-name.current: f38587 => b65582\n'
            'repo-partial-name.staging: f38587 => b65582\n'
            'repo-partial-name.desired: f38587 => b65582\n'
            'HOARD.desired: f38587 => b65582\n'
            '/move-all-inside/first-point/test.me.1=>/moved-data/test.me.1\n'
            '/move-all-inside/first-point/wat/test.me.2=>/moved-data/wat/test.me.2\n'
            'Moving 1 repos:\n'
            '[repo-partial-name] /move-all-inside/first-point => /moved-data\n'
            'DONE'), res)

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual((
            'Root: b6558267774b76f40b7801ed1ed1ec84526c544c\n'
            'Remote repo-partial-name current=b65582 staging=b65582 desired=b65582\n'
            '/\n'
            '/moved-data => (repo-partial-name:.)\n'
            '/moved-data/test.me.1 = a:1\n'
            '/moved-data/wat => (repo-partial-name:wat)\n'
            '/moved-data/wat/test.me.2 = a:1\n'
            'DONE'), res)

        res = await hoard_cmd.move_mounts(from_path="/moved-data", to_path="/")
        self.assertEqual(
            "Moving files and folders:\n"
            'repo-partial-name.current: b65582 => f9bfc2\n'
            'repo-partial-name.staging: b65582 => f9bfc2\n'
            'repo-partial-name.desired: b65582 => f9bfc2\n'
            'HOARD.desired: b65582 => f9bfc2\n'
            "/moved-data/test.me.1=>/test.me.1\n"
            "/moved-data/wat/test.me.2=>/wat/test.me.2\n"
            "Moving 1 repos:\n"
            "[repo-partial-name] /moved-data => /\n"
            "DONE", res)

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual((
            'Root: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            '/ => (repo-partial-name:.)\n'
            '/test.me.1 = a:1\n'
            '/wat => (repo-partial-name:wat)\n'
            '/wat/test.me.2 = a:1\n'
            'DONE'), res)

        await partial_cave_cmd.refresh(show_details=False)

        res = await hoard_cmd.contents.pull("repo-partial-name", ignore_epoch=True)  # needs to do nothing
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [f9bfc2] <- repo [curr: f9bfc2, stg: f9bfc2, des: f9bfc2]\n'
            'After: Hoard [f9bfc2], repo [curr: f9bfc2, stg: f9bfc2, des: f9bfc2]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res.strip())

    async def test_restore_missing_local_file_on_refresh(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = await init_complex_hoard(
            self.tmpdir.name)

        res = await hoard_cmd.contents.pull("repo-partial-name")
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [None] <- repo [curr: None, stg: f9bfc2, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'HOARD_FILE_ADDED /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'HOARD_FILE_ADDED /wat/test.me.2\n'
            'updated repo-partial-name from None to f9bfc2\n'
            'updated repo-full-name from None to f9bfc2\n'
            'updated repo-backup-name from None to f9bfc2\n'
            'After: Hoard [f9bfc2], repo [curr: f9bfc2, stg: f9bfc2, des: f9bfc2]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.pull("repo-backup-name")
        self.assertEqual((
            'Pulling repo-backup-name...\n'
            'Before: Hoard [f9bfc2] <- repo [curr: None, stg: 3a0889, des: f9bfc2]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.2\n'
            'After: Hoard [f9bfc2], repo [curr: 3a0889, stg: 3a0889, des: f9bfc2]\n'
            "Sync'ed repo-backup-name to hoard!\n"
            'DONE'), res)

        self.assertEqual((
            'Root: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe\n'
            'Remote repo-backup-name current=3a0889 staging=3a0889 desired=f9bfc2\n'
            'Remote repo-full-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            '/\n'
            '/test.me.1 = a:2 g:1\n'
            '/wat\n'
            '/wat/test.me.2 = a:1 g:2\n'
            '/wat/test.me.3 = c:1\n'
            'DONE'), await hoard_cmd.contents.ls(depth=2))

        res = await hoard_cmd.files.push("repo-backup-name")
        self.assertEqual((
            'Before push:\n'
            'Remote repo-backup-name current=3a0889 staging=3a0889 desired=f9bfc2\n'
            'Remote repo-full-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            'repo-backup-name:\n'
            'REMOTE_COPY [repo-partial-name] wat/test.me.2\n'
            'repo-backup-name:\n'
            'd wat/test.me.3\n'
            'After:\n'
            'Remote repo-backup-name current=f9bfc2 staging=3a0889 desired=f9bfc2\n'
            'Remote repo-full-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            'DONE'), res.strip())

        self.assertEqual([
            'repo-backup/test.me.1',
            'repo-backup/wat/test.me.2'], dump_file_list(self.tmpdir.name, 'repo-backup'))

        self.assertEqual((
            'Root: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe\n'
            'Remote repo-backup-name current=f9bfc2 staging=3a0889 desired=f9bfc2\n'
            'Remote repo-full-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            '/\n'
            '/test.me.1 = a:2 g:1\n'
            '/wat\n'
            '/wat/test.me.2 = a:2 g:1\n'
            'DONE'), await hoard_cmd.contents.ls(depth=2))

        self.assertEqual([
            'repo-partial/test.me.1',
            'repo-partial/wat/test.me.2'],
            dump_file_list(self.tmpdir.name, 'repo-partial'))

        os.remove(join(self.tmpdir.name, 'repo-partial/wat/test.me.2'))

        res = await partial_cave_cmd.status()
        self.assertEqual(
            f"{partial_cave_cmd.current_uuid()} [f9bfc2be6cc201aa81b733b9d83c1030cc88bffe]:\n"
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
            'old: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe\n'
            'current: 57a93ffeacf20f3d2de15ebce34a47b20478eaf2\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull("repo-partial-name", force_fetch_local_missing=True)
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [f9bfc2] <- repo [curr: f9bfc2, stg: 57a93f, des: f9bfc2]\n'
            'After: Hoard [f9bfc2], repo [curr: 57a93f, stg: 57a93f, des: f9bfc2]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        self.assertEqual((
            'Root: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe\n'
            'Remote repo-backup-name current=f9bfc2 staging=3a0889 desired=f9bfc2\n'
            'Remote repo-full-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=57a93f staging=57a93f desired=f9bfc2\n'
            '/\n'
            '/test.me.1 = a:2 g:1\n'
            '/wat\n'
            '/wat/test.me.2 = a:1 g:2\n'
            'DONE'), await hoard_cmd.contents.ls(depth=2))

        res = await hoard_cmd.files.push("repo-partial-name")
        self.assertEqual((
            'Before push:\n'
            'Remote repo-backup-name current=f9bfc2 staging=3a0889 desired=f9bfc2\n'
            'Remote repo-full-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=57a93f staging=57a93f desired=f9bfc2\n'
            'repo-partial-name:\n'
            'REMOTE_COPY [repo-backup-name] wat/test.me.2\n'
            'repo-partial-name:\n'
            'After:\n'
            'Remote repo-backup-name current=f9bfc2 staging=3a0889 desired=f9bfc2\n'
            'Remote repo-full-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=57a93f desired=f9bfc2\n'
            'DONE'), res)

        self.assertEqual(
            ['repo-partial/test.me.1', 'repo-partial/wat/test.me.2'],
            dump_file_list(self.tmpdir.name, 'repo-partial'))

    async def test_pull_on_missing_contents_are_skipped(self):
        populate_repotypes(self.tmpdir.name)
        tmpdir = self.tmpdir.name

        hoard_cmd = TotalCommand(path=join(tmpdir, "hoard")).hoard
        await hoard_cmd.init()

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
        self.assertEqual((
            f'Repo repo-partial-name[{partial_cave_cmd.current_uuid()}] has no current contents available!\n'
            'DONE'), res)

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual((
            f'Repo repo-partial-name[{partial_cave_cmd.current_uuid()}] has no current contents available!\n'
            'DONE'), res)

        res = await partial_cave_cmd.refresh(show_details=False)
        self.assertEqual('old: None\ncurrent: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe\nRefresh done!', res)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [None] <- repo [curr: None, stg: f9bfc2, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'HOARD_FILE_ADDED /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'HOARD_FILE_ADDED /wat/test.me.2\n'
            'updated repo-partial-name from None to f9bfc2\n'
            'After: Hoard [f9bfc2], repo [curr: f9bfc2, stg: f9bfc2, des: f9bfc2]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        full_cave_cmd = TotalCommand(path=join(tmpdir, "repo-full")).cave
        full_cave_cmd.init()
        await full_cave_cmd.refresh(show_details=False)

        res = hoard_cmd.add_remote(
            remote_path=join(tmpdir, "repo-full"), name="repo-full-name", mount_point="/",
            type=CaveType.PARTIAL, fetch_new=True)
        self.assertEqual(f"Added repo-full-name[{full_cave_cmd.current_uuid()}] at {join(tmpdir, 'repo-full')}!", res)

        res = await hoard_cmd.contents.tree_differences("repo-full-name")
        self.assertEqual(['Tree Differences up to level 3:', 'DONE'], res.splitlines())

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual((
            'Skipping update as repo-partial-name.staging has not changed: f9bfc2\n'
            'Pulling repo-full-name...\n'
            'Before: Hoard [f9bfc2] <- repo [curr: None, stg: 1ad9e0, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.4\n'
            'HOARD_FILE_ADDED /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3\n'
            'HOARD_FILE_ADDED /wat/test.me.3\n'
            'updated repo-full-name from None to 1ad9e0\n'
            'After: Hoard [1ad9e0], repo [curr: 1ad9e0, stg: 1ad9e0, des: 1ad9e0]\n'
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
    await hoard_cmd.init()

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


class HoardFilesIterator(TreeGenerator[BlobObject, Tuple[str, HoardFileProps]]):
    def __init__(self, objects: Objects, parent: "HoardContents"):
        self.parent = parent
        self.objects = objects

    def compute_on_level(
            self, path: List[str], original: FastAssociation[StoredObject]
    ) -> Iterable[Tuple[FastPosixPath, HoardFileProps]]:
        path = FastPosixPath("/" + "/".join(path))
        file_obj: BlobObject | None = original.get_if_present("HOARD")

        if file_obj is None:
            # fixme this is the legacy case where we iterate over current but not desired files. remove!
            file_obj: BlobObject | None = next(
                (f for root_name, f in original.available_items() if f.object_type == ObjectType.BLOB), None)

        if not file_obj or file_obj.object_type != ObjectType.BLOB:
            logging.debug("Skipping path %s as it is not a BlobObject", path)
            return

        assert isinstance(file_obj, FileObject)
        yield path, HoardFileProps(self.parent, path, file_obj.size, file_obj.fasthash, by_root=original)

    def should_drill_down(self, path: List[str], trees: ByRoot[TreeObject], files: ByRoot[BlobObject]) -> bool:
        return True

    @staticmethod
    def DEPRECATED_all(parent: "HoardContents") -> Iterable[Tuple[FastPosixPath, HoardFileProps]]:
        hoard_root, root_ids = find_roots(parent)

        obj_ids = ByRoot(
            [name for name, _ in root_ids] + ["HOARD"],
            root_ids + [("HOARD", hoard_root)])

        with parent.env.objects(write=False) as objects:
            yield from HoardFilesIterator(objects, parent).execute(obj_ids=obj_ids)


def find_roots(parent: "HoardContents") -> (MaybeObjectID, List[Tuple[str, MaybeObjectID]]):
    roots = parent.env.roots(write=False)
    hoard_root = roots["HOARD"].desired
    all_roots = roots.all_roots
    with roots:
        root_data = [(r.name, r.load_from_storage) for r in all_roots]
    root_ids = sum(
        [[("current@" + name, data.current), ("desired@" + name, data.desired)] for name, data in root_data],
        # fixme should only iterate over desired files
        [])
    return hoard_root, root_ids
