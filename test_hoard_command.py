import os
import pathlib
import tempfile
import unittest
from os.path import join
from time import sleep
from typing import Tuple, List

from config import CaveType
from contents import HoardContents
from dragon import TotalCommand
from test_repo_command import populate, write_contents, pretty_file_writer


def populate_hoard(tmpdir: str):
    populate(tmpdir)

    os.mkdir(join(tmpdir, "hoard"))


class TestRepoCommand(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        populate_hoard(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_create_hoard(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        cave_cmd.refresh()

        repo_uuid = cave_cmd.current_uuid()

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()
        res = hoard_cmd.remotes()

        self.assertEqual("0 total remotes.\nMounts:\nDONE", res.strip())

        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local", mount_point="/")
        res = hoard_cmd.remotes()
        self.assertEqual(
            f"1 total remotes.\n"
            f"  [repo-in-local] {repo_uuid} (partial)\n"
            f"Mounts:\n"
            f"  / -> repo-in-local\n"
            f"DONE", res.strip())

    def test_sync_to_hoard(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        cave_cmd.refresh()

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()
        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local", mount_point="/")

        repo_uuid = cave_cmd.current_uuid()

        res = hoard_cmd.status("repo-in-local")
        self.assertEqual(
            f"Status of {repo_uuid}:\nA /wat/test.me.different\nA /wat/test.me.once\nA /wat/test.me.twice\nAF /wat\nDONE",
            res.strip())

        res = hoard_cmd.refresh("repo-in-local")
        self.assertEqual(
            "+/wat/test.me.different\n"
            "+/wat/test.me.once\n"
            "+/wat/test.me.twice\n"
            "Sync'ed repo-in-local to hoard!", res.strip())

        hoard_contents = HoardContents.load(hoard_cmd._hoard_contents_filename())
        self._assert_hoard_contents(
            hoard_contents,
            files_exp=[
                ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),
                ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                ('/wat/test.me.twice', 6, 1, '1881f6f9784fb08bf6690e9763b76ac3')],
            dirs_exp=["/wat"])

        res = hoard_cmd.status("repo-in-local")
        self.assertEqual(f"Status of {repo_uuid}:\nDF /wat\nDONE", res.strip())

    def _assert_hoard_contents(
            self, hoard_contents: HoardContents, files_exp: List[Tuple[str, int, int, str]], dirs_exp: List[str]):
        files = sorted(
            (f, prop.size, len(prop.available_at), prop.fasthash) for f, prop in hoard_contents.fsobjects.files.items())
        dirs = sorted(f for f, _ in hoard_contents.fsobjects.dirs.items())
        self.assertEqual(sorted(files_exp), sorted(files))
        self.assertEqual(sorted(dirs_exp), sorted(dirs))

    def test_sync_two_repos(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        cave_cmd.refresh()

        repo_uuid = cave_cmd.current_uuid()

        cave_cmd2 = TotalCommand(path=join(self.tmpdir.name, "repo-2")).cave
        cave_cmd2.init()
        cave_cmd2.refresh()
        repo_uuid2 = cave_cmd2.current_uuid()

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()
        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local", mount_point="/")
        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-2"), name="repo-in-local-2", type=CaveType.BACKUP,
            mount_point="/wat")

        hoard_cmd.refresh("repo-in-local")

        self._assert_hoard_contents(
            HoardContents.load(hoard_cmd._hoard_contents_filename()),
            files_exp=[
                ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),
                ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                ('/wat/test.me.twice', 6, 1, '1881f6f9784fb08bf6690e9763b76ac3')],
            dirs_exp=["/wat"])

        res = hoard_cmd.refresh("repo-in-local")
        self.assertEqual("Skipping update as past epoch 1 is not after hoard epoch 1", res)

        res = hoard_cmd.refresh("repo-in-local-2")
        self.assertEqual(
            "=/wat/test.me.twice\nSync'ed repo-in-local-2 to hoard!", res.strip())

        self._assert_hoard_contents(
            HoardContents.load(hoard_cmd._hoard_contents_filename()),
            files_exp=[
                ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),
                ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                ('/wat/test.me.twice', 6, 2, '1881f6f9784fb08bf6690e9763b76ac3')],
            dirs_exp=["/wat"])

        res = hoard_cmd.refresh("repo-in-local", ignore_epoch=True)
        self.assertEqual("Sync'ed repo-in-local to hoard!", res)
        self._assert_hoard_contents(
            HoardContents.load(hoard_cmd._hoard_contents_filename()),
            files_exp=[
                ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),  # retained only from repo
                ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                ('/wat/test.me.twice', 6, 2, '1881f6f9784fb08bf6690e9763b76ac3')],
            dirs_exp=["/wat"])

        res = hoard_cmd.status("repo-in-local-2")
        self.assertEqual(
            f"Status of {repo_uuid2}:\n"
            f"M /wat/test.me.different\n"
            f"D /wat/test.me.once\n"
            "DF /wat\n"
            f"DONE", res.strip())

        res = hoard_cmd.status("repo-in-local")
        self.assertEqual(f"Status of {repo_uuid}:\nDF /wat\nDONE", res.strip())

        res = hoard_cmd.remotes()
        self.assertEqual(
            f"2 total remotes.\n"
            f"  [repo-in-local] {repo_uuid} (partial)\n"
            f"  [repo-in-local-2] {repo_uuid2} (backup)\n"
            f"Mounts:\n"
            f"  / -> repo-in-local\n"
            f"  /wat -> repo-in-local-2\n"
            f"DONE",
            res.strip())

        res = hoard_cmd.health()
        self.assertEqual(
            "Health stats:\n2 total remotes.\n"
            f"  [repo-in-local] {repo_uuid}: 2 with no other copy\n"
            f"  [repo-in-local-2] {repo_uuid2}: 0 with no other copy\n"
            "Hoard health stats:\n"
            "  1 copies: 2 files\n"
            "  2 copies: 1 files\nDONE", res)

    def test_changing_data(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        cave_cmd.refresh()

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()
        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local", mount_point="/")

        repo_uuid = cave_cmd.current_uuid()
        hoard_cmd.refresh("repo-in-local")

        self.assertEqual(f"Status of {repo_uuid}:\nDF /wat\nDONE", hoard_cmd.status("repo-in-local").strip())

        os.mkdir(join(self.tmpdir.name, "repo", "newdir"))
        write_contents(join(self.tmpdir.name, "repo", "newdir", "newfile.is"), "lhiWFELHFE")
        os.remove(join(self.tmpdir.name, "repo", "wat", 'test.me.different'))

        # as is not refreshed, no change in status
        self.assertEqual(f"Status of {repo_uuid}:\nDF /wat\nDONE", hoard_cmd.status("repo-in-local").strip())

        cave_cmd.refresh()
        self.assertEqual(
            f"Status of {repo_uuid}:\nA /newdir/newfile.is\nD /wat/test.me.different\nAF /newdir\nDF /wat\nDONE",
            hoard_cmd.status("repo-in-local").strip())

        res = hoard_cmd.refresh("repo-in-local")
        self.assertEqual(
            "+/newdir/newfile.is\n"
            "-/wat/test.me.different\n"
            "Sync'ed repo-in-local to hoard!", res)

        self.assertEqual(
            f"Status of {repo_uuid}:\n"
            f"D /wat/test.me.different\n"
            "DF /wat\n"
            "DF /newdir\n"
            f"DONE", hoard_cmd.status("repo-in-local").strip())

    def test_clone(self):
        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()

        new_repo_path = join(self.tmpdir.name, "cloned-repo")
        os.mkdir(new_repo_path)

        res = hoard_cmd.clone(to_path=new_repo_path, mount_at="/wat", name="cloned-repo")
        self.assertEqual("DONE", res)

        new_uuid = hoard_cmd._resolve_remote_uuid("cloned-repo")

        res = hoard_cmd.health()
        self.assertEqual(
            "Health stats:\n"
            "1 total remotes.\n"
            f"  [cloned-repo] {new_uuid}: 0 with no other copy\n"
            "Hoard health stats:\n"
            "DONE", res)

        res = hoard_cmd.status(new_uuid)
        self.assertEqual(f"Status of {new_uuid}:\nDONE", res)

    def test_populate_one_repo_from_other_repo(self):
        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()

        new_repo_path = join(self.tmpdir.name, "cloned-repo")
        os.mkdir(new_repo_path)

        hoard_cmd.clone(to_path=new_repo_path, mount_at="/wat", name="cloned-repo", fetch_new=True)

        cloned_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "cloned-repo")).cave

        orig_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        orig_cave_cmd.init()
        orig_cave_cmd.refresh()

        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local", mount_point="/")

        # status should be still empty hoard
        new_uuid = hoard_cmd._resolve_remote_uuid("cloned-repo")
        res = hoard_cmd.status(new_uuid)
        self.assertEqual(f"Status of {new_uuid}:\nDONE", res)

        hoard_cmd.refresh("repo-in-local")

        # after population by other repo, it is now lacking files
        res = hoard_cmd.status(new_uuid)
        self.assertEqual(
            f"Status of {new_uuid}:\n"
            "D /wat/test.me.different\n"
            "D /wat/test.me.once\n"
            "D /wat/test.me.twice\n"
            "DF /wat\n"
            f"DONE", res)

        res = hoard_cmd.sync_contents(repo="cloned-repo")
        self.assertEqual(
            f"{cloned_cave_cmd.current_uuid()}:\n"
            "+ test.me.different\n"
            "+ test.me.once\n"
            "+ test.me.twice\n"
            f"{cloned_cave_cmd.current_uuid()}:\n"
            "DONE", res.strip())

        res = cloned_cave_cmd.refresh()
        self.assertEqual("Refresh done!", res)

        res = hoard_cmd.status(new_uuid)
        self.assertEqual(
            f"Status of {new_uuid}:\n"
            "DF /wat\n"
            f"DONE", res.strip())

        res = hoard_cmd.sync_contents(repo="cloned-repo")
        self.assertEqual(
            f"{cloned_cave_cmd.current_uuid()}:\n"
            f"{cloned_cave_cmd.current_uuid()}:\n"
            "DONE", res.strip())

        self.assertEqual([
            'cloned-repo/test.me.different',
            'cloned-repo/test.me.once',
            'cloned-repo/test.me.twice'], dump_file_list(self.tmpdir.name, "cloned-repo"))

    def _init_complex_hoard(self):
        partial_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo-partial")).cave
        partial_cave_cmd.init()
        partial_cave_cmd.refresh()

        full_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo-full")).cave
        full_cave_cmd.init()
        full_cave_cmd.refresh()

        backup_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo-backup")).cave
        backup_cave_cmd.init()
        backup_cave_cmd.refresh()

        incoming_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo-incoming")).cave
        incoming_cave_cmd.init()
        incoming_cave_cmd.refresh()

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-partial"), name="repo-partial-name", mount_point="/",
            type=CaveType.PARTIAL)

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-full"), name="repo-full-name", mount_point="/",
            type=CaveType.PARTIAL, fetch_new=True)

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-backup"), name="repo-backup-name", mount_point="/",
            type=CaveType.BACKUP)

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-incoming"), name="repo-incoming-name", mount_point="/",
            type=CaveType.INCOMING)

        res = hoard_cmd.remotes()
        self.assertEqual(
            "4 total remotes."
            f"\n  [repo-partial-name] {partial_cave_cmd.current_uuid()} (partial)"
            f"\n  [repo-full-name] {full_cave_cmd.current_uuid()} (partial)"
            f"\n  [repo-backup-name] {backup_cave_cmd.current_uuid()} (backup)"
            f"\n  [repo-incoming-name] {incoming_cave_cmd.current_uuid()} (incoming)"
            "\nMounts:"
            "\n  / -> repo-partial-name, repo-full-name, repo-backup-name, repo-incoming-name"
            "\nDONE", res.strip())

        return hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd

    def test_create_repo_types(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = self._init_complex_hoard()

    def test_sync_hoard_definitions(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = self._init_complex_hoard()

        res = hoard_cmd.status("repo-partial-name")
        self.assertEqual(
            f"Status of {partial_cave_cmd.current_uuid()}:\n"
            "A /test.me.1\n"
            "A /wat/test.me.2\n"
            "AF /wat\n"
            "DONE", res.strip())

        res = hoard_cmd.refresh("repo-partial-name")
        self.assertEqual("+/test.me.1\n+/wat/test.me.2\nSync'ed repo-partial-name to hoard!", res.strip())

        res = hoard_cmd.ls()
        self.assertEqual(
            "/test.me.1 = a:1 g:2\n"
            "/wat/test.me.2 = a:1 g:2\n"
            "DONE", res)

        res = hoard_cmd.refresh("repo-partial-name", ignore_epoch=True)  # does noting...
        self.assertEqual("Sync'ed repo-partial-name to hoard!", res.strip())

        res = hoard_cmd.ls()
        self.assertEqual(
            "/test.me.1 = a:1 g:2\n"
            "/wat/test.me.2 = a:1 g:2\n"
            "DONE", res)

        res = hoard_cmd.refresh("repo-full-name")
        self.assertEqual(
            "=/test.me.1\n"
            "+/test.me.4\n"
            "=/wat/test.me.2\n"
            "+/wat/test.me.3\n"
            "Sync'ed repo-full-name to hoard!", res.strip())

        res = hoard_cmd.refresh("repo-full-name", ignore_epoch=True)  # does nothing ...
        self.assertEqual("Sync'ed repo-full-name to hoard!", res.strip())

        res = hoard_cmd.refresh("repo-backup-name")  # just registers the files already in backup
        self.assertEqual(
            "=/test.me.1\n"
            "=/wat/test.me.3\n"
            "Sync'ed repo-backup-name to hoard!", res.strip())

        res = hoard_cmd.refresh("repo-backup-name")  # does nothing
        self.assertEqual("Skipping update as past epoch 1 is not after hoard epoch 1", res.strip())

        res = hoard_cmd.ls()
        self.assertEqual(
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:1 g:1\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.3 = a:2\n"
            "DONE", res)

        res = hoard_cmd.refresh("repo-incoming-name")
        self.assertEqual(
            "-/test.me.4\n"
            "<+/test.me.5\n"
            "u/wat/test.me.3\n"
            "<+/wat/test.me.6\n"
            "Sync'ed repo-incoming-name to hoard!", res.strip())

        res = incoming_cave_cmd.refresh()
        self.assertEqual("Refresh done!", res)

        res = hoard_cmd.refresh("repo-incoming-name")
        self.assertEqual(
            "-/test.me.4\n"
            "-/test.me.5\n"
            "-/wat/test.me.3\n"
            "-/wat/test.me.6\n"
            "Sync'ed repo-incoming-name to hoard!", res.strip())

        res = hoard_cmd.ls()
        self.assertEqual(
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:1 g:1 c:1\n"
            "/test.me.5 = g:2 c:1\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.3 = g:2 c:1\n"
            "/wat/test.me.6 = g:2 c:1\n"
            "DONE", res)

    def test_sync_hoard_file_contents_one(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = self._init_complex_hoard()

        hoard_cmd.refresh("repo-partial-name")
        hoard_cmd.refresh("repo-full-name")
        hoard_cmd.refresh("repo-backup-name")  # just registers the files already in backup
        hoard_cmd.refresh("repo-incoming-name")

        res = hoard_cmd.ls()
        self.assertEqual(
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:1 g:1 c:1\n"
            "/test.me.5 = g:2 c:1\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.3 = g:2 c:1\n"
            "/wat/test.me.6 = g:2 c:1\n"
            "DONE", res)

        self.assertEqual([
            'repo-partial/test.me.1',
            'repo-partial/wat/test.me.2', ], dump_file_list(self.tmpdir.name, 'repo-partial'))

        res = hoard_cmd.sync_contents("repo-full-name")
        self.assertEqual(
            f"{full_cave_cmd.current_uuid()}:\n"
            "+ test.me.5\n"
            "+ wat/test.me.3\n"
            "+ wat/test.me.6\n"
            f"{full_cave_cmd.current_uuid()}:\n"
            "DONE", res)

        self.assertEqual([
            'repo-full/test.me.1',
            'repo-full/test.me.4',
            'repo-full/test.me.5',
            'repo-full/wat/test.me.2',
            'repo-full/wat/test.me.3',
            'repo-full/wat/test.me.6'], dump_file_list(self.tmpdir.name, 'repo-full'))

        res = hoard_cmd.ls()
        self.assertEqual(
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:1 g:1 c:1\n"
            "/test.me.5 = a:1 g:1 c:1\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.3 = a:1 g:1 c:1\n"
            "/wat/test.me.6 = a:1 g:1 c:1\n"
            "DONE", res)

        res = full_cave_cmd.refresh()
        self.assertEqual("Refresh done!", res)

        res = hoard_cmd.status("repo-full-name")
        self.assertEqual(
            f"Status of {full_cave_cmd.current_uuid()}:\n"
            f"DF /wat\n"
            f"DONE", res)

    def test_sync_hoard_file_contents_all(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = self._init_complex_hoard()

        hoard_cmd.refresh("repo-partial-name")
        hoard_cmd.refresh("repo-full-name")
        hoard_cmd.refresh("repo-backup-name")  # just registers the files already in backup
        hoard_cmd.refresh("repo-incoming-name")

        res = hoard_cmd.sync_contents()
        self.assertEqual(
            f"{partial_cave_cmd.current_uuid()}:\n"
            f"{full_cave_cmd.current_uuid()}:\n"
            "+ test.me.5\n"
            "+ wat/test.me.3\n"
            "+ wat/test.me.6\n"
            f"{backup_cave_cmd.current_uuid()}:\n"
            "+ test.me.4\n"
            "+ test.me.5\n"
            "+ wat/test.me.2\n"
            "+ wat/test.me.3\n"
            "+ wat/test.me.6\n"
            f"{incoming_cave_cmd.current_uuid()}:\n"
            f"{partial_cave_cmd.current_uuid()}:\n"
            f"{full_cave_cmd.current_uuid()}:\n"
            f"{backup_cave_cmd.current_uuid()}:\n"
            f"{incoming_cave_cmd.current_uuid()}:\n"
            "c test.me.4\n"
            "c test.me.5\n"
            "c wat/test.me.3\n"
            "c wat/test.me.6\n"
            "DONE", res)

        self.assertEqual([
            'repo-partial/test.me.1',
            'repo-partial/wat/test.me.2'],
            dump_file_list(self.tmpdir.name, 'repo-partial'))

        res = hoard_cmd.ls()
        self.assertEqual(
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

    def test_partial_cloning(self):
        populate_repotypes(self.tmpdir.name)
        pfw = pretty_file_writer(self.tmpdir.name)
        pfw("repo-full/wat/inner/another.file", "asdafaqw")

        full_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo-full")).cave
        full_cave_cmd.init()
        full_cave_cmd.refresh()

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-full"), name="repo-full-name", mount_point="/",
            type=CaveType.PARTIAL, fetch_new=True)

        res = hoard_cmd.refresh("repo-full-name")
        self.assertEqual(
            "+/test.me.1\n"
            "+/test.me.4\n"
            "+/wat/test.me.2\n"
            "+/wat/test.me.3\n"
            "+/wat/inner/another.file\n"
            "Sync'ed repo-full-name to hoard!", res)

        os.mkdir(join(self.tmpdir.name, "repo-cloned-wat"))
        res = hoard_cmd.clone(
            to_path=join(self.tmpdir.name, "repo-cloned-wat"), mount_at="/wat", name="repo-cloned-wat", fetch_new=True)
        self.assertEqual("DONE", res)

        cloned_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo-cloned-wat")).cave

        res = hoard_cmd.enable_content(repo="repo-cloned-wat", path="inner")
        self.assertEqual("+/wat/inner/another.file\nDONE", res)

        res = hoard_cmd.ls()
        self.assertEqual(
            "/test.me.1 = a:1\n"
            "/test.me.4 = a:1\n"
            "/wat/inner/another.file = a:1 g:1\n"
            "/wat/test.me.2 = a:1\n"
            "/wat/test.me.3 = a:1\n"
            "DONE", res)

        self.assertEqual([], dump_file_list(self.tmpdir.name, "repo-cloned-wat/"))  # no files yet

        res = hoard_cmd.sync_contents("repo-cloned-wat")
        self.assertEqual(
            f"{cloned_cave_cmd.current_uuid()}:\n"
            "+ inner/another.file\n"
            f"{cloned_cave_cmd.current_uuid()}:\n"
            "DONE", res)

        self.assertEqual(
            ['repo-cloned-wat/inner/another.file'],
            dump_file_list(self.tmpdir.name, "repo-cloned-wat/"))

        res = cloned_cave_cmd.refresh()
        self.assertEqual("Refresh done!", res)

        res = hoard_cmd.enable_content(repo="repo-cloned-wat")
        self.assertEqual("+/wat/test.me.2\n+/wat/test.me.3\nDONE", res)

        res = hoard_cmd.sync_contents("repo-cloned-wat")
        self.assertEqual(
            f"{cloned_cave_cmd.current_uuid()}:\n"
            "+ test.me.2\n"
            "+ test.me.3\n"
            f"{cloned_cave_cmd.current_uuid()}:\n"
            "DONE", res)

        self.assertEqual([
            'repo-cloned-wat/inner/another.file',
            'repo-cloned-wat/test.me.2',
            'repo-cloned-wat/test.me.3'],
            dump_file_list(self.tmpdir.name, "repo-cloned-wat/"))


def dump_file_list(tmpdir: str, path: str) -> List[str]:
    return sorted([
        pathlib.Path(join(dirpath, filename)).relative_to(tmpdir).as_posix()
        for dirpath, dirnames, filenames in os.walk(join(tmpdir, path), topdown=True)
        for filename in filenames if dirpath.find(".hoard") == -1])


def populate_repotypes(tmpdir: str):
    # f"D /wat/test.me.different\n"
    # f"D /wat/test.me.once\n"
    # f"D /wat/test.me.twice\nDONE"
    pfw = pretty_file_writer(tmpdir)
    sleep(0.01)
    pfw('repo-partial/test.me.1', "gsadfs")
    pfw('repo-partial/wat/test.me.2', "gsadf3dq")

    sleep(0.01)
    pfw('repo-full/test.me.1', "gsadfs")
    pfw('repo-full/test.me.4', "fwadeaewdsa")
    pfw('repo-full/wat/test.me.2', "gsadf3dq")
    pfw('repo-full/wat/test.me.3', "afaswewfas")

    sleep(0.01)
    pfw('repo-backup/test.me.1', "gsadfs")
    pfw('repo-backup/wat/test.me.3', "afaswewfas")

    sleep(0.01)
    pfw('repo-incoming/wat/test.me.3', "asdgvarfa")
    pfw('repo-incoming/test.me.4', "fwadeaewdsa")
    pfw('repo-incoming/test.me.5', "adsfg")
    pfw('repo-incoming/wat/test.me.6', "f2fwsdf")
