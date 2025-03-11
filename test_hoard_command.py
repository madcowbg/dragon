import os
import tempfile
import unittest
from os.path import join
from time import sleep
from typing import Tuple, List

from config import CaveType
from contents import HoardContents
from main import TotalCommand
from test_repo_command import populate, write_contents


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
        self.assertEqual(f"Status of {repo_uuid}:\nDONE", res.strip())

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

        res = hoard_cmd.refresh("repo-in-local-2")
        self.assertEqual(
            "=/wat/test.me.twice\nSync'ed repo-in-local-2 to hoard!", res.strip())

        self._assert_hoard_contents(
            HoardContents.load(hoard_cmd._hoard_contents_filename()),
            files_exp=[
                ('/wat/test.me.different', 8, 1, 'd6dcdb1bc4677aab619798004537c4e3'),
                ('/wat/test.me.twice', 6, 2, '1881f6f9784fb08bf6690e9763b76ac3')],
            dirs_exp=["/wat"])

        hoard_cmd.refresh("repo-in-local")
        self._assert_hoard_contents(
            HoardContents.load(hoard_cmd._hoard_contents_filename()),
            files_exp=[
                ('/wat/test.me.different', 8, 1, 'd6dcdb1bc4677aab619798004537c4e3'),  # retained only from repo-2
                ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                ('/wat/test.me.twice', 6, 2, '1881f6f9784fb08bf6690e9763b76ac3')],
            dirs_exp=["/wat"])

        res = hoard_cmd.status("repo-in-local-2")
        self.assertEqual(f"Status of {repo_uuid2}:\nD /wat/test.me.once\nDONE", res.strip())

        res = hoard_cmd.status("repo-in-local")
        self.assertEqual(f"Status of {repo_uuid}:\nM- /wat/test.me.different\nDONE", res.strip())

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
        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local", mount_point="/")

        repo_uuid = cave_cmd.current_uuid()
        hoard_cmd.refresh("repo-in-local")

        self.assertEqual(f"Status of {repo_uuid}:\nDONE", hoard_cmd.status("repo-in-local").strip())

        os.mkdir(join(self.tmpdir.name, "repo", "newdir"))
        write_contents(join(self.tmpdir.name, "repo", "newdir", "newfile.is"), "lhiWFELHFE")
        os.remove(join(self.tmpdir.name, "repo", "wat", 'test.me.different'))

        # as is not refreshed, no change in status
        self.assertEqual(f"Status of {repo_uuid}:\nDONE", hoard_cmd.status("repo-in-local").strip())

        cave_cmd.refresh()
        self.assertEqual(
            f"Status of {repo_uuid}:\nA /newdir/newfile.is\nD /wat/test.me.different\nAF /newdir\nDONE",
            hoard_cmd.status("repo-in-local").strip())

        res = hoard_cmd.refresh("repo-in-local")
        self.assertEqual("+/newdir/newfile.is\nSync'ed repo-in-local to hoard!", res)

        self.assertEqual(f"Status of {repo_uuid}:\nDONE", hoard_cmd.status("repo-in-local").strip())

    def test_clone(self):
        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard

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

        new_repo_path = join(self.tmpdir.name, "cloned-repo")
        os.mkdir(new_repo_path)

        hoard_cmd.clone(to_path=new_repo_path, mount_at="/wat", name="cloned-repo")

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
            f"D /wat/test.me.different\n"
            f"D /wat/test.me.once\n"
            f"D /wat/test.me.twice\nDONE", res)

        res = hoard_cmd.populate(to_repo="cloned-repo")
        self.assertEqual("errors: 0\nrestored: 3\nskipped: 0\nDONE", res.strip())

        res = hoard_cmd.status(new_uuid)
        self.assertEqual(
            f"Status of {new_uuid}:\n"
            f"DONE", res.strip())

        res = hoard_cmd.populate(to_repo="cloned-repo")
        self.assertEqual("errors: 0\nrestored: 0\nskipped: 3\nDONE", res.strip())

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

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-partial"), name="repo-partial-name", mount_point="/",
            type=CaveType.PARTIAL)

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-full"), name="repo-full-name", mount_point="/",
            type=CaveType.PARTIAL)

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

    def test_sync_partial(self):
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


def populate_repotypes(tmpdir: str):
    # f"D /wat/test.me.different\n"
    # f"D /wat/test.me.once\n"
    # f"D /wat/test.me.twice\nDONE"

    sleep(0.01)
    os.mkdir(join(tmpdir, 'repo-partial'))
    os.mkdir(join(tmpdir, 'repo-partial', 'wat'))
    write_contents(join(tmpdir, 'repo-partial', 'test.me.1'), "gsadfs")
    write_contents(join(tmpdir, 'repo-partial', 'wat', 'test.me.2'), "gsadf3dq")

    sleep(0.01)
    os.mkdir(join(tmpdir, 'repo-full'))
    os.mkdir(join(tmpdir, 'repo-full', 'wat'))
    write_contents(join(tmpdir, 'repo-full', 'test.me.1'), "gsadfs")
    write_contents(join(tmpdir, 'repo-full', 'wat', 'test.me.2'), "gsadf3dq")
    write_contents(join(tmpdir, 'repo-full', 'wat', 'test.me.3'), "afaswewfas")
    write_contents(join(tmpdir, 'repo-full', 'test.me.4'), "fwadeaewdsa")

    sleep(0.01)
    os.mkdir(join(tmpdir, 'repo-backup'))
    os.mkdir(join(tmpdir, 'repo-backup', 'wat'))
    write_contents(join(tmpdir, 'repo-backup', 'test.me.1'), "gsadfs")
    write_contents(join(tmpdir, 'repo-backup', 'wat', 'test.me.3'), "afaswewfas")

    sleep(0.01)
    os.mkdir(join(tmpdir, 'repo-incoming'))
    os.mkdir(join(tmpdir, 'repo-incoming', 'wat'))
    write_contents(join(tmpdir, 'repo-incoming', 'wat', 'test.me.3'), "asdgvarfa")
    write_contents(join(tmpdir, 'repo-incoming', 'test.me.4'), "fwadeaewdsa")
    write_contents(join(tmpdir, 'repo-incoming', 'test.me.5'), "adsfg")
    write_contents(join(tmpdir, 'repo-incoming', 'test.me.6'), "f2fwsdf")
