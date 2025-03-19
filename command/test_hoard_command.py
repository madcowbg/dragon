import os
import pathlib
import tempfile
import unittest
from os.path import join
from time import sleep
from typing import Tuple, List, Dict

from config import CaveType
from contents.hoard import HoardContents
from contents.props import DirProps, HoardFileProps
from dragon import TotalCommand
from command.repo_command import RepoCommand
from resolve_uuid import resolve_remote_uuid
from command.test_repo_command import populate, write_contents, pretty_file_writer


def populate_hoard(tmpdir: str):
    populate(tmpdir)

    os.mkdir(join(tmpdir, "hoard"))


class TestHoardCommand(unittest.TestCase):
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

        res = hoard_cmd.deprecated.status_deprecated("repo-in-local")
        self.assertEqual(
            f"Status of {repo_uuid}:\nA /wat/test.me.different\nA /wat/test.me.once\nA /wat/test.me.twice\nAF /wat\nDONE",
            res.strip())

        res = hoard_cmd.contents.pull("repo-in-local")
        self.assertEqual(
            "+/wat/test.me.different\n"
            "+/wat/test.me.once\n"
            "+/wat/test.me.twice\n"
            "Sync'ed repo-in-local to hoard!", res.strip())

        with HoardContents.load(hoard_cmd.hoard.hoard_contents_filename()) as hoard_contents:
            self._assert_hoard_contents(
                hoard_contents,
                files_exp=[
                    ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),
                    ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                    ('/wat/test.me.twice', 6, 1, '1881f6f9784fb08bf6690e9763b76ac3')],
                dirs_exp=["/wat"])

        res = hoard_cmd.deprecated.status_deprecated("repo-in-local")
        self.assertEqual(f"Status of {repo_uuid}:\nDF /wat\nDONE", res.strip())

    def _assert_hoard_contents(
            self, hoard_contents: HoardContents, files_exp: List[Tuple[str, int, int, str]], dirs_exp: List[str]):
        files = sorted(
            (f, prop.size, len(prop.available_at), prop.fasthash)
            for f, prop in hoard_contents.fsobjects if isinstance(prop, HoardFileProps))
        dirs = sorted(f for f, prop in hoard_contents.fsobjects if isinstance(prop, DirProps))
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

        hoard_cmd.contents.pull("repo-in-local")

        with HoardContents.load(hoard_cmd.hoard.hoard_contents_filename()) as hc:
            self._assert_hoard_contents(
                hc,
                files_exp=[
                    ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),
                    ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                    ('/wat/test.me.twice', 6, 1, '1881f6f9784fb08bf6690e9763b76ac3')],
                dirs_exp=["/wat"])

        res = hoard_cmd.contents.pull("repo-in-local")
        self.assertEqual("Skipping update as past epoch 1 is not after hoard epoch 1", res)

        res = hoard_cmd.contents.pull("repo-in-local-2")
        self.assertEqual(
            "=/wat/test.me.twice\nSync'ed repo-in-local-2 to hoard!", res.strip())

        with HoardContents.load(hoard_cmd.hoard.hoard_contents_filename()) as hc:
            self._assert_hoard_contents(
                hc,
                files_exp=[
                    ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),
                    ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                    ('/wat/test.me.twice', 6, 2, '1881f6f9784fb08bf6690e9763b76ac3')],
                dirs_exp=["/wat"])

        res = hoard_cmd.contents.pull("repo-in-local", ignore_epoch=True)
        self.assertEqual("Sync'ed repo-in-local to hoard!", res)

        with HoardContents.load(hoard_cmd.hoard.hoard_contents_filename()) as hc:
            self._assert_hoard_contents(
                hc,
                files_exp=[
                    ('/wat/test.me.different', 5, 1, '5a818396160e4189911989d69d857bd2'),  # retained only from repo
                    ('/wat/test.me.once', 8, 1, '34fac39930874b0f6bc627c3b3fc4b5e'),
                    ('/wat/test.me.twice', 6, 2, '1881f6f9784fb08bf6690e9763b76ac3')],
                dirs_exp=["/wat"])

        res = hoard_cmd.deprecated.status_deprecated("repo-in-local-2")
        self.assertEqual(
            f"Status of {repo_uuid2}:\n"
            f"M /wat/test.me.different\n"
            f"D /wat/test.me.once\n"
            "DF /wat\n"
            f"DONE", res.strip())

        res = hoard_cmd.deprecated.status_deprecated("repo-in-local")
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
        hoard_cmd.contents.pull("repo-in-local")

        self.assertEqual(f"Status of {repo_uuid}:\nDF /wat\nDONE",
                         hoard_cmd.deprecated.status_deprecated("repo-in-local").strip())

        os.mkdir(join(self.tmpdir.name, "repo", "newdir"))
        write_contents(join(self.tmpdir.name, "repo", "newdir", "newfile.is"), "lhiWFELHFE")
        os.remove(join(self.tmpdir.name, "repo", "wat", 'test.me.different'))

        res = cave_cmd.status()
        self.assertEqual(
            f"{cave_cmd.current_uuid()}:\n"
            "files:\n"
            "    same: 2 (66.7%)\n"
            "     mod: 0 (0.0%)\n"
            "     new: 1 (33.3%)\n"
            " current: 3\n"
            " in repo: 3\n"
            " deleted: 1 (33.3%)\n"
            "dirs:\n"
            "    same: 1\n"
            "     new: 1 (50.0%)\n"
            " current: 2\n"
            " in repo: 1\n"
            " deleted: 0 (0.0%)\n", res)

        # touch file without changing contents, will assume a change if we skip integrity checks
        pathlib.Path(join(self.tmpdir.name, "repo/wat/test.me.once")).touch()
        res = cave_cmd.status(skip_integrity_checks=True)
        self.assertEqual(
            f"{cave_cmd.current_uuid()}:\n"
            "files:\n"
            "    same: 1 (33.3%)\n"
            "     mod: 1 (33.3%)\n"
            "     new: 1 (33.3%)\n"
            " current: 3\n"
            " in repo: 3\n"
            " deleted: 1 (33.3%)\n"
            "dirs:\n"
            "    same: 1\n"
            "     new: 1 (50.0%)\n"
            " current: 2\n"
            " in repo: 1\n"
            " deleted: 0 (0.0%)\n", res)

        # as is not refreshed, no change in status
        self.assertEqual(f"Status of {repo_uuid}:\nDF /wat\nDONE",
                         hoard_cmd.deprecated.status_deprecated("repo-in-local").strip())

        cave_cmd.refresh()
        self.assertEqual(
            f"Status of {repo_uuid}:\n"
            f"A /newdir/newfile.is\n"
            f"AF /newdir\n"
            f"D /wat/test.me.different\n"
            f"DF /wat\n"
            f"DONE",
            hoard_cmd.deprecated.status_deprecated("repo-in-local").strip())

        res = hoard_cmd.contents.pull("repo-in-local")
        self.assertEqual(
            "+/newdir/newfile.is\n"
            "-/wat/test.me.different\n"
            "Sync'ed repo-in-local to hoard!", res)

        self.assertEqual(
            f"Status of {repo_uuid}:\n"
            f"D /wat/test.me.different\n"
            "DF /wat\n"
            "DF /newdir\n"
            f"DONE", hoard_cmd.deprecated.status_deprecated("repo-in-local").strip())

    def test_clone(self):
        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()

        new_repo_path = join(self.tmpdir.name, "cloned-repo")
        os.mkdir(new_repo_path)

        res = hoard_cmd.clone(to_path=new_repo_path, mount_at="/wat", name="cloned-repo")
        self.assertEqual("DONE", res)

        new_uuid = resolve_remote_uuid(hoard_cmd.hoard.config(), "cloned-repo")

        res = hoard_cmd.health()
        self.assertEqual(
            "Health stats:\n"
            "1 total remotes.\n"
            f"  [cloned-repo] {new_uuid}: 0 with no other copy\n"
            "Hoard health stats:\n"
            "DONE", res)

        res = hoard_cmd.deprecated.status_deprecated(new_uuid)
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
        new_uuid = resolve_remote_uuid(hoard_cmd.hoard.config(), "cloned-repo")
        res = hoard_cmd.deprecated.status_deprecated(new_uuid)
        self.assertEqual(f"Status of {new_uuid}:\nDONE", res)

        hoard_cmd.contents.pull("repo-in-local")

        # after population by other repo, it is now lacking files
        res = hoard_cmd.deprecated.status_deprecated(new_uuid)
        self.assertEqual(
            f"Status of {new_uuid}:\n"
            "D /wat/test.me.different\n"
            "D /wat/test.me.once\n"
            "D /wat/test.me.twice\n"
            "DF /wat\n"
            f"DONE", res)

        res = hoard_cmd.files.sync_contents(repo="cloned-repo")
        self.assertEqual(
            f"{cloned_cave_cmd.current_uuid()}:\n"
            "+ test.me.different\n"
            "+ test.me.once\n"
            "+ test.me.twice\n"
            f"{cloned_cave_cmd.current_uuid()}:\n"
            "DONE", res.strip())

        res = cloned_cave_cmd.refresh()
        self.assertEqual("Refresh done!", res)

        res = hoard_cmd.deprecated.status_deprecated(new_uuid)
        self.assertEqual(
            f"Status of {new_uuid}:\n"
            "DF /wat\n"
            f"DONE", res.strip())

        res = hoard_cmd.files.sync_contents(repo="cloned-repo")
        self.assertEqual(
            f"{cloned_cave_cmd.current_uuid()}:\n"
            f"{cloned_cave_cmd.current_uuid()}:\n"
            "DONE", res.strip())

        self.assertEqual([
            'cloned-repo/test.me.different',
            'cloned-repo/test.me.once',
            'cloned-repo/test.me.twice'], dump_file_list(self.tmpdir.name, "cloned-repo"))

    def test_create_repo_types(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = init_complex_hoard(
            self.tmpdir.name)

    def test_sync_hoard_definitions(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = init_complex_hoard(
            self.tmpdir.name)

        res = hoard_cmd.deprecated.status_deprecated("repo-partial-name")
        self.assertEqual(
            f"Status of {partial_cave_cmd.current_uuid()}:\n"
            "A /test.me.1\n"
            "A /wat/test.me.2\n"
            "AF /wat\n"
            "DONE", res.strip())

        res = hoard_cmd.contents.pull("repo-partial-name")
        self.assertEqual("+/test.me.1\n+/wat/test.me.2\nSync'ed repo-partial-name to hoard!", res.strip())

        res = hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual(
            "/test.me.1 = a:1 g:2\n"
            "/wat/test.me.2 = a:1 g:2\n"
            "DONE", res)

        res = hoard_cmd.contents.pull("repo-partial-name", ignore_epoch=True)  # does noting...
        self.assertEqual("Sync'ed repo-partial-name to hoard!", res.strip())

        res = hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual(
            "/test.me.1 = a:1 g:2\n"
            "/wat/test.me.2 = a:1 g:2\n"
            "DONE", res)

        res = hoard_cmd.contents.pull("repo-full-name")
        self.assertEqual(
            "=/test.me.1\n"
            "+/test.me.4\n"
            "=/wat/test.me.2\n"
            "+/wat/test.me.3\n"
            "Sync'ed repo-full-name to hoard!", res.strip())

        res = hoard_cmd.contents.pull("repo-full-name", ignore_epoch=True)  # does nothing ...
        self.assertEqual("Sync'ed repo-full-name to hoard!", res.strip())

        res = hoard_cmd.contents.pull("repo-backup-name")  # just registers the files already in backup
        self.assertEqual(
            "=/test.me.1\n"
            "=/wat/test.me.3\n"
            "Sync'ed repo-backup-name to hoard!", res.strip())

        res = hoard_cmd.contents.pull("repo-backup-name")  # does nothing
        self.assertEqual("Skipping update as past epoch 1 is not after hoard epoch 1", res.strip())

        res = hoard_cmd.contents.ls(skip_folders=True)
        self.assertEqual(
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:1 g:1\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.3 = a:2\n"
            "DONE", res)

        res = hoard_cmd.contents.pull("repo-incoming-name")
        self.assertEqual(
            "-/test.me.4\n"
            "<+/test.me.5\n"
            "u/wat/test.me.3\n"
            "<+/wat/test.me.6\n"
            "Sync'ed repo-incoming-name to hoard!", res.strip())

        res = incoming_cave_cmd.refresh()
        self.assertEqual("Refresh done!", res)

        res = hoard_cmd.contents.pull("repo-incoming-name")
        self.assertEqual(
            "-/test.me.4\n"
            "-/test.me.5\n"
            "-/wat/test.me.3\n"
            "-/wat/test.me.6\n"
            "Sync'ed repo-incoming-name to hoard!", res.strip())

        res = hoard_cmd.contents.ls(skip_folders=True)
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
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = init_complex_hoard(
            self.tmpdir.name)

        hoard_cmd.contents.pull("repo-partial-name")
        hoard_cmd.contents.pull("repo-full-name")
        hoard_cmd.contents.pull("repo-backup-name")  # just registers the files already in backup
        res = hoard_cmd.contents.pull("repo-incoming-name")
        self.assertEqual(
            "-/test.me.4\n"
            "<+/test.me.5\n"
            "u/wat/test.me.3\n"
            "<+/wat/test.me.6\n"
            "Sync'ed repo-incoming-name to hoard!", res)

        res = hoard_cmd.contents.ls(skip_folders=True)
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

        res = hoard_cmd.files.sync_contents("repo-full-name")
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

        res = hoard_cmd.contents.ls(skip_folders=True)
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

        res = hoard_cmd.deprecated.status_deprecated("repo-full-name")
        self.assertEqual(
            f"Status of {full_cave_cmd.current_uuid()}:\n"
            f"DF /wat\n"
            f"DONE", res)

    def test_sync_hoard_file_contents_all(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = init_complex_hoard(
            self.tmpdir.name)

        hoard_cmd.contents.pull("repo-partial-name")
        hoard_cmd.contents.pull("repo-full-name")
        hoard_cmd.contents.pull("repo-backup-name")  # just registers the files already in backup
        hoard_cmd.contents.pull("repo-incoming-name")

        res = hoard_cmd.contents.status()
        self.assertEqual(
            ""
            "|Num Files                |             updated|total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |                 now|         6|         1|         5|          |          |\n"
            "|repo-full-name           |                 now|         6|         3|         3|          |          |\n"
            "|repo-incoming-name       |                 now|         4|          |          |          |         4|\n"
            "|repo-partial-name        |                 now|         2|         2|          |          |          |\n"
            "\n"
            "|Size                     |             updated|total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |                 now|        46|         6|        40|          |          |\n"
            "|repo-full-name           |                 now|        46|        25|        21|          |          |\n"
            "|repo-incoming-name       |                 now|        32|          |          |          |        32|\n"
            "|repo-partial-name        |                 now|        14|        14|          |          |          |\n",
            res)

        res = hoard_cmd.files.sync_contents()
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
            "d test.me.4\n"
            "d test.me.5\n"
            "d wat/test.me.3\n"
            "d wat/test.me.6\n"
            "DONE", res)

        self.assertEqual([
            'repo-partial/test.me.1',
            'repo-partial/wat/test.me.2'],
            dump_file_list(self.tmpdir.name, 'repo-partial'))

        res = hoard_cmd.contents.ls(skip_folders=True)
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

        res = hoard_cmd.contents.pull("repo-full-name")
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

        res = hoard_cmd.contents.get(repo="repo-cloned-wat", path="inner")
        self.assertEqual("+/wat/inner/another.file\nDONE", res)

        res = hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
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

        res = hoard_cmd.files.sync_contents("repo-cloned-wat")
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

        res = hoard_cmd.contents.get(repo="repo-cloned-wat")
        self.assertEqual("+/wat/test.me.2\n+/wat/test.me.3\nDONE", res)

        res = hoard_cmd.files.sync_contents("repo-cloned-wat")
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

    def test_moving_locations_no_files(self):
        populate_repotypes(self.tmpdir.name)
        partial_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo-partial")).cave
        partial_cave_cmd.init()
        partial_cave_cmd.refresh()

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-partial"), name="repo-partial-name", mount_point="/first-point",
            type=CaveType.PARTIAL, fetch_new=True)

        res = hoard_cmd.contents.pull("repo-partial-name")
        self.assertEqual(
            "+/first-point/test.me.1\n"
            "+/first-point/wat/test.me.2\n"
            "Sync'ed repo-partial-name to hoard!", res.strip())

        res = hoard_cmd.remotes()
        self.assertEqual(
            f"1 total remotes.\n"
            f"  [repo-partial-name] {partial_cave_cmd.current_uuid()} (partial)\n"
            "Mounts:\n"
            "  /first-point -> repo-partial-name\n"
            "DONE", res.strip())

        res = hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/\n"
            "/first-point => (repo-partial-name:.)\n"
            "/first-point/test.me.1 = a:1\n"
            "/first-point/wat => (repo-partial-name:wat)\n"
            "/first-point/wat/test.me.2 = a:1\n"
            "DONE", res)

        res = hoard_cmd.move_mounts(from_path="/first-point/inner", to_path="/cant-move-files")
        self.assertEqual(
            "Can't move /first-point/inner to /cant-move-files, requires moving files in repo-partial-name:inner.",
            res.strip())

        res = hoard_cmd.move_mounts(from_path="/", to_path="/move-all-inside")
        self.assertEqual(
            "Moving files and folders:\n"
            "/first-point/test.me.1=>/move-all-inside/first-point/test.me.1\n"
            "/first-point/wat/test.me.2=>/move-all-inside/first-point/wat/test.me.2\n"
            "/first-point/wat=>/move-all-inside/first-point/wat\n"
            "Moving 1 repos:\n"
            "[repo-partial-name] /first-point => /move-all-inside/first-point\n"
            "DONE", res)

        res = hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/\n"
            "/move-all-inside\n"
            "/move-all-inside/first-point => (repo-partial-name:.)\n"
            "/move-all-inside/first-point/test.me.1 = a:1\n"
            "/move-all-inside/first-point/wat => (repo-partial-name:wat)\n"
            "/move-all-inside/first-point/wat/test.me.2 = a:1\n"
            "DONE", res)

        res = hoard_cmd.remotes()
        self.assertEqual(
            f"1 total remotes.\n"
            f"  [repo-partial-name] {partial_cave_cmd.current_uuid()} (partial)\n"
            "Mounts:\n"
            "  /move-all-inside/first-point -> repo-partial-name\n"
            "DONE", res.strip())

        res = hoard_cmd.move_mounts(from_path="/first-point", to_path="/moved-data")
        self.assertEqual("No repos to move!", res.strip())

        res = hoard_cmd.move_mounts(from_path="/move-all-inside/first-point", to_path="/moved-data")
        self.assertEqual(
            "Moving files and folders:\n"
            "/move-all-inside/first-point/test.me.1=>/moved-data/test.me.1\n"
            "/move-all-inside/first-point/wat/test.me.2=>/moved-data/wat/test.me.2\n"
            "/move-all-inside/first-point/wat=>/moved-data/wat\n"
            "Moving 1 repos:\n"
            "[repo-partial-name] /move-all-inside/first-point => /moved-data\n"
            "DONE", res)

        res = hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/\n"
            "/moved-data => (repo-partial-name:.)\n"
            "/moved-data/test.me.1 = a:1\n"
            "/moved-data/wat => (repo-partial-name:wat)\n"
            "/moved-data/wat/test.me.2 = a:1\n"
            "DONE", res)

        res = hoard_cmd.move_mounts(from_path="/moved-data", to_path="/")
        self.assertEqual(
            "Moving files and folders:\n"
            "/moved-data/test.me.1=>/test.me.1\n"
            "/moved-data/wat/test.me.2=>/wat/test.me.2\n"
            "/moved-data/wat=>/wat\n"
            "Moving 1 repos:\n"
            "[repo-partial-name] /moved-data => /\n"
            "DONE", res)

        res = hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/ => (repo-partial-name:.)\n"
            "/test.me.1 = a:1\n"
            "/wat => (repo-partial-name:wat)\n"
            "/wat/test.me.2 = a:1\n"
            "DONE", res)

        partial_cave_cmd.refresh()

        res = hoard_cmd.contents.pull("repo-partial-name")  # needs to do nothing
        self.assertEqual("Sync'ed repo-partial-name to hoard!", res.strip())

    def test_copy_locations_of_files(self):
        populate_repotypes(self.tmpdir.name)
        partial_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo-partial")).cave
        partial_cave_cmd.init()
        partial_cave_cmd.refresh()

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "repo-partial"), name="repo-partial-name",
            mount_point="/first-point",
            type=CaveType.PARTIAL, fetch_new=True)

        # "+/first-point/test.me.1\n"
        # "+/first-point/wat/test.me.2\n"
        hoard_cmd.contents.pull("repo-partial-name")

        res = hoard_cmd.move_mounts(from_path="/first-point", to_path="/moved-data")
        self.assertEqual(
            "Moving files and folders:\n"
            "/first-point/test.me.1=>/moved-data/test.me.1\n"
            "/first-point/wat/test.me.2=>/moved-data/wat/test.me.2\n"
            "/first-point/wat=>/moved-data/wat\n"
            "Moving 1 repos:\n"
            "[repo-partial-name] /first-point => /moved-data\n"
            "DONE", res)

        res = hoard_cmd.contents.copy(from_path="/moved-data/wat", to_path="/moved-data/zed")
        self.assertEqual(
            "c+ /moved-data/zed/test.me.2\n"
            "c+ /moved-data/zed\n"
            "DONE", res)

        res = hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/\n"
            "/moved-data => (repo-partial-name:.)\n"
            "/moved-data/test.me.1 = a:1\n"
            "/moved-data/wat => (repo-partial-name:wat)\n"
            "/moved-data/wat/test.me.2 = a:1\n"
            "/moved-data/zed => (repo-partial-name:zed)\n"
            "/moved-data/zed/test.me.2 = x:1\n"
            "DONE", res)

        res = hoard_cmd.files.sync_contents("repo-partial-name")
        self.assertEqual(
            f"{partial_cave_cmd.current_uuid()}:\n"
            "c+ zed/test.me.2\n"
            f"{partial_cave_cmd.current_uuid()}:\n"
            "DONE", res)

        res = hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
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

    def test_restore_missing_local_file_on_refresh(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = init_complex_hoard(
            self.tmpdir.name)

        res = hoard_cmd.contents.pull("repo-partial-name")
        self.assertEqual("+/test.me.1\n+/wat/test.me.2\nSync'ed repo-partial-name to hoard!", res)

        res = hoard_cmd.contents.pull("repo-backup-name")
        self.assertEqual("=/test.me.1\n?/wat/test.me.3\nSync'ed repo-backup-name to hoard!", res)

        self.assertEqual(
            "/\n"
            "/test.me.1 = a:2 g:1\n"
            "/wat\n"
            "/wat/test.me.2 = a:1 g:2\n"
            "DONE", hoard_cmd.contents.ls(depth=2))

        res = hoard_cmd.files.sync_contents("repo-backup-name")
        self.assertEqual(
            f"{backup_cave_cmd.current_uuid()}:\n"
            "+ wat/test.me.2\n"
            f"{backup_cave_cmd.current_uuid()}:\n"
            "DONE", res.strip())

        self.assertEqual([
            'repo-backup/test.me.1',
            'repo-backup/wat/test.me.2',
            'repo-backup/wat/test.me.3'], dump_file_list(self.tmpdir.name, 'repo-backup'))

        self.assertEqual(
            "/\n"
            "/test.me.1 = a:2 g:1\n"
            "/wat\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "DONE", hoard_cmd.contents.ls(depth=2))

        self.assertEqual([
            'repo-partial/test.me.1',
            'repo-partial/wat/test.me.2'],
            dump_file_list(self.tmpdir.name, 'repo-partial'))

        os.remove(join(self.tmpdir.name, 'repo-partial/wat/test.me.2'))

        res = partial_cave_cmd.status()
        self.assertEqual(
            f"{partial_cave_cmd.current_uuid()}:\n"
            "files:\n"
            "    same: 1 (100.0%)\n"
            "     mod: 0 (0.0%)\n"
            "     new: 0 (0.0%)\n"
            " current: 1\n"
            " in repo: 2\n"
            " deleted: 1 (50.0%)\n"
            "dirs:\n"
            "    same: 1\n"
            "     new: 0 (0.0%)\n"
            " current: 1\n"
            " in repo: 1\n"
            " deleted: 0 (0.0%)\n", res)

        res = partial_cave_cmd.refresh()
        self.assertEqual("Refresh done!", res)

        res = hoard_cmd.contents.pull("repo-partial-name", force_fetch_local_missing=True)
        self.assertEqual(
            "g/wat/test.me.2\n"
            "Sync'ed repo-partial-name to hoard!", res)

        self.assertEqual(
            "/\n"
            "/test.me.1 = a:2 g:1\n"
            "/wat\n"
            "/wat/test.me.2 = a:1 g:2\n"
            "DONE", hoard_cmd.contents.ls(depth=2))

        res = hoard_cmd.files.sync_contents("repo-partial-name")
        self.assertEqual(
            f"{partial_cave_cmd.current_uuid()}:\n"
            f"+ wat/test.me.2\n"
            f"{partial_cave_cmd.current_uuid()}:\n"
            f"DONE", res)

        self.assertEqual(
            ['repo-partial/test.me.1', 'repo-partial/wat/test.me.2'],
            dump_file_list(self.tmpdir.name, 'repo-partial'))


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


def init_complex_hoard(tmpdir: str):
    partial_cave_cmd = TotalCommand(path=join(tmpdir, "repo-partial")).cave
    partial_cave_cmd.init()
    partial_cave_cmd.refresh()

    full_cave_cmd = TotalCommand(path=join(tmpdir, "repo-full")).cave
    full_cave_cmd.init()
    full_cave_cmd.refresh()

    backup_cave_cmd = TotalCommand(path=join(tmpdir, "repo-backup")).cave
    backup_cave_cmd.init()
    backup_cave_cmd.refresh()

    incoming_cave_cmd = TotalCommand(path=join(tmpdir, "repo-incoming")).cave
    incoming_cave_cmd.init()
    incoming_cave_cmd.refresh()

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

    res = hoard_cmd.remotes()
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
