import os
import tempfile
import unittest
from os.path import join

from command.test_hoard_command import populate_repotypes, init_complex_hoard, dump_file_list
from command.test_repo_command import pretty_file_writer
from config import CaveType
from dragon import TotalCommand


def populate(tmpdir: str):
    os.mkdir(join(tmpdir, "hoard"))


class TestFileChangingFlows(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        populate(self.tmpdir.name)
        populate_repotypes(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_adding_full_then_adding_partial(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            init_complex_hoard(self.tmpdir.name)

        res = hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            f"+/test.me.1\n"
            f"+/test.me.4\n"
            f"+/wat/test.me.2\n"
            f"+/wat/test.me.3"f"\n"
            f"Sync'ed repo-full-name to hoard!\nDONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         4|          |         4|          |          |\n"
            "|repo-full-name           |         4|         4|          |          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        35|          |        35|          |          |\n"
            "|repo-full-name           |        35|        35|          |          |          |\n",
            res)

        res = hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual(
            f"=/test.me.1\n"
            f"=/wat/test.me.2\n"
            f"Sync'ed repo-partial-name to hoard!\nDONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         4|          |         4|          |          |\n"
            "|repo-full-name           |         4|         4|          |          |          |\n"
            "|repo-partial-name        |         2|         2|          |          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        35|          |        35|          |          |\n"
            "|repo-full-name           |        35|        35|          |          |          |\n"
            "|repo-partial-name        |        14|        14|          |          |          |\n",
            res)

        res = hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), (repo-partial-name:.)\n"
            "/test.me.1 = a:2 g:1\n"
            "/test.me.4 = a:1 g:1\n"
            "/wat => (repo-backup-name:wat), (repo-full-name:wat), (repo-incoming-name:wat), (repo-partial-name:wat)\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.3 = a:1 g:1\n"
            "DONE", res)

    def test_initial_population(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            init_complex_hoard(self.tmpdir.name)
        pfw = pretty_file_writer(self.tmpdir.name)

        res = hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual(f"+/test.me.1\n+/wat/test.me.2\nSync'ed repo-partial-name to hoard!\nDONE", res)

        res = hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            f"=/test.me.1\n+/test.me.4\n=/wat/test.me.2\n+/wat/test.me.3"
            f"\nSync'ed repo-full-name to hoard!\nDONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         4|          |         4|          |          |\n"
            "|repo-full-name           |         4|         4|          |          |          |\n"
            "|repo-partial-name        |         2|         2|          |          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        35|          |        35|          |          |\n"
            "|repo-full-name           |        35|        35|          |          |          |\n"
            "|repo-partial-name        |        14|        14|          |          |          |\n",
            res)

        res = hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), (repo-partial-name:.)\n"
            "/test.me.1 = a:2 g:1\n"
            "/test.me.4 = a:1 g:1\n"
            "/wat => (repo-backup-name:wat), (repo-full-name:wat), (repo-incoming-name:wat), (repo-partial-name:wat)\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.3 = a:1 g:1\n"
            "DONE", res)

        # delete file before it is backed up
        assert os.path.isfile(join(self.tmpdir.name, 'repo-full/wat/test.me.3'))
        os.remove(join(self.tmpdir.name, 'repo-full/wat/test.me.3'))
        pfw('repo-full/wat/test.me.z', "whut-whut-in-the-but")

        res = full_cave_cmd.refresh()
        self.assertEqual("Refresh done!", res)

        res = hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            f"+/wat/test.me.z\n"
            f"-/wat/test.me.3\n"
            f"remove dangling /wat/test.me.3\n"
            f"Sync'ed repo-full-name to hoard!\nDONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         4|          |         4|          |          |\n"
            "|repo-full-name           |         4|         4|          |          |          |\n"
            "|repo-partial-name        |         2|         2|          |          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        45|          |        45|          |          |\n"
            "|repo-full-name           |        45|        45|          |          |          |\n"
            "|repo-partial-name        |        14|        14|          |          |          |\n",
            res)

        res = hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), (repo-partial-name:.)\n"
            "/test.me.1 = a:2 g:1\n"
            "/test.me.4 = a:1 g:1\n"
            "/wat => (repo-backup-name:wat), (repo-full-name:wat), (repo-incoming-name:wat), (repo-partial-name:wat)\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.z = a:1 g:1\n"
            "DONE", res)

        # new file in partial
        pfw('repo-partial/test.me.5', "adsfgasd")

        res = partial_cave_cmd.refresh()
        self.assertEqual("Refresh done!", res)

        res = hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual(f"+/test.me.5\nSync'ed repo-partial-name to hoard!\nDONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         5|          |         5|          |          |\n"
            "|repo-full-name           |         5|         4|         1|          |          |\n"
            "|repo-partial-name        |         3|         3|          |          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        53|          |        53|          |          |\n"
            "|repo-full-name           |        53|        45|         8|          |          |\n"
            "|repo-partial-name        |        22|        22|          |          |          |\n",
            res)

        res = hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), (repo-partial-name:.)\n"
            "/test.me.1 = a:2 g:1\n"
            "/test.me.4 = a:1 g:1\n"
            "/test.me.5 = a:1 g:2\n"
            "/wat => (repo-backup-name:wat), (repo-full-name:wat), (repo-incoming-name:wat), (repo-partial-name:wat)\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.z = a:1 g:1\n"
            "DONE", res)

        res = hoard_cmd.contents.get("repo-partial-name", "/wat")
        self.assertEqual("Path /wat must be relative, but is absolute.", res)

        res = hoard_cmd.contents.get("repo-partial-name", "wat")
        self.assertEqual(
            "+/wat/test.me.z\n"
            "Considered 5 files.\n"
            "DONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         5|          |         5|          |          |\n"
            "|repo-full-name           |         5|         4|         1|          |          |\n"
            "|repo-partial-name        |         4|         3|         1|          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        53|          |        53|          |          |\n"
            "|repo-full-name           |        53|        45|         8|          |          |\n"
            "|repo-partial-name        |        42|        22|        20|          |          |\n",
            res)

        res = hoard_cmd.files.push("repo-partial-name")
        self.assertEqual(
            f"repo-partial-name:\n"
            "+ wat/test.me.z\n"
            f"repo-partial-name:\n"
            "DONE", res)

        res = hoard_cmd.files.push("repo-partial-name")
        self.assertEqual(
            f"repo-partial-name:\n"
            f"repo-partial-name:\n"
            "DONE", res)

    def test_file_is_deleted_before_copied(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            init_complex_hoard(self.tmpdir.name)
        pfw = pretty_file_writer(self.tmpdir.name)

        hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        hoard_cmd.contents.pull(full_cave_cmd.current_uuid())

        # delete file before it is backed up
        assert os.path.isfile(join(self.tmpdir.name, 'repo-full/wat/test.me.3'))
        os.remove(join(self.tmpdir.name, 'repo-full/wat/test.me.3'))
        pfw('repo-full/wat/test.me.z', "whut-whut-in-the-but")

        # still shows the file is presumed there
        res = hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), (repo-partial-name:.)\n"
            "/test.me.1 = a:2 g:1\n"
            "/test.me.4 = a:1 g:1\n"
            "/wat => (repo-backup-name:wat), (repo-full-name:wat), (repo-incoming-name:wat), (repo-partial-name:wat)\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.3 = a:1 g:1\n"
            "DONE", res)

        # try to fetch - will have some errors
        res = hoard_cmd.files.push("repo-backup-name")
        self.assertEqual(
            f"repo-backup-name:\n"
            f"+ test.me.1\n"
            f"+ test.me.4\n"
            f"+ wat/test.me.2\n"
            f"E wat/test.me.3\n"
            f"repo-backup-name:\n"
            f"DONE", res)

        res = hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), (repo-partial-name:.)\n"
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:2\n"
            "/wat => (repo-backup-name:wat), (repo-full-name:wat), (repo-incoming-name:wat), (repo-partial-name:wat)\n"
            "/wat/test.me.2 = a:3\n"
            "/wat/test.me.3 = a:1 g:1\n"
            "DONE", res)

        # try to fetch - errors will remain
        res = hoard_cmd.files.push("repo-backup-name")
        self.assertEqual(
            f"repo-backup-name:\n"
            f"E wat/test.me.3\n"
            f"repo-backup-name:\n"
            "DONE", res)

        # do refresh and pull to detect deleted file and its state
        res = full_cave_cmd.refresh()
        self.assertEqual("Refresh done!", res)

        res = hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            f"+/wat/test.me.z\n"
            f"-/wat/test.me.3\n"
            f"remove dangling /wat/test.me.3\n"
            f"Sync'ed repo-full-name to hoard!\nDONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         4|         3|         1|          |          |\n"
            "|repo-full-name           |         4|         4|          |          |          |\n"
            "|repo-partial-name        |         2|         2|          |          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        45|        25|        20|          |          |\n"
            "|repo-full-name           |        45|        45|          |          |          |\n"
            "|repo-partial-name        |        14|        14|          |          |          |\n",
            res)

        res = hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), (repo-partial-name:.)\n"
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:2\n"
            "/wat => (repo-backup-name:wat), (repo-full-name:wat), (repo-incoming-name:wat), (repo-partial-name:wat)\n"
            "/wat/test.me.2 = a:3\n"
            "/wat/test.me.z = a:1 g:1\n"
            "DONE", res)

    def test_file_is_deleted_after_copied(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            init_complex_hoard(self.tmpdir.name)
        pfw = pretty_file_writer(self.tmpdir.name)

        hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        hoard_cmd.files.push(backup_cave_cmd.current_uuid())

        res = hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), (repo-partial-name:.)\n"
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:2\n"
            "/wat => (repo-backup-name:wat), (repo-full-name:wat), (repo-incoming-name:wat), (repo-partial-name:wat)\n"
            "/wat/test.me.2 = a:3\n"
            "/wat/test.me.3 = a:2\n"
            "DONE", res)

        # remove backed up file
        os.remove(join(self.tmpdir.name, 'repo-full/wat/test.me.2'))

        res = full_cave_cmd.refresh()
        self.assertEqual("Refresh done!", res)

        res = hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(f"-/wat/test.me.2\nSync'ed repo-full-name to hoard!\nDONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         4|         3|          |          |         1|\n"
            "|repo-full-name           |         3|         3|          |          |          |\n"
            "|repo-partial-name        |         2|         1|          |          |         1|\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        35|        27|          |          |         8|\n"
            "|repo-full-name           |        27|        27|          |          |          |\n"
            "|repo-partial-name        |        14|         6|          |          |         8|\n", res)

        res = hoard_cmd.contents.ls()
        self.assertEqual(
            "/\n"
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:2\n"
            "/wat\n"
            "/wat/test.me.2 = c:2"
            "\n/wat/test.me.3 = a:2\n"
            "DONE", res)

        res = hoard_cmd.files.push("repo-backup-name")
        self.assertEqual(
            f"repo-backup-name:\n"
            f"repo-backup-name:\n"
            "d wat/test.me.2\n"
            "DONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         3|         3|          |          |          |\n"
            "|repo-full-name           |         3|         3|          |          |          |\n"
            "|repo-partial-name        |         2|         1|          |          |         1|\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        27|        27|          |          |          |\n"
            "|repo-full-name           |        27|        27|          |          |          |\n"
            "|repo-partial-name        |        14|         6|          |          |         8|\n", res)

        res = hoard_cmd.files.push("repo-full-name")
        self.assertEqual(
            f"repo-full-name:\n"
            f"repo-full-name:\n"
            "DONE", res)

        res = hoard_cmd.files.push("repo-partial-name")
        self.assertEqual(
            f"repo-partial-name:\n"
            f"repo-partial-name:\n"
            "d wat/test.me.2\n"
            "remove dangling /wat/test.me.2\n"
            "DONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         3|         3|          |          |          |\n"
            "|repo-full-name           |         3|         3|          |          |          |\n"
            "|repo-partial-name        |         1|         1|          |          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        27|        27|          |          |          |\n"
            "|repo-full-name           |        27|        27|          |          |          |\n"
            "|repo-partial-name        |         6|         6|          |          |          |\n", res)

        res = hoard_cmd.contents.ls()
        self.assertEqual(
            "/\n"
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:2\n"
            "/wat\n"
            "/wat/test.me.3 = a:2\n"
            "DONE", res)

    def test_add_fetch_new_repo_after_content_is_in(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            init_complex_hoard(self.tmpdir.name)
        pfw = pretty_file_writer(self.tmpdir.name)

        os.mkdir(join(self.tmpdir.name, "new-contents"))
        pfw("new-contents/one-new.file", "eqrghjl9asd")

        # initial pull only partial
        hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())

        new_content_cmd = TotalCommand(path=join(self.tmpdir.name, "new-contents")).cave
        new_content_cmd.init()
        new_content_cmd.refresh()

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "new-contents"), name="repo-new-contents-name",
            mount_point="/wat", type=CaveType.PARTIAL, fetch_new=True)

        res = hoard_cmd.contents.status()
        self.assertEqual(
            "|Num Files                |             updated|total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |               never|         2|          |         2|          |          |\n"
            "|repo-full-name           |               never|         2|          |         2|          |          |\n"
            "|repo-partial-name        |                 now|         2|         2|          |          |          |\n"
            "\n"
            "|Size                     |             updated|total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |               never|        14|          |        14|          |          |\n"
            "|repo-full-name           |               never|        14|          |        14|          |          |\n"
            "|repo-partial-name        |                 now|        14|        14|          |          |          |\n"
            "", res)

        # refresh new contents file
        hoard_cmd.contents.pull(new_content_cmd.current_uuid())

        # pull full as well - its files will be added to the new repop
        hoard_cmd.contents.pull(full_cave_cmd.current_uuid())

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         5|          |         5|          |          |\n"
            "|repo-full-name           |         5|         4|         1|          |          |\n"
            "|repo-new-contents-name   |         2|         1|         1|          |          |\n"
            "|repo-partial-name        |         2|         2|          |          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        46|          |        46|          |          |\n"
            "|repo-full-name           |        46|        35|        11|          |          |\n"
            "|repo-new-contents-name   |        21|        11|        10|          |          |\n"
            "|repo-partial-name        |        14|        14|          |          |          |\n",
            res)

        res = hoard_cmd.contents.get(repo="repo-new-contents-name", path="")
        self.assertEqual(
            "+/wat/test.me.2\n"
            "Considered 3 files.\n"
            "DONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         5|          |         5|          |          |\n"
            "|repo-full-name           |         5|         4|         1|          |          |\n"
            "|repo-new-contents-name   |         3|         1|         2|          |          |\n"
            "|repo-partial-name        |         2|         2|          |          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        46|          |        46|          |          |\n"
            "|repo-full-name           |        46|        35|        11|          |          |\n"
            "|repo-new-contents-name   |        29|        11|        18|          |          |\n"
            "|repo-partial-name        |        14|        14|          |          |          |\n",
            res)

        res = new_content_cmd.refresh()
        self.assertEqual("Refresh done!", res)

        res = hoard_cmd.contents.pull(new_content_cmd.current_uuid())
        self.assertEqual("Sync'ed repo-new-contents-name to hoard!\nDONE", res)

        res = hoard_cmd.files.push(repo="repo-new-contents-name")
        self.assertEqual(
            f"repo-new-contents-name:\n"
            f"+ test.me.2\n"
            f"+ test.me.3\n"
            f"repo-new-contents-name:\n"
            f"DONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         5|          |         5|          |          |\n"
            "|repo-full-name           |         5|         4|         1|          |          |\n"
            "|repo-new-contents-name   |         3|         3|          |          |          |\n"
            "|repo-partial-name        |         2|         2|          |          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        46|          |        46|          |          |\n"
            "|repo-full-name           |        46|        35|        11|          |          |\n"
            "|repo-new-contents-name   |        29|        29|          |          |          |\n"
            "|repo-partial-name        |        14|        14|          |          |          |\n",
            res)

        res = hoard_cmd.files.push(repo=full_cave_cmd.current_uuid())
        self.assertEqual(
            f"repo-full-name:\n"
            f"+ wat/one-new.file\n"
            f"repo-full-name:\n"
            f"DONE", res)

        self.assertDictEqual(
            dump_file_list(self.tmpdir.name + "/repo-full/wat", "", data=True),
            dump_file_list(self.tmpdir.name + "/new-contents", "", data=True))

    def test_resetting_file_contents(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            init_complex_hoard(self.tmpdir.name)

        pfw = pretty_file_writer(self.tmpdir.name)
        os.mkdir(join(self.tmpdir.name, "changed-cave"))
        pfw('changed-cave/test.me.1', "w3q45yhq3g")
        pfw('changed-cave/test.me.4', "fwadeaewdsa")
        pfw('changed-cave/wat/test.me.2', "gsadf3dq")
        pfw('changed-cave/wat/test.me.3', "'psejmfw'")

        changed_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "changed-cave")).cave
        changed_cave_cmd.init()
        res = changed_cave_cmd.refresh()
        self.assertEqual("Refresh done!", res)

        res = hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "changed-cave"), name="repo-changed-cave-name",
            mount_point="/", type=CaveType.PARTIAL, fetch_new=False)
        self.assertEqual(
            fr"Added repo-changed-cave-name[{changed_cave_cmd.current_uuid()}] at {self.tmpdir.name}\changed-cave!",
            res)

        res = hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual(
            "+/test.me.1\n"
            "+/wat/test.me.2\n"
            "Sync'ed repo-partial-name to hoard!\nDONE", res)

        res = hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            "=/test.me.1\n"
            "+/test.me.4\n"
            "=/wat/test.me.2\n"
            "+/wat/test.me.3\n"
            "Sync'ed repo-full-name to hoard!\n"
            "DONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         4|          |         4|          |          |\n"
            "|repo-full-name           |         4|         4|          |          |          |\n"
            "|repo-partial-name        |         2|         2|          |          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        35|          |        35|          |          |\n"
            "|repo-full-name           |        35|        35|          |          |          |\n"
            "|repo-partial-name        |        14|        14|          |          |          |\n", res)

        res = hoard_cmd.contents.pull(changed_cave_cmd.current_uuid(), assume_current=True)
        self.assertEqual(
            "RESETTING /test.me.1\n"
            "=/test.me.4\n"
            "=/wat/test.me.2\n"
            "RESETTING /wat/test.me.3\n"
            "Sync'ed repo-changed-cave-name to hoard!\nDONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         4|          |         4|          |          |\n"
            "|repo-changed-cave-name   |         4|         4|          |          |          |\n"
            "|repo-full-name           |         4|         2|         2|          |          |\n"
            "|repo-partial-name        |         2|         1|         1|          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        38|          |        38|          |          |\n"
            "|repo-changed-cave-name   |        38|        38|          |          |          |\n"
            "|repo-full-name           |        38|        19|        19|          |          |\n"
            "|repo-partial-name        |        18|         8|        10|          |          |\n", res)

        res = hoard_cmd.files.pending()
        self.assertEqual(
            "repo-partial-name:\n"
            "TO_GET (from 1) /test.me.1\n"
            " repo-changed-cave-name has 1 files\n"
            "repo-full-name:\n"
            "TO_GET (from 1) /test.me.1\n"
            "TO_GET (from 1) /wat/test.me.3\n"
            " repo-changed-cave-name has 2 files\n"
            "repo-backup-name:\n"
            "TO_GET (from 3) /wat/test.me.2\n"
            "TO_GET (from 2) /test.me.4\n"
            "TO_GET (from 1) /test.me.1\n"
            "TO_GET (from 1) /wat/test.me.3\n"
            " repo-changed-cave-name has 4 files\n"
            " repo-full-name has 2 files\n"
            " repo-partial-name has 1 files\n"
            "repo-incoming-name:\n"
            "repo-changed-cave-name:\n"
            "DONE", res)

        # resetting pending ops
        res = hoard_cmd.contents.reset("repo-full-name")
        self.assertEqual(
            "repo-full-name:\n"
            "WONT_GET /test.me.1\n"
            "WONT_GET /wat/test.me.3\n"
            "DONE", res)

        res = hoard_cmd.files.pending()
        self.assertEqual(
            "repo-partial-name:\n"
            "TO_GET (from 1) /test.me.1\n"
            " repo-changed-cave-name has 1 files\n"
            "repo-full-name:\n"
            "repo-backup-name:\n"
            "TO_GET (from 3) /wat/test.me.2\n"
            "TO_GET (from 2) /test.me.4\n"
            "TO_GET (from 1) /test.me.1\n"
            "TO_GET (from 1) /wat/test.me.3\n"
            " repo-changed-cave-name has 4 files\n"
            " repo-full-name has 2 files\n"
            " repo-partial-name has 1 files\n"
            "repo-incoming-name:\n"
            "repo-changed-cave-name:\n"
            "DONE", res)

        # resetting existing contents to what repo-full-name should contain!
        res = hoard_cmd.contents.reset_with_existing("repo-full-name")
        self.assertEqual(
            "repo-full-name:\n"
            "RESET /test.me.1\n"
            "RESET /wat/test.me.3\n"
            "DONE", res)

        res = hoard_cmd.files.pending()
        self.assertEqual(
            "repo-partial-name:\n"
            "TO_GET (from 1) /test.me.1\n"
            " repo-full-name has 1 files\n"
            "repo-full-name:\n"
            "repo-backup-name:\n"
            "TO_GET (from 3) /wat/test.me.2\n"
            "TO_GET (from 2) /test.me.4\n"
            "TO_GET (from 1) /test.me.1\n"
            "TO_GET (from 1) /wat/test.me.3\n"
            " repo-changed-cave-name has 2 files\n"
            " repo-full-name has 4 files\n"
            " repo-partial-name has 1 files\n"
            "repo-incoming-name:\n"
            "repo-changed-cave-name:\n"
            "TO_GET (from 1) /test.me.1\n"
            "TO_GET (from 1) /wat/test.me.3\n"
            " repo-full-name has 2 files\n"
            "DONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         4|          |         4|          |          |\n"
            "|repo-changed-cave-name   |         4|         2|         2|          |          |\n"
            "|repo-full-name           |         4|         4|          |          |          |\n"
            "|repo-partial-name        |         2|         1|         1|          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        35|          |        35|          |          |\n"
            "|repo-changed-cave-name   |        35|        19|        16|          |          |\n"
            "|repo-full-name           |        35|        35|          |          |          |\n"
            "|repo-partial-name        |        14|         8|         6|          |          |\n", res)
