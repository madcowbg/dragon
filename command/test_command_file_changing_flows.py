import os
import pathlib
import shutil
import tempfile
from os.path import join
from unittest import IsolatedAsyncioTestCase

from command.test_hoard_command import populate_repotypes, init_complex_hoard, dump_file_list
from command.test_repo_command import pretty_file_writer
from config import CaveType
from dragon import TotalCommand


def populate(tmpdir: str):
    os.mkdir(join(tmpdir, "hoard"))


class TestFileChangingFlows(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        populate(self.tmpdir.name)
        populate_repotypes(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    async def test_adding_full_then_adding_partial(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir.name)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            f"+/test.me.1\n"
            f"+/test.me.4\n"
            f"+/wat/test.me.2\n"
            f"+/wat/test.me.3"f"\n"
            f"Sync'ed repo-full-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         4|          |         4|\n"
            "|repo-full-name           |         4|         4|          |\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        35|          |        35|\n"
            "|repo-full-name           |        35|        35|          |\n",
            res)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual(
            f"=/test.me.1\n"
            f"=/wat/test.me.2\n"
            f"Sync'ed repo-partial-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         4|          |         4|\n"
            "|repo-full-name           |         4|         4|          |\n"
            "|repo-partial-name        |         2|         2|          |\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        35|          |        35|\n"
            "|repo-full-name           |        35|        35|          |\n"
            "|repo-partial-name        |        14|        14|          |\n",
            res)

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), (repo-partial-name:.)\n"
            "/test.me.1 = a:2 g:1\n"
            "/test.me.4 = a:1 g:1\n"
            "/wat => (repo-backup-name:wat), (repo-full-name:wat), (repo-incoming-name:wat), (repo-partial-name:wat)\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.3 = a:1 g:1\n"
            "DONE", res)

    async def test_initial_population(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir.name)
        pfw = pretty_file_writer(self.tmpdir.name)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual(f"+/test.me.1\n+/wat/test.me.2\nSync'ed repo-partial-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            f"=/test.me.1\n=/wat/test.me.2\n+/test.me.4\n+/wat/test.me.3"
            f"\nSync'ed repo-full-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         4|          |         4|\n"
            "|repo-full-name           |         4|         4|          |\n"
            "|repo-partial-name        |         2|         2|          |\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        35|          |        35|\n"
            "|repo-full-name           |        35|        35|          |\n"
            "|repo-partial-name        |        14|        14|          |\n",
            res)

        res = await hoard_cmd.contents.ls(show_remotes=True)
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

        res = await full_cave_cmd.refresh(show_details=False)
        self.assertEqual("Refresh done!", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            f"+/wat/test.me.z\n"
            f"-/wat/test.me.3\n"
            f"remove dangling /wat/test.me.3\n"
            f"Sync'ed repo-full-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         4|          |         4|\n"
            "|repo-full-name           |         4|         4|          |\n"
            "|repo-partial-name        |         2|         2|          |\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        45|          |        45|\n"
            "|repo-full-name           |        45|        45|          |\n"
            "|repo-partial-name        |        14|        14|          |\n",
            res)

        res = await hoard_cmd.contents.ls(show_remotes=True)
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

        res = await partial_cave_cmd.refresh(show_details=False)
        self.assertEqual("Refresh done!", res)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual(f"+/test.me.5\nSync'ed repo-partial-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         5|          |         5|\n"
            "|repo-full-name           |         5|         4|         1|\n"
            "|repo-partial-name        |         3|         3|          |\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        53|          |        53|\n"
            "|repo-full-name           |        53|        45|         8|\n"
            "|repo-partial-name        |        22|        22|          |\n",
            res)

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), (repo-partial-name:.)\n"
            "/test.me.1 = a:2 g:1\n"
            "/test.me.4 = a:1 g:1\n"
            "/test.me.5 = a:1 g:2\n"
            "/wat => (repo-backup-name:wat), (repo-full-name:wat), (repo-incoming-name:wat), (repo-partial-name:wat)\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.z = a:1 g:1\n"
            "DONE", res)

        res = await hoard_cmd.contents.get("repo-partial-name", "/wat")
        self.assertEqual("Path /wat must be relative, but is absolute.", res)

        res = await hoard_cmd.contents.get("repo-partial-name", "wat")
        self.assertEqual(
            "+/wat/test.me.z\n"
            "Considered 2 files.\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         5|          |         5|\n"
            "|repo-full-name           |         5|         4|         1|\n"
            "|repo-partial-name        |         4|         3|         1|\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        53|          |        53|\n"
            "|repo-full-name           |        53|        45|         8|\n"
            "|repo-partial-name        |        42|        22|        20|\n",
            res)

        res = await hoard_cmd.files.push("repo-partial-name")
        self.assertEqual(
            f"repo-partial-name:\n"
            "+ wat/test.me.z\n"
            f"repo-partial-name:\n"
            "DONE", res)

        res = await hoard_cmd.files.push("repo-partial-name")
        self.assertEqual(
            f"repo-partial-name:\n"
            f"repo-partial-name:\n"
            "DONE", res)

    async def test_file_is_deleted_before_copied(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir.name)
        pfw = pretty_file_writer(self.tmpdir.name)

        await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())

        # delete file before it is backed up
        assert os.path.isfile(join(self.tmpdir.name, 'repo-full/wat/test.me.3'))
        os.remove(join(self.tmpdir.name, 'repo-full/wat/test.me.3'))
        pfw('repo-full/wat/test.me.z', "whut-whut-in-the-but")

        # still shows the file is presumed there
        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), (repo-partial-name:.)\n"
            "/test.me.1 = a:2 g:1\n"
            "/test.me.4 = a:1 g:1\n"
            "/wat => (repo-backup-name:wat), (repo-full-name:wat), (repo-incoming-name:wat), (repo-partial-name:wat)\n"
            "/wat/test.me.2 = a:2 g:1\n"
            "/wat/test.me.3 = a:1 g:1\n"
            "DONE", res)

        # try to fetch - will have some errors
        res = await hoard_cmd.files.push("repo-backup-name")
        self.assertEqual(
            f"repo-backup-name:\n"
            f"+ test.me.1\n"
            f"+ test.me.4\n"
            f"+ wat/test.me.2\n"
            f"E wat/test.me.3\n"
            f"repo-backup-name:\n"
            f"DONE", res)

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), (repo-partial-name:.)\n"
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:2\n"
            "/wat => (repo-backup-name:wat), (repo-full-name:wat), (repo-incoming-name:wat), (repo-partial-name:wat)\n"
            "/wat/test.me.2 = a:3\n"
            "/wat/test.me.3 = a:1 g:1\n"
            "DONE", res)

        # try to fetch - errors will remain
        res = await hoard_cmd.files.push("repo-backup-name")
        self.assertEqual(
            f"repo-backup-name:\n"
            f"E wat/test.me.3\n"
            f"repo-backup-name:\n"
            "DONE", res)

        # do refresh and pull to detect deleted file and its state
        res = await full_cave_cmd.refresh(show_details=False)
        self.assertEqual("Refresh done!", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            f"+/wat/test.me.z\n"
            f"-/wat/test.me.3\n"
            f"remove dangling /wat/test.me.3\n"
            f"Sync'ed repo-full-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         4|         3|         1|\n"
            "|repo-full-name           |         4|         4|          |\n"
            "|repo-partial-name        |         2|         2|          |\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        45|        25|        20|\n"
            "|repo-full-name           |        45|        45|          |\n"
            "|repo-partial-name        |        14|        14|          |\n",
            res)

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            "/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), (repo-partial-name:.)\n"
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:2\n"
            "/wat => (repo-backup-name:wat), (repo-full-name:wat), (repo-incoming-name:wat), (repo-partial-name:wat)\n"
            "/wat/test.me.2 = a:3\n"
            "/wat/test.me.z = a:1 g:1\n"
            "DONE", res)

    async def test_file_is_deleted_after_copied(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir.name)
        pfw = pretty_file_writer(self.tmpdir.name)

        await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        await hoard_cmd.files.push(backup_cave_cmd.current_uuid())

        res = await hoard_cmd.contents.ls(show_remotes=True)
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

        res = await full_cave_cmd.refresh(show_details=False)
        self.assertEqual("Refresh done!", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(f"-/wat/test.me.2\nSync'ed repo-full-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |cleanup   |\n"
            "|repo-backup-name         |         4|         3|         1|\n"
            "|repo-full-name           |         3|         3|          |\n"
            "|repo-partial-name        |         2|         1|         1|\n"
            "\n"
            "|Size                     |total     |available |cleanup   |\n"
            "|repo-backup-name         |        35|        27|         8|\n"
            "|repo-full-name           |        27|        27|          |\n"
            "|repo-partial-name        |        14|         6|         8|\n", res)

        res = await hoard_cmd.contents.ls()
        self.assertEqual(
            "/\n"
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:2\n"
            "/wat\n"
            "/wat/test.me.2 = c:2"
            "\n/wat/test.me.3 = a:2\n"
            "DONE", res)

        res = await hoard_cmd.files.push("repo-backup-name")
        self.assertEqual(
            f"repo-backup-name:\n"
            f"repo-backup-name:\n"
            "d wat/test.me.2\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |cleanup   |\n"
            "|repo-backup-name         |         3|         3|          |\n"
            "|repo-full-name           |         3|         3|          |\n"
            "|repo-partial-name        |         2|         1|         1|\n"
            "\n"
            "|Size                     |total     |available |cleanup   |\n"
            "|repo-backup-name         |        27|        27|          |\n"
            "|repo-full-name           |        27|        27|          |\n"
            "|repo-partial-name        |        14|         6|         8|\n", res)

        res = await hoard_cmd.files.push("repo-full-name")
        self.assertEqual(
            f"repo-full-name:\n"
            f"repo-full-name:\n"
            "DONE", res)

        res = await hoard_cmd.files.push("repo-partial-name")
        self.assertEqual(
            f"repo-partial-name:\n"
            f"repo-partial-name:\n"
            "d wat/test.me.2\n"
            "remove dangling /wat/test.me.2\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |\n"
            "|repo-backup-name         |         3|         3|\n"
            "|repo-full-name           |         3|         3|\n"
            "|repo-partial-name        |         1|         1|\n"
            "\n"
            "|Size                     |total     |available |\n"
            "|repo-backup-name         |        27|        27|\n"
            "|repo-full-name           |        27|        27|\n"
            "|repo-partial-name        |         6|         6|\n", res)

        res = await hoard_cmd.contents.ls()
        self.assertEqual(
            "/\n"
            "/test.me.1 = a:3\n"
            "/test.me.4 = a:2\n"
            "/wat\n"
            "/wat/test.me.3 = a:2\n"
            "DONE", res)

    async def test_add_fetch_new_repo_after_content_is_in(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir.name)
        pfw = pretty_file_writer(self.tmpdir.name)

        os.mkdir(join(self.tmpdir.name, "new-contents"))
        pfw("new-contents/one-new.file", "eqrghjl9asd")

        # initial pull only partial
        await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())

        new_content_cmd = TotalCommand(path=join(self.tmpdir.name, "new-contents")).cave
        new_content_cmd.init()
        await new_content_cmd.refresh(show_details=False)

        hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "new-contents"), name="repo-new-contents-name",
            mount_point="/wat", type=CaveType.PARTIAL, fetch_new=True)

        res = await hoard_cmd.contents.status()
        self.assertEqual(
            "|Num Files                |             updated|     max|total     |available |get       |\n"
            "|repo-backup-name         |               never|   3.6TB|         2|          |         2|\n"
            "|repo-full-name           |               never|   3.6TB|         2|          |         2|\n"
            "|repo-partial-name        |                 now|   3.6TB|         2|         2|          |\n"
            "\n"
            "|Size                     |             updated|     max|total     |available |get       |\n"
            "|repo-backup-name         |               never|   3.6TB|        14|          |        14|\n"
            "|repo-full-name           |               never|   3.6TB|        14|          |        14|\n"
            "|repo-partial-name        |                 now|   3.6TB|        14|        14|          |\n"
            "", res)

        # refresh new contents file
        res = await hoard_cmd.contents.pull(new_content_cmd.current_uuid())
        self.assertEqual(
            "+/wat/one-new.file\n"
            "Sync'ed repo-new-contents-name to hoard!\n"
            "DONE", res)

        # pull full as well - its files will be added to the new repop
        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            "=/test.me.1\n"
            "=/wat/test.me.2\n"
            "+/test.me.4\n"
            "+/wat/test.me.3\n"
            "Sync'ed repo-full-name to hoard!\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         5|          |         5|\n"
            "|repo-full-name           |         5|         4|         1|\n"
            "|repo-new-contents-name   |         2|         1|         1|\n"
            "|repo-partial-name        |         2|         2|          |\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        46|          |        46|\n"
            "|repo-full-name           |        46|        35|        11|\n"
            "|repo-new-contents-name   |        21|        11|        10|\n"
            "|repo-partial-name        |        14|        14|          |\n",
            res)

        res = await hoard_cmd.contents.get(repo="repo-new-contents-name", path="")
        self.assertEqual(
            "+/wat/test.me.2\n"
            "Considered 3 files.\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         5|          |         5|\n"
            "|repo-full-name           |         5|         4|         1|\n"
            "|repo-new-contents-name   |         3|         1|         2|\n"
            "|repo-partial-name        |         2|         2|          |\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        46|          |        46|\n"
            "|repo-full-name           |        46|        35|        11|\n"
            "|repo-new-contents-name   |        29|        11|        18|\n"
            "|repo-partial-name        |        14|        14|          |\n",
            res)

        res = await new_content_cmd.refresh(show_details=False)
        self.assertEqual("Refresh done!", res)

        res = await hoard_cmd.contents.pull(new_content_cmd.current_uuid())
        self.assertEqual("Sync'ed repo-new-contents-name to hoard!\nDONE", res)

        res = await hoard_cmd.files.push(repo="repo-new-contents-name")
        self.assertEqual(
            f"repo-new-contents-name:\n"
            f"+ test.me.2\n"
            f"+ test.me.3\n"
            f"repo-new-contents-name:\n"
            f"DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         5|          |         5|\n"
            "|repo-full-name           |         5|         4|         1|\n"
            "|repo-new-contents-name   |         3|         3|          |\n"
            "|repo-partial-name        |         2|         2|          |\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        46|          |        46|\n"
            "|repo-full-name           |        46|        35|        11|\n"
            "|repo-new-contents-name   |        29|        29|          |\n"
            "|repo-partial-name        |        14|        14|          |\n",
            res)

        res = await hoard_cmd.files.push(repo=full_cave_cmd.current_uuid())
        self.assertEqual(
            f"repo-full-name:\n"
            f"+ wat/one-new.file\n"
            f"repo-full-name:\n"
            f"DONE", res)

        self.assertDictEqual(
            dump_file_list(self.tmpdir.name + "/repo-full/wat", "", data=True),
            dump_file_list(self.tmpdir.name + "/new-contents", "", data=True))

    async def test_resetting_file_contents(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir.name)

        pfw = pretty_file_writer(self.tmpdir.name)
        os.mkdir(join(self.tmpdir.name, "changed-cave"))
        pfw('changed-cave/test.me.1', "w3q45yhq3g")
        pfw('changed-cave/test.me.4', "fwadeaewdsa")
        pfw('changed-cave/wat/test.me.2', "gsadf3dq")
        pfw('changed-cave/wat/test.me.3', "'psejmfw'")

        changed_cave_cmd = TotalCommand(path=join(self.tmpdir.name, "changed-cave")).cave
        changed_cave_cmd.init()
        res = await changed_cave_cmd.refresh(show_details=False)
        self.assertEqual("Refresh done!", res)

        res = hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "changed-cave"), name="repo-changed-cave-name",
            mount_point="/", type=CaveType.PARTIAL, fetch_new=False)
        self.assertEqual(
            fr"Added repo-changed-cave-name[{changed_cave_cmd.current_uuid()}] at {self.tmpdir.name}\changed-cave!",
            res)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual(
            "+/test.me.1\n"
            "+/wat/test.me.2\n"
            "Sync'ed repo-partial-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            "=/test.me.1\n"
            "=/wat/test.me.2\n"
            "+/test.me.4\n"
            "+/wat/test.me.3\n"
            "Sync'ed repo-full-name to hoard!\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         4|          |         4|\n"
            "|repo-full-name           |         4|         4|          |\n"
            "|repo-partial-name        |         2|         2|          |\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        35|          |        35|\n"
            "|repo-full-name           |        35|        35|          |\n"
            "|repo-partial-name        |        14|        14|          |\n", res)

        res = await hoard_cmd.contents.pull(changed_cave_cmd.current_uuid(), assume_current=True)
        self.assertEqual(
            "=/test.me.4\n"
            "=/wat/test.me.2\n"
            "RESETTING /test.me.1\n"
            "RESETTING /wat/test.me.3\n"
            "Sync'ed repo-changed-cave-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         4|          |         4|\n"
            "|repo-changed-cave-name   |         4|         4|          |\n"
            "|repo-full-name           |         4|         2|         2|\n"
            "|repo-partial-name        |         2|         1|         1|\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        38|          |        38|\n"
            "|repo-changed-cave-name   |        38|        38|          |\n"
            "|repo-full-name           |        38|        19|        19|\n"
            "|repo-partial-name        |        18|         8|        10|\n", res)

        res = await hoard_cmd.files.pending()
        self.assertEqual(
            "repo-partial-name:\n"
            "TO_GET (from 1) /test.me.1\n"
            " repo-changed-cave-name has 1 files\n"
            "repo-full-name:\n"
            "TO_GET (from 1) /test.me.1\n"
            "TO_GET (from 1) /wat/test.me.3\n"
            " repo-changed-cave-name has 2 files\n"
            "repo-backup-name:\n"
            "TO_GET (from 1) /test.me.1\n"
            "TO_GET (from 3) /wat/test.me.2\n"
            "TO_GET (from 2) /test.me.4\n"
            "TO_GET (from 1) /wat/test.me.3\n"
            " repo-changed-cave-name has 4 files\n"
            " repo-full-name has 2 files\n"
            " repo-partial-name has 1 files\n"
            "repo-incoming-name:\n"
            "repo-changed-cave-name:\n"
            "DONE", res)

        # resetting pending ops
        res = await hoard_cmd.contents.reset("repo-full-name")
        self.assertEqual(
            "repo-full-name:\n"
            "WONT_GET /test.me.1\n"
            "WONT_GET /wat/test.me.3\n"
            "DONE", res)

        res = await hoard_cmd.files.pending()
        self.assertEqual(
            "repo-partial-name:\n"
            "TO_GET (from 1) /test.me.1\n"
            " repo-changed-cave-name has 1 files\n"
            "repo-full-name:\n"
            "repo-backup-name:\n"
            "TO_GET (from 1) /test.me.1\n"
            "TO_GET (from 3) /wat/test.me.2\n"
            "TO_GET (from 2) /test.me.4\n"
            "TO_GET (from 1) /wat/test.me.3\n"
            " repo-changed-cave-name has 4 files\n"
            " repo-full-name has 2 files\n"
            " repo-partial-name has 1 files\n"
            "repo-incoming-name:\n"
            "repo-changed-cave-name:\n"
            "DONE", res)

        # resetting existing contents to what repo-full-name should contain!
        res = await hoard_cmd.contents.reset_with_existing("repo-full-name")
        self.assertEqual(
            "repo-full-name:\n"
            "RESET /test.me.1\n"
            "RESET /wat/test.me.3\n"
            "DONE", res)

        res = await hoard_cmd.files.pending()
        self.assertEqual(
            "repo-partial-name:\n"
            "TO_GET (from 1) /test.me.1\n"
            " repo-full-name has 1 files\n"
            "repo-full-name:\n"
            "repo-backup-name:\n"
            "TO_GET (from 1) /test.me.1\n"
            "TO_GET (from 3) /wat/test.me.2\n"
            "TO_GET (from 2) /test.me.4\n"
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

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         4|          |         4|\n"
            "|repo-changed-cave-name   |         4|         2|         2|\n"
            "|repo-full-name           |         4|         4|          |\n"
            "|repo-partial-name        |         2|         1|         1|\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        35|          |        35|\n"
            "|repo-changed-cave-name   |        35|        19|        16|\n"
            "|repo-full-name           |        35|        35|          |\n"
            "|repo-partial-name        |        14|         8|         6|\n", res)

    async def test_moving_of_files_in_hoard(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir.name)

        os.mkdir(join(self.tmpdir.name, 'repo-copy'))
        copy_cave_cmd = TotalCommand(path=join(self.tmpdir.name, 'repo-copy')).cave
        copy_cave_cmd.init()
        await copy_cave_cmd.refresh()

        res = hoard_cmd.add_remote(
            join(self.tmpdir.name, 'repo-copy'), name="repo-copy-name", mount_point="/",
            type=CaveType.PARTIAL, fetch_new=True)
        self.assertEqual(
            fr"Added repo-copy-name[{copy_cave_cmd.current_uuid()}] at {self.tmpdir.name}\repo-copy!", res)

        pfw = pretty_file_writer(self.tmpdir.name)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual(f"+/test.me.1\n+/wat/test.me.2\nSync'ed repo-partial-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.pending(full_cave_cmd.current_uuid())
        self.assertEqual(
            "Status of repo-full-name:\n"
            "PRESENT /test.me.4\n"
            "PRESENT /wat/test.me.3\n"
            "DONE", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            f"=/test.me.1\n=/wat/test.me.2\n+/test.me.4\n+/wat/test.me.3"
            f"\nSync'ed repo-full-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         4|          |         4|\n"
            "|repo-copy-name           |         4|          |         4|\n"
            "|repo-full-name           |         4|         4|          |\n"
            "|repo-partial-name        |         2|         2|          |\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        35|          |        35|\n"
            "|repo-copy-name           |        35|          |        35|\n"
            "|repo-full-name           |        35|        35|          |\n"
            "|repo-partial-name        |        14|        14|          |\n",
            res)

        res = await hoard_cmd.files.push(copy_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-copy-name:\n"
            "+ test.me.1\n"
            "+ test.me.4\n"
            "+ wat/test.me.2\n"
            "+ wat/test.me.3\n"
            "repo-copy-name:\n"
            "DONE", res)

        res = await full_cave_cmd.refresh()
        self.assertEqual("NO CHANGES\nRefresh done!", res)

        pathlib.Path(join(self.tmpdir.name, 'repo-full/lets_get_it_started')).mkdir(parents=True)

        pfw('repo-full/test.me.1', "age44")
        pfw('repo-full/test.me.added', "fhagf")
        shutil.move(
            join(self.tmpdir.name, 'repo-full/test.me.4'),
            join(self.tmpdir.name, 'repo-full/lets_get_it_started/test.me.4-renamed'))

        pfw('repo-full/lets_get_it_started/test.me.2-butnew', "gsadf3dq")
        shutil.move(
            join(self.tmpdir.name, 'repo-full/wat/test.me.2'),
            join(self.tmpdir.name, 'repo-full/lets_get_it_started/test.me.2-butsecond'))

        res = await full_cave_cmd.status()
        self.assertEqual(
            f"{full_cave_cmd.current_uuid()}:\n"
            f"files:\n"
            f"    same: 1 (20.0%)\n"
            f"     mod: 1 (20.0%)\n"
            f"     new: 3 (60.0%)\n"
            f"   moved: 1 (20.0%)\n"
            f" current: 5\n"
            f" in repo: 4\n"
            f" deleted: 1 (25.0%)\n", res)

        res = await full_cave_cmd.refresh()
        self.assertEqual(
            "MOVED test.me.4 TO lets_get_it_started/test.me.4-renamed\n"
            "REMOVED_FILE_FALLBACK_TOO_MANY wat/test.me.2\n"
            "ADDED_FILE test.me.added\n"
            "ADDED_FILE lets_get_it_started/test.me.2-butnew\n"
            "ADDED_FILE lets_get_it_started/test.me.2-butsecond\n"
            "MODIFIED_FILE test.me.1\n"
            "Refresh done!", res)

        res = await hoard_cmd.contents.pending(full_cave_cmd.current_uuid())
        self.assertEqual(
            "Status of repo-full-name:\n"
            "ADDED /lets_get_it_started/test.me.2-butnew\n"
            "ADDED /lets_get_it_started/test.me.2-butsecond\n"
            "ADDED /lets_get_it_started/test.me.4-renamed\n"
            "MODIFIED /test.me.1\n"
            "ADDED /test.me.added\n"
            "DELETED /wat/test.me.2\n"
            "MOVED /test.me.4\n"
            "DONE", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            "+/lets_get_it_started/test.me.2-butnew\n"
            "+/lets_get_it_started/test.me.2-butsecond\n"
            "+/lets_get_it_started/test.me.4-renamed\n"  # todo should not be logged, as we move it
            "+/test.me.added\n"
            "u/test.me.1\n"
            "-/wat/test.me.2\n"
            "MOVE repo-copy-name: /test.me.4 to /lets_get_it_started/test.me.4-renamed\n"
            "CLEANUP_MOVED /test.me.4\n"
            "Sync'ed repo-full-name to hoard!\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |move      |cleanup   |\n"
            "|repo-backup-name         |         7|          |         6|          |         1|\n"
            "|repo-copy-name           |         8|         1|         4|         1|         2|\n"
            "|repo-full-name           |         6|         6|          |          |          |\n"
            "|repo-partial-name        |         2|          |         1|          |         1|\n"
            "\n"
            "|Size                     |total     |available |get       |move      |cleanup   |\n"
            "|repo-backup-name         |        58|          |        47|          |        11|\n"
            "|repo-copy-name           |        66|        10|        26|        11|        19|\n"
            "|repo-full-name           |        47|        47|          |          |          |\n"
            "|repo-partial-name        |        13|          |         5|          |         8|\n",
            res)

        res = await hoard_cmd.files.push(full_cave_cmd.current_uuid())
        self.assertEqual("repo-full-name:\nrepo-full-name:\nDONE", res)

        res = await hoard_cmd.files.pending(copy_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-copy-name:\n"
            "TO_GET (from 1) /test.me.1\n"
            "TO_CLEANUP (is in 0) /wat/test.me.2\n"
            "TO_CLEANUP (is in 0) /test.me.4\n"
            "TO_GET (from 1) /lets_get_it_started/test.me.2-butnew\n"
            "TO_GET (from 1) /lets_get_it_started/test.me.2-butsecond\n"
            "TO_MOVE /lets_get_it_started/test.me.4-renamed from /test.me.4\n"
            "TO_GET (from 1) /test.me.added\n"
            " repo-full-name has 4 files\n"
            "DONE", res)

        res = await hoard_cmd.files.push(copy_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-copy-name:\n"
            "+ lets_get_it_started/test.me.2-butnew\n"
            "+ lets_get_it_started/test.me.2-butsecond\n"
            "MOVED /test.me.4 to /lets_get_it_started/test.me.4-renamed\n"
            "+ test.me.1\n"
            "+ test.me.added\n"
            "repo-copy-name:\n"
            "d test.me.4\n"
            "d wat/test.me.2\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |cleanup   |\n"
            "|repo-backup-name         |         7|          |         6|         1|\n"
            "|repo-copy-name           |         6|         6|          |          |\n"
            "|repo-full-name           |         6|         6|          |          |\n"
            "|repo-partial-name        |         2|          |         1|         1|\n"
            "\n"
            "|Size                     |total     |available |get       |cleanup   |\n"
            "|repo-backup-name         |        58|          |        47|        11|\n"
            "|repo-copy-name           |        47|        47|          |          |\n"
            "|repo-full-name           |        47|        47|          |          |\n"
            "|repo-partial-name        |        13|          |         5|         8|\n",
            res)

        res = await hoard_cmd.files.pending(backup_cave_cmd.current_uuid())
        self.assertEqual(
            'repo-backup-name:\n'
            'TO_GET (from 2) /test.me.1\n'
            'TO_CLEANUP (is in 0) /test.me.4\n'
            'TO_GET (from 2) /wat/test.me.3\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.2-butnew\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.2-butsecond\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.4-renamed\n'
            'TO_GET (from 2) /test.me.added\n'
            ' repo-copy-name has 6 files\n'
            ' repo-full-name has 6 files\n'
            'DONE', res)

        res = await hoard_cmd.files.push(copy_cave_cmd.current_uuid())
        self.assertEqual("repo-copy-name:\nrepo-copy-name:\nDONE", res)

        res = await copy_cave_cmd.refresh()
        self.assertEqual(
            "ADDED_FILE test.me.1\n"
            "ADDED_FILE test.me.added\n"
            "ADDED_FILE lets_get_it_started/test.me.2-butnew\n"
            "ADDED_FILE lets_get_it_started/test.me.2-butsecond\n"
            "ADDED_FILE lets_get_it_started/test.me.4-renamed\n"
            "ADDED_FILE wat/test.me.3\n"
            "Refresh done!", res)

        res = await full_cave_cmd.refresh()
        self.assertEqual("NO CHANGES\nRefresh done!", res)

        res = await hoard_cmd.contents.pull(copy_cave_cmd.current_uuid())
        self.assertEqual("Sync'ed repo-copy-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual("Sync'ed repo-full-name to hoard!\nDONE", res)

    async def test_moving_of_files_in_hoard_with_backups(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir.name)

        os.mkdir(join(self.tmpdir.name, 'repo-copy'))
        copy_cave_cmd = TotalCommand(path=join(self.tmpdir.name, 'repo-copy')).cave
        copy_cave_cmd.init()
        await copy_cave_cmd.refresh()

        res = hoard_cmd.add_remote(
            join(self.tmpdir.name, 'repo-copy'), name="repo-copy-name", mount_point="/",
            type=CaveType.PARTIAL, fetch_new=True)
        self.assertEqual(
            fr"Added repo-copy-name[{copy_cave_cmd.current_uuid()}] at {self.tmpdir.name}\repo-copy!", res)

        pfw = pretty_file_writer(self.tmpdir.name)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual(f"+/test.me.1\n+/wat/test.me.2\nSync'ed repo-partial-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.pending(full_cave_cmd.current_uuid())
        self.assertEqual(
            "Status of repo-full-name:\n"
            "PRESENT /test.me.4\n"
            "PRESENT /wat/test.me.3\n"
            "DONE", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            f"=/test.me.1\n=/wat/test.me.2\n+/test.me.4\n+/wat/test.me.3"
            f"\nSync'ed repo-full-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.pull(backup_cave_cmd.current_uuid())
        self.assertEqual(
            f"=/test.me.1\n"
            f"=/wat/test.me.3\n"
            f"Sync'ed repo-backup-name to hoard!\n"
            f"DONE", res)

        res = await hoard_cmd.files.push(backup_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-backup-name:\n"
            "+ test.me.4\n"
            "+ wat/test.me.2\n"
            "repo-backup-name:\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         4|         4|          |\n"
            "|repo-copy-name           |         4|          |         4|\n"
            "|repo-full-name           |         4|         4|          |\n"
            "|repo-partial-name        |         2|         2|          |\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        35|        35|          |\n"
            "|repo-copy-name           |        35|          |        35|\n"
            "|repo-full-name           |        35|        35|          |\n"
            "|repo-partial-name        |        14|        14|          |\n",
            res)

        res = await hoard_cmd.files.push(copy_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-copy-name:\n"
            "+ test.me.1\n"
            "+ test.me.4\n"
            "+ wat/test.me.2\n"
            "+ wat/test.me.3\n"
            "repo-copy-name:\n"
            "DONE", res)

        res = await full_cave_cmd.refresh()
        self.assertEqual("NO CHANGES\nRefresh done!", res)

        pathlib.Path(join(self.tmpdir.name, 'repo-full/lets_get_it_started')).mkdir(parents=True)

        pfw('repo-full/test.me.1', "age44")
        pfw('repo-full/test.me.added', "fhagf")
        shutil.move(
            join(self.tmpdir.name, 'repo-full/test.me.4'),
            join(self.tmpdir.name, 'repo-full/lets_get_it_started/test.me.4-renamed'))

        pfw('repo-full/lets_get_it_started/test.me.2-butnew', "gsadf3dq")
        shutil.move(
            join(self.tmpdir.name, 'repo-full/wat/test.me.2'),
            join(self.tmpdir.name, 'repo-full/lets_get_it_started/test.me.2-butsecond'))

        res = await full_cave_cmd.refresh()
        self.assertEqual(
            "MOVED test.me.4 TO lets_get_it_started/test.me.4-renamed\n"
            "REMOVED_FILE_FALLBACK_TOO_MANY wat/test.me.2\n"
            "ADDED_FILE test.me.added\n"
            "ADDED_FILE lets_get_it_started/test.me.2-butnew\n"
            "ADDED_FILE lets_get_it_started/test.me.2-butsecond\n"
            "MODIFIED_FILE test.me.1\n"
            "Refresh done!", res)

        res = full_cave_cmd.status_index(show_dates=False)
        self.assertEqual(
            "lets_get_it_started/test.me.2-butnew: added @ 3\n"
            "lets_get_it_started/test.me.2-butsecond: added @ 3\n"
            "lets_get_it_started/test.me.4-renamed: added @ 3\n"
            "test.me.1: modified @ 3\n"
            "test.me.4: moved_from @ 3\n"
            "test.me.added: added @ 3\n"
            "wat/test.me.2: deleted @ 3\n"
            "wat/test.me.3: present @ 1\n"
            "--- SUMMARY ---\n"
            "Result for local\n"
            "Max size: 3.6TB\n"
            f"UUID: {full_cave_cmd.current_uuid()}\n"
            "  # files = 6 of size 47\n", res)

        res = await hoard_cmd.contents.pending(full_cave_cmd.current_uuid())
        self.assertEqual(
            "Status of repo-full-name:\n"
            "ADDED /lets_get_it_started/test.me.2-butnew\n"
            "ADDED /lets_get_it_started/test.me.2-butsecond\n"
            "ADDED /lets_get_it_started/test.me.4-renamed\n"
            "MODIFIED /test.me.1\n"
            "ADDED /test.me.added\n"
            "DELETED /wat/test.me.2\n"
            "MOVED /test.me.4\n"
            "DONE", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            "+/lets_get_it_started/test.me.2-butnew\n"
            "+/lets_get_it_started/test.me.2-butsecond\n"
            "+/lets_get_it_started/test.me.4-renamed\n"  # todo should not be logged, as we move it
            "+/test.me.added\n"
            "u/test.me.1\n"
            "-/wat/test.me.2\n"
            "MOVE repo-backup-name: /test.me.4 to /lets_get_it_started/test.me.4-renamed\n"
            "MOVE repo-copy-name: /test.me.4 to /lets_get_it_started/test.me.4-renamed\n"
            "CLEANUP_MOVED /test.me.4\n"
            "Sync'ed repo-full-name to hoard!\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |move      |cleanup   |\n"
            "|repo-backup-name         |         8|         1|         4|         1|         2|\n"
            "|repo-copy-name           |         8|         1|         4|         1|         2|\n"
            "|repo-full-name           |         6|         6|          |          |          |\n"
            "|repo-partial-name        |         2|          |         1|          |         1|\n"
            "\n"
            "|Size                     |total     |available |get       |move      |cleanup   |\n"
            "|repo-backup-name         |        66|        10|        26|        11|        19|\n"
            "|repo-copy-name           |        66|        10|        26|        11|        19|\n"
            "|repo-full-name           |        47|        47|          |          |          |\n"
            "|repo-partial-name        |        13|          |         5|          |         8|\n",
            res)

        res = await hoard_cmd.files.push(full_cave_cmd.current_uuid())
        self.assertEqual("repo-full-name:\nrepo-full-name:\nDONE", res)

        res = await hoard_cmd.files.pending(copy_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-copy-name:\n"
            "TO_GET (from 1) /test.me.1\n"
            "TO_CLEANUP (is in 0) /wat/test.me.2\n"
            "TO_CLEANUP (is in 0) /test.me.4\n"
            "TO_GET (from 1) /lets_get_it_started/test.me.2-butnew\n"
            "TO_GET (from 1) /lets_get_it_started/test.me.2-butsecond\n"
            "TO_MOVE /lets_get_it_started/test.me.4-renamed from /test.me.4\n"
            "TO_GET (from 1) /test.me.added\n"
            " repo-full-name has 4 files\n"
            "DONE", res)

        res = await hoard_cmd.files.push(copy_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-copy-name:\n"
            "+ lets_get_it_started/test.me.2-butnew\n"
            "+ lets_get_it_started/test.me.2-butsecond\n"
            "MOVED /test.me.4 to /lets_get_it_started/test.me.4-renamed\n"
            "+ test.me.1\n"
            "+ test.me.added\n"
            "repo-copy-name:\n"
            "d test.me.4\n"
            "d wat/test.me.2\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |move      |cleanup   |\n"
            "|repo-backup-name         |         8|         1|         4|         1|         2|\n"
            "|repo-copy-name           |         6|         6|          |          |          |\n"
            "|repo-full-name           |         6|         6|          |          |          |\n"
            "|repo-partial-name        |         2|          |         1|          |         1|\n"
            "\n"
            "|Size                     |total     |available |get       |move      |cleanup   |\n"
            "|repo-backup-name         |        66|        10|        26|        11|        19|\n"
            "|repo-copy-name           |        47|        47|          |          |          |\n"
            "|repo-full-name           |        47|        47|          |          |          |\n"
            "|repo-partial-name        |        13|          |         5|          |         8|\n",
            res)

        res = await hoard_cmd.files.pending(backup_cave_cmd.current_uuid())
        self.assertEqual(
            'repo-backup-name:\n'
            'TO_GET (from 2) /test.me.1\n'
            'TO_CLEANUP (is in 0) /wat/test.me.2\n'
            'TO_CLEANUP (is in 0) /test.me.4\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.2-butnew\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.2-butsecond\n'
            'TO_MOVE /lets_get_it_started/test.me.4-renamed from /test.me.4\n'
            'TO_GET (from 2) /test.me.added\n'
            ' repo-copy-name has 4 files\n'
            ' repo-full-name has 4 files\n'
            'DONE', res)

        res = await hoard_cmd.files.push(copy_cave_cmd.current_uuid())
        self.assertEqual("repo-copy-name:\nrepo-copy-name:\nDONE", res)

        res = await copy_cave_cmd.refresh()
        self.assertEqual(
            "ADDED_FILE test.me.1\n"
            "ADDED_FILE test.me.added\n"
            "ADDED_FILE lets_get_it_started/test.me.2-butnew\n"
            "ADDED_FILE lets_get_it_started/test.me.2-butsecond\n"
            "ADDED_FILE lets_get_it_started/test.me.4-renamed\n"
            "ADDED_FILE wat/test.me.3\n"
            "Refresh done!", res)

        res = await hoard_cmd.files.push(backup_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-backup-name:\n"
            "+ lets_get_it_started/test.me.2-butnew\n"
            "+ lets_get_it_started/test.me.2-butsecond\n"
            "MOVED /test.me.4 to /lets_get_it_started/test.me.4-renamed\n"
            "+ test.me.1\n"
            "+ test.me.added\n"
            "repo-backup-name:\n"
            "d test.me.4\n"
            "d wat/test.me.2\n"
            "remove dangling /test.me.4\n"
            "DONE", res)

        res = await backup_cave_cmd.refresh()
        self.assertEqual(
            "ADDED_FILE test.me.added\n"
            "ADDED_FILE lets_get_it_started/test.me.2-butnew\n"
            "ADDED_FILE lets_get_it_started/test.me.2-butsecond\n"
            "ADDED_FILE lets_get_it_started/test.me.4-renamed\n"
            "MODIFIED_FILE test.me.1\n"
            "Refresh done!", res)

        res = await full_cave_cmd.refresh()
        self.assertEqual("NO CHANGES\nRefresh done!", res)

        res = await hoard_cmd.contents.pull(copy_cave_cmd.current_uuid())
        self.assertEqual("Sync'ed repo-copy-name to hoard!\nDONE", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual("Sync'ed repo-full-name to hoard!\nDONE", res)

        res = await hoard_cmd.files.pending(partial_cave_cmd.current_uuid())
        self.assertEqual(
            'repo-partial-name:\n'
            'TO_GET (from 3) /test.me.1\n'
            'TO_CLEANUP (is in 0) /wat/test.me.2\n'
            ' repo-backup-name has 1 files\n'
            ' repo-copy-name has 1 files\n'
            ' repo-full-name has 1 files\n'
            'DONE', res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |cleanup   |\n"
            "|repo-backup-name         |         6|         6|          |          |\n"
            "|repo-copy-name           |         6|         6|          |          |\n"
            "|repo-full-name           |         6|         6|          |          |\n"
            "|repo-partial-name        |         2|          |         1|         1|\n"  # fixme error!
            "\n"
            "|Size                     |total     |available |get       |cleanup   |\n"
            "|repo-backup-name         |        47|        47|          |          |\n"
            "|repo-copy-name           |        47|        47|          |          |\n"
            "|repo-full-name           |        47|        47|          |          |\n"
            "|repo-partial-name        |        13|          |         5|         8|\n", res)

        # move file before being synch-ed
        shutil.move(
            join(self.tmpdir.name, 'repo-partial/test.me.1'),
            join(self.tmpdir.name, 'repo-partial/test.me.1-newlocation'))

        res = await partial_cave_cmd.refresh()
        self.assertEqual("MOVED test.me.1 TO test.me.1-newlocation\nRefresh done!", res)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual(
            "?/wat/test.me.2\n"
            "+/test.me.1-newlocation\n"
            # expected error - move will fail, but the fallback is to just get it
            "ERROR_ON_MOVE bad current status = HoardFileStatus.GET, won't move.\n"
            "Sync'ed repo-partial-name to hoard!\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |cleanup   |\n"
            "|repo-backup-name         |         7|         6|         1|          |\n"
            "|repo-copy-name           |         7|         6|         1|          |\n"
            "|repo-full-name           |         7|         6|         1|          |\n"
            "|repo-partial-name        |         3|         1|         1|         1|\n"
            "\n"
            "|Size                     |total     |available |get       |cleanup   |\n"
            "|repo-backup-name         |        53|        47|         6|          |\n"
            "|repo-copy-name           |        53|        47|         6|          |\n"
            "|repo-full-name           |        53|        47|         6|          |\n"
            "|repo-partial-name        |        19|         6|         5|         8|\n", res)

        res = await hoard_cmd.files.push(partial_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-partial-name:\n"
            "+ test.me.1\n"
            "repo-partial-name:\n"
            "d wat/test.me.2\n"
            "remove dangling /wat/test.me.2\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         7|         6|         1|\n"
            "|repo-copy-name           |         7|         6|         1|\n"
            "|repo-full-name           |         7|         6|         1|\n"
            "|repo-partial-name        |         2|         2|          |\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        53|        47|         6|\n"
            "|repo-copy-name           |        53|        47|         6|\n"
            "|repo-full-name           |        53|        47|         6|\n"
            "|repo-partial-name        |        11|        11|          |\n", res)

    async def test_moving_of_files_before_first_refresh(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir.name)

        pfw = pretty_file_writer(self.tmpdir.name)

        os.mkdir(join(self.tmpdir.name, 'repo-copy'))
        copy_cave_cmd = TotalCommand(path=join(self.tmpdir.name, 'repo-copy')).cave
        copy_cave_cmd.init()
        await copy_cave_cmd.refresh()

        hoard_cmd.add_remote(
            join(self.tmpdir.name, 'repo-copy'), name="repo-copy-name", mount_point="/",
            type=CaveType.PARTIAL, fetch_new=True)

        await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         2|          |         2|\n"
            "|repo-copy-name           |         2|          |         2|\n"
            "|repo-full-name           |         2|          |         2|\n"
            "|repo-partial-name        |         2|         2|          |\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        14|          |        14|\n"
            "|repo-copy-name           |        14|          |        14|\n"
            "|repo-full-name           |        14|          |        14|\n"
            "|repo-partial-name        |        14|        14|          |\n", res)

        res = full_cave_cmd.status_index(show_dates=False)
        self.assertEqual(
            "test.me.1: present @ 1\n"
            "test.me.4: present @ 1\n"
            "wat/test.me.2: present @ 1\n"
            "wat/test.me.3: present @ 1\n"
            "--- SUMMARY ---\n"
            "Result for local\n"
            "Max size: 3.6TB\n"
            f"UUID: {full_cave_cmd.current_uuid()}\n"
            "  # files = 4 of size 35\n", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            "=/test.me.1\n"
            "=/wat/test.me.2\n"
            "+/test.me.4\n"
            "+/wat/test.me.3\n"
            "Sync'ed repo-full-name to hoard!\n"
            "DONE", res)

        res = await hoard_cmd.files.push(backup_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-backup-name:\n"
            "+ test.me.1\n"
            "+ test.me.4\n"
            "+ wat/test.me.2\n"
            "+ wat/test.me.3\n"
            "repo-backup-name:\n"
            "DONE", res)

        # modify contents
        pathlib.Path(join(self.tmpdir.name, 'repo-full/lets_get_it_started')).mkdir(parents=True)
        pfw('repo-full/test.me.1', "age44")
        pfw('repo-full/test.me.added', "fhagf")
        shutil.move(
            join(self.tmpdir.name, 'repo-full/test.me.4'),
            join(self.tmpdir.name, 'repo-full/lets_get_it_started/test.me.4-renamed'))

        # simulate removing of epoch and data
        os.unlink(join(self.tmpdir.name, 'repo-full', '.hoard', f'{full_cave_cmd.current_uuid()}.contents'))

        res = await hoard_cmd.export_contents_to_repo(full_cave_cmd.current_uuid())
        self.assertEqual(
            "PRESENT test.me.1\n"
            "PRESENT wat/test.me.2\n"
            "PRESENT test.me.4\n"
            "PRESENT wat/test.me.3\n"
            "DONE", res)

        res = await full_cave_cmd.refresh()
        self.assertEqual(
            "MOVED test.me.4 TO lets_get_it_started/test.me.4-renamed\n"
            "ADDED_FILE test.me.added\n"
            "MODIFIED_FILE test.me.1\n"
            "Refresh done!", res)

        res = full_cave_cmd.status_index(show_dates=False)
        self.assertEqual(
            "lets_get_it_started/test.me.4-renamed: added @ 2\n"
            "test.me.1: modified @ 2\n"
            "test.me.4: moved_from @ 2\n"
            "test.me.added: added @ 2\n"
            "wat/test.me.2: present @ 1\n"
            "wat/test.me.3: present @ 1\n"
            "--- SUMMARY ---\n"
            "Result for local\n"
            "Max size: 3.6TB\n"
            f"UUID: {full_cave_cmd.current_uuid()}\n"
            "  # files = 5 of size 39\n", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            "+/lets_get_it_started/test.me.4-renamed\n"
            "+/test.me.added\n"
            "u/test.me.1\n"
            "MOVE repo-backup-name: /test.me.4 to /lets_get_it_started/test.me.4-renamed\n"
            "CLEANUP_MOVED /test.me.4\n"
            "Sync'ed repo-full-name to hoard!\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |move      |cleanup   |\n"
            "|repo-backup-name         |         6|         2|         2|         1|         1|\n"
            "|repo-copy-name           |         6|          |         5|          |         1|\n"
            "|repo-full-name           |         5|         5|          |          |          |\n"
            "|repo-partial-name        |         2|         1|         1|          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |move      |cleanup   |\n"
            "|repo-backup-name         |        50|        18|        10|        11|        11|\n"
            "|repo-copy-name           |        50|          |        39|          |        11|\n"
            "|repo-full-name           |        39|        39|          |          |          |\n"
            "|repo-partial-name        |        13|         8|         5|          |          |\n", res)

        res = await hoard_cmd.contents.pull(backup_cave_cmd.current_uuid())
        self.assertEqual(
            f"ALREADY_MARKED_GET /test.me.1\n"
            f"g/wat/test.me.2\n"
            f"Sync'ed repo-backup-name to hoard!\n"
            f"DONE", res)

        res = await hoard_cmd.files.push(backup_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-backup-name:\n"
            "MOVED /test.me.4 to /lets_get_it_started/test.me.4-renamed\n"
            "+ test.me.1\n"
            "+ test.me.added\n"
            "+ wat/test.me.2\n"
            "repo-backup-name:\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |cleanup   |\n"
            "|repo-backup-name         |         5|         5|          |          |\n"
            "|repo-copy-name           |         6|          |         5|         1|\n"
            "|repo-full-name           |         5|         5|          |          |\n"
            "|repo-partial-name        |         2|         1|         1|          |\n"
            "\n"
            "|Size                     |total     |available |get       |cleanup   |\n"
            "|repo-backup-name         |        39|        39|          |          |\n"
            "|repo-copy-name           |        50|          |        39|        11|\n"
            "|repo-full-name           |        39|        39|          |          |\n"
            "|repo-partial-name        |        13|         8|         5|          |\n", res)

        res = await hoard_cmd.files.push(copy_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-copy-name:\n"
            "+ lets_get_it_started/test.me.4-renamed\n"
            "+ test.me.1\n"
            "+ test.me.added\n"
            "+ wat/test.me.2\n"
            "+ wat/test.me.3\n"
            "repo-copy-name:\n"
            "d test.me.4\n"
            "remove dangling /test.me.4\n"
            "DONE", res)

        res = await full_cave_cmd.refresh()
        self.assertEqual("NO CHANGES\nRefresh done!", res)

        pfw('repo-full/lets_get_it_started/test.me.2-butnew', "gsadf3dq")
        shutil.move(
            join(self.tmpdir.name, 'repo-full/wat/test.me.2'),
            join(self.tmpdir.name, 'repo-full/lets_get_it_started/test.me.2-butsecond'))

        res = await full_cave_cmd.refresh()
        self.assertEqual(
            "REMOVED_FILE_FALLBACK_TOO_MANY wat/test.me.2\n"
            "ADDED_FILE lets_get_it_started/test.me.2-butnew\n"
            "ADDED_FILE lets_get_it_started/test.me.2-butsecond\n"
            "Refresh done!", res)

        res = await hoard_cmd.contents.pending(full_cave_cmd.current_uuid())
        self.assertEqual(
            "Status of repo-full-name:\n"
            "ADDED /lets_get_it_started/test.me.2-butnew\n"
            "ADDED /lets_get_it_started/test.me.2-butsecond\n"
            "DELETED /wat/test.me.2\n"
            "DONE", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            "+/lets_get_it_started/test.me.2-butnew\n"
            "+/lets_get_it_started/test.me.2-butsecond\n"
            "-/wat/test.me.2\n"
            "Sync'ed repo-full-name to hoard!\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |cleanup   |\n"
            "|repo-backup-name         |         7|         4|         2|         1|\n"
            "|repo-copy-name           |         7|         4|         2|         1|\n"
            "|repo-full-name           |         6|         6|          |          |\n"
            "|repo-partial-name        |         2|          |         1|         1|\n"
            "\n"
            "|Size                     |total     |available |get       |cleanup   |\n"
            "|repo-backup-name         |        55|        31|        16|         8|\n"
            "|repo-copy-name           |        55|        31|        16|         8|\n"
            "|repo-full-name           |        47|        47|          |          |\n"
            "|repo-partial-name        |        13|          |         5|         8|\n",
            res)
