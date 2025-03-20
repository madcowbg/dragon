import os
import tempfile
import unittest
from os.path import join

from command.test_hoard_command import populate_hoard, populate_repotypes, init_complex_hoard
from command.test_repo_command import pretty_file_writer


class TestFileChangingFlows(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        populate_hoard(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_adding_full_then_adding_partial(self):
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            init_complex_hoard(self.tmpdir.name)

        res = hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            f"+/test.me.1\n"
            f"+/test.me.4\n"
            f"+/wat/test.me.2\n"
            f"+/wat/test.me.3"f"\n"
            f"Sync'ed {full_cave_cmd.current_uuid()} to hoard!", res)

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
            f"Sync'ed {partial_cave_cmd.current_uuid()} to hoard!", res)

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
        populate_repotypes(self.tmpdir.name)
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            init_complex_hoard(self.tmpdir.name)
        pfw = pretty_file_writer(self.tmpdir.name)

        res = hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual(f"+/test.me.1\n+/wat/test.me.2\nSync'ed {partial_cave_cmd.current_uuid()} to hoard!", res)

        res = hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            f"=/test.me.1\n+/test.me.4\n=/wat/test.me.2\n+/wat/test.me.3"
            f"\nSync'ed {full_cave_cmd.current_uuid()} to hoard!", res)

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
        self.assertEqual(f"+/wat/test.me.z\n-/wat/test.me.3\nSync'ed {full_cave_cmd.current_uuid()} to hoard!", res)

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
            "/wat/test.me.3 = \n"
            "/wat/test.me.z = a:1 g:1\n"
            "DONE", res)

        # new file in partial
        pfw('repo-partial/test.me.5', "adsfgasd")

        res = partial_cave_cmd.refresh()
        self.assertEqual("Refresh done!", res)

        res = hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual(f"+/test.me.5\nSync'ed {partial_cave_cmd.current_uuid()} to hoard!", res)

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
            "/wat/test.me.3 = \n"  # FIXME a problem
            "/wat/test.me.z = a:1 g:1\n"
            "DONE", res)

        res = hoard_cmd.contents.get("repo-partial-name", "/wat")
        self.assertEqual("Path /wat must be relative, but is absolute.", res)

        res = hoard_cmd.contents.get("repo-partial-name", "wat")
        self.assertEqual("+/wat/test.me.3\n+/wat/test.me.z\nDONE", res)

        res = hoard_cmd.contents.status(hide_time=True)
        self.assertEqual(
            "|Num Files                |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |         5|          |         5|          |          |\n"
            "|repo-full-name           |         5|         4|         1|          |          |\n"
            "|repo-partial-name        |         5|         3|         2|          |          |\n"
            "\n"
            "|Size                     |total     |available |get       |copy      |cleanup   |\n"
            "|repo-backup-name         |        53|          |        53|          |          |\n"
            "|repo-full-name           |        53|        45|         8|          |          |\n"
            "|repo-partial-name        |        52|        22|        30|          |          |\n",
            res)

        res = hoard_cmd.files.sync_contents("repo-partial-name")
        self.assertEqual(
            f"{partial_cave_cmd.current_uuid()}:\n"
            "E wat/test.me.3\n"  # FIXME a problem
            "+ wat/test.me.z\n"
            f"{partial_cave_cmd.current_uuid()}:\n"
            "DONE", res)

        res = hoard_cmd.files.sync_contents("repo-partial-name")
        self.assertEqual(
            f"{partial_cave_cmd.current_uuid()}:\n"
            "E wat/test.me.3\n"  # FIXME a problem
            f"{partial_cave_cmd.current_uuid()}:\n"
            "DONE", res)
