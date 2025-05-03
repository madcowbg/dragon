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
    os.makedirs(join(tmpdir, "hoard"), exist_ok=True)


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
        self.assertEqual((
            'Before: Hoard [a80f91] <- repo [curr: a80f91, stg: d99580, des: a80f91]\n'
            'ADD_NEW_TO_HOARD /test.me.1\n'
            'ADD_NEW_TO_HOARD /test.me.4\n'
            'ADD_NEW_TO_HOARD /wat/test.me.2\n'
            'ADD_NEW_TO_HOARD /wat/test.me.3\n'
            'After: Hoard [d99580], repo [curr: d99580, stg: d99580, des: d99580]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: d995800c80add686a027bac8628ca610418c64b6\n'
            "|Num Files                |total     |available |get       |\n"
            "|repo-backup-name         |         4|          |         4|\n"
            "|repo-full-name           |         4|         4|          |\n"
            "\n"
            "|Size                     |total     |available |get       |\n"
            "|repo-backup-name         |        35|          |        35|\n"
            "|repo-full-name           |        35|        35|          |\n",
            res)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [d99580] <- repo [curr: a80f91, stg: f6a740, des: a80f91]\n'
            '=/test.me.1\n'
            '=/wat/test.me.2\n'
            'After: Hoard [d99580], repo [curr: f6a740, stg: f6a740, des: f6a740]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: d995800c80add686a027bac8628ca610418c64b6\n'
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
            'Root: d995800c80add686a027bac8628ca610418c64b6\n'
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
        self.assertEqual((
            'Before: Hoard [a80f91] <- repo [curr: a80f91, stg: f6a740, des: a80f91]\n'
            'ADD_NEW_TO_HOARD /test.me.1\n'
            'ADD_NEW_TO_HOARD /wat/test.me.2\n'
            'After: Hoard [f6a740], repo [curr: f6a740, stg: f6a740, des: f6a740]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [f6a740] <- repo [curr: a80f91, stg: d99580, des: f6a740]\n'
            '=/test.me.1\n'
            '=/wat/test.me.2\n'
            'ADD_NEW_TO_HOARD /test.me.4\n'
            'ADD_NEW_TO_HOARD /wat/test.me.3\n'
            'After: Hoard [d99580], repo [curr: d99580, stg: d99580, des: d99580]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: d995800c80add686a027bac8628ca610418c64b6\n'
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
            'Root: d995800c80add686a027bac8628ca610418c64b6\n'
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
        self.assertEqual((
            'old: d995800c80add686a027bac8628ca610418c64b6\n'
            'current: 186d7031d0360ed2af8c373d0fc4976edda01bdc\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [d99580] <- repo [curr: d99580, stg: 186d70, des: d99580]\n'
            'ADD_NEW_TO_HOARD /wat/test.me.z\n'
            'DELETE_FROM_HOARD /wat/test.me.3\n'
            'After: Hoard [186d70], repo [curr: 186d70, stg: 186d70, des: 186d70]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: 186d7031d0360ed2af8c373d0fc4976edda01bdc\n'
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
            'Root: 186d7031d0360ed2af8c373d0fc4976edda01bdc\n'
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
        self.assertEqual((
            'old: f6a74030fa0a826b18e424d44f8aca9be8c657f3\n'
            'current: d404e3b21ddbcbfa7ed13eda074f95f4350d910a\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [186d70] <- repo [curr: f6a740, stg: d404e3, des: f6a740]\n'
            'ADD_NEW_TO_HOARD /test.me.5\n'
            'After: Hoard [038992], repo [curr: d404e3, stg: d404e3, des: d404e3]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: 03899221e1d3cd93f3931713d99d41612762995d\n'
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
            'Root: 03899221e1d3cd93f3931713d99d41612762995d\n'
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
            'Root: 03899221e1d3cd93f3931713d99d41612762995d\n'
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
            'Root: d995800c80add686a027bac8628ca610418c64b6\n'
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
            'Root: d995800c80add686a027bac8628ca610418c64b6\n'
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
        self.assertEqual((
            'old: d995800c80add686a027bac8628ca610418c64b6\n'
            'current: 186d7031d0360ed2af8c373d0fc4976edda01bdc\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [d99580] <- repo [curr: d99580, stg: 186d70, des: d99580]\n'
            'ADD_NEW_TO_HOARD /wat/test.me.z\n'
            'DELETE_FROM_HOARD /wat/test.me.3\n'
            'After: Hoard [186d70], repo [curr: 186d70, stg: 186d70, des: 186d70]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: 186d7031d0360ed2af8c373d0fc4976edda01bdc\n'
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
            'Root: 186d7031d0360ed2af8c373d0fc4976edda01bdc\n'
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
            'Root: d995800c80add686a027bac8628ca610418c64b6\n'
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
        self.assertEqual(
            'old: d995800c80add686a027bac8628ca610418c64b6\n'
            'current: ef6ec6e2eb93912f9e24f92f70040f792a7897ca\n'
            "Refresh done!", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [d99580] <- repo [curr: d99580, stg: ef6ec6, des: d99580]\n'
            'DELETE_FROM_HOARD /wat/test.me.2\n'
            'After: Hoard [ef6ec6], repo [curr: ef6ec6, stg: ef6ec6, des: ef6ec6]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: ef6ec6e2eb93912f9e24f92f70040f792a7897ca\n'
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
            'Root: ef6ec6e2eb93912f9e24f92f70040f792a7897ca\n'
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
            'Root: ef6ec6e2eb93912f9e24f92f70040f792a7897ca\n'
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
            'Root: ef6ec6e2eb93912f9e24f92f70040f792a7897ca\n'
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
            'Root: ef6ec6e2eb93912f9e24f92f70040f792a7897ca\n'
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
            'Root: f6a74030fa0a826b18e424d44f8aca9be8c657f3\n'
            "|Num Files                |             updated|     max|total     |available |get       |\n"
            "|repo-backup-name         |               never|   3.5TB|         2|          |         2|\n"
            "|repo-full-name           |               never|   3.5TB|         2|          |         2|\n"
            "|repo-partial-name        |                 now|   3.5TB|         2|         2|          |\n"
            "\n"
            "|Size                     |             updated|     max|total     |available |get       |\n"
            "|repo-backup-name         |               never|   3.5TB|        14|          |        14|\n"
            "|repo-full-name           |               never|   3.5TB|        14|          |        14|\n"
            "|repo-partial-name        |                 now|   3.5TB|        14|        14|          |\n"
            "", res)

        # refresh new contents file
        res = await hoard_cmd.contents.pull(new_content_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [f6a740] <- repo [curr: a80f91, stg: b4d515, des: a80f91]\n'
            'ADD_NEW_TO_HOARD /wat/one-new.file\n'
            'After: Hoard [7e6fc7], repo [curr: b4d515, stg: b4d515, des: b4d515]\n'
            "Sync'ed repo-new-contents-name to hoard!\n"
            'DONE'), res)

        # pull full as well - its files will be added to the new repop
        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [7e6fc7] <- repo [curr: a80f91, stg: d99580, des: 7e6fc7]\n'
            '=/test.me.1\n'
            '=/wat/test.me.2\n'
            'ADD_NEW_TO_HOARD /test.me.4\n'
            'ADD_NEW_TO_HOARD /wat/test.me.3\n'
            'After: Hoard [fe7ed2], repo [curr: d99580, stg: d99580, des: fe7ed2]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: fe7ed28d04115b0faf86aa27ef7c6f504c2a0fc1\n'
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
            'Root: fe7ed28d04115b0faf86aa27ef7c6f504c2a0fc1\n'
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
        self.assertEqual((
            'old: b632f923eba9b54b204d0855602a9952084c387f\n'
            'current: b632f923eba9b54b204d0855602a9952084c387f\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull(new_content_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [fe7ed2] <- repo [curr: b4d515, stg: b4d515, des: 41170b]\n'
            'After: Hoard [fe7ed2], repo [curr: b4d515, stg: b4d515, des: 41170b]\n'
            "Sync'ed repo-new-contents-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.files.push(repo="repo-new-contents-name")
        self.assertEqual(
            f"repo-new-contents-name:\n"
            f"+ test.me.2\n"
            f"+ test.me.3\n"
            f"repo-new-contents-name:\n"
            f"DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: fe7ed28d04115b0faf86aa27ef7c6f504c2a0fc1\n'
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
        self.assertEqual('old: None\ncurrent: 71cadb6c406ad3af078c7b025297c4211eba8509\nRefresh done!', res)

        res = hoard_cmd.add_remote(
            remote_path=join(self.tmpdir.name, "changed-cave"), name="repo-changed-cave-name",
            mount_point="/", type=CaveType.PARTIAL, fetch_new=False)
        self.assertEqual(
            fr"Added repo-changed-cave-name[{changed_cave_cmd.current_uuid()}] at {self.tmpdir.name}\changed-cave!",
            res)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [a80f91] <- repo [curr: a80f91, stg: f6a740, des: a80f91]\n'
            'ADD_NEW_TO_HOARD /test.me.1\n'
            'ADD_NEW_TO_HOARD /wat/test.me.2\n'
            'After: Hoard [f6a740], repo [curr: f6a740, stg: f6a740, des: f6a740]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [f6a740] <- repo [curr: a80f91, stg: d99580, des: f6a740]\n'
            '=/test.me.1\n'
            '=/wat/test.me.2\n'
            'ADD_NEW_TO_HOARD /test.me.4\n'
            'ADD_NEW_TO_HOARD /wat/test.me.3\n'
            'After: Hoard [d99580], repo [curr: d99580, stg: d99580, des: d99580]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: d995800c80add686a027bac8628ca610418c64b6\n'
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
        self.assertEqual((
            'Before: Hoard [d99580] <- repo [curr: a80f91, stg: 71cadb, des: a80f91]\n'
            '=/test.me.4\n'
            '=/wat/test.me.2\n'
            'RESETTING /test.me.1\n'
            'RESETTING /wat/test.me.3\n'
            'After: Hoard [71cadb], repo [curr: 71cadb, stg: 71cadb, des: 71cadb]\n'
            "Sync'ed repo-changed-cave-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: 71cadb6c406ad3af078c7b025297c4211eba8509\n'
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
            "TO_GET (from 2) /test.me.4\n"
            "TO_GET (from 3) /wat/test.me.2\n"
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
            "TO_GET (from 2) /test.me.4\n"
            "TO_GET (from 3) /wat/test.me.2\n"
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
            "TO_GET (from 2) /test.me.4\n"
            "TO_GET (from 3) /wat/test.me.2\n"
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
            'Root: 71cadb6c406ad3af078c7b025297c4211eba8509\n'
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
        self.assertEqual((
            'Before: Hoard [a80f91] <- repo [curr: a80f91, stg: f6a740, des: a80f91]\n'
            'ADD_NEW_TO_HOARD /test.me.1\n'
            'ADD_NEW_TO_HOARD /wat/test.me.2\n'
            'After: Hoard [f6a740], repo [curr: f6a740, stg: f6a740, des: f6a740]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.differences(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Root: f6a74030fa0a826b18e424d44f8aca9be8c657f3\n'
            'Status of repo-full-name:\n'
            'PRESENT /test.me.4\n'
            'PRESENT /wat/test.me.3\n'
            'DONE'), res)

        res = await hoard_cmd.contents.pending_pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Status of repo-full-name:',
            'Hoard root: f6a74030fa0a826b18e424d44f8aca9be8c657f3:',
            'Repo root: d995800c80add686a027bac8628ca610418c64b6:',
            'REPO_DESIRED_FILE_TO_GET /test.me.1',
            'REPO_DESIRED_FILE_ADDED /test.me.4',
            'HOARD_FILE_ADDED /test.me.4',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.2',
            'REPO_DESIRED_FILE_ADDED /wat/test.me.3',
            'HOARD_FILE_ADDED /wat/test.me.3'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [f6a740] <- repo [curr: a80f91, stg: d99580, des: f6a740]\n'
            '=/test.me.1\n'
            '=/wat/test.me.2\n'
            'ADD_NEW_TO_HOARD /test.me.4\n'
            'ADD_NEW_TO_HOARD /wat/test.me.3\n'
            'After: Hoard [d99580], repo [curr: d99580, stg: d99580, des: d99580]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: d995800c80add686a027bac8628ca610418c64b6\n'
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
        self.assertEqual((
            'NO CHANGES\n'
            'old: d995800c80add686a027bac8628ca610418c64b6\n'
            'current: d995800c80add686a027bac8628ca610418c64b6\n'
            'Refresh done!'), res)

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
            f"{full_cave_cmd.current_uuid()} [d995800c80add686a027bac8628ca610418c64b6]:\n"
            f"files:\n"
            f"    same: 1 (20.0%)\n"
            f"     mod: 1 (20.0%)\n"
            f"     new: 3 (60.0%)\n"
            f"   moved: 1 (20.0%)\n"
            f" current: 5\n"
            f" in repo: 4\n"
            f" deleted: 1 (25.0%)\n", res)

        res = await full_cave_cmd.refresh()
        self.assertEqual((
            'MOVED test.me.4 TO lets_get_it_started/test.me.4-renamed\n'
            'REMOVED_FILE_FALLBACK_TOO_MANY wat/test.me.2\n'
            'PRESENT_FILE lets_get_it_started/test.me.2-butnew\n'
            'PRESENT_FILE lets_get_it_started/test.me.2-butsecond\n'
            'MODIFIED_FILE test.me.1\n'
            'PRESENT_FILE test.me.added\n'
            'old: d995800c80add686a027bac8628ca610418c64b6\n'
            'current: 93044fb853800db246860982a52eeb78e214ca4a\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.differences(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Root: d995800c80add686a027bac8628ca610418c64b6\n'
            'Status of repo-full-name:\n'
            'PRESENT /lets_get_it_started/test.me.2-butnew\n'
            'PRESENT /lets_get_it_started/test.me.2-butsecond\n'
            'PRESENT /lets_get_it_started/test.me.4-renamed\n'
            'MODIFIED /test.me.1\n'
            'DELETED /test.me.4\n'
            'PRESENT /test.me.added\n'
            'DELETED /wat/test.me.2\n'
            'DONE'), res)

        res = await hoard_cmd.contents.pending_pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Status of repo-full-name:',
            'Hoard root: d995800c80add686a027bac8628ca610418c64b6:',
            'Repo root: 93044fb853800db246860982a52eeb78e214ca4a:',
            'REPO_DESIRED_FILE_ADDED /lets_get_it_started/test.me.2-butnew',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butnew',
            'REPO_DESIRED_FILE_ADDED /lets_get_it_started/test.me.2-butsecond',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butsecond',
            'REPO_DESIRED_FILE_ADDED /lets_get_it_started/test.me.4-renamed',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.4-renamed',
            'REPO_DESIRED_FILE_CHANGED /test.me.1',
            'HOARD_FILE_CHANGED /test.me.1',
            'REPO_FILE_TO_DELETE /test.me.4',
            'HOARD_FILE_DELETED /test.me.4',
            'REPO_DESIRED_FILE_ADDED /test.me.added',
            'HOARD_FILE_ADDED /test.me.added',
            'REPO_FILE_TO_DELETE /wat/test.me.2',
            'HOARD_FILE_DELETED /wat/test.me.2'
        ], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual(
            ('Before: Hoard [d99580] <- repo [curr: d99580, stg: 93044f, des: d99580]\n'
             'ADD_NEW_TO_HOARD /lets_get_it_started/test.me.2-butnew\n'
             'ADD_NEW_TO_HOARD /lets_get_it_started/test.me.2-butsecond\n'
             'ADD_NEW_TO_HOARD /lets_get_it_started/test.me.4-renamed\n'  # todo should not be logged, as we move it
             'ADD_NEW_TO_HOARD /test.me.added\n'
             'g/test.me.1\n'
             'DELETE_FROM_HOARD /test.me.4\n'
             'DELETE_FROM_HOARD /wat/test.me.2\n'
             'After: Hoard [9b791e], repo [curr: bd737a, stg: 93044f, des: 9b791e]\n'
             "Sync'ed repo-full-name to hoard!\n"
             'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: 9b791ecf69d7f132e8636bfd6ae5a609815c4d9b\n'
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |         6|          |         6|          |\n'
            '|repo-copy-name           |         8|         2|         4|         2|\n'
            '|repo-full-name           |         6|         5|         1|          |\n'
            '|repo-partial-name        |         2|         1|          |         1|\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |        48|          |        48|          |\n'
            '|repo-copy-name           |        67|        16|        32|        19|\n'
            '|repo-full-name           |        48|        42|         6|          |\n'
            '|repo-partial-name        |        14|         6|          |         8|\n'), res)

        res = await hoard_cmd.files.push(full_cave_cmd.current_uuid())
        self.assertEqual('repo-full-name:\n+ test.me.1\nrepo-full-name:\nDONE', res)

        res = await hoard_cmd.files.pending(copy_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-copy-name:\n"
            # "TO_GET (from 1) /test.me.1\n"
            "TO_GET (from 1) /lets_get_it_started/test.me.2-butnew\n"
            "TO_GET (from 1) /lets_get_it_started/test.me.2-butsecond\n"
            "TO_GET (from 1) /lets_get_it_started/test.me.4-renamed\n"  # fixme should MOVE instead
            "TO_CLEANUP (is in 0) /test.me.4\n"
            "TO_GET (from 1) /test.me.added\n"
            "TO_CLEANUP (is in 0) /wat/test.me.2\n"
            " repo-full-name has 4 files\n"
            "DONE", res)

        res = await hoard_cmd.files.push(copy_cave_cmd.current_uuid())
        self.assertEqual((
            'repo-copy-name:\n'
            '+ lets_get_it_started/test.me.2-butnew\n'
            '+ lets_get_it_started/test.me.2-butsecond\n'
            '+ lets_get_it_started/test.me.4-renamed\n'  # fixme should MOVE instead
            '+ test.me.added\n'
            'repo-copy-name:\n'
            'd test.me.4\n'
            'd wat/test.me.2\n'
            'remove dangling /test.me.4\n'
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: 9b791ecf69d7f132e8636bfd6ae5a609815c4d9b\n'
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |         6|          |         6|          |\n'
            '|repo-copy-name           |         6|         6|          |          |\n'
            '|repo-full-name           |         6|         6|          |          |\n'
            '|repo-partial-name        |         2|         1|          |         1|\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |        48|          |        48|          |\n'
            '|repo-copy-name           |        48|        48|          |          |\n'
            '|repo-full-name           |        48|        48|          |          |\n'
            '|repo-partial-name        |        14|         6|          |         8|\n'),
            res)

        res = await hoard_cmd.files.pending(backup_cave_cmd.current_uuid())
        self.assertEqual(
            'repo-backup-name:\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.2-butnew\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.2-butsecond\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.4-renamed\n'
            'TO_GET (from 3) /test.me.1\n'
            # 'TO_CLEANUP (is in 0) /test.me.4\n'  # fixme do it
            'TO_GET (from 2) /test.me.added\n'
            'TO_GET (from 2) /wat/test.me.3\n'
            ' repo-copy-name has 6 files\n'
            ' repo-full-name has 6 files\n'
            ' repo-partial-name has 1 files\n'  # fixme shan't
            'DONE', res)

        res = await hoard_cmd.files.push(copy_cave_cmd.current_uuid())
        self.assertEqual("repo-copy-name:\nrepo-copy-name:\nDONE", res)

        res = await copy_cave_cmd.refresh()
        self.assertEqual((
            'PRESENT_FILE lets_get_it_started/test.me.2-butnew\n'
            'PRESENT_FILE lets_get_it_started/test.me.2-butsecond\n'
            'PRESENT_FILE lets_get_it_started/test.me.4-renamed\n'
            'PRESENT_FILE test.me.1\n'
            'PRESENT_FILE test.me.added\n'
            'PRESENT_FILE wat/test.me.3\n'
            'old: a80f91bc48850a1fb3459bb76b9f6308d4d35710\n'
            'current: 9b791ecf69d7f132e8636bfd6ae5a609815c4d9b\n'
            'Refresh done!'), res)

        res = await full_cave_cmd.refresh()
        self.assertEqual((
            'MODIFIED_FILE test.me.1\n'
            'old: 93044fb853800db246860982a52eeb78e214ca4a\n'
            'current: 9b791ecf69d7f132e8636bfd6ae5a609815c4d9b\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull(copy_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [9b791e] <- repo [curr: 9b791e, stg: 9b791e, des: 9b791e]\n'
            'After: Hoard [9b791e], repo [curr: 9b791e, stg: 9b791e, des: 9b791e]\n'
            "Sync'ed repo-copy-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [9b791e] <- repo [curr: 9b791e, stg: 9b791e, des: 9b791e]\n'
            'After: Hoard [9b791e], repo [curr: 9b791e, stg: 9b791e, des: 9b791e]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

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
        self.assertEqual((
            'Before: Hoard [a80f91] <- repo [curr: a80f91, stg: f6a740, des: a80f91]\n'
            'ADD_NEW_TO_HOARD /test.me.1\n'
            'ADD_NEW_TO_HOARD /wat/test.me.2\n'
            'After: Hoard [f6a740], repo [curr: f6a740, stg: f6a740, des: f6a740]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.differences(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Root: f6a74030fa0a826b18e424d44f8aca9be8c657f3\n'
            'Status of repo-full-name:\n'
            'PRESENT /test.me.4\n'
            'PRESENT /wat/test.me.3\n'
            'DONE'), res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [f6a740] <- repo [curr: a80f91, stg: d99580, des: f6a740]\n'
            '=/test.me.1\n'
            '=/wat/test.me.2\n'
            'ADD_NEW_TO_HOARD /test.me.4\n'
            'ADD_NEW_TO_HOARD /wat/test.me.3\n'
            'After: Hoard [d99580], repo [curr: d99580, stg: d99580, des: d99580]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.pull(backup_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [d99580] <- repo [curr: a80f91, stg: 9fbdcf, des: d99580]\n'
            '=/test.me.1\n'
            '=/wat/test.me.3\n'
            'After: Hoard [d99580], repo [curr: 9fbdcf, stg: 9fbdcf, des: d99580]\n'
            "Sync'ed repo-backup-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.files.push(backup_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-backup-name:\n"
            "+ test.me.4\n"
            "+ wat/test.me.2\n"
            "repo-backup-name:\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: d995800c80add686a027bac8628ca610418c64b6\n'
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
        self.assertEqual((
            'NO CHANGES\n'
            'old: d995800c80add686a027bac8628ca610418c64b6\n'
            'current: d995800c80add686a027bac8628ca610418c64b6\n'
            'Refresh done!'), res)

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
        self.assertEqual((
            'MOVED test.me.4 TO lets_get_it_started/test.me.4-renamed\n'
            'REMOVED_FILE_FALLBACK_TOO_MANY wat/test.me.2\n'
            'PRESENT_FILE lets_get_it_started/test.me.2-butnew\n'
            'PRESENT_FILE lets_get_it_started/test.me.2-butsecond\n'
            'MODIFIED_FILE test.me.1\n'
            'PRESENT_FILE test.me.added\n'
            'old: d995800c80add686a027bac8628ca610418c64b6\n'
            'current: 93044fb853800db246860982a52eeb78e214ca4a\n'
            'Refresh done!'), res)

        res = full_cave_cmd.status_index(show_dates=False)
        self.assertEqual(
            'lets_get_it_started/test.me.2-butnew: present @ -1\n'
            'lets_get_it_started/test.me.2-butsecond: present @ -1\n'
            'lets_get_it_started/test.me.4-renamed: present @ -1\n'
            'test.me.1: present @ -1\n'
            'test.me.added: present @ -1\n'
            'wat/test.me.3: present @ -1\n'
            "--- SUMMARY ---\n"
            "Result for local [93044fb853800db246860982a52eeb78e214ca4a]:\n"
            "Max size: 3.5TB\n"
            f"UUID: {full_cave_cmd.current_uuid()}\n"
            "  # files = 6 of size 47\n", res)

        res = await hoard_cmd.contents.differences(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Root: d995800c80add686a027bac8628ca610418c64b6\n'
            'Status of repo-full-name:\n'
            'PRESENT /lets_get_it_started/test.me.2-butnew\n'
            'PRESENT /lets_get_it_started/test.me.2-butsecond\n'
            'PRESENT /lets_get_it_started/test.me.4-renamed\n'
            'MODIFIED /test.me.1\n'
            'DELETED /test.me.4\n'
            'PRESENT /test.me.added\n'
            'DELETED /wat/test.me.2\n'
            'DONE'), res)

        res = await hoard_cmd.contents.pending_pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Status of repo-full-name:',
            'Hoard root: d995800c80add686a027bac8628ca610418c64b6:',
            'Repo root: 93044fb853800db246860982a52eeb78e214ca4a:',
            'REPO_DESIRED_FILE_ADDED /lets_get_it_started/test.me.2-butnew',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butnew',
            'REPO_DESIRED_FILE_ADDED /lets_get_it_started/test.me.2-butsecond',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butsecond',
            'REPO_DESIRED_FILE_ADDED /lets_get_it_started/test.me.4-renamed',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.4-renamed',
            'REPO_DESIRED_FILE_CHANGED /test.me.1',
            'HOARD_FILE_CHANGED /test.me.1',
            'REPO_FILE_TO_DELETE /test.me.4',
            'HOARD_FILE_DELETED /test.me.4',
            'REPO_DESIRED_FILE_ADDED /test.me.added',
            'HOARD_FILE_ADDED /test.me.added',
            'REPO_FILE_TO_DELETE /wat/test.me.2',
            'HOARD_FILE_DELETED /wat/test.me.2'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [d99580] <- repo [curr: d99580, stg: 93044f, des: d99580]\n'
            'ADD_NEW_TO_HOARD /lets_get_it_started/test.me.2-butnew\n'
            'ADD_NEW_TO_HOARD /lets_get_it_started/test.me.2-butsecond\n'
            'ADD_NEW_TO_HOARD /lets_get_it_started/test.me.4-renamed\n'
            'ADD_NEW_TO_HOARD /test.me.added\n'
            'g/test.me.1\n'
            'DELETE_FROM_HOARD /test.me.4\n'
            'DELETE_FROM_HOARD /wat/test.me.2\n'
            # "MOVE repo-backup-name: /test.me.4 to /lets_get_it_started/test.me.4-renamed\n"
            # "MOVE repo-copy-name: /test.me.4 to /lets_get_it_started/test.me.4-renamed\n"
            'After: Hoard [9b791e], repo [curr: bd737a, stg: 93044f, des: 9b791e]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: 9b791ecf69d7f132e8636bfd6ae5a609815c4d9b\n'
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |         8|         2|         4|         2|\n'
            '|repo-copy-name           |         8|         2|         4|         2|\n'
            '|repo-full-name           |         6|         5|         1|          |\n'
            '|repo-partial-name        |         2|         1|          |         1|\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |        67|        16|        32|        19|\n'
            '|repo-copy-name           |        67|        16|        32|        19|\n'
            '|repo-full-name           |        48|        42|         6|          |\n'
            '|repo-partial-name        |        14|         6|          |         8|\n'),
            res)

        res = await hoard_cmd.files.push(full_cave_cmd.current_uuid())
        self.assertEqual(
            'repo-full-name:\n'
            '+ test.me.1\n'  # fixme shan't do that
            'repo-full-name:\n'
            'DONE', res)

        res = await hoard_cmd.files.pending(copy_cave_cmd.current_uuid())
        self.assertEqual((
            'repo-copy-name:\n'
            'TO_GET (from 1) /lets_get_it_started/test.me.2-butnew\n'
            'TO_GET (from 1) /lets_get_it_started/test.me.2-butsecond\n'
            'TO_GET (from 1) /lets_get_it_started/test.me.4-renamed\n'
            'TO_CLEANUP (is in 0) /test.me.4\n'
            'TO_GET (from 1) /test.me.added\n'
            'TO_CLEANUP (is in 0) /wat/test.me.2\n'
            ' repo-full-name has 4 files\n'
            'DONE'), res)

        res = await hoard_cmd.files.push(copy_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-copy-name:\n"
            "+ lets_get_it_started/test.me.2-butnew\n"
            "+ lets_get_it_started/test.me.2-butsecond\n"
            '+ lets_get_it_started/test.me.4-renamed\n'  # fixme should be MOVED
            "+ test.me.added\n"
            "repo-copy-name:\n"
            "d test.me.4\n"
            "d wat/test.me.2\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: 9b791ecf69d7f132e8636bfd6ae5a609815c4d9b\n'
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |         8|         2|         4|         2|\n'
            '|repo-copy-name           |         6|         6|          |          |\n'
            '|repo-full-name           |         6|         6|          |          |\n'
            '|repo-partial-name        |         2|         1|          |         1|\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |        67|        16|        32|        19|\n'
            '|repo-copy-name           |        48|        48|          |          |\n'
            '|repo-full-name           |        48|        48|          |          |\n'
            '|repo-partial-name        |        14|         6|          |         8|\n'), res)

        res = await hoard_cmd.files.pending(backup_cave_cmd.current_uuid())
        self.assertEqual((
            'repo-backup-name:\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.2-butnew\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.2-butsecond\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.4-renamed\n'  # fixme MOVED
            'TO_CLEANUP (is in 0) /test.me.4\n'
            'TO_GET (from 2) /test.me.added\n'
            'TO_CLEANUP (is in 0) /wat/test.me.2\n'
            ' repo-copy-name has 4 files\n'
            ' repo-full-name has 4 files\n'
            'DONE'), res)

        res = await hoard_cmd.files.push(copy_cave_cmd.current_uuid())
        self.assertEqual("repo-copy-name:\nrepo-copy-name:\nDONE", res)

        res = await copy_cave_cmd.refresh()
        self.assertEqual((
            'PRESENT_FILE lets_get_it_started/test.me.2-butnew\n'
            'PRESENT_FILE lets_get_it_started/test.me.2-butsecond\n'
            'PRESENT_FILE lets_get_it_started/test.me.4-renamed\n'
            'PRESENT_FILE test.me.1\n'
            'PRESENT_FILE test.me.added\n'
            'PRESENT_FILE wat/test.me.3\n'
            'old: a80f91bc48850a1fb3459bb76b9f6308d4d35710\n'
            'current: 9b791ecf69d7f132e8636bfd6ae5a609815c4d9b\n'
            'Refresh done!'), res)

        res = await hoard_cmd.files.push(backup_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-backup-name:\n"
            "+ lets_get_it_started/test.me.2-butnew\n"
            "+ lets_get_it_started/test.me.2-butsecond\n"
            '+ lets_get_it_started/test.me.4-renamed\n'  # fixme moved
            # "+ test.me.1\n"
            "+ test.me.added\n"
            "repo-backup-name:\n"
            "d test.me.4\n"
            "d wat/test.me.2\n"
            "remove dangling /test.me.4\n"
            "DONE", res)

        res = await backup_cave_cmd.refresh()
        self.assertEqual((
            'PRESENT_FILE lets_get_it_started/test.me.2-butnew\n'
            'PRESENT_FILE lets_get_it_started/test.me.2-butsecond\n'
            'PRESENT_FILE lets_get_it_started/test.me.4-renamed\n'
            'PRESENT_FILE test.me.added\n'
            'old: 9fbdcfe094f258f954ba6f65c4a3641d25b32e06\n'
            'current: 9b791ecf69d7f132e8636bfd6ae5a609815c4d9b\n'
            'Refresh done!'), res)

        res = await full_cave_cmd.refresh()
        self.assertEqual((
            'MODIFIED_FILE test.me.1\n'
            'old: 93044fb853800db246860982a52eeb78e214ca4a\n'
            'current: 9b791ecf69d7f132e8636bfd6ae5a609815c4d9b\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull(copy_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [9b791e] <- repo [curr: 9b791e, stg: 9b791e, des: 9b791e]\n'
            'After: Hoard [9b791e], repo [curr: 9b791e, stg: 9b791e, des: 9b791e]\n'
            "Sync'ed repo-copy-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [9b791e] <- repo [curr: 9b791e, stg: 9b791e, des: 9b791e]\n'
            'After: Hoard [9b791e], repo [curr: 9b791e, stg: 9b791e, des: 9b791e]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.files.pending(partial_cave_cmd.current_uuid())
        self.assertEqual(
            'repo-partial-name:\n'
            # 'TO_GET (from 3) /test.me.1\n'
            'TO_CLEANUP (is in 0) /wat/test.me.2\n'
            # ' repo-backup-name has 1 files\n'
            # ' repo-copy-name has 1 files\n'
            # ' repo-full-name has 1 files\n'
            'DONE', res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: 9b791ecf69d7f132e8636bfd6ae5a609815c4d9b\n'
            '|Num Files                |total     |available |cleanup   |\n'
            '|repo-backup-name         |         6|         6|          |\n'
            '|repo-copy-name           |         6|         6|          |\n'
            '|repo-full-name           |         6|         6|          |\n'
            '|repo-partial-name        |         2|         1|         1|\n'
            '\n'
            '|Size                     |total     |available |cleanup   |\n'
            '|repo-backup-name         |        48|        48|          |\n'
            '|repo-copy-name           |        48|        48|          |\n'
            '|repo-full-name           |        48|        48|          |\n'
            '|repo-partial-name        |        14|         6|         8|\n'), res)

        # move file before being synch-ed
        shutil.move(
            join(self.tmpdir.name, 'repo-partial/test.me.1'),
            join(self.tmpdir.name, 'repo-partial/test.me.1-newlocation'))

        res = await partial_cave_cmd.refresh()
        self.assertEqual((
            'MOVED test.me.1 TO test.me.1-newlocation\n'
            'old: f6a74030fa0a826b18e424d44f8aca9be8c657f3\n'
            'current: dde8f8938acf1e0fee6cf55b7ca2dac96d376d47\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [9b791e] <- repo [curr: f6a740, stg: dde8f8, des: 10a305]\n'
            '?/wat/test.me.2\n'
            'ADD_NEW_TO_HOARD /test.me.1-newlocation\n'
            'DELETE_FROM_HOARD /test.me.1\n'
            # fixme we expected error - move will fail, but the fallback is to just get it
            'After: Hoard [18e59d], repo [curr: dde8f8, stg: dde8f8, des: f238b2]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual([
            'Root: 18e59dbf70e879de51aefd09762362976ecca4fb',
            '|Num Files                |total     |available |get       |cleanup   |',
            '|repo-backup-name         |         7|         5|         1|         1|',
            '|repo-copy-name           |         7|         5|         1|         1|',
            '|repo-full-name           |         7|         5|         1|         1|',
            '|repo-partial-name        |         2|         1|          |         1|',
            '',
            '|Size                     |total     |available |get       |cleanup   |',
            '|repo-backup-name         |        54|        42|         6|         6|',
            '|repo-copy-name           |        54|        42|         6|         6|',
            '|repo-full-name           |        54|        42|         6|         6|',
            '|repo-partial-name        |        14|         6|          |         8|'], res.splitlines())

        res = await hoard_cmd.files.push(partial_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-partial-name:\n"
            "repo-partial-name:\n"
            "d wat/test.me.2\n"
            "remove dangling /wat/test.me.2\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual([
            'Root: 18e59dbf70e879de51aefd09762362976ecca4fb',
            '|Num Files                |total     |available |get       |cleanup   |',
            '|repo-backup-name         |         7|         5|         1|         1|',
            '|repo-copy-name           |         7|         5|         1|         1|',
            '|repo-full-name           |         7|         5|         1|         1|',
            '|repo-partial-name        |         1|         1|          |          |',
            '',
            '|Size                     |total     |available |get       |cleanup   |',
            '|repo-backup-name         |        54|        42|         6|         6|',
            '|repo-copy-name           |        54|        42|         6|         6|',
            '|repo-full-name           |        54|        42|         6|         6|',
            '|repo-partial-name        |         6|         6|          |          |'], res.splitlines())

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
            'Root: f6a74030fa0a826b18e424d44f8aca9be8c657f3\n'
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
            "test.me.1: present @ -1\n"
            "test.me.4: present @ -1\n"
            "wat/test.me.2: present @ -1\n"
            "wat/test.me.3: present @ -1\n"
            "--- SUMMARY ---\n"
            'Result for local [d995800c80add686a027bac8628ca610418c64b6]:\n'
            "Max size: 3.5TB\n"
            f"UUID: {full_cave_cmd.current_uuid()}\n"
            "  # files = 4 of size 35\n", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [f6a740] <- repo [curr: a80f91, stg: d99580, des: f6a740]\n'
            '=/test.me.1\n'
            '=/wat/test.me.2\n'
            'ADD_NEW_TO_HOARD /test.me.4\n'
            'ADD_NEW_TO_HOARD /wat/test.me.3\n'
            'After: Hoard [d99580], repo [curr: d99580, stg: d99580, des: d99580]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

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
        pathlib.Path(join(self.tmpdir.name, 'repo-full', '.hoard', f'{full_cave_cmd.current_uuid()}.contents')) \
            .unlink(missing_ok=True)
        shutil.rmtree(join(self.tmpdir.name, 'repo-full', '.hoard', f'{full_cave_cmd.current_uuid()}.contents.lmdb'))

        res = await hoard_cmd.export_contents_to_repo(full_cave_cmd.current_uuid())
        self.assertEqual(
            "PRESENT test.me.1\n"
            "PRESENT test.me.4\n"
            "PRESENT wat/test.me.2\n"
            "PRESENT wat/test.me.3\n"
            "DONE", res)

        res = await full_cave_cmd.refresh()
        self.assertEqual((
            'MOVED test.me.4 TO lets_get_it_started/test.me.4-renamed\n'
            'MODIFIED_FILE test.me.1\n'
            'PRESENT_FILE test.me.added\n'
            'old: d995800c80add686a027bac8628ca610418c64b6\n'
            'current: 3aabb1ed7e4d7b7c655a00ab183383c323700a1f\n'
            'Refresh done!'), res)

        res = full_cave_cmd.status_index(show_dates=False)
        self.assertEqual(
            "lets_get_it_started/test.me.4-renamed: present @ -1\n"
            "test.me.1: present @ -1\n"
            # "test.me.4: moved_from @ 2\n"  fixme should be used
            "test.me.added: present @ -1\n"
            "wat/test.me.2: present @ -1\n"
            "wat/test.me.3: present @ -1\n"
            "--- SUMMARY ---\n"
            'Result for local [3aabb1ed7e4d7b7c655a00ab183383c323700a1f]:\n'
            "Max size: 3.5TB\n"
            f"UUID: {full_cave_cmd.current_uuid()}\n"
            "  # files = 5 of size 39\n", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [d99580] <- repo [curr: d99580, stg: 3aabb1, des: d99580]\n'
            'ADD_NEW_TO_HOARD /lets_get_it_started/test.me.4-renamed\n'
            'ADD_NEW_TO_HOARD /test.me.added\n'
            'g/test.me.1\n'
            'DELETE_FROM_HOARD /test.me.4\n'
            'After: Hoard [96be5f], repo [curr: 38d7f4, stg: 3aabb1, des: 96be5f]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: 96be5f3037f27a34a3acad1b9f3106652efdd8f7\n'
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |         6|         3|         2|         1|\n'
            '|repo-copy-name           |         5|          |         5|          |\n'
            '|repo-full-name           |         5|         4|         1|          |\n'
            '|repo-partial-name        |         2|         2|          |          |\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |        51|        24|        16|        11|\n'
            '|repo-copy-name           |        40|          |        40|          |\n'
            '|repo-full-name           |        40|        34|         6|          |\n'
            '|repo-partial-name        |        14|        14|          |          |\n'), res)

        res = await hoard_cmd.contents.pull(backup_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [96be5f] <- repo [curr: d99580, stg: 9fbdcf, des: 96be5f]\n'
            '?/test.me.4\n'
            'g/wat/test.me.2\n'
            'After: Hoard [96be5f], repo [curr: ef6ec6, stg: 9fbdcf, des: 96be5f]\n'
            "Sync'ed repo-backup-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.files.push(backup_cave_cmd.current_uuid())
        self.assertEqual((
            'repo-backup-name:\n'
            '+ lets_get_it_started/test.me.4-renamed\n'
            '+ test.me.added\n'
            '+ wat/test.me.2\n'
            'repo-backup-name:\n'
            'd test.me.4\n'
            'remove dangling /test.me.4\n'
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: 96be5f3037f27a34a3acad1b9f3106652efdd8f7\n'
            '|Num Files                |total     |available |get       |\n'
            '|repo-backup-name         |         5|         5|          |\n'
            '|repo-copy-name           |         5|          |         5|\n'
            '|repo-full-name           |         5|         4|         1|\n'
            '|repo-partial-name        |         2|         2|          |\n'
            '\n'
            '|Size                     |total     |available |get       |\n'
            '|repo-backup-name         |        40|        40|          |\n'
            '|repo-copy-name           |        40|          |        40|\n'
            '|repo-full-name           |        40|        34|         6|\n'
            '|repo-partial-name        |        14|        14|          |\n'), res)

        res = await hoard_cmd.files.push(copy_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-copy-name:\n"
            "+ lets_get_it_started/test.me.4-renamed\n"
            "+ test.me.1\n"
            "+ test.me.added\n"
            "+ wat/test.me.2\n"
            "+ wat/test.me.3\n"
            "repo-copy-name:\n"
            # "d test.me.4\n"  # fixme should remove
            # "remove dangling /test.me.4\n"
            "DONE", res)

        res = await full_cave_cmd.refresh()
        self.assertEqual((
            'NO CHANGES\n'
            'old: 3aabb1ed7e4d7b7c655a00ab183383c323700a1f\n'
            'current: 3aabb1ed7e4d7b7c655a00ab183383c323700a1f\n'
            'Refresh done!'), res)

        pfw('repo-full/lets_get_it_started/test.me.2-butnew', "gsadf3dq")
        shutil.move(
            join(self.tmpdir.name, 'repo-full/wat/test.me.2'),
            join(self.tmpdir.name, 'repo-full/lets_get_it_started/test.me.2-butsecond'))

        res = await full_cave_cmd.refresh()
        self.assertEqual((
            'REMOVED_FILE_FALLBACK_TOO_MANY wat/test.me.2\n'
            'PRESENT_FILE lets_get_it_started/test.me.2-butnew\n'
            'PRESENT_FILE lets_get_it_started/test.me.2-butsecond\n'
            'old: 3aabb1ed7e4d7b7c655a00ab183383c323700a1f\n'
            'current: 93044fb853800db246860982a52eeb78e214ca4a\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.differences(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Root: 96be5f3037f27a34a3acad1b9f3106652efdd8f7\n'
            'Status of repo-full-name:\n'
            'PRESENT /lets_get_it_started/test.me.2-butnew\n'
            'PRESENT /lets_get_it_started/test.me.2-butsecond\n'
            'MODIFIED /test.me.1\n'  # fixme shan't do that
            'DELETED /wat/test.me.2\n'
            'DONE'), res)

        res = await hoard_cmd.contents.pending_pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Status of repo-full-name:',
            'Hoard root: 96be5f3037f27a34a3acad1b9f3106652efdd8f7:',
            'Repo root: 93044fb853800db246860982a52eeb78e214ca4a:',
            'REPO_DESIRED_FILE_ADDED /lets_get_it_started/test.me.2-butnew',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butnew',
            'REPO_DESIRED_FILE_ADDED /lets_get_it_started/test.me.2-butsecond',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butsecond',
            'REPO_DESIRED_FILE_ADDED /test.me.1',
            'HOARD_FILE_CHANGED /test.me.1',
            'REPO_FILE_TO_DELETE /wat/test.me.2',
            'HOARD_FILE_DELETED /wat/test.me.2'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Before: Hoard [96be5f] <- repo [curr: 38d7f4, stg: 93044f, des: 96be5f]\n'
            'ADD_NEW_TO_HOARD /lets_get_it_started/test.me.2-butnew\n'
            'ADD_NEW_TO_HOARD /lets_get_it_started/test.me.2-butsecond\n'
            'ALREADY_MARKED_GET /test.me.1\n'  # fixme shan't do
            'DELETE_FROM_HOARD /wat/test.me.2\n'
            'After: Hoard [9b791e], repo [curr: bd737a, stg: 93044f, des: 9b791e]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: 9b791ecf69d7f132e8636bfd6ae5a609815c4d9b\n'
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |         7|         4|         2|         1|\n'
            '|repo-copy-name           |         7|         4|         2|         1|\n'
            '|repo-full-name           |         6|         5|         1|          |\n'
            '|repo-partial-name        |         2|         1|          |         1|\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |        56|        32|        16|         8|\n'
            '|repo-copy-name           |        56|        32|        16|         8|\n'
            '|repo-full-name           |        48|        42|         6|          |\n'
            '|repo-partial-name        |        14|         6|          |         8|\n'), res)

    async def test_restoring_modified_state_from_hoard(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir.name)

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual([
            'Before: Hoard [a80f91] <- repo [curr: a80f91, stg: f6a740, des: a80f91]',
            'ADD_NEW_TO_HOARD /test.me.1',
            'ADD_NEW_TO_HOARD /wat/test.me.2',
            'After: Hoard [f6a740], repo [curr: f6a740, stg: f6a740, des: f6a740]',
            "Sync'ed repo-partial-name to hoard!",
            'Before: Hoard [f6a740] <- repo [curr: a80f91, stg: d99580, des: f6a740]',
            '=/test.me.1',
            '=/wat/test.me.2',
            'ADD_NEW_TO_HOARD /test.me.4',
            'ADD_NEW_TO_HOARD /wat/test.me.3',
            'After: Hoard [d99580], repo [curr: d99580, stg: d99580, des: d99580]',
            "Sync'ed repo-full-name to hoard!",
            'Before: Hoard [d99580] <- repo [curr: a80f91, stg: 9fbdcf, des: d99580]',
            '=/test.me.1',
            '=/wat/test.me.3',
            'After: Hoard [d99580], repo [curr: 9fbdcf, stg: 9fbdcf, des: d99580]',
            "Sync'ed repo-backup-name to hoard!",
            'Before: Hoard [d99580] <- repo [curr: a80f91, stg: e9ce07, des: a80f91]',
            'CLEANUP_SAME /test.me.4',
            'INCOMING_TO_HOARD /test.me.5',
            'INCOMING_TO_HOARD /wat/test.me.6',
            'CLEANUP_DIFFERENT /wat/test.me.3',
            'After: Hoard [89527b], repo [curr: 843a75, stg: e9ce07, des: a80f91]',
            "Sync'ed repo-incoming-name to hoard!",
            'DONE'], res.splitlines())

        res = await hoard_cmd.files.push(full_cave_cmd.current_uuid())
        self.assertEqual([
            'repo-full-name:',
            '+ test.me.5',
            '+ wat/test.me.6',
            'repo-full-name:',
            'DONE'], res.splitlines())

        self.assertDictEqual({
            'repo-full/test.me.1': 'gsadfs',
            'repo-full/test.me.4': 'fwadeaewdsa',
            'repo-full/test.me.5': 'adsfg',
            'repo-full/wat/test.me.2': 'gsadf3dq',
            'repo-full/wat/test.me.3': 'afaswewfas',
            'repo-full/wat/test.me.6': 'f2fwsdf'},
            dump_file_list(self.tmpdir.name, "repo-full", data=True))

        res = await full_cave_cmd.refresh()
        self.assertEqual([
            'PRESENT_FILE test.me.5',
            'PRESENT_FILE wat/test.me.6',
            'old: d995800c80add686a027bac8628ca610418c64b6',
            'current: 89527b0fa576e127d04089d9cb5aab0e5619696d',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Before: Hoard [89527b] <- repo [curr: 89527b, stg: 89527b, des: 89527b]',
            'After: Hoard [89527b], repo [curr: 89527b, stg: 89527b, des: 89527b]',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())

        # modify file contents
        pfw = pretty_file_writer(self.tmpdir.name)
        pfw('repo-full/wat/test.me.2', None)  # delete file
        pfw('repo-full/test.me.4', "alhifqh98;wf")  # modify file
        shutil.move(
            join(self.tmpdir.name, 'repo-full/wat/test.me.6'),
            join(self.tmpdir.name, 'repo-full/test.me.6-moved'))

        res = await full_cave_cmd.refresh()
        self.assertEqual([
            'DELETED_NO_COPY wat/test.me.2',
            'MOVED wat/test.me.6 TO test.me.6-moved',
            'MODIFIED_FILE test.me.4',
            'old: 89527b0fa576e127d04089d9cb5aab0e5619696d',
            'current: 0577b25af683498909c9834d8b54d299ffb47d03',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pending_pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Status of repo-full-name:',
            'Hoard root: 89527b0fa576e127d04089d9cb5aab0e5619696d:',
            'Repo root: 0577b25af683498909c9834d8b54d299ffb47d03:',
            'REPO_DESIRED_FILE_CHANGED /test.me.4',
            'HOARD_FILE_CHANGED /test.me.4',
            'REPO_DESIRED_FILE_ADDED /test.me.6-moved',
            'HOARD_FILE_ADDED /test.me.6-moved',
            'REPO_FILE_TO_DELETE /wat/test.me.2',
            'HOARD_FILE_DELETED /wat/test.me.2',
            'REPO_FILE_TO_DELETE /wat/test.me.6',
            'HOARD_FILE_DELETED /wat/test.me.6'], res.splitlines())

        res = await hoard_cmd.files.pending(full_cave_cmd.current_uuid())
        self.assertEqual(['repo-full-name:', 'DONE'], res.splitlines())

        res = await hoard_cmd.contents.ls(show_remotes=False)
        self.assertEqual([
            'Root: 89527b0fa576e127d04089d9cb5aab0e5619696d',
            '/',
            '/test.me.1 = a:3',
            '/test.me.4 = a:1 g:1 c:1',
            '/test.me.5 = a:1 g:1 c:1',
            '/wat',
            '/wat/test.me.2 = a:2 g:1',
            '/wat/test.me.3 = a:2 c:1',
            '/wat/test.me.6 = a:1 g:1 c:1',
            'DONE'], res.splitlines())

        res = await hoard_cmd.contents.restore(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Before: Hoard [89527b] <- repo [curr: 89527b, stg: 0577b2, des: 89527b]',
            'DELETE /test.me.6-moved',  # fixme should clean up unnecessary files maybe?
            'g/test.me.4',
            'g/wat/test.me.2',
            'g/wat/test.me.6',
            'After: Hoard [89527b], repo [curr: d6c45b, stg: 0577b2, des: 89527b]',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())

        res = await hoard_cmd.files.pending(full_cave_cmd.current_uuid())
        self.assertEqual([
            'repo-full-name:',
            'TO_GET (from 0) /test.me.4',
            'TO_CLEANUP (is in 0) /test.me.6-moved',
            'TO_GET (from 1) /wat/test.me.2',
            'TO_GET (from 0) /wat/test.me.6',
            ' repo-partial-name has 1 files',
            'DONE'], res.splitlines())

        res = await hoard_cmd.files.push(full_cave_cmd.current_uuid())
        self.assertEqual([
            'repo-full-name:',
            '+ test.me.4',
            '+ wat/test.me.2',
            '+ wat/test.me.6',
            'repo-full-name:',
            'd test.me.6-moved',
            'remove dangling /test.me.6-moved',
            'DONE'], res.splitlines())

        self.assertDictEqual({
            'repo-full/test.me.1': 'gsadfs',
            'repo-full/test.me.4': 'fwadeaewdsa',
            'repo-full/test.me.5': 'adsfg',
            'repo-full/wat/test.me.2': 'gsadf3dq',
            'repo-full/wat/test.me.3': 'afaswewfas',
            'repo-full/wat/test.me.6': 'f2fwsdf'},
            dump_file_list(self.tmpdir.name, "repo-full", data=True))

        res = await hoard_cmd.contents.ls(show_remotes=False)
        self.assertEqual([
            'Root: 89527b0fa576e127d04089d9cb5aab0e5619696d',
            '/',
            '/test.me.1 = a:3',
            '/test.me.4 = a:1 g:1 c:1',
            '/test.me.5 = a:1 g:1 c:1',
            '/wat',
            '/wat/test.me.2 = a:2 g:1',
            '/wat/test.me.3 = a:2 c:1',
            '/wat/test.me.6 = a:1 g:1 c:1',
            'DONE'], res.splitlines())

        res = await full_cave_cmd.refresh()
        self.assertEqual([
            'MOVED test.me.6-moved TO wat/test.me.6',
            'MODIFIED_FILE test.me.4',
            'PRESENT_FILE wat/test.me.2',
            'old: 0577b25af683498909c9834d8b54d299ffb47d03',
            'current: 89527b0fa576e127d04089d9cb5aab0e5619696d',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pending_pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Status of repo-full-name:',
            'Hoard root: 89527b0fa576e127d04089d9cb5aab0e5619696d:',
            'Repo root: 89527b0fa576e127d04089d9cb5aab0e5619696d:'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Before: Hoard [89527b] <- repo [curr: 89527b, stg: 89527b, des: 89527b]',
            'After: Hoard [89527b], repo [curr: 89527b, stg: 89527b, des: 89527b]',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())
