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
            'Pulling repo-full-name...\n'
            'Before: Hoard [None] <- repo [curr: None, stg: 1ad9e0, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'HOARD_FILE_ADDED /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.4\n'
            'HOARD_FILE_ADDED /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'HOARD_FILE_ADDED /wat/test.me.2\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3\n'
            'HOARD_FILE_ADDED /wat/test.me.3\n'
            'updated repo-full-name from None to 1ad9e0\n'
            'updated repo-backup-name from None to 1ad9e0\n'
            'After: Hoard [1ad9e0], repo [curr: 1ad9e0, stg: 1ad9e0, des: 1ad9e0]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
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
            'Pulling repo-partial-name...\n'
            'Before: Hoard [1ad9e0] <- repo [curr: None, stg: f9bfc2, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'updated repo-partial-name from None to f9bfc2\n'
            'After: Hoard [1ad9e0], repo [curr: f9bfc2, stg: f9bfc2, des: f9bfc2]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
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
            'Root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'Remote repo-backup-name current=None staging=None desired=1ad9e0\n'
            'Remote repo-full-name current=1ad9e0 staging=1ad9e0 desired=1ad9e0\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
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

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
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
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
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
        self.assertEqual((
            'Root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'Remote repo-backup-name current=None staging=None desired=1ad9e0\n'
            'Remote repo-full-name current=1ad9e0 staging=1ad9e0 desired=1ad9e0\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            '/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), '
            '(repo-partial-name:.)\n'
            '/test.me.1 = a:2 g:1\n'
            '/test.me.4 = a:1 g:1\n'
            '/wat => (repo-backup-name:wat), (repo-full-name:wat), '
            '(repo-incoming-name:wat), (repo-partial-name:wat)\n'
            '/wat/test.me.2 = a:2 g:1\n'
            '/wat/test.me.3 = a:1 g:1\n'
            'DONE'), res)

        # delete file before it is backed up
        assert os.path.isfile(join(self.tmpdir.name, 'repo-full/wat/test.me.3'))
        os.remove(join(self.tmpdir.name, 'repo-full/wat/test.me.3'))
        pfw('repo-full/wat/test.me.z', "whut-whut-in-the-but")

        res = await full_cave_cmd.refresh(show_details=False)
        self.assertEqual((
            'old: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'current: b2effc3b7e6a77096f705c7d24e3909cd6f347e2\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [1ad9e0] <- repo [curr: 1ad9e0, stg: b2effc, des: 1ad9e0]\n'
            'REPO_FILE_TO_DELETE /wat/test.me.3\n'
            'HOARD_FILE_DELETED /wat/test.me.3\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.z\n'
            'HOARD_FILE_ADDED /wat/test.me.z\n'
            'updated repo-full-name from 1ad9e0 to b2effc\n'
            'updated repo-backup-name from 1ad9e0 to b2effc\n'
            'After: Hoard [b2effc], repo [curr: b2effc, stg: b2effc, des: b2effc]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: b2effc3b7e6a77096f705c7d24e3909cd6f347e2\n'
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
        self.assertEqual((
            'Root: b2effc3b7e6a77096f705c7d24e3909cd6f347e2\n'
            'Remote repo-backup-name current=None staging=None desired=b2effc\n'
            'Remote repo-full-name current=b2effc staging=b2effc desired=b2effc\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            '/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), '
            '(repo-partial-name:.)\n'
            '/test.me.1 = a:2 g:1\n'
            '/test.me.4 = a:1 g:1\n'
            '/wat => (repo-backup-name:wat), (repo-full-name:wat), '
            '(repo-incoming-name:wat), (repo-partial-name:wat)\n'
            '/wat/test.me.2 = a:2 g:1\n'
            '/wat/test.me.z = a:1 g:1\n'
            'DONE'), res)

        # new file in partial
        pfw('repo-partial/test.me.5', "adsfgasd")

        res = await partial_cave_cmd.refresh(show_details=False)
        self.assertEqual((
            'old: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe\n'
            'current: 499273be7613adfbfa809a42a7d2b6e9be0245e6\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [b2effc] <- repo [curr: f9bfc2, stg: 499273, des: f9bfc2]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.5\n'
            'HOARD_FILE_ADDED /test.me.5\n'
            'updated repo-partial-name from f9bfc2 to 499273\n'
            'updated repo-full-name from b2effc to 7672cb\n'
            'updated repo-backup-name from b2effc to 7672cb\n'
            'After: Hoard [7672cb], repo [curr: 499273, stg: 499273, des: 499273]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: 7672cb5914a2bb1ae0ba43506eeb54f6fbeb5ad9\n'
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
        self.assertEqual((
            'Root: 7672cb5914a2bb1ae0ba43506eeb54f6fbeb5ad9\n'
            'Remote repo-backup-name current=None staging=None desired=7672cb\n'
            'Remote repo-full-name current=b2effc staging=b2effc desired=7672cb\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=499273 staging=499273 desired=499273\n'
            '/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), '
            '(repo-partial-name:.)\n'
            '/test.me.1 = a:2 g:1\n'
            '/test.me.4 = a:1 g:1\n'
            '/test.me.5 = a:1 g:2\n'
            '/wat => (repo-backup-name:wat), (repo-full-name:wat), '
            '(repo-incoming-name:wat), (repo-partial-name:wat)\n'
            '/wat/test.me.2 = a:2 g:1\n'
            '/wat/test.me.z = a:1 g:1\n'
            'DONE'), res)

        res = await hoard_cmd.contents.get("repo-partial-name", "/wat")
        self.assertEqual("Path /wat must be relative, but is absolute.", res)

        res = await hoard_cmd.contents.get("repo-partial-name", "wat")
        self.assertEqual(
            "+/wat/test.me.z\n"
            "Considered 2 files.\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: 7672cb5914a2bb1ae0ba43506eeb54f6fbeb5ad9\n'
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
        self.assertEqual((
            'Root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'Remote repo-backup-name current=None staging=None desired=1ad9e0\n'
            'Remote repo-full-name current=1ad9e0 staging=1ad9e0 desired=1ad9e0\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            '/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), '
            '(repo-partial-name:.)\n'
            '/test.me.1 = a:2 g:1\n'
            '/test.me.4 = a:1 g:1\n'
            '/wat => (repo-backup-name:wat), (repo-full-name:wat), '
            '(repo-incoming-name:wat), (repo-partial-name:wat)\n'
            '/wat/test.me.2 = a:2 g:1\n'
            '/wat/test.me.3 = a:1 g:1\n'
            'DONE'), res)

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
        self.assertEqual((
            'Root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'Remote repo-backup-name current=d696be staging=None desired=1ad9e0\n'
            'Remote repo-full-name current=1ad9e0 staging=1ad9e0 desired=1ad9e0\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            '/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), '
            '(repo-partial-name:.)\n'
            '/test.me.1 = a:3\n'
            '/test.me.4 = a:2\n'
            '/wat => (repo-backup-name:wat), (repo-full-name:wat), '
            '(repo-incoming-name:wat), (repo-partial-name:wat)\n'
            '/wat/test.me.2 = a:3\n'
            '/wat/test.me.3 = a:1 g:1\n'
            'DONE'), res)

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
            'old: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'current: b2effc3b7e6a77096f705c7d24e3909cd6f347e2\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [1ad9e0] <- repo [curr: 1ad9e0, stg: b2effc, des: 1ad9e0]\n'
            'REPO_FILE_TO_DELETE /wat/test.me.3\n'
            'HOARD_FILE_DELETED /wat/test.me.3\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.z\n'
            'HOARD_FILE_ADDED /wat/test.me.z\n'
            'updated repo-full-name from 1ad9e0 to b2effc\n'
            'updated repo-backup-name from 1ad9e0 to b2effc\n'
            'After: Hoard [b2effc], repo [curr: b2effc, stg: b2effc, des: b2effc]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: b2effc3b7e6a77096f705c7d24e3909cd6f347e2\n'
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
        self.assertEqual((
            'Root: b2effc3b7e6a77096f705c7d24e3909cd6f347e2\n'
            'Remote repo-backup-name current=d696be staging=None desired=b2effc\n'
            'Remote repo-full-name current=b2effc staging=b2effc desired=b2effc\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            '/ => (repo-backup-name:.), (repo-full-name:.), (repo-incoming-name:.), '
            '(repo-partial-name:.)\n'
            '/test.me.1 = a:3\n'
            '/test.me.4 = a:2\n'
            '/wat => (repo-backup-name:wat), (repo-full-name:wat), '
            '(repo-incoming-name:wat), (repo-partial-name:wat)\n'
            '/wat/test.me.2 = a:3\n'
            '/wat/test.me.z = a:1 g:1\n'
            'DONE'), res)

    async def test_file_is_deleted_after_copied(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir.name)
        pfw = pretty_file_writer(self.tmpdir.name)

        await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        await hoard_cmd.files.push(backup_cave_cmd.current_uuid())

        res = await hoard_cmd.contents.ls(show_remotes=True)
        self.assertEqual(
            'Root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'Remote repo-backup-name current=1ad9e0 staging=None desired=1ad9e0\n'
            'Remote repo-full-name current=1ad9e0 staging=1ad9e0 desired=1ad9e0\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
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
            'old: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'current: 22e45e6a6041b705c05bedfef6c451457240ea1f\n'
            "Refresh done!", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [1ad9e0] <- repo [curr: 1ad9e0, stg: 22e45e, des: 1ad9e0]\n'
            'REPO_FILE_TO_DELETE /wat/test.me.2\n'
            'HOARD_FILE_DELETED /wat/test.me.2\n'
            'updated repo-partial-name from f9bfc2 to 57a93f\n'
            'updated repo-full-name from 1ad9e0 to 22e45e\n'
            'updated repo-backup-name from 1ad9e0 to 22e45e\n'
            'After: Hoard [22e45e], repo [curr: 22e45e, stg: 22e45e, des: 22e45e]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: 22e45e6a6041b705c05bedfef6c451457240ea1f\n'
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
        self.assertEqual((
            'Root: 22e45e6a6041b705c05bedfef6c451457240ea1f\n'
            'Remote repo-backup-name current=1ad9e0 staging=None desired=22e45e\n'
            'Remote repo-full-name current=22e45e staging=22e45e desired=22e45e\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=57a93f\n'
            '/\n'
            '/test.me.1 = a:3\n'
            '/test.me.4 = a:2\n'
            '/wat\n'
            '/wat/test.me.2 = c:2\n'
            '/wat/test.me.3 = a:2\n'
            'DONE'), res)

        res = await hoard_cmd.files.push("repo-backup-name")
        self.assertEqual(
            f"repo-backup-name:\n"
            f"repo-backup-name:\n"
            "d wat/test.me.2\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: 22e45e6a6041b705c05bedfef6c451457240ea1f\n'
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
            'Root: 22e45e6a6041b705c05bedfef6c451457240ea1f\n'
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
        self.assertEqual((
            'Root: 22e45e6a6041b705c05bedfef6c451457240ea1f\n'
            'Remote repo-backup-name current=22e45e staging=None desired=22e45e\n'
            'Remote repo-full-name current=22e45e staging=22e45e desired=22e45e\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=57a93f staging=f9bfc2 desired=57a93f\n'
            '/\n'
            '/test.me.1 = a:3\n'
            '/test.me.4 = a:2\n'
            '/wat\n'
            '/wat/test.me.3 = a:2\n'
            'DONE'), res)

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

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe\n'
            '|Num Files                |total     |available |get       |\n'
            '|repo-backup-name         |         2|          |         2|\n'
            '|repo-full-name           |         2|          |         2|\n'
            '|repo-partial-name        |         2|         2|          |\n'
            '\n'
            '|Size                     |total     |available |get       |\n'
            '|repo-backup-name         |        14|          |        14|\n'
            '|repo-full-name           |        14|          |        14|\n'
            '|repo-partial-name        |        14|        14|          |\n'), res)

        # refresh new contents file
        res = await hoard_cmd.contents.pull(new_content_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-new-contents-name...\n'
            'Before: Hoard [f9bfc2] <- repo [curr: None, stg: b66f52, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /wat/one-new.file\n'
            'HOARD_FILE_ADDED /wat/one-new.file\n'
            'updated repo-full-name from f9bfc2 to 6f8ca1\n'
            'updated repo-backup-name from f9bfc2 to 6f8ca1\n'
            'updated repo-new-contents-name from None to b66f52\n'
            'After: Hoard [6f8ca1], repo [curr: b66f52, stg: b66f52, des: b66f52]\n'
            "Sync'ed repo-new-contents-name to hoard!\n"
            'DONE'), res)

        # pull full as well - its files will be added to the new repop
        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [6f8ca1] <- repo [curr: None, stg: 1ad9e0, des: 6f8ca1]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.4\n'
            'HOARD_FILE_ADDED /test.me.4\n'
            'REPO_DESIRED_FILE_TO_GET /wat/one-new.file\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3\n'
            'HOARD_FILE_ADDED /wat/test.me.3\n'
            'updated repo-full-name from 6f8ca1 to d934e1\n'
            'updated repo-backup-name from 6f8ca1 to d934e1\n'
            'updated repo-new-contents-name from b66f52 to 66736a\n'
            'After: Hoard [d934e1], repo [curr: 1ad9e0, stg: 1ad9e0, des: d934e1]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: d934e1c4b772efce39429b51960f79d93c60ca7d\n'
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
            'Root: d934e1c4b772efce39429b51960f79d93c60ca7d\n'
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
            'old: 3884dba707937a10d461979214213b4cbbba3f6f\n'
            'current: 3884dba707937a10d461979214213b4cbbba3f6f\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull(new_content_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-new-contents-name...\n'
            'Before: Hoard [d934e1] <- repo [curr: b66f52, stg: b66f52, des: adcdce]\n'
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.2\n'
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.3\n'
            'After: Hoard [d934e1], repo [curr: b66f52, stg: b66f52, des: adcdce]\n'
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
            'Root: d934e1c4b772efce39429b51960f79d93c60ca7d\n'
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
            'Pulling repo-partial-name...\n'
            'Before: Hoard [None] <- repo [curr: None, stg: f9bfc2, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'HOARD_FILE_ADDED /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'HOARD_FILE_ADDED /wat/test.me.2\n'
            'updated repo-partial-name from None to f9bfc2\n'
            'updated repo-full-name from None to f9bfc2\n'
            'updated repo-backup-name from None to f9bfc2\n'
            'updated repo-copy-name from None to f9bfc2\n'
            'After: Hoard [f9bfc2], repo [curr: f9bfc2, stg: f9bfc2, des: f9bfc2]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.differences(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Root: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe\n'
            'Remote repo-backup-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-copy-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-full-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            'Status of repo-full-name:\n'
            'PRESENT /test.me.4\n'
            'PRESENT /wat/test.me.3\n'
            'DONE'), res)

        res = await hoard_cmd.contents.pending_pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Status of repo-full-name:',
            'Hoard root: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe:',
            'Repo current=None staging=1ad9e0 desired=f9bfc2',
            'Repo root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad:',
            'REPO_MARK_FILE_AVAILABLE /test.me.1',
            'REPO_MARK_FILE_AVAILABLE /test.me.4',
            'HOARD_FILE_ADDED /test.me.4',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3',
            'HOARD_FILE_ADDED /wat/test.me.3'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
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
            'updated repo-copy-name from f9bfc2 to 1ad9e0\n'
            'After: Hoard [1ad9e0], repo [curr: 1ad9e0, stg: 1ad9e0, des: 1ad9e0]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual(
            'Root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
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
            'old: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'current: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
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
            f"{full_cave_cmd.current_uuid()} [1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad]:\n"
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
            'old: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'current: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.differences(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'Remote repo-backup-name current=None staging=None desired=1ad9e0\n'
            'Remote repo-copy-name current=1ad9e0 staging=None desired=1ad9e0\n'
            'Remote repo-full-name current=1ad9e0 staging=1ad9e0 desired=1ad9e0\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
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
            'Hoard root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad:',
            'Repo current=1ad9e0 staging=01152a desired=1ad9e0',
            'Repo root: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e:',
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.2-butnew',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butnew',
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.2-butsecond',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butsecond',
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.4-renamed',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.4-renamed',
            'REPO_DESIRED_FILE_CHANGED /test.me.1',
            'HOARD_FILE_CHANGED /test.me.1',
            'REPO_FILE_TO_DELETE /test.me.4',
            'HOARD_FILE_DELETED /test.me.4',
            'REPO_MARK_FILE_AVAILABLE /test.me.added',
            'HOARD_FILE_ADDED /test.me.added',
            'REPO_FILE_TO_DELETE /wat/test.me.2',
            'HOARD_FILE_DELETED /wat/test.me.2'
        ], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [1ad9e0] <- repo [curr: 1ad9e0, stg: 01152a, des: 1ad9e0]\n'
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.2-butnew\n'
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butnew\n'
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.2-butsecond\n'
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butsecond\n'
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.4-renamed\n'
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.4-renamed\n'
            'REPO_DESIRED_FILE_CHANGED /test.me.1\n'
            'HOARD_FILE_CHANGED /test.me.1\n'
            'REPO_FILE_TO_DELETE /test.me.4\n'
            'HOARD_FILE_DELETED /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.added\n'
            'HOARD_FILE_ADDED /test.me.added\n'
            'REPO_FILE_TO_DELETE /wat/test.me.2\n'
            'HOARD_FILE_DELETED /wat/test.me.2\n'
            'updated repo-partial-name from f9bfc2 to fc111c\n'
            'updated repo-full-name from 1ad9e0 to 01152a\n'
            'updated repo-backup-name from 1ad9e0 to 01152a\n'
            'updated repo-copy-name from 1ad9e0 to 01152a\n'
            'After: Hoard [01152a], repo [curr: 01152a, stg: 01152a, des: 01152a]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |         6|          |         6|          |\n'
            '|repo-copy-name           |         8|         1|         5|         2|\n'
            '|repo-full-name           |         6|         6|          |          |\n'
            '|repo-partial-name        |         2|          |         1|         1|\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |        47|          |        47|          |\n'
            '|repo-copy-name           |        66|        10|        37|        19|\n'
            '|repo-full-name           |        47|        47|          |          |\n'
            '|repo-partial-name        |        13|          |         5|         8|\n'), res)

        res = await hoard_cmd.files.push(full_cave_cmd.current_uuid())
        self.assertEqual(
            'repo-full-name:\n'
            'repo-full-name:\n'
            'DONE', res)

        res = await hoard_cmd.files.pending(copy_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-copy-name:\n"
            "TO_GET (from 1) /lets_get_it_started/test.me.2-butnew\n"
            "TO_GET (from 1) /lets_get_it_started/test.me.2-butsecond\n"
            "TO_GET (from 1) /lets_get_it_started/test.me.4-renamed\n"  # fixme should MOVE instead
            "TO_GET (from 1) /test.me.1\n"
            "TO_CLEANUP (is in 0) /test.me.4\n"
            "TO_GET (from 1) /test.me.added\n"
            "TO_CLEANUP (is in 0) /wat/test.me.2\n"
            " repo-full-name has 5 files\n"
            "DONE", res)

        res = await hoard_cmd.files.push(copy_cave_cmd.current_uuid())
        self.assertEqual((
            'repo-copy-name:\n'
            '+ lets_get_it_started/test.me.2-butnew\n'
            '+ lets_get_it_started/test.me.2-butsecond\n'
            '+ lets_get_it_started/test.me.4-renamed\n'  # fixme should MOVE instead
            '+ test.me.1\n'
            '+ test.me.added\n'
            'repo-copy-name:\n'
            'd test.me.4\n'
            'd wat/test.me.2\n'
            'remove dangling /test.me.4\n'
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |         6|          |         6|          |\n'
            '|repo-copy-name           |         6|         6|          |          |\n'
            '|repo-full-name           |         6|         6|          |          |\n'
            '|repo-partial-name        |         2|          |         1|         1|\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |        47|          |        47|          |\n'
            '|repo-copy-name           |        47|        47|          |          |\n'
            '|repo-full-name           |        47|        47|          |          |\n'
            '|repo-partial-name        |        13|          |         5|         8|\n'),
            res)

        res = await hoard_cmd.files.pending(backup_cave_cmd.current_uuid())
        self.assertEqual(
            'repo-backup-name:\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.2-butnew\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.2-butsecond\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.4-renamed\n'
            'TO_GET (from 2) /test.me.1\n'
            # 'TO_CLEANUP (is in 0) /test.me.4\n'  # fixme do it
            'TO_GET (from 2) /test.me.added\n'
            'TO_GET (from 2) /wat/test.me.3\n'
            ' repo-copy-name has 6 files\n'
            ' repo-full-name has 6 files\n'
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
            'current: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
            'Refresh done!'), res)

        res = await full_cave_cmd.refresh()
        self.assertEqual((
            'NO CHANGES\n'
            'old: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
            'current: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull(copy_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-copy-name...\n'
            'Before: Hoard [01152a] <- repo [curr: 01152a, stg: 01152a, des: 01152a]\n'
            'After: Hoard [01152a], repo [curr: 01152a, stg: 01152a, des: 01152a]\n'
            "Sync'ed repo-copy-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [01152a] <- repo [curr: 01152a, stg: 01152a, des: 01152a]\n'
            'After: Hoard [01152a], repo [curr: 01152a, stg: 01152a, des: 01152a]\n'
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
            'Pulling repo-partial-name...\n'
            'Before: Hoard [None] <- repo [curr: None, stg: f9bfc2, des: None]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'HOARD_FILE_ADDED /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2\n'
            'HOARD_FILE_ADDED /wat/test.me.2\n'
            'updated repo-partial-name from None to f9bfc2\n'
            'updated repo-full-name from None to f9bfc2\n'
            'updated repo-backup-name from None to f9bfc2\n'
            'updated repo-copy-name from None to f9bfc2\n'
            'After: Hoard [f9bfc2], repo [curr: f9bfc2, stg: f9bfc2, des: f9bfc2]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.differences(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Root: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe\n'
            'Remote repo-backup-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-copy-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-full-name current=None staging=None desired=f9bfc2\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
            'Status of repo-full-name:\n'
            'PRESENT /test.me.4\n'
            'PRESENT /wat/test.me.3\n'
            'DONE'), res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
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
            'updated repo-copy-name from f9bfc2 to 1ad9e0\n'
            'After: Hoard [1ad9e0], repo [curr: 1ad9e0, stg: 1ad9e0, des: 1ad9e0]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.pull(backup_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-backup-name...\n'
            'Before: Hoard [1ad9e0] <- repo [curr: None, stg: 3a0889, des: 1ad9e0]\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1\n'
            'REPO_DESIRED_FILE_TO_GET /test.me.4\n'
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.2\n'
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3\n'
            'After: Hoard [1ad9e0], repo [curr: 3a0889, stg: 3a0889, des: 1ad9e0]\n'
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
            'Root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
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
            'old: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'current: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
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
            'old: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'current: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
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
            "Result for local [01152ae75c4fbc81c40b8e9eba8ce23ab770630e]:\n"
            "Max size: 3.5TB\n"
            f"UUID: {full_cave_cmd.current_uuid()}\n"
            "  # files = 6 of size 47\n", res)

        res = await hoard_cmd.contents.differences(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'Remote repo-backup-name current=1ad9e0 staging=3a0889 desired=1ad9e0\n'
            'Remote repo-copy-name current=1ad9e0 staging=None desired=1ad9e0\n'
            'Remote repo-full-name current=1ad9e0 staging=1ad9e0 desired=1ad9e0\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2\n'
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
            'Hoard root: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad:',
            'Repo current=1ad9e0 staging=01152a desired=1ad9e0',
            'Repo root: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e:',
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.2-butnew',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butnew',
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.2-butsecond',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butsecond',
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.4-renamed',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.4-renamed',
            'REPO_DESIRED_FILE_CHANGED /test.me.1',
            'HOARD_FILE_CHANGED /test.me.1',
            'REPO_FILE_TO_DELETE /test.me.4',
            'HOARD_FILE_DELETED /test.me.4',
            'REPO_MARK_FILE_AVAILABLE /test.me.added',
            'HOARD_FILE_ADDED /test.me.added',
            'REPO_FILE_TO_DELETE /wat/test.me.2',
            'HOARD_FILE_DELETED /wat/test.me.2'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [1ad9e0] <- repo [curr: 1ad9e0, stg: 01152a, des: 1ad9e0]\n'
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.2-butnew\n'
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butnew\n'
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.2-butsecond\n'
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butsecond\n'
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.4-renamed\n'
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.4-renamed\n'
            'REPO_DESIRED_FILE_CHANGED /test.me.1\n'
            'HOARD_FILE_CHANGED /test.me.1\n'
            'REPO_FILE_TO_DELETE /test.me.4\n'
            'HOARD_FILE_DELETED /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.added\n'
            'HOARD_FILE_ADDED /test.me.added\n'
            'REPO_FILE_TO_DELETE /wat/test.me.2\n'
            'HOARD_FILE_DELETED /wat/test.me.2\n'
            'updated repo-partial-name from f9bfc2 to fc111c\n'
            'updated repo-full-name from 1ad9e0 to 01152a\n'
            'updated repo-backup-name from 1ad9e0 to 01152a\n'
            'updated repo-copy-name from 1ad9e0 to 01152a\n'
            'After: Hoard [01152a], repo [curr: 01152a, stg: 01152a, des: 01152a]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |         8|         1|         5|         2|\n'
            '|repo-copy-name           |         8|         1|         5|         2|\n'
            '|repo-full-name           |         6|         6|          |          |\n'
            '|repo-partial-name        |         2|          |         1|         1|\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |        66|        10|        37|        19|\n'
            '|repo-copy-name           |        66|        10|        37|        19|\n'
            '|repo-full-name           |        47|        47|          |          |\n'
            '|repo-partial-name        |        13|          |         5|         8|\n'),
            res)

        res = await hoard_cmd.files.push(full_cave_cmd.current_uuid())
        self.assertEqual(
            'repo-full-name:\n'
            'repo-full-name:\n'
            'DONE', res)

        res = await hoard_cmd.files.pending(copy_cave_cmd.current_uuid())
        self.assertEqual((
            'repo-copy-name:\n'
            'TO_GET (from 1) /lets_get_it_started/test.me.2-butnew\n'
            'TO_GET (from 1) /lets_get_it_started/test.me.2-butsecond\n'
            'TO_GET (from 1) /lets_get_it_started/test.me.4-renamed\n'
            'TO_GET (from 1) /test.me.1\n'
            'TO_CLEANUP (is in 0) /test.me.4\n'
            'TO_GET (from 1) /test.me.added\n'
            'TO_CLEANUP (is in 0) /wat/test.me.2\n'
            ' repo-full-name has 5 files\n'
            'DONE'), res)

        res = await hoard_cmd.files.push(copy_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-copy-name:\n"
            "+ lets_get_it_started/test.me.2-butnew\n"
            "+ lets_get_it_started/test.me.2-butsecond\n"
            '+ lets_get_it_started/test.me.4-renamed\n'  # fixme should be MOVED
            '+ test.me.1\n'
            "+ test.me.added\n"
            "repo-copy-name:\n"
            "d test.me.4\n"
            "d wat/test.me.2\n"
            "DONE", res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |         8|         1|         5|         2|\n'
            '|repo-copy-name           |         6|         6|          |          |\n'
            '|repo-full-name           |         6|         6|          |          |\n'
            '|repo-partial-name        |         2|          |         1|         1|\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |        66|        10|        37|        19|\n'
            '|repo-copy-name           |        47|        47|          |          |\n'
            '|repo-full-name           |        47|        47|          |          |\n'
            '|repo-partial-name        |        13|          |         5|         8|\n'), res)

        res = await hoard_cmd.files.pending(backup_cave_cmd.current_uuid())
        self.assertEqual((
            'repo-backup-name:\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.2-butnew\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.2-butsecond\n'
            'TO_GET (from 2) /lets_get_it_started/test.me.4-renamed\n'  # fixme MOVED
            'TO_GET (from 2) /test.me.1\n'
            'TO_CLEANUP (is in 0) /test.me.4\n'
            'TO_GET (from 2) /test.me.added\n'
            'TO_CLEANUP (is in 0) /wat/test.me.2\n'
            ' repo-copy-name has 5 files\n'
            ' repo-full-name has 5 files\n'
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
            'current: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
            'Refresh done!'), res)

        res = await hoard_cmd.files.push(backup_cave_cmd.current_uuid())
        self.assertEqual(
            "repo-backup-name:\n"
            "+ lets_get_it_started/test.me.2-butnew\n"
            "+ lets_get_it_started/test.me.2-butsecond\n"
            '+ lets_get_it_started/test.me.4-renamed\n'  # fixme moved
            '+ test.me.1\n'
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
            'MODIFIED_FILE test.me.1\n'
            'PRESENT_FILE test.me.added\n'
            'old: 3a0889e00c0c4ace24843be76d59b3baefb16d77\n'
            'current: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
            'Refresh done!'), res)

        res = await full_cave_cmd.refresh()
        self.assertEqual((
            'NO CHANGES\n'
            'old: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
            'current: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull(copy_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-copy-name...\n'
            'Before: Hoard [01152a] <- repo [curr: 01152a, stg: 01152a, des: 01152a]\n'
            'After: Hoard [01152a], repo [curr: 01152a, stg: 01152a, des: 01152a]\n'
            "Sync'ed repo-copy-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [01152a] <- repo [curr: 01152a, stg: 01152a, des: 01152a]\n'
            'After: Hoard [01152a], repo [curr: 01152a, stg: 01152a, des: 01152a]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

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
        self.assertEqual((
            'Root: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |         6|         6|          |          |\n'
            '|repo-copy-name           |         6|         6|          |          |\n'
            '|repo-full-name           |         6|         6|          |          |\n'
            '|repo-partial-name        |         2|          |         1|         1|\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |        47|        47|          |          |\n'
            '|repo-copy-name           |        47|        47|          |          |\n'
            '|repo-full-name           |        47|        47|          |          |\n'
            '|repo-partial-name        |        13|          |         5|         8|\n'), res)

        # move file before being synch-ed
        shutil.move(
            join(self.tmpdir.name, 'repo-partial/test.me.1'),
            join(self.tmpdir.name, 'repo-partial/test.me.1-newlocation'))

        res = await partial_cave_cmd.refresh()
        self.assertEqual((
            'MOVED test.me.1 TO test.me.1-newlocation\n'
            'old: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe\n'
            'current: 563f87e74860dc2cfa0a0b498258b7067ed338d4\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.pull(partial_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-partial-name...\n'
            'Before: Hoard [01152a] <- repo [curr: f9bfc2, stg: 563f87, des: fc111c]\n'
            'REPO_FILE_TO_DELETE /test.me.1\n'
            'HOARD_FILE_DELETED /test.me.1\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.1-newlocation\n'
            'HOARD_FILE_ADDED /test.me.1-newlocation\n'
            'REPO_FILE_TO_DELETE /wat/test.me.2\n'
            'updated repo-partial-name from fc111c to 7d1569\n'
            'updated repo-full-name from 01152a to 1d6997\n'
            'updated repo-backup-name from 01152a to 1d6997\n'
            'updated repo-copy-name from 01152a to 1d6997\n'
            'After: Hoard [1d6997], repo [curr: 563f87, stg: 563f87, des: 7d1569]\n'
            "Sync'ed repo-partial-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual([
            'Root: 1d6997b657a82d81f72b833b4551a0d389dd37c9',
            '|Num Files                |total     |available |get       |cleanup   |',
            '|repo-backup-name         |         7|         5|         1|         1|',
            '|repo-copy-name           |         7|         5|         1|         1|',
            '|repo-full-name           |         7|         5|         1|         1|',
            '|repo-partial-name        |         2|         1|          |         1|',
            '',
            '|Size                     |total     |available |get       |cleanup   |',
            '|repo-backup-name         |        53|        42|         6|         5|',
            '|repo-copy-name           |        53|        42|         6|         5|',
            '|repo-full-name           |        53|        42|         6|         5|',
            '|repo-partial-name        |        14|         6|          |         8|'], res.splitlines())

        res = await hoard_cmd.files.push(partial_cave_cmd.current_uuid())
        self.assertEqual((
            'repo-partial-name:\n'
            'repo-partial-name:\n'
            'd wat/test.me.2\n'
            'remove dangling /wat/test.me.2\n'
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual([
            'Root: 1d6997b657a82d81f72b833b4551a0d389dd37c9',
            '|Num Files                |total     |available |get       |cleanup   |',
            '|repo-backup-name         |         7|         5|         1|         1|',
            '|repo-copy-name           |         7|         5|         1|         1|',
            '|repo-full-name           |         7|         5|         1|         1|',
            '|repo-partial-name        |         1|         1|          |          |',
            '',
            '|Size                     |total     |available |get       |cleanup   |',
            '|repo-backup-name         |        53|        42|         6|         5|',
            '|repo-copy-name           |        53|        42|         6|         5|',
            '|repo-full-name           |        53|        42|         6|         5|',
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
            'Root: f9bfc2be6cc201aa81b733b9d83c1030cc88bffe\n'
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
            'Result for local [1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad]:\n'
            "Max size: 3.5TB\n"
            f"UUID: {full_cave_cmd.current_uuid()}\n"
            "  # files = 4 of size 35\n", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
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
            'updated repo-copy-name from f9bfc2 to 1ad9e0\n'
            'After: Hoard [1ad9e0], repo [curr: 1ad9e0, stg: 1ad9e0, des: 1ad9e0]\n'
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
            'old: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad\n'
            'current: e48d88d5cde10ff5d84be87b3e79c20e77c05ba9\n'
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
            'Result for local [e48d88d5cde10ff5d84be87b3e79c20e77c05ba9]:\n'
            "Max size: 3.5TB\n"
            f"UUID: {full_cave_cmd.current_uuid()}\n"
            "  # files = 5 of size 39\n", res)

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [1ad9e0] <- repo [curr: 1ad9e0, stg: e48d88, des: 1ad9e0]\n'
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.4-renamed\n'
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.4-renamed\n'
            'REPO_DESIRED_FILE_CHANGED /test.me.1\n'
            'HOARD_FILE_CHANGED /test.me.1\n'
            'REPO_FILE_TO_DELETE /test.me.4\n'
            'HOARD_FILE_DELETED /test.me.4\n'
            'REPO_MARK_FILE_AVAILABLE /test.me.added\n'
            'HOARD_FILE_ADDED /test.me.added\n'
            'updated repo-partial-name from f9bfc2 to 8c1a36\n'
            'updated repo-full-name from 1ad9e0 to e48d88\n'
            'updated repo-backup-name from 1ad9e0 to e48d88\n'
            'updated repo-copy-name from 1ad9e0 to e48d88\n'
            'After: Hoard [e48d88], repo [curr: e48d88, stg: e48d88, des: e48d88]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: e48d88d5cde10ff5d84be87b3e79c20e77c05ba9\n'
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |         6|         2|         3|         1|\n'
            '|repo-copy-name           |         5|          |         5|          |\n'
            '|repo-full-name           |         5|         5|          |          |\n'
            '|repo-partial-name        |         2|         1|         1|          |\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |        50|        18|        21|        11|\n'
            '|repo-copy-name           |        39|          |        39|          |\n'
            '|repo-full-name           |        39|        39|          |          |\n'
            '|repo-partial-name        |        13|         8|         5|          |\n'), res)

        res = await hoard_cmd.contents.pull(backup_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-backup-name...\n'
            'Before: Hoard [e48d88] <- repo [curr: 1ad9e0, stg: 3a0889, des: e48d88]\n'
            'REPO_DESIRED_FILE_TO_GET /lets_get_it_started/test.me.4-renamed\n'
            'REPO_DESIRED_FILE_CHANGED /test.me.1\n'
            'REPO_FILE_TO_DELETE /test.me.4\n'
            'REPO_DESIRED_FILE_TO_GET /test.me.added\n'
            'After: Hoard [e48d88], repo [curr: 3a0889, stg: 3a0889, des: e48d88]\n'
            "Sync'ed repo-backup-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.files.push(backup_cave_cmd.current_uuid())
        self.assertEqual((
            'repo-backup-name:\n'
            '+ lets_get_it_started/test.me.4-renamed\n'
            '+ test.me.1\n'
            '+ test.me.added\n'
            '+ wat/test.me.2\n'
            'repo-backup-name:\n'
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: e48d88d5cde10ff5d84be87b3e79c20e77c05ba9\n'
            '|Num Files                |total     |available |get       |\n'
            '|repo-backup-name         |         5|         5|          |\n'
            '|repo-copy-name           |         5|          |         5|\n'
            '|repo-full-name           |         5|         5|          |\n'
            '|repo-partial-name        |         2|         1|         1|\n'
            '\n'
            '|Size                     |total     |available |get       |\n'
            '|repo-backup-name         |        39|        39|          |\n'
            '|repo-copy-name           |        39|          |        39|\n'
            '|repo-full-name           |        39|        39|          |\n'
            '|repo-partial-name        |        13|         8|         5|\n'), res)

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
            'old: e48d88d5cde10ff5d84be87b3e79c20e77c05ba9\n'
            'current: e48d88d5cde10ff5d84be87b3e79c20e77c05ba9\n'
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
            'old: e48d88d5cde10ff5d84be87b3e79c20e77c05ba9\n'
            'current: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
            'Refresh done!'), res)

        res = await hoard_cmd.contents.differences(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Root: e48d88d5cde10ff5d84be87b3e79c20e77c05ba9\n'
            'Remote repo-backup-name current=e48d88 staging=3a0889 desired=e48d88\n'
            'Remote repo-copy-name current=e48d88 staging=None desired=e48d88\n'
            'Remote repo-full-name current=e48d88 staging=e48d88 desired=e48d88\n'
            'Remote repo-incoming-name current=None staging=None desired=None\n'
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=8c1a36\n'
            'Status of repo-full-name:\n'
            'PRESENT /lets_get_it_started/test.me.2-butnew\n'
            'PRESENT /lets_get_it_started/test.me.2-butsecond\n'
            'DELETED /wat/test.me.2\n'
            'DONE'), res)

        res = await hoard_cmd.contents.pending_pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Status of repo-full-name:',
            'Hoard root: e48d88d5cde10ff5d84be87b3e79c20e77c05ba9:',
            'Repo current=e48d88 staging=01152a desired=e48d88',
            'Repo root: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e:',
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.2-butnew',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butnew',
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.2-butsecond',
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butsecond',
            'REPO_FILE_TO_DELETE /wat/test.me.2',
            'HOARD_FILE_DELETED /wat/test.me.2'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual((
            'Pulling repo-full-name...\n'
            'Before: Hoard [e48d88] <- repo [curr: e48d88, stg: 01152a, des: e48d88]\n'
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.2-butnew\n'
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butnew\n'
            'REPO_MARK_FILE_AVAILABLE /lets_get_it_started/test.me.2-butsecond\n'
            'HOARD_FILE_ADDED /lets_get_it_started/test.me.2-butsecond\n'
            'REPO_FILE_TO_DELETE /wat/test.me.2\n'
            'HOARD_FILE_DELETED /wat/test.me.2\n'
            'updated repo-partial-name from 8c1a36 to fc111c\n'
            'updated repo-full-name from e48d88 to 01152a\n'
            'updated repo-backup-name from e48d88 to 01152a\n'
            'updated repo-copy-name from e48d88 to 01152a\n'
            'After: Hoard [01152a], repo [curr: 01152a, stg: 01152a, des: 01152a]\n'
            "Sync'ed repo-full-name to hoard!\n"
            'DONE'), res)

        res = await hoard_cmd.contents.status(hide_time=True, hide_disk_sizes=True)
        self.assertEqual((
            'Root: 01152ae75c4fbc81c40b8e9eba8ce23ab770630e\n'
            '|Num Files                |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |         7|         4|         2|         1|\n'
            '|repo-copy-name           |         7|         4|         2|         1|\n'
            '|repo-full-name           |         6|         6|          |          |\n'
            '|repo-partial-name        |         2|          |         1|         1|\n'
            '\n'
            '|Size                     |total     |available |get       |cleanup   |\n'
            '|repo-backup-name         |        55|        31|        16|         8|\n'
            '|repo-copy-name           |        55|        31|        16|         8|\n'
            '|repo-full-name           |        47|        47|          |          |\n'
            '|repo-partial-name        |        13|          |         5|         8|\n'), res)

    async def test_restoring_modified_state_from_hoard(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = \
            await init_complex_hoard(self.tmpdir.name)

        res = await hoard_cmd.contents.pull(all=True)
        self.assertEqual([
            'Pulling repo-partial-name...',
            'Before: Hoard [None] <- repo [curr: None, stg: f9bfc2, des: None]',
            'REPO_MARK_FILE_AVAILABLE /test.me.1',
            'HOARD_FILE_ADDED /test.me.1',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2',
            'HOARD_FILE_ADDED /wat/test.me.2',
            'updated repo-partial-name from None to f9bfc2',
            'updated repo-full-name from None to f9bfc2',
            'updated repo-backup-name from None to f9bfc2',
            'After: Hoard [f9bfc2], repo [curr: f9bfc2, stg: f9bfc2, des: f9bfc2]',
            "Sync'ed repo-partial-name to hoard!",
            'Pulling repo-full-name...',
            'Before: Hoard [f9bfc2] <- repo [curr: None, stg: 1ad9e0, des: f9bfc2]',
            'REPO_MARK_FILE_AVAILABLE /test.me.1',
            'REPO_MARK_FILE_AVAILABLE /test.me.4',
            'HOARD_FILE_ADDED /test.me.4',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.2',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3',
            'HOARD_FILE_ADDED /wat/test.me.3',
            'updated repo-full-name from f9bfc2 to 1ad9e0',
            'updated repo-backup-name from f9bfc2 to 1ad9e0',
            'After: Hoard [1ad9e0], repo [curr: 1ad9e0, stg: 1ad9e0, des: 1ad9e0]',
            "Sync'ed repo-full-name to hoard!",
            'Pulling repo-backup-name...',
            'Before: Hoard [1ad9e0] <- repo [curr: None, stg: 3a0889, des: 1ad9e0]',
            'REPO_MARK_FILE_AVAILABLE /test.me.1',
            'REPO_DESIRED_FILE_TO_GET /test.me.4',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.2',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.3',
            'After: Hoard [1ad9e0], repo [curr: 3a0889, stg: 3a0889, des: 1ad9e0]',
            "Sync'ed repo-backup-name to hoard!",
            'Pulling repo-incoming-name...',
            'Before: Hoard [1ad9e0] <- repo [curr: None, stg: 3d1726, des: None]',
            'REPO_MARK_FILE_AVAILABLE /test.me.4',
            'REPO_MARK_FILE_AVAILABLE /test.me.5',
            'HOARD_FILE_ADDED /test.me.5',
            'REPO_MARK_FILE_AVAILABLE /wat/test.me.6',
            'HOARD_FILE_ADDED /wat/test.me.6',
            'updated repo-full-name from 1ad9e0 to 8da760',
            'updated repo-backup-name from 1ad9e0 to 8da760',
            'After: Hoard [8da760], repo [curr: 3d1726, stg: 3d1726, des: None]',
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
            'old: 1ad9e0f92a8411689b1aee57f9ccf36c1f09a1ad',
            'current: 8da76083b9eab9f49945d8f2487df38ab909b7df',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Pulling repo-full-name...',
            'Before: Hoard [8da760] <- repo [curr: 8da760, stg: 8da760, des: 8da760]',
            'After: Hoard [8da760], repo [curr: 8da760, stg: 8da760, des: 8da760]',
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
            'old: 8da76083b9eab9f49945d8f2487df38ab909b7df',
            'current: 20b513612f72da81150668cbae96dc90cd623cb9',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pending_pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Status of repo-full-name:',
            'Hoard root: 8da76083b9eab9f49945d8f2487df38ab909b7df:',
            'Repo current=8da760 staging=20b513 desired=8da760',
            'Repo root: 20b513612f72da81150668cbae96dc90cd623cb9:',
            'REPO_DESIRED_FILE_CHANGED /test.me.4',
            'HOARD_FILE_CHANGED /test.me.4',
            'REPO_MARK_FILE_AVAILABLE /test.me.6-moved',
            'HOARD_FILE_ADDED /test.me.6-moved',
            'REPO_FILE_TO_DELETE /wat/test.me.2',
            'HOARD_FILE_DELETED /wat/test.me.2',
            'REPO_FILE_TO_DELETE /wat/test.me.6',
            'HOARD_FILE_DELETED /wat/test.me.6'], res.splitlines())

        res = await hoard_cmd.files.pending(full_cave_cmd.current_uuid())
        self.assertEqual(['repo-full-name:', 'DONE'], res.splitlines())

        res = await hoard_cmd.contents.ls(show_remotes=False)
        self.assertEqual([
            'Root: 8da76083b9eab9f49945d8f2487df38ab909b7df',
            'Remote repo-backup-name current=3a0889 staging=3a0889 desired=8da760',
            'Remote repo-full-name current=8da760 staging=20b513 desired=8da760',
            'Remote repo-incoming-name current=3d1726 staging=3d1726 desired=None',
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2',
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
            'Before: Hoard [8da760] <- repo [curr: 8da760, stg: 20b513, des: 8da760]',
            'REPO_DESIRED_FILE_CHANGED /test.me.4',
            'REPO_FILE_TO_DELETE /test.me.6-moved',  # fixme should clean up unnecessary files maybe?
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.2',
            'REPO_DESIRED_FILE_TO_GET /wat/test.me.6',
            'After: Hoard [8da760], repo [curr: 20b513, stg: 20b513, des: 8da760]',
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
            'Root: 8da76083b9eab9f49945d8f2487df38ab909b7df',
            'Remote repo-backup-name current=3a0889 staging=3a0889 desired=8da760',
            'Remote repo-full-name current=8da760 staging=20b513 desired=8da760',
            'Remote repo-incoming-name current=3d1726 staging=3d1726 desired=None',
            'Remote repo-partial-name current=f9bfc2 staging=f9bfc2 desired=f9bfc2',
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
            'old: 20b513612f72da81150668cbae96dc90cd623cb9',
            'current: 8da76083b9eab9f49945d8f2487df38ab909b7df',
            'Refresh done!'], res.splitlines())

        res = await hoard_cmd.contents.pending_pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Status of repo-full-name:',
            'Hoard root: 8da76083b9eab9f49945d8f2487df38ab909b7df:',
            'Repo current=8da760 staging=8da760 desired=8da760',
            'Repo root: 8da76083b9eab9f49945d8f2487df38ab909b7df:'], res.splitlines())

        res = await hoard_cmd.contents.pull(full_cave_cmd.current_uuid())
        self.assertEqual([
            'Pulling repo-full-name...',
            'Before: Hoard [8da760] <- repo [curr: 8da760, stg: 8da760, des: 8da760]',
            'After: Hoard [8da760], repo [curr: 8da760, stg: 8da760, des: 8da760]',
            "Sync'ed repo-full-name to hoard!",
            'DONE'], res.splitlines())
