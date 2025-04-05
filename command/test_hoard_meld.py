import os
import pathlib
import shutil
import tempfile
from itertools import cycle
from os.path import join
from unittest import IsolatedAsyncioTestCase

from command.test_hoard_command import populate_hoard, populate_repotypes, init_complex_hoard, dump_file_list
from command.test_repo_command import write_contents


class TestHoardCommand(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        populate_hoard(self.tmpdir.name)

        populate_repotypes(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    async def test_recover(self):
        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = await init_complex_hoard(
            self.tmpdir.name)

        res = await hoard_cmd.contents.pull("repo-full-name")
        self.assertEqual(
            "+/test.me.1\n"
            "+/test.me.4\n"
            "+/wat/test.me.2\n"
            "+/wat/test.me.3\n"
            "Sync'ed repo-full-name to hoard!\nDONE", res)

        all_files = dump_file_list(self.tmpdir.name, 'repo-full', data=True)
        self.assertDictEqual({
            'repo-full/test.me.1': 'gsadfs',
            'repo-full/test.me.4': 'fwadeaewdsa',
            'repo-full/wat/test.me.2': 'gsadf3dq',
            'repo-full/wat/test.me.3': 'afaswewfas'}, all_files)

        dump_path = join(self.tmpdir.name, 'dump')
        os.mkdir(dump_path)
        os.mkdir(join(dump_path, 'inner'))
        for file, dest, rnd in zip(all_files, cycle(["", "b2"]), range(len(all_files))):
            dest_path = pathlib.Path(dump_path).joinpath(dest)
            rnd = f"{rnd}.bak"
            destfile = dest_path.joinpath(rnd)
            print(f"saving {file} to {destfile}")
            dest_path.mkdir(parents=True, exist_ok=True)

            shutil.copy(join(self.tmpdir.name, file), destfile)

        write_contents(join(dump_path, "red-herring.one"), "lhnuaFwe")
        write_contents(join(dump_path, 'b2/red-herring.two'), ";nmikas'pjoawe")

        all_files = dump_file_list(self.tmpdir.name, 'dump')
        self.assertEqual([
            'dump/0.bak',
            'dump/2.bak',
            'dump/b2/1.bak',
            'dump/b2/3.bak',
            'dump/b2/red-herring.two',
            'dump/red-herring.one'], all_files)

        recover_path = join(self.tmpdir.name, 'recover-copy')
        os.mkdir(recover_path)

        res = await hoard_cmd.meld(source=dump_path, dest=recover_path, move=False, junk_folder="_tralala_")
        self.assertEqual(
            "A/test.me.1\n"
            "A/wat/test.me.2\n"
            "s_tralala_/red-herring.one\n"
            "A/test.me.4\n"
            "A/wat/test.me.3\n"
            "s_tralala_/b2/red-herring.two\n"
            "Copied: 4 to Dest: 4, Mismatched: 2, Errors: 0 and Skipped: 0\n", res)

        all_files = dump_file_list(self.tmpdir.name, 'recover-copy', data=True)
        self.assertDictEqual({
            'recover-copy/test.me.1': 'gsadfs',
            'recover-copy/test.me.4': 'fwadeaewdsa',
            'recover-copy/wat/test.me.2': 'gsadf3dq',
            'recover-copy/wat/test.me.3': 'afaswewfas'}, all_files)

        recover_path = join(self.tmpdir.name, 'recover')
        os.mkdir(recover_path)

        res = await hoard_cmd.meld(source=dump_path, dest=recover_path, move=True)
        self.assertEqual(
            "M/test.me.1\n"
            "M/wat/test.me.2\n"
            "+_JUNK_/red-herring.one\n"
            "M/test.me.4\n"
            "M/wat/test.me.3\n"
            "+_JUNK_/b2/red-herring.two\n"
            "Copied: 4 to Dest: 4, Mismatched: 2, Errors: 0 and Skipped: 0\n", res)

        all_files = dump_file_list(self.tmpdir.name, 'recover', data=True)
        self.assertDictEqual({
            'recover/_JUNK_/b2/red-herring.two': ";nmikas'pjoawe",
            'recover/_JUNK_/red-herring.one': 'lhnuaFwe',
            'recover/test.me.1': 'gsadfs',
            'recover/test.me.4': 'fwadeaewdsa',
            'recover/wat/test.me.2': 'gsadf3dq',
            'recover/wat/test.me.3': 'afaswewfas'}, all_files)
