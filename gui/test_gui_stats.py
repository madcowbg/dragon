import os
import tempfile
from os.path import join
from unittest import IsolatedAsyncioTestCase

from command.fast_path import FastPosixPath
from command.test_hoard_command import populate_repotypes, init_complex_hoard


class TestHoardCommand(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        os.mkdir(join(self.tmpdir.name, "hoard"))
        populate_repotypes(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    async def test_query_stats(self):
        hoard_path = join(self.tmpdir.name, "hoard")

        hoard_cmd, partial_cave_cmd, full_cave_cmd, backup_cave_cmd, incoming_cave_cmd = await init_complex_hoard(
            self.tmpdir.name)

        await hoard_cmd.contents.pull(all=True)

        async with hoard_cmd.hoard.open_contents(create_missing=False) as hoard_contents:
            query = hoard_contents.fsobjects.query
            file_paths = [path.as_posix() for path, _ in hoard_contents.fsobjects.hoard_files()]
            folder_paths = ["/", "/wat"]
            paths = folder_paths + [ "/wat/nonexistent.file", '/wat/test.me.6']

            count_nondeleted = dict((path, query.count_non_deleted(FastPosixPath(path))) for path in folder_paths)
            self.assertEqual({
                '/': 6, '/wat': 3},
                count_nondeleted)

            count_without_src = dict((path, query.num_without_source(FastPosixPath(path))) for path in folder_paths)
            self.assertEqual({
                '/': 2, '/wat': 1},
                count_without_src)

            stats_in_folder = dict((path, query.stats_in_folder(FastPosixPath(path))) for path in folder_paths)
            self.assertEqual({
                '/': (6, 47),
                '/wat': (3, 25),}, stats_in_folder)

            used_size = dict(
                (repo.name, query.used_size(repo.uuid)) for repo in hoard_contents.hoard_config.remotes.all())
            self.assertEqual({
                'repo-backup-name': 47,
                'repo-full-name': 47,
                'repo-incoming-name': 32,
                'repo-partial-name': 14}, used_size)

            is_deleted = dict((path, query.is_deleted(FastPosixPath(path))) for path in file_paths)
            self.assertEqual({
                '/test.me.1': False,
                '/test.me.4': False,
                '/test.me.5': False,
                '/wat/test.me.2': False,
                '/wat/test.me.3': False,
                '/wat/test.me.6': False}, is_deleted)

            num_sources = dict((path, query.num_sources(FastPosixPath(path))) for path in file_paths)
            self.assertEqual({
                '/test.me.1': 3,
                '/test.me.4': 1,
                '/test.me.5': 0,
                '/wat/test.me.2': 2,
                '/wat/test.me.3': 2,
                '/wat/test.me.6': 0}, num_sources)
