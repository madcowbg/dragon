import tempfile
import unittest
from os.path import join
from unittest import IsolatedAsyncioTestCase

from command.hoard import Hoard
from command.repo import ProspectiveRepo
from command.test_hoard_command import populate_hoard
from contents.hoard_props import HoardFileProps
from contents.repo_props import RepoFileProps
from dragon import TotalCommand


class TestHoardCommand(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        populate_hoard(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    async def test_dump_cave_contents(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        await cave_cmd.refresh(show_details=False)

        repo_contents = ProspectiveRepo(join(self.tmpdir.name, "repo")).open_repo().connect(False).open_contents(True)
        with repo_contents:
            all_fsobjects = [
                (file_or_dir.as_posix(), props.size if isinstance(props, RepoFileProps) else True)
                for file_or_dir, props in repo_contents.fsobjects.all_status()]

            self.assertEqual([
                ('wat/test.me.different', 5),
                ('wat/test.me.once', 8),
                ('wat/test.me.twice', 6)], all_fsobjects)

    async def test_dump_hoard_contents(self):
        cave_cmd = TotalCommand(path=join(self.tmpdir.name, "repo")).cave
        cave_cmd.init()
        await cave_cmd.refresh(show_details=False)

        hoard_cmd = TotalCommand(path=join(self.tmpdir.name, "hoard")).hoard
        hoard_cmd.init()

        hoard_cmd.add_remote(remote_path=join(self.tmpdir.name, "repo"), name="repo-in-local", mount_point="/")

        res = await hoard_cmd.contents.pull(cave_cmd.current_uuid())
        self.assertEqual([
            '+/wat/test.me.different',
            '+/wat/test.me.once',
            '+/wat/test.me.twice',
            "Sync'ed repo-in-local to hoard!",
            'DONE'], res.splitlines())

        repo_uuid = cave_cmd.current_uuid()
        async with Hoard(join(self.tmpdir.name, "hoard")).open_contents(False, True) as hoard_contents:
            all_fsobjects = [
                (file_or_dir.as_posix(), str([f"{repo}: {status.value}" for repo, status in props.presence.items()])
                if isinstance(props, HoardFileProps) else "DIR")
                for file_or_dir, props in hoard_contents.fsobjects]
            self.assertEqual([
                ('/wat/test.me.different', f"['{repo_uuid}: available']"),
                ('/wat/test.me.once', f"['{repo_uuid}: available']"),
                ('/wat/test.me.twice', f"['{repo_uuid}: available']")], all_fsobjects)
