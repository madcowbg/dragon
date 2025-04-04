import os
import pathlib
import tempfile
import unittest
from os.path import join
from typing import Tuple, Iterable
from unittest import IsolatedAsyncioTestCase

from command.comparison_repo import walk_repo
from command.hoard_ignore import HoardIgnore, DEFAULT_IGNORE_GLOBS
from command.test_repo_command import pretty_file_writer


class TestIgnoreLogic(IsolatedAsyncioTestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        pfw = pretty_file_writer(self.tmpdir.name)

        pfw("System Volume Information/lala", "dasda")
        pfw("System Volume Information/tralala", "lalalala")

        os.mkdir(join(self.tmpdir.name, ".hoard"))

        pathlib.Path(self.tmpdir.name).joinpath('someother').joinpath('.hoard').joinpath('shouldnotbeignored').mkdir(
            parents=True)

        pfw("Thumbs.db", "thumbnails in here")
        pfw("$Recycle.Bin/recycled", "dasda")
        pfw("RECYCLE1/recycled", "dasda")

        pfw("some/RECYCLE1/wontignore", "dasda")
        pfw("other/RECYCLE1/wontignore", "dasda")
        pfw("some/System Volume Information/wontignore", "dasda")

        pfw("wontignore/.hoard/meeee", "dasada")
        pfw("willignore/tHumbs.db", "adaw")

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_no_matching_yields_all(self):
        self.assertEqual([
            ('Thumbs.db', None),
            (None, '$Recycle.Bin'),
            (None, '.hoard'),
            (None, 'other'),
            (None, 'RECYCLE1'),
            (None, 'some'),
            (None, 'someother'),
            (None, 'System Volume Information'),
            (None, 'willignore'),
            (None, 'wontignore'),
            ('$Recycle.Bin/recycled', None),
            (None, 'other/RECYCLE1'),
            ('other/RECYCLE1/wontignore', None),
            ('RECYCLE1/recycled', None),
            (None, 'some/RECYCLE1'),
            (None, 'some/System Volume Information'),
            ('some/RECYCLE1/wontignore', None),
            ('some/System Volume Information/wontignore', None),
            (None, 'someother/.hoard'),
            (None, 'someother/.hoard/shouldnotbeignored'),
            ('System Volume Information/lala', None),
            ('System Volume Information/tralala', None),
            ('willignore/tHumbs.db', None),
            (None, 'wontignore/.hoard'),
            ('wontignore/.hoard/meeee', None)], self.make_relative(walk_repo(self.tmpdir.name, HoardIgnore([]))))

    def test_matching_is_correct(self):
        ignorer = HoardIgnore(DEFAULT_IGNORE_GLOBS)

        self.assertEqual([
            (None, 'other'),
            (None, 'some'),
            (None, 'someother'),
            (None, 'willignore'),
            (None, 'wontignore'),
            (None, 'other/RECYCLE1'),
            ('other/RECYCLE1/wontignore', None),
            (None, 'some/RECYCLE1'),
            (None, 'some/System Volume Information'),
            ('some/RECYCLE1/wontignore', None),
            ('some/System Volume Information/wontignore', None),
            (None, 'someother/.hoard'),
            (None, 'someother/.hoard/shouldnotbeignored'),
            (None, 'wontignore/.hoard'),
            ('wontignore/.hoard/meeee', None)], self.make_relative(walk_repo(self.tmpdir.name, ignorer)))

    def make_relative(self, file_folder: Iterable[Tuple[pathlib.Path | None, pathlib.Path | None]]):
        return [
            (a.relative_to(self.tmpdir.name).as_posix() if a else None,
             b.relative_to(self.tmpdir.name).as_posix() if b else None)
            for a, b in file_folder]
