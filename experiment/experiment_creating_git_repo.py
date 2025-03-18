import asyncio
import logging
import os
import pathlib
import tempfile
import unittest
from os.path import join
from typing import Dict, Any

import rtoml
from alive_progress import alive_it, alive_bar
from git import Repo

from contents_hoard import HoardContents
from contents_props import HoardFileProps, DirProps
from util import run_in_parallel_threads


class TestCreatingGitRepo(unittest.TestCase):
    # def setUp(self):
    #     self.tmpdir = tempfile.TemporaryDirectory()
    #     logging.basicConfig(level=logging.INFO)
    #
    # def tearDown(self):
    #     self.tmpdir.cleanup()

    def test_create_repo_from_contents(self):
        # mirror_path = pathlib.Path(self.tmpdir.name).joinpath('mirror')

        hoard_contents_path = r"C:\Users\Bono\refiler-tests\hoard.contents.bak"
        mirror_path = pathlib.Path(os.path.split(hoard_contents_path)[0]).joinpath("mirror")

        logging.info(mirror_path)
        mirror_path.mkdir(parents=True, exist_ok=True)

        _populate_mirror_folder(hoard_contents_path, mirror_path)

        self.assertTrue(False)

        repo = Repo.init(mirror_path.as_posix())

    def _test_repo(self, mirror_path, repo):
        self.assertTrue(not repo.is_dirty())
        mirror_path.joinpath('fasdas.txt').touch()
        self.assertEqual(['fasdas.txt'], repo.untracked_files)
        mirror_path.joinpath('fasdas.txt').unlink()
        self.assertEqual([], repo.untracked_files)


def _populate_mirror_folder(hoard_contents_path, mirror_path):
    logging.info("Opening db...")
    with HoardContents.load(hoard_contents_path) as hoard_contents:
        logging.info("DB opened!")

        def create_doc(props: HoardFileProps) -> Dict[str, Any]:
            return {
                "size": props.size,
                "fasthash": props.fasthash,
                "status": dict([uuid, status.value] for uuid, status in props.presence.items())
            }

        logging.info("Loading repo into docs...")
        files = [(fs, create_doc(props)) for fs, props in alive_it(hoard_contents.fsobjects) if
                 isinstance(props, HoardFileProps)]

        with alive_bar(len(files)) as bar:
            logging.info(f"Creating mirror for {len(files)} files...")

            def create(fs: str, doc: Dict[str, Any]):
                assert os.path.isabs(fs)
                mirror_file_path = pathlib.Path(mirror_path.as_posix() + fs)
                # mirror_file_path = mirror_path.joinpath(fs)

                # logging.info("Creating mirror file...")
                folder, file = os.path.split(mirror_file_path.as_posix())
                pathlib.Path(folder).mkdir(parents=True, exist_ok=True)

                rtoml.dump(doc, mirror_file_path)
                bar()

            results = run_in_parallel_threads(files, fun=create, ntasks=30)
            assert len(results) == len(files)
    logging.info("Done poulating mirror folder!")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    test = TestCreatingGitRepo()
    test.setUp()
    try:
        test.test_create_repo_from_contents()
    finally:
        test.tearDown()
