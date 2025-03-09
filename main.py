import fire
import logging

from hoard_command import HoardCommand
from repo_command import RepoCommand

NONE_TOML = "MISSING"


class TotalCommand(object):
    def __init__(self, verbose: bool = False, path: str = "."):
        if verbose:
            logging.basicConfig(level=logging.INFO)
        self.cave = RepoCommand(path=path)
        self.hoard = HoardCommand(path=path)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    fire.Fire(TotalCommand)
