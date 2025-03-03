import fire
import logging

from hoard_command import HoardCommand
from repo_command import RepoCommand

NONE_TOML = "MISSING"


class TotalCommand(object):
    def __init__(self, verbose: bool = False, **kwargs):
        if verbose:
            logging.basicConfig(level=logging.INFO)
        self.kwargs = kwargs

    @property
    def cave(self): return RepoCommand(**self.kwargs)

    @property
    def hoard(self): return HoardCommand(**self.kwargs)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    fire.Fire(TotalCommand)
