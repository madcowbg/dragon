import logging
from typing import Optional

import fire

from command.command_hoard import HoardCommand
from command.command_repo import RepoCommand

NONE_TOML = "MISSING"


class TotalCommand(object):
    def __init__(self, verbose: bool = False, path: str = ".", name: Optional[str] = None):
        logging.basicConfig(
            level=logging.INFO if verbose else logging.WARNING,
            format='%(asctime)s - %(funcName)20s() - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')
        self.cave = RepoCommand(path=path, name=name)
        self.hoard = HoardCommand(path=path)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    fire.Fire(TotalCommand)
