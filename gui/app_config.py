import os
from typing import Dict

import rtoml

config: Dict[any, any] = {}

if os.path.isfile("hoard_explorer.toml"):
    with open("hoard_explorer.toml", 'r') as f:
        config = rtoml.load(f)


def _write_config():
    with open("hoard_explorer.toml", 'w') as f:
        rtoml.dump(config, f)
