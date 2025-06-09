import os
from typing import Dict

import rtoml

_config: Dict[any, any] = None

def config() -> Dict[any, any]:
    global _config
    if _config is None:
        if os.path.isfile("hoard_explorer.toml"):
            with open("hoard_explorer.toml", 'r') as f:
                _config = rtoml.load(f)
        else:
            _config = {}
    return _config


def _write_config():
    with open("hoard_explorer.toml", 'w') as f:
        rtoml.dump(_config, f)
