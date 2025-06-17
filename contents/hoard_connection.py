import logging
import os
import pathlib
from datetime import datetime

import rtoml

from config import HoardConfig
from contents.hoard import HoardContents, HOARD_CONTENTS_TOML
from lmdb_storage.deferred_operations import HoardDeferredOperations


class ReadonlyHoardContentsConn:
    def __init__(self, folder: pathlib.Path, config: HoardConfig):
        self.folder = folder
        self.config = config

    def __enter__(self) -> "HoardContents":
        self.contents = HoardContents(self.folder, True, self.config)
        deferred_ops = HoardDeferredOperations(self.contents)
        if deferred_ops.have_deferred_ops():
            logging.error("Have deferred operations, will apply them just in case even with readonly ops!")
            deferred_ops.apply_deferred_queue()
        return self.contents

    def __exit__(self, exc_type, exc_val, exc_tb):
        if HoardDeferredOperations(self.contents).have_deferred_ops():
            raise ValueError("Have deferred operations that cannot even be applied!!!")

        self.contents.close(False)
        return None

    def writeable(self):
        return HoardContentsConn(self.folder, self.config)


class HoardContentsConn:
    def __init__(self, folder: pathlib.Path, config: HoardConfig):
        self.config = config

        toml_filename = os.path.join(folder, HOARD_CONTENTS_TOML)
        if not os.path.isfile(toml_filename):
            with open(toml_filename, "w") as f:
                rtoml.dump({
                    "updated": datetime.now().isoformat()
                }, f)

        self.folder = folder

    def __enter__(self) -> "HoardContents":
        self.contents = HoardContents(self.folder, False, self.config)
        deferred_ops = HoardDeferredOperations(self.contents)
        if deferred_ops.have_deferred_ops():
            logging.error("Have deferred operations, will apply them just in case!")
            deferred_ops.apply_deferred_queue()
        return self.contents

    def __exit__(self, exc_type, exc_val, exc_tb):
        if HoardDeferredOperations(self.contents).have_deferred_ops():
            raise ValueError("Have deferred operations that were not applied!!!")

        self.contents.close(True)
        return None
