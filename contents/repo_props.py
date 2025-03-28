import enum
from datetime import datetime


class RepoFileStatus(enum.Enum):
    PRESENT = "present"
    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"


class RepoFileProps:
    def __init__(
            self, size: int, mtime: float, fasthash: str, md5: str | None,
            last_status: RepoFileStatus, last_update_epoch: datetime):
        self.size = size
        self.mtime = mtime
        self.fasthash = fasthash
        self.md5 = md5
        self.last_status = last_status
        self.last_update_epoch = last_update_epoch


class RepoDirProps:
    def __init__(self, last_status: RepoFileStatus, last_update_epoch: datetime):
        self.last_status = last_status
        self.last_update_epoch = last_update_epoch
