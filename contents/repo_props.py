import enum
from datetime import datetime


class RepoFileStatus(enum.Enum):
    PRESENT = "present"
    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"
    MOVED_FROM = "moved_from"

class FileDesc:
    def __init__(self, size: int, mtime: float, fasthash: str, md5: str | None):
        self.size = size
        self.mtime = mtime
        self.fasthash = fasthash
        self.md5 = md5


class RepoFileProps(FileDesc):
    def __init__(self, size: int, mtime: float, fasthash: str, md5: str | None, last_status: RepoFileStatus,
                 last_update_epoch: datetime, last_related_fullpath: str | None):
        super().__init__(size, mtime, fasthash, md5)
        assert last_status != RepoFileStatus.MOVED_FROM or last_related_fullpath is not None

        self.last_status = last_status
        self.last_update_epoch = last_update_epoch
        self.last_related_fullpath = last_related_fullpath
