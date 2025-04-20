import enum


class RepoFileStatus(enum.Enum):
    PRESENT = "present"
    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"
    MOVED_FROM = "moved_from"


class FileDesc:
    def __init__(self, size: int, fasthash: str, md5: str | None):
        self.size = size
        self.fasthash = fasthash
        self.md5 = md5

        self.last_status = RepoFileStatus.PRESENT  # fixme move to diff state
