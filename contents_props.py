import abc
import enum
from typing import Dict, Any, List


class RepoFileProps:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc

    @property
    def size(self) -> int:
        return self.doc["size"]

    @property
    def mtime(self) -> float:
        return self.doc["mtime"]

    @property
    def fasthash(self) -> str:
        return self.doc["fasthash"]


class DirProps:  # fixme split in twain (or just remove...)
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc


class FileStatus(enum.Enum):
    AVAILABLE = "available"
    GET = "get"
    CLEANUP = "cleanup"
    COPY = "copy"
    UNKNOWN = "UNKNOWN"


class HoardFileProps:
    def __init__(self, parent: "HoardContents", fsobject_id: int, size: int, fasthash: str):
        self.parent = parent
        self.fsobject_id = fsobject_id

        self.size = size
        self.fasthash = fasthash

    def replace_file(self, new_props: RepoFileProps, available_uuid: str):
        self.size = new_props.size
        self.fasthash = new_props.fasthash

        self.parent.conn.execute(
            "UPDATE fsobject SET size = ?, fasthash = ? WHERE fsobject_id = ?",
            (new_props.size, new_props.fasthash, self.fsobject_id))

        # mark for re-fetching everywhere it is already available
        self.parent.conn.execute(
            "UPDATE fspresence SET status = ? "
            "WHERE fsobject_id = ? AND status = ?",
            (FileStatus.GET.value, self.fsobject_id, FileStatus.AVAILABLE.value))

        # mark that is available here
        self.mark_available(available_uuid)

    def mark_available(self, remote_uuid: str):
        self.parent.conn.execute(
            "INSERT OR REPLACE INTO fspresence(fsobject_id, uuid, status) VALUES (?, ?, ?)",
            (self.fsobject_id, remote_uuid, FileStatus.AVAILABLE.value))

    @property
    def available_at(self) -> List[str]:
        return self.by_status(FileStatus.AVAILABLE)

    @property
    def presence(self) -> Dict[str, FileStatus]:
        return dict((repo_uuid, FileStatus(status)) for repo_uuid, status in self.parent.conn.execute(
            "SELECT uuid, status FROM fspresence WHERE fsobject_id = ?",
            (self.fsobject_id,)))

    def by_status(self, selected_status: FileStatus):
        return [u[0] for u in self.parent.conn.execute(
            "SELECT uuid FROM fspresence WHERE fsobject_id = ? AND status = ?",
            (self.fsobject_id, selected_status.value))]

    def mark_for_cleanup(self, repo_uuid: str):
        self.parent.conn.execute(
            "INSERT OR REPLACE INTO fspresence (fsobject_id, uuid, status) VALUES (?, ?, ?)",
            (self.fsobject_id, repo_uuid, FileStatus.CLEANUP.value))

    def status(self, repo_uuid: str) -> FileStatus:
        current = self.parent.conn.execute(
            "SELECT status FROM fspresence WHERE fsobject_id = ? and uuid=?",
            (self.fsobject_id, repo_uuid)
        ).fetchone()
        return FileStatus(current[0]) if current is not None else FileStatus.UNKNOWN

    def mark_to_get(self, repo_uuid: str):
        self.parent.conn.execute(
            "INSERT OR REPLACE INTO fspresence(fsobject_id, uuid, status) VALUES (?, ?, ?)",
            (self.fsobject_id, repo_uuid, FileStatus.GET.value))

    def mark_to_delete(self):
        # remove places that still haven't gotten it
        self.parent.conn.execute(
            "DELETE FROM fspresence "
            "WHERE fsobject_id = ? and status = ?",
            (self.fsobject_id, FileStatus.GET.value))

        # mark for cleanup places where it is available
        self.parent.conn.execute(
            "UPDATE fspresence SET status = ? "
            "WHERE fsobject_id = ? AND status = ?",
            (FileStatus.CLEANUP.value, self.fsobject_id, FileStatus.AVAILABLE.value))

    def remove_status(self, remote_uuid: str):
        self.parent.conn.execute(
            "DELETE FROM fspresence "
            "WHERE fsobject_id = ? AND uuid=?",
            (self.fsobject_id, remote_uuid))

    def status_to_copy(self) -> List[str]:
        return [row[0] for row in self.parent.conn.execute(
            "SELECT uuid FROM fspresence "
            "WHERE fsobject_id = ? AND status IN (?, ?, ?)",
            (self.fsobject_id, *STATUSES_TO_COPY))]


STATUSES_TO_COPY = [FileStatus.COPY.value, FileStatus.GET.value, FileStatus.AVAILABLE.value]

type FSObjectProps = RepoFileProps | HoardFileProps | DirProps
