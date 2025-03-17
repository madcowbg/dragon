import abc
import enum
from typing import Dict, Any, List


class RepoFileProps:
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc

    @property
    def size(self) -> float:
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
    @property
    @abc.abstractmethod
    def size(self): pass

    @property
    @abc.abstractmethod
    def fasthash(self): pass

    @abc.abstractmethod
    def replace_file(self, new_props: RepoFileProps, available_uuid: str): pass

    @abc.abstractmethod
    def mark_available(self, remote_uuid: str): pass

    @property
    @abc.abstractmethod
    def available_at(self) -> List[str]: pass

    @property
    @abc.abstractmethod
    def presence(self) -> Dict[str, FileStatus]: pass

    @abc.abstractmethod
    def by_status(self, selected_status: FileStatus): pass

    @abc.abstractmethod
    def mark_for_cleanup(self, repo_uuid: str): pass

    @abc.abstractmethod
    def status(self, repo_uuid: str) -> FileStatus: pass

    @abc.abstractmethod
    def mark_to_get(self, repo_uuid: str): pass

    @abc.abstractmethod
    def mark_to_delete(self): pass

    @abc.abstractmethod
    def remove_status(self, remote_uuid: str): pass

    @abc.abstractmethod
    def status_to_copy(self) -> List[str]: pass


class SQLHoardFileProps(HoardFileProps):
    def __init__(self, parent: "SQLHoardContents", fsobject_id: int):
        self.parent = parent
        self.fsobject_id = fsobject_id

    @property
    def size(self):
        return self.parent.conn.execute(
            "SELECT size FROM fsobject WHERE fsobject_id = ?",
            (self.fsobject_id,)).fetchone()[0]

    @property
    def fasthash(self):
        return self.parent.conn.execute(
            "SELECT fasthash FROM fsobject WHERE fsobject_id = ?",
            (self.fsobject_id,)).fetchone()[0]

    def replace_file(self, new_props: RepoFileProps, available_uuid: str):
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


class TOMLHoardFileProps(HoardFileProps):
    def __init__(self, doc: Dict[str, Any]):
        self.doc = doc

    @property
    def size(self):
        return self.doc["size"]

    @property
    def fasthash(self):
        return self.doc["fasthash"]

    def replace_file(self, new_props: RepoFileProps, available_uuid: str):
        self.doc["size"] = new_props.size
        self.doc["fasthash"] = new_props.fasthash

        # mark for re-fetching everywhere it is already available, cancel getting it
        for uuid, status in self.doc["status"].copy().items():
            if status == FileStatus.AVAILABLE.value:
                self.mark_to_get(uuid)
            elif status == FileStatus.GET.value or status == FileStatus.CLEANUP.value:
                pass
            else:
                raise ValueError(f"Unknown status: {status}")

        self.doc["status"][available_uuid] = FileStatus.AVAILABLE.value

    def mark_available(self, remote_uuid: str):
        self.doc["status"][remote_uuid] = FileStatus.AVAILABLE.value

    @property
    def available_at(self) -> List[str]:
        return self.by_status(FileStatus.AVAILABLE)

    @property
    def presence(self):
        return dict((repo_uuid, FileStatus(status)) for repo_uuid, status in self.doc["status"].items())

    def by_status(self, selected_status: FileStatus):
        return [uuid for uuid, status in self.doc["status"].items() if status == selected_status.value]

    def mark_for_cleanup(self, repo_uuid: str):
        self.doc["status"][repo_uuid] = FileStatus.CLEANUP.value

    def status(self, repo_uuid: str) -> FileStatus:
        return FileStatus(self.doc["status"][repo_uuid]) if repo_uuid in self.doc["status"] else FileStatus.UNKNOWN

    def mark_to_get(self, repo_uuid: str):
        self.doc["status"][repo_uuid] = FileStatus.GET.value

    def mark_to_delete(self):
        for uuid, status in self.doc["status"].copy().items():
            assert status != FileStatus.UNKNOWN.value

            if status == FileStatus.GET.value:
                self.remove_status(uuid)
            elif status == FileStatus.AVAILABLE.value:
                self.mark_for_cleanup(uuid)
            elif status == FileStatus.CLEANUP.value:
                pass
            else:
                raise ValueError(f"Unknown status: {status}")

    def remove_status(self, remote_uuid: str):
        self.doc["status"].pop(remote_uuid)

    def status_to_copy(self) -> List[str]:
        return [uuid for uuid, status in self.doc["status"].items() if status in STATUSES_TO_COPY]


STATUSES_TO_COPY = [FileStatus.COPY.value, FileStatus.GET.value, FileStatus.AVAILABLE.value]

type FSObjectProps = RepoFileProps | HoardFileProps | DirProps
