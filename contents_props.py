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
