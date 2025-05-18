import enum
from typing import Dict, List, Iterable

from util import FIRST_VALUE


class HoardFileStatus(enum.Enum):
    AVAILABLE = "available"
    GET = "get"
    CLEANUP = "cleanup"
    COPY = "copy"  # fixme remove as copying is kinda the same as getting, and "move" should capture the real need
    MOVE = "move"
    UNKNOWN = "UNKNOWN"


class HoardFileProps:
    def __init__(self, parent: "HoardContents", fsobject_id: int, size: int, fasthash: str):
        self.parent = parent
        self.fsobject_id = fsobject_id

        self.size = size
        self.fasthash = fasthash

    def mark_available(self, remote_uuid: str):
        self.set_status([remote_uuid], HoardFileStatus.AVAILABLE)

    @property
    def available_at(self) -> List[str]:
        return self.by_status(HoardFileStatus.AVAILABLE)

    @property
    def presence(self) -> Dict[str, HoardFileStatus]:
        return dict((repo_uuid, HoardFileStatus(status)) for repo_uuid, status in self.parent.conn.execute(
            "SELECT uuid, status FROM fspresence WHERE fsobject_id = ?",
            (self.fsobject_id,)))

    def by_status(self, selected_status: HoardFileStatus) -> List[str]:
        return [u[0] for u in self.parent.conn.execute(
            "SELECT uuid FROM fspresence WHERE fsobject_id = ? AND status = ?",
            (self.fsobject_id, selected_status.value))]

    def by_statuses(self, *selected_statuses: HoardFileStatus) -> List[str]:
        return [u[0] for u in self.parent.conn.execute(
            f"SELECT uuid FROM fspresence "
            f"WHERE fsobject_id = ? AND status in ({', '.join('?' * len(selected_statuses))})",
            (self.fsobject_id, *[s.value for s in selected_statuses]))]

    def mark_for_cleanup(self, repos: Iterable[str]):
        self.set_status(repos, HoardFileStatus.CLEANUP)

    def get_status(self, repo_uuid: str) -> HoardFileStatus:
        current = self.parent.conn.execute(
            "SELECT status FROM fspresence WHERE fsobject_id = ? and uuid=?",
            (self.fsobject_id, repo_uuid)
        ).fetchone()
        return HoardFileStatus(current[0]) if current is not None else HoardFileStatus.UNKNOWN

    def set_status(self, repos: Iterable[str], status: HoardFileStatus):
        assert status != HoardFileStatus.MOVE
        self.parent.conn.executemany(
            "INSERT OR REPLACE INTO fspresence (fsobject_id, uuid, status, move_from) VALUES (?, ?, ?, NULL)",
            ((self.fsobject_id, repo_uuid, status.value) for repo_uuid in repos))

    def mark_to_get(self, repos: Iterable[str]):
        self.set_status(repos, HoardFileStatus.GET)

    def mark_to_delete_everywhere(self):
        # remove places that still haven't gotten it
        self.parent.conn.execute(
            "DELETE FROM fspresence "
            "WHERE fsobject_id = ? and status in (?, ?)",
            (self.fsobject_id, HoardFileStatus.GET.value, HoardFileStatus.COPY.value))

        # mark for cleanup places where it is available
        self.parent.conn.execute(
            "UPDATE fspresence SET status = ? "
            "WHERE fsobject_id = ? AND status = ?",
            (HoardFileStatus.CLEANUP.value, self.fsobject_id, HoardFileStatus.AVAILABLE.value))

    def remove_status(self, remote_uuid: str):
        self.parent.conn.execute(
            "DELETE FROM fspresence "
            "WHERE fsobject_id = ? AND uuid=?",
            (self.fsobject_id, remote_uuid))

    def repos_having_status(self, *statuses: HoardFileStatus) -> List[str]:
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE

        return curr.execute(
            f"SELECT uuid FROM fspresence "
            f"WHERE fsobject_id = ? AND status IN ({', '.join('?' * len(statuses))})",
            (self.fsobject_id, *(s.value for s in statuses))).fetchall()

    def get_move_file(self, repo_uuid: str) -> str:
        curr = self.parent.conn.cursor()
        curr.row_factory = FIRST_VALUE

        return curr.execute(
            f"SELECT move_from FROM fspresence "
            f"WHERE fsobject_id = ? AND uuid = ? ",
            (self.fsobject_id, repo_uuid)).fetchone()
