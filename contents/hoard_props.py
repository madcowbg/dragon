import enum
from typing import Dict, List

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

    def get_status(self, repo_uuid: str) -> HoardFileStatus:
        current = self.parent.conn.execute(
            "SELECT status FROM fspresence WHERE fsobject_id = ? and uuid=?",
            (self.fsobject_id, repo_uuid)
        ).fetchone()
        return HoardFileStatus(current[0]) if current is not None else HoardFileStatus.UNKNOWN

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
