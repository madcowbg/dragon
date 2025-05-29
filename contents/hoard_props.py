import enum
from functools import cached_property
from typing import Dict, List

from command.fast_path import FastPosixPath
from lmdb_storage.operations.fast_association import FastAssociation
from lmdb_storage.tree_operations import get_child
from lmdb_storage.tree_object import StoredObject, ObjectID, MaybeObjectID


class HoardFileStatus(enum.Enum):
    AVAILABLE = "available"
    GET = "get"
    CLEANUP = "cleanup"
    COPY = "copy"  # fixme remove as copying is kinda the same as getting, and "move" should capture the real need
    MOVE = "move"
    UNKNOWN = "UNKNOWN"


def compute_status(
        hoard_sub_id: ObjectID | None, sub_id_in_remote_current: ObjectID | None,
        sub_id_in_remote_desired: ObjectID | None) -> HoardFileStatus | None:
    if hoard_sub_id is None:  # is a deleted file
        assert sub_id_in_remote_desired is None, "Can't have a desired file that is not in the hoard"
        if sub_id_in_remote_current is not None:
            return HoardFileStatus.CLEANUP
        return None
    elif sub_id_in_remote_current is not None:  # file is in current
        if sub_id_in_remote_desired is not None:
            if sub_id_in_remote_desired == sub_id_in_remote_current:
                return HoardFileStatus.AVAILABLE
            else:
                return HoardFileStatus.GET
        else:
            return HoardFileStatus.CLEANUP

    else:
        if sub_id_in_remote_desired is not None:
            return HoardFileStatus.GET
        else:
            return None  # file not desired and not current


class HoardFileProps:
    def __init__(self, parent: "HoardContents", path: FastPosixPath, size: int, fasthash: str, *, by_root: FastAssociation[StoredObject] | None=None, file_id: MaybeObjectID = None):
        self.parent = parent
        self._path = path
        self._maybe_by_root = by_root
        self._maybe_file_id = file_id

        self.size = size
        self.fasthash = fasthash

    @cached_property  # fixme make dynamic
    def remote_names(self) -> Dict[str, str]:
        return dict((r.uuid, r.name) for r in self.parent.hoard_config.remotes.all())

    @cached_property  # fixme make dynamic
    def remote_roots(self):
        roots = self.parent.env.roots(write=False)
        return sorted(
            [(uuid, roots[uuid].current, roots[uuid].desired) for uuid in self.parent.config.remote_uuids()],
            key=lambda ucd: self.remote_names[ucd[0]])

    @cached_property  # fixme make dynamic
    def hoard_root_id(self):
        roots = self.parent.env.roots(write=False)
        return roots["HOARD"].desired

    @property
    def available_at(self) -> List[str]:
        return [uuid for uuid, status in self.presence.items() if status == HoardFileStatus.AVAILABLE]

    @property
    def presence(self) -> Dict[str, HoardFileStatus]:
        if self._maybe_by_root is not None: # fast path
            hoard_id = self._maybe_by_root.get_if_present("HOARD")
            presents = dict()
            for key, value in self._maybe_by_root.keyed_items():
                if key.endswith("HOARD"):
                    continue
                if key.startswith("current@"):
                    uuid = key[8:]
                    if uuid not in presents:
                        presents[uuid] = [value, None]
                    else:
                        presents[uuid][0] = value
                elif key.startswith("desired@"):
                    uuid = key[8:]
                    if uuid not in presents:
                        presents[uuid] = [None, value]
                    else:
                        presents[uuid][1] = value

            result = dict()
            for uuid, (current_id, desired_id) in presents.items():
                status = compute_status(hoard_id, current_id, desired_id)
                if status is not None:
                    result[uuid] = status
            return result

        result = dict()
        with self.parent.env.objects(write=False) as objects:
            hoard_id = get_child(objects, self._path._rem, self.hoard_root_id)
            for uuid, current_root_id, desired_root_id in self.remote_roots:
                current_id = get_child(objects, self._path._rem, current_root_id)
                desired_id = get_child(objects, self._path._rem, desired_root_id)

                computed_status = compute_status(hoard_id, current_id, desired_id)
                if computed_status is not None:
                    result[uuid] = computed_status
        return result

    def by_status(self, selected_status: HoardFileStatus) -> List[str]:
        return [uuid for uuid, status in self.presence.items() if status == selected_status]

    def by_statuses(self, *selected_statuses: HoardFileStatus) -> List[str]:
        return [uuid for uuid, status in self.presence.items() if status in selected_statuses]

    def get_status(self, repo_uuid: str) -> HoardFileStatus:
        return self.presence.get(repo_uuid, HoardFileStatus.UNKNOWN)

    def repos_having_status(self, *statuses: HoardFileStatus) -> List[str]:
        return self.by_statuses(*statuses)

    def get_move_file(self, repo_uuid: str) -> str:
        raise NotImplementedError()
