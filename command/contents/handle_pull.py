import abc
import enum
import logging
import re
from io import StringIO
from typing import Iterable, Tuple

from command.content_prefs import ContentPrefs
from command.contents.comparisons import DEPRECATED_compare_local_to_hoard
from command.fast_path import FastPosixPath
from command.pathing import HoardPathing
from config import HoardConfig
from contents.hoard import HoardContents
from contents.hoard_props import HoardFileStatus, HoardFileProps
from contents.repo_props import RepoFileStatus
from contents_diff import DiffType, Diff
from util import group_to_dict


class PullIntention(enum.Enum):
    FAIL = None
    ADD_TO_HOARD = "add_to_hoard"
    IGNORE = "ignore"
    CLEANUP = "cleanup"
    ADD_TO_HOARD_AND_CLEANUP = "add_to_hoard_and_cleanup"
    RESTORE_FROM_HOARD = "restore_from_hoard"
    MOVE_IN_HOARD = "move_in_hoard"
    DELETE_FROM_HOARD = "delete_from_hoard"
    ACCEPT_FROM_HOARD = "accept_from_hoard"


class PullPreferences:
    def __init__(
            self, local_uuid: str,
            on_same_file_is_present: PullIntention,
            on_file_added_or_present: PullIntention,
            on_file_is_different_and_modified: PullIntention,
            on_file_is_different_and_added: PullIntention,
            on_file_is_different_but_present: PullIntention,
            on_hoard_only_local_deleted: PullIntention,
            on_hoard_only_local_unknown: PullIntention,
            on_hoard_only_local_moved: PullIntention,
            force_fetch_local_missing):
        self.local_uuid = local_uuid

        self.on_file_added_or_present = on_file_added_or_present

        self.force_fetch_local_missing = force_fetch_local_missing


class Action(abc.ABC):
    @classmethod
    def action_type(cls):
        def snake_case(string):
            return re.sub(r'(?<=[a-z])(?=[A-Z])|[^a-zA-Z]', '_', string).strip('_').lower()

        return snake_case(cls.__name__)

    diff: Diff

    def __init__(self, diff: Diff):
        self.diff = diff

    @property
    def file_being_acted_on(self): return self.diff.hoard_file

    @abc.abstractmethod
    def execute(self, local_uuid: str, content_prefs: ContentPrefs, hoard: HoardContents, out: StringIO) -> None: pass


# todo unify with the case when adding
class AddToHoardAndCleanupNewBehavior(Action):
    def __init__(self, diff: Diff):
        super().__init__(diff)

    def execute(self, local_uuid: str, content_prefs: ContentPrefs, hoard: HoardContents, out: StringIO):
        hoard_props = hoard.fsobjects.add_or_replace_file(self.diff.hoard_file, self.diff.local_props)

        # add status for new repos
        hoard_props.set_status(
            list(content_prefs.repos_to_add(self.diff.hoard_file, self.diff.local_props)),
            HoardFileStatus.GET)
        _incoming__safe_mark_for_cleanup(local_uuid, self.diff, hoard_props, out)


class AddNewFileBehavior(Action):
    def __init__(self, diff: Diff):
        super().__init__(diff)

    def execute(self, local_uuid: str, content_prefs: ContentPrefs, hoard: HoardContents, out: StringIO):
        hoard_props = hoard.fsobjects.add_or_replace_file(self.diff.hoard_file, self.diff.local_props)
        # add status for new repos
        hoard_props.set_status(
            content_prefs.repos_to_add(self.diff.hoard_file, self.diff.local_props),
            HoardFileStatus.GET)
        # set status here
        hoard_props.mark_available(local_uuid)


class AddFileToDeleteBehavior(Action):
    def __init__(self, diff: Diff):
        super().__init__(diff)

    def execute(self, local_uuid: str, content_prefs: ContentPrefs, hoard: HoardContents, out: StringIO):
        hoard_props = hoard.fsobjects.add_or_replace_file(self.diff.hoard_file, self.diff.local_props)

        # set status here to clean up
        hoard_props.mark_for_cleanup([local_uuid])


class ResetLocalAsCurrentBehavior(Action):  # fixme a better name
    def __init__(self, diff: Diff):
        super().__init__(diff)

    def execute(self, local_uuid: str, content_prefs: ContentPrefs, hoard: HoardContents, out: StringIO):
        hoard_file = self.diff.hoard_file
        hoard_props = self.diff.hoard_props
        local_props = self.diff.local_props
        past_available = hoard_props.by_statuses(HoardFileStatus.AVAILABLE, HoardFileStatus.GET, HoardFileStatus.COPY)

        hoard_props = hoard.fsobjects.add_or_replace_file(hoard_file, local_props)
        hoard_props.mark_to_get(past_available)
        hoard_props.mark_available(local_uuid)


def _calculate_local_only(behavior: PullIntention, diff: Diff, out: StringIO) -> Iterable[Action]:
    assert diff.diff_type == DiffType.FileOnlyInLocal

    if behavior == PullIntention.ADD_TO_HOARD_AND_CLEANUP:
        yield AddToHoardAndCleanupNewBehavior(diff)
        out.write(f"INCOMING_TO_HOARD {diff.hoard_file.as_posix()}\n")
    elif behavior == PullIntention.IGNORE:
        logging.info(f"Ignoring local-only file {diff.hoard_file}")
        out.write(f"?{diff.hoard_file.as_posix()}\n")
    elif behavior == PullIntention.ADD_TO_HOARD:
        yield AddNewFileBehavior(diff)
        out.write(f"ADD_NEW_TO_HOARD {diff.hoard_file.as_posix()}\n")
    elif behavior == PullIntention.CLEANUP:
        yield AddFileToDeleteBehavior(diff)
        out.write(f"DELETE {diff.hoard_file.as_posix()}\n")
    else:
        raise ValueError(f"unrecognized on_file_added_or_present={behavior} for {diff.hoard_file}")


def _incoming__safe_mark_for_cleanup(local_uuid: str, diff: Diff, hoard_file: HoardFileProps, out: StringIO):
    assert diff.diff_type in (DiffType.FileIsSame, DiffType.FileOnlyInLocal, DiffType.FileContentsDiffer)

    logging.info(f"safe marking {diff.hoard_file} for cleanup from {local_uuid}")

    repos_to_get_file = hoard_file.by_statuses(HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.AVAILABLE)
    if local_uuid in repos_to_get_file:
        repos_to_get_file.remove(local_uuid)
    if len(repos_to_get_file) > 0:
        logging.info(f"marking {diff.hoard_file} for cleanup from {local_uuid}")
        hoard_file.mark_for_cleanup([local_uuid])
    else:
        logging.error(f"No place will preserve {diff.hoard_file}, will NOT cleanup.")
        hoard_file.mark_available(local_uuid)

        out.write(f"NO_REPO_ACCEPTING {diff.hoard_file.as_posix()}\n")
