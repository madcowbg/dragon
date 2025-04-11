import abc
import enum
import logging
import re
from io import StringIO
from typing import Iterable, Tuple

from alive_progress import alive_it

from command.content_prefs import ContentPrefs
from command.contents.comparisons import compare_local_to_hoard
from command.fast_path import FastPosixPath
from command.pathing import HoardPathing
from config import HoardConfig
from contents.hoard import HoardContents
from contents.hoard_props import HoardFileStatus, HoardFileProps
from contents.repo import RepoContents
from contents.repo_props import RepoFileStatus
from contents_diff import DiffType, Diff
from util import group_to_dict


class PullIntention(enum.Enum):
    FAIL = None
    ADD_TO_HOARD = "add_to_hoard"
    IGNORE = "ignore"
    ADD_TO_HOARD_AND_CLEANUP = "add_to_hoard_and_cleanup"
    RESTORE_FROM_HOARD = "restore_from_hoard"
    MOVE_IN_HOARD = "move_in_hoard"
    DELETE_FROM_HOARD = "delete_from_hoard"
    ACCEPT_FROM_HOARD = "accept_from_hoard"


class PullPreferences:
    def __init__(
            self, local_uuid: str, content_prefs: ContentPrefs,
            on_same_file_is_present: PullIntention,
            on_file_added_or_present: PullIntention,
            on_file_is_different_and_modified: PullIntention,
            on_file_is_different_and_added: PullIntention,
            on_file_is_different_but_present: PullIntention,
            on_hoard_only_local_deleted: PullIntention,
            on_hoard_only_local_unknown: PullIntention,
            on_hoard_only_local_moved: PullIntention):
        self.local_uuid = local_uuid
        self.content_prefs = content_prefs

        self.on_same_file_is_present = on_same_file_is_present
        self.on_file_added_or_present = on_file_added_or_present

        self.on_file_is_different_and_modified = on_file_is_different_and_modified
        self.on_file_is_different_and_added = on_file_is_different_and_added
        self.on_file_is_different_but_present = on_file_is_different_but_present

        self.on_hoard_only_local_deleted = on_hoard_only_local_deleted
        self.on_hoard_only_local_unknown = on_hoard_only_local_unknown
        self.on_hoard_only_local_moved = on_hoard_only_local_moved


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

class MarkIsAvailableBehavior(Action):
    def __init__(self, diff: Diff):
        super().__init__(diff)

    def execute(self, local_uuid: str, content_prefs: ContentPrefs, hoard: HoardContents, out: StringIO):
        self.diff.hoard_props.mark_available(local_uuid)


# todo unify with the case when adding
class AddToHoardAndCleanupSameBehavior(Action):
    def __init__(self, diff: Diff):
        super().__init__(diff)

    def execute(self, local_uuid: str, content_prefs: ContentPrefs, hoard: HoardContents, out: StringIO):
        already_available = self.diff.hoard_props.by_status(HoardFileStatus.AVAILABLE)

        # content prefs want to add it, and if not in an already available repo
        repos_to_add = [
            uuid for uuid in content_prefs.repos_to_add(self.diff.hoard_file, self.diff.local_props)
            if uuid not in already_available]

        # add status for new repos
        self.diff.hoard_props.set_status(repos_to_add, HoardFileStatus.GET)
        _incoming__safe_mark_for_cleanup(local_uuid, self.diff, self.diff.hoard_props, out)


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


class MarkToGetBehavior(Action):
    def __init__(self, diff: Diff):
        super().__init__(diff)

    def execute(self, local_uuid: str, content_prefs: ContentPrefs, hoard: HoardContents, out: StringIO):
        self.diff.hoard_props.mark_to_get([local_uuid])


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


class RemoveLocalStatusBehavior(Action):
    def __init__(self, diff: Diff):
        super().__init__(diff)

    def execute(self, local_uuid: str, content_prefs: ContentPrefs, hoard: HoardContents, out: StringIO):
        self.diff.hoard_props.remove_status(local_uuid)


class DeleteFileFromHoardBehavior(Action):
    def __init__(self, diff: Diff):
        super().__init__(diff)

    def execute(self, local_uuid: str, content_prefs: ContentPrefs, hoard: HoardContents, out: StringIO):
        self.diff.hoard_props.mark_to_delete_everywhere()
        self.diff.hoard_props.remove_status(local_uuid)


class MoveFileBehavior(Action):
    def __init__(self, diff: Diff, config: HoardConfig, pathing: HoardPathing):
        super().__init__(diff)
        self.config = config
        self.pathing = pathing

    def execute(self, local_uuid: str, content_prefs: ContentPrefs, hoard: HoardContents, out: StringIO):
        hoard_new_path = self.pathing.in_local(
            FastPosixPath(self.diff.local_props.last_related_fullpath), local_uuid) \
            .at_hoard().as_pure_path
        hoard_new_path_props = hoard.fsobjects[hoard_new_path]
        assert isinstance(hoard_new_path_props, HoardFileProps)
        assert hoard_new_path_props.fasthash == self.diff.hoard_props.fasthash and \
               hoard_new_path_props.fasthash == self.diff.local_props.fasthash
        _move_locally(self.config, local_uuid, self.diff, hoard_new_path.as_posix(), hoard_new_path_props, out)


def _calculate_file_is_same(
        behavior: PullIntention, local_uuid: str, diff: Diff, out: StringIO) -> Iterable[Action]:
    assert diff.diff_type == DiffType.FileIsSame

    assert behavior in (PullIntention.ADD_TO_HOARD_AND_CLEANUP, PullIntention.ADD_TO_HOARD)

    if behavior == PullIntention.ADD_TO_HOARD_AND_CLEANUP:
        logging.info(f"incoming file is already recorded in hoard.")

        yield AddToHoardAndCleanupSameBehavior(diff)
        out.write(f"ADD_TO_HOARD_AND_CLEANUP {diff.hoard_file.as_posix()}\n")
    else:
        assert behavior == PullIntention.ADD_TO_HOARD
        goal_status = diff.hoard_props.get_status(local_uuid)
        if goal_status in (HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.MOVE, HoardFileStatus.UNKNOWN):
            logging.info(f"mark {diff.hoard_file} as available here!")
            yield MarkIsAvailableBehavior(diff)
            out.write(f"={diff.hoard_file.as_posix()}\n")
        elif goal_status == HoardFileStatus.CLEANUP:
            logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
            out.write(f"?{diff.hoard_file.as_posix()}\n")
        elif goal_status == HoardFileStatus.AVAILABLE:
            pass
        else:
            raise ValueError(f"unrecognized hoard state for {diff.hoard_file}: {goal_status}")


def _calculate_local_only(behavior: PullIntention, diff: Diff, out: StringIO) -> Iterable[Action]:
    assert diff.diff_type == DiffType.FileOnlyInLocal

    if behavior == PullIntention.ADD_TO_HOARD_AND_CLEANUP:
        yield AddToHoardAndCleanupNewBehavior(diff)
        out.write(f"<+{diff.hoard_file.as_posix()}\n")
    elif behavior == PullIntention.IGNORE:
        logging.info(f"Ignoring local-only file {diff.hoard_file}")
        out.write(f"?{diff.hoard_file.as_posix()}\n")
    elif behavior == PullIntention.ADD_TO_HOARD:
        yield AddNewFileBehavior(diff)
        out.write(f"ADD_NEW_TO_HOARD {diff.hoard_file.as_posix()}\n")
    else:
        raise ValueError(f"unrecognized on_file_added_or_present={behavior} for {diff.hoard_file}")


def _calculate_file_contents_differ(
        behavior: PullIntention, local_uuid: str, diff: Diff, out: StringIO) -> Iterable[Action]:
    assert diff.diff_type == DiffType.FileContentsDiffer

    assert behavior in (
        PullIntention.RESTORE_FROM_HOARD, PullIntention.ADD_TO_HOARD, PullIntention.ADD_TO_HOARD_AND_CLEANUP)
    logging.info(f"Behavior for differing file: {behavior}")

    goal_status = diff.hoard_props.get_status(local_uuid)
    if behavior == PullIntention.ADD_TO_HOARD_AND_CLEANUP:
        yield AddToHoardAndCleanupNewBehavior(diff)
        out.write(f"u{diff.hoard_file.as_posix()}\n")
    elif behavior == PullIntention.RESTORE_FROM_HOARD:
        if goal_status == HoardFileStatus.AVAILABLE:  # was backed-up here, get it again
            yield MarkToGetBehavior(diff)
            out.write(f"g{diff.hoard_file.as_posix()}\n")
        elif goal_status in (HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.MOVE):
            logging.info(f"current file is out of date and had been marked to be obtained: {diff.hoard_file}")
            out.write(f"ALREADY_MARKED_{goal_status.value.upper()} {diff.hoard_file.as_posix()}\n")
        elif goal_status == HoardFileStatus.CLEANUP:
            logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
            out.write(f"?{diff.hoard_file.as_posix()}\n")
        elif goal_status == HoardFileStatus.UNKNOWN:
            logging.info(f"Current file is not marked in hoard, but will restore it.")
            yield MarkToGetBehavior(diff)
            out.write(f"RESTORE {diff.hoard_file.as_posix()}\n")
        else:
            raise ValueError(f"Invalid goal status:{goal_status}")
    elif behavior == PullIntention.ADD_TO_HOARD:
        yield ResetLocalAsCurrentBehavior(diff)

        if goal_status == HoardFileStatus.AVAILABLE:
            # file was changed in-place, but is different now
            out.write(f"u{diff.hoard_file.as_posix()}\n")
        elif goal_status == HoardFileStatus.UNKNOWN:  # fixme this should disappear if we track repository contents
            # file is added as different then what is in the hoard
            out.write(f"RESETTING {diff.hoard_file.as_posix()}\n")
        elif goal_status in (HoardFileStatus.CLEANUP, HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.MOVE):
            out.write(f"RESETTING {diff.hoard_file.as_posix()}\n")
        else:
            raise ValueError(f"Invalid goal status:{goal_status}")
    else:
        raise ValueError(f"Invalid behavior={behavior}")


def _calculate_hoard_only_with_behavior(diff: Diff, behavior: PullIntention, local_uuid: str, out: StringIO) -> \
        Iterable[Action]:
    assert diff.diff_type in (
        DiffType.FileOnlyInHoardLocalDeleted, DiffType.FileOnlyInHoardLocalUnknown, DiffType.FileOnlyInHoardLocalMoved)

    assert behavior in (
        PullIntention.IGNORE, PullIntention.RESTORE_FROM_HOARD, PullIntention.ACCEPT_FROM_HOARD,
        PullIntention.DELETE_FROM_HOARD)
    goal_status = diff.hoard_props.get_status(local_uuid)
    if behavior == PullIntention.IGNORE:
        logging.info(f"skipping file {diff.hoard_file} not in local.")

        if goal_status == HoardFileStatus.CLEANUP:
            yield RemoveLocalStatusBehavior(diff)

            out.write(f"IGNORED {diff.hoard_file.as_posix()}\n")
        elif goal_status == HoardFileStatus.UNKNOWN:
            pass
        else:
            logging.error(f"File in hoard only, but status in repo is not {HoardFileStatus.CLEANUP}")
            out.write(f"E{diff.hoard_file.as_posix()}\n")
            yield RemoveLocalStatusBehavior(diff)
            out.write(f"IGNORED {diff.hoard_file.as_posix()}\n")

    elif behavior == PullIntention.RESTORE_FROM_HOARD:
        if goal_status == HoardFileStatus.AVAILABLE:  # was backed-up here, get it again
            yield MarkToGetBehavior(diff)

            out.write(f"g{diff.hoard_file.as_posix()}\n")
        elif goal_status == HoardFileStatus.CLEANUP:  # file already deleted
            yield RemoveLocalStatusBehavior(diff)
        elif goal_status in (HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.MOVE):
            pass
        elif goal_status == HoardFileStatus.UNKNOWN:
            logging.info("File not recognized by this backup, skipping")
        else:
            raise NotImplementedError(f"Unrecognized goal status {goal_status}")
    elif behavior == PullIntention.DELETE_FROM_HOARD:
        if goal_status == HoardFileStatus.UNKNOWN:
            logging.info(f"file not related to repo, skipping!")
        else:  # file was here, is no longer
            logging.info(f"deleting file {diff.hoard_file} because it was deleted in local")

            yield DeleteFileFromHoardBehavior(diff)
            out.write(f"DELETE_FROM_HOARD {diff.hoard_file.as_posix()}\n")
    elif behavior == PullIntention.ACCEPT_FROM_HOARD:
        if goal_status == HoardFileStatus.CLEANUP:  # file already deleted
            yield RemoveLocalStatusBehavior(diff)
            out.write(f"ACCEPT_CLEANUP {diff.hoard_file.as_posix()}\n")
        else:
            logging.info(f"Ignoring missing file {diff.hoard_file}, desired state={goal_status}")
    else:
        raise ValueError(f"Invalid behavior={behavior}")


def _calculate_hoard_only_moved(
        behavior: PullIntention, local_uuid: str, diff: Diff, pathing: HoardPathing, config: HoardConfig,
        out: StringIO) -> Iterable[Action]:
    assert diff.diff_type == DiffType.FileOnlyInHoardLocalMoved
    if behavior == PullIntention.MOVE_IN_HOARD:
        goal_status = diff.hoard_props.get_status(local_uuid)
        if goal_status == HoardFileStatus.UNKNOWN:
            logging.info(f"File {diff.hoard_file} is unknown, can't move!")
        elif goal_status == HoardFileStatus.AVAILABLE:
            yield MoveFileBehavior(diff, config, pathing)
        else:
            out.write(f"ERROR_ON_MOVE bad current status = {goal_status}, won't move.\n")
            yield DeleteFileFromHoardBehavior(diff)
            out.write(f"DELETE_FROM_HOARD {diff.hoard_file.as_posix()}\n")
    else:
        assert behavior in (
            PullIntention.IGNORE, PullIntention.RESTORE_FROM_HOARD, PullIntention.ACCEPT_FROM_HOARD,
            PullIntention.DELETE_FROM_HOARD)
        yield from _calculate_hoard_only_with_behavior(diff, behavior, local_uuid, out)


def _move_locally(
        config: HoardConfig, local_uuid: str, diff: Diff,
        hoard_new_path: str, hoard_new_path_props: HoardFileProps, out: StringIO):
    assert diff.diff_type == DiffType.FileOnlyInHoardLocalMoved
    logging.info(f"Marking moving of {diff.hoard_file} to {hoard_new_path}.")
    named_statuses = sorted([
        (other_uuid, old_status, config.remotes[other_uuid].name)
        for other_uuid, old_status in diff.hoard_props.presence.items()], key=lambda x: x[2])
    for other_uuid, old_status, name in named_statuses:
        if other_uuid != local_uuid and old_status == HoardFileStatus.AVAILABLE:
            logging.info(
                f"File {diff.hoard_file} is available in {other_uuid}, will mark {hoard_new_path} as move!")

            hoard_new_path_props.set_to_move_from_local(other_uuid, diff.hoard_file.as_posix())
            out.write(f"MOVE {name}: {diff.hoard_file.as_posix()} to {hoard_new_path}\n")

    # mark to clear from old locations
    diff.hoard_props.mark_for_cleanup(diff.hoard_props.presence.keys())
    # mark already cleared from here
    diff.hoard_props.remove_status(local_uuid)
    out.write(f"CLEANUP_MOVED {diff.hoard_file.as_posix()}\n")


async def pull_repo_contents_to_hoard(
        hoard_contents: HoardContents, pathing: HoardPathing, config: HoardConfig, current_contents: RepoContents,
        preferences: PullPreferences, out: StringIO, progress_tool=alive_it):
    resolutions = await resolution_to_match_repo_and_hoard(
        current_contents, hoard_contents, pathing, preferences, progress_tool)

    for action in calculate_actions(preferences, resolutions, pathing, config, out):
        action.execute(preferences.local_uuid, preferences.content_prefs, hoard_contents, out)

def calculate_actions(
        preferences: PullPreferences, resolutions: Iterable[Tuple[Diff, PullIntention]],
        pathing: HoardPathing, config: HoardConfig,
        out: StringIO) -> Iterable[Action]:
    for diff, behavior in resolutions:
        if diff.diff_type == DiffType.FileIsSame:
            yield from _calculate_file_is_same(behavior, preferences.local_uuid, diff, out)
        elif diff.diff_type == DiffType.FileOnlyInLocal:
            yield from _calculate_local_only(behavior, diff, out)
        elif diff.diff_type == DiffType.FileContentsDiffer:
            yield from _calculate_file_contents_differ(behavior, preferences.local_uuid, diff, out)
        elif diff.diff_type == DiffType.FileOnlyInHoardLocalDeleted:
            yield from _calculate_hoard_only_with_behavior(diff, behavior, preferences.local_uuid, out)
        elif diff.diff_type == DiffType.FileOnlyInHoardLocalUnknown:
            yield from _calculate_hoard_only_with_behavior(diff, behavior, preferences.local_uuid, out)
        elif diff.diff_type == DiffType.FileOnlyInHoardLocalMoved:
            yield from _calculate_hoard_only_moved(behavior, preferences.local_uuid, diff, pathing, config, out)
        else:
            raise ValueError(f"Invalid diff type {diff.diff_type}")


async def resolution_to_match_repo_and_hoard(current_contents, hoard_contents, pathing, preferences, progress_tool):
    all_diffs = [
        diff async for diff in compare_local_to_hoard(current_contents, hoard_contents, pathing, progress_tool)]
    return compute_resolutions(all_diffs, preferences)


def compute_resolutions(all_diffs: Iterable[Diff], preferences: PullPreferences) -> Iterable[
    Tuple[Diff, PullIntention]]:
    diffs_by_type = group_to_dict(all_diffs, key=lambda diff: diff.diff_type)
    for dt, diffs in diffs_by_type.items():
        logging.debug(f"# diffs of class {dt}={len(diffs)}")

    for diff in diffs_by_type.pop(DiffType.FileIsSame, []):
        assert diff.diff_type == DiffType.FileIsSame
        yield diff, preferences.on_same_file_is_present

    for diff in diffs_by_type.pop(DiffType.FileOnlyInLocal, []):
        assert diff.diff_type == DiffType.FileOnlyInLocal
        yield diff, preferences.on_file_added_or_present

    for diff in diffs_by_type.pop(DiffType.FileContentsDiffer, []):
        assert diff.diff_type == DiffType.FileContentsDiffer
        if diff.local_props.last_status == RepoFileStatus.PRESENT:
            behavior = preferences.on_file_is_different_but_present
        elif diff.local_props.last_status == RepoFileStatus.ADDED:
            behavior = preferences.on_file_is_different_and_added
        elif diff.local_props.last_status == RepoFileStatus.MODIFIED:
            behavior = preferences.on_file_is_different_and_modified
        else:
            raise ValueError(f"Unallowed local_props.last_status={diff.local_props.last_status}")

        yield diff, behavior

    for diff in diffs_by_type.pop(DiffType.FileOnlyInHoardLocalDeleted, []):
        assert diff.diff_type == DiffType.FileOnlyInHoardLocalDeleted
        if diff.diff_type == DiffType.FileOnlyInHoardLocalMoved:
            behavior = preferences.on_hoard_only_local_moved
        elif diff.diff_type == DiffType.FileOnlyInHoardLocalUnknown:
            behavior = preferences.on_hoard_only_local_unknown
        elif diff.diff_type == DiffType.FileOnlyInHoardLocalDeleted:
            behavior = preferences.on_hoard_only_local_deleted
        else:
            raise ValueError(f"Invalid diff tyoe {type(diff)}")
        yield diff, behavior

    for diff in diffs_by_type.pop(DiffType.FileOnlyInHoardLocalUnknown, []):
        assert diff.diff_type == DiffType.FileOnlyInHoardLocalUnknown
        if diff.diff_type == DiffType.FileOnlyInHoardLocalMoved:
            behavior = preferences.on_hoard_only_local_moved
        elif diff.diff_type == DiffType.FileOnlyInHoardLocalUnknown:
            behavior = preferences.on_hoard_only_local_unknown
        elif diff.diff_type == DiffType.FileOnlyInHoardLocalDeleted:
            behavior = preferences.on_hoard_only_local_deleted
        else:
            raise ValueError(f"Invalid diff type {type(diff)}")

        yield diff, behavior
    for diff in diffs_by_type.pop(DiffType.FileOnlyInHoardLocalMoved, []):
        assert diff.diff_type == DiffType.FileOnlyInHoardLocalMoved

        yield diff, preferences.on_hoard_only_local_moved
    for unrecognized_type, unrecognized_diffs in diffs_by_type.items():
        logging.error(f"Unrecognized {len(unrecognized_diffs)} of type: {unrecognized_type}")

    if len(diffs_by_type) > 0:
        raise ValueError(f"Unrecognized diffs of types {list(diffs_by_type.keys())}")


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

        out.write(f"~{diff.hoard_file.as_posix()}\n")
