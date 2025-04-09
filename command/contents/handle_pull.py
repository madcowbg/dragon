import enum
import logging
from io import StringIO

from alive_progress import alive_it

from command.fast_path import FastPosixPath

from command.content_prefs import ContentPrefs
from command.contents.comparisons import compare_local_to_hoard
from command.pathing import HoardPathing
from config import HoardConfig
from contents.hoard import HoardContents
from contents.hoard_props import HoardFileStatus, HoardFileProps
from contents.repo import RepoContents
from contents.repo_props import RepoFileProps, RepoFileStatus
from contents_diff import FileIsSame, FileOnlyInLocal, FileContentsDiffer, \
    FileOnlyInHoardLocalDeleted, FileOnlyInHoardLocalUnknown, FileOnlyInHoardLocalMoved, DiffType
from util import group_to_dict


class PullBehavior(enum.Enum):
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
            on_same_file_is_present: PullBehavior,
            on_file_added_or_present: PullBehavior,
            on_file_is_different_and_modified: PullBehavior,
            on_file_is_different_and_added: PullBehavior,
            on_file_is_different_but_present: PullBehavior,
            on_hoard_only_local_deleted: PullBehavior,
            on_hoard_only_local_unknown: PullBehavior,
            on_hoard_only_local_moved: PullBehavior):
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


def _handle_file_is_same(
        behavior: PullBehavior, local_uuid: str, content_prefs: ContentPrefs, diff: "FileIsSame", out: StringIO):
    assert behavior in (PullBehavior.ADD_TO_HOARD_AND_CLEANUP, PullBehavior.ADD_TO_HOARD)
    goal_status = diff.hoard_props.get_status(local_uuid)

    if behavior == PullBehavior.ADD_TO_HOARD_AND_CLEANUP:  # todo unify with the case when adding
        logging.info(f"incoming file is already recorded in hoard.")

        already_available = diff.hoard_props.by_status(HoardFileStatus.AVAILABLE)
        # content prefs want to add it, and if not in an already available repo
        repos_to_add = [
            uuid for uuid in content_prefs.repos_to_add(diff.hoard_file, diff.local_props)
            if uuid not in already_available]

        # add status for new repos
        diff.hoard_props.set_status(repos_to_add, HoardFileStatus.GET)
        _incoming__safe_mark_for_cleanup(local_uuid, diff, diff.hoard_props, out)
        out.write(f"-{diff.hoard_file.as_posix()}\n")
    else:
        assert behavior == PullBehavior.ADD_TO_HOARD
        if goal_status in (HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.MOVE, HoardFileStatus.UNKNOWN):
            logging.info(f"mark {diff.hoard_file} as available here!")
            diff.hoard_props.mark_available(local_uuid)
            out.write(f"={diff.hoard_file.as_posix()}\n")
        elif goal_status == HoardFileStatus.CLEANUP:
            logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
            out.write(f"?{diff.hoard_file.as_posix()}\n")
        elif goal_status == HoardFileStatus.AVAILABLE:
            pass
        else:
            raise ValueError(f"unrecognized hoard state for {diff.hoard_file}: {goal_status}")


def _handle_local_only(
        behavior: PullBehavior, local_uuid: str, diff: FileOnlyInLocal, content_prefs: ContentPrefs,
        hoard: HoardContents, out: StringIO):
    if behavior == PullBehavior.ADD_TO_HOARD_AND_CLEANUP:
        props = hoard.fsobjects.add_or_replace_file(diff.hoard_file, diff.local_props)

        # add status for new repos
        props.set_status(
            list(content_prefs.repos_to_add(diff.hoard_file, diff.local_props)),
            HoardFileStatus.GET)

        _incoming__safe_mark_for_cleanup(local_uuid, diff, props, out)
        out.write(f"<+{diff.hoard_file.as_posix()}\n")
    elif behavior == PullBehavior.IGNORE:
        logging.info(f"Ignoring local-only file {diff.hoard_file}")
        out.write(f"?{diff.hoard_file.as_posix()}\n")
    elif behavior == PullBehavior.ADD_TO_HOARD:
        hoard_props = hoard.fsobjects.add_or_replace_file(diff.hoard_file, diff.local_props)

        # add status for new repos
        hoard_props.set_status(
            content_prefs.repos_to_add(diff.hoard_file, diff.local_props),
            HoardFileStatus.GET)

        # set status here
        hoard_props.mark_available(local_uuid)

        out.write(f"+{diff.hoard_file.as_posix()}\n")
    else:
        raise ValueError(
            f"unrecognized on_file_added_or_present={behavior} for {diff.hoard_file}")


def _handle_file_contents_differ(
        behavior: PullBehavior, local_uuid: str, content_prefs: ContentPrefs,
        diff: FileContentsDiffer, hoard: HoardContents, out: StringIO):
    assert behavior in (
        PullBehavior.RESTORE_FROM_HOARD, PullBehavior.ADD_TO_HOARD, PullBehavior.ADD_TO_HOARD_AND_CLEANUP)
    logging.info(f"Behavior for differing file: {behavior}")

    goal_status = diff.hoard_props.get_status(local_uuid)
    if behavior == PullBehavior.ADD_TO_HOARD_AND_CLEANUP:
        logging.info(f"incoming file has different contents.")
        hoard_props = hoard.fsobjects.add_or_replace_file(diff.hoard_file, diff.local_props)

        # add status for new repos
        hoard_props.set_status(
            list(content_prefs.repos_to_add(diff.hoard_file, diff.local_props)),
            HoardFileStatus.GET)

        _incoming__safe_mark_for_cleanup(local_uuid, diff, hoard_props, out)
        out.write(f"u{diff.hoard_file.as_posix()}\n")
    elif behavior == PullBehavior.RESTORE_FROM_HOARD:
        if goal_status == HoardFileStatus.AVAILABLE:  # was backed-up here, get it again
            diff.hoard_props.mark_to_get([local_uuid])
            out.write(f"g{diff.hoard_file.as_posix()}\n")
        elif goal_status in (HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.MOVE):
            logging.info(f"current file is out of date and had been marked to be obtained: {diff.hoard_file}")
            out.write(f"ALREADY_MARKED_{goal_status.value.upper()} {diff.hoard_file.as_posix()}\n")
        elif goal_status == HoardFileStatus.CLEANUP:
            logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
            out.write(f"?{diff.hoard_file.as_posix()}\n")
        elif goal_status == HoardFileStatus.UNKNOWN:
            logging.info(f"Current file is not marked in hoard, but will restore it.")
            diff.hoard_props.mark_to_get([local_uuid])
            out.write(f"RESTORE {diff.hoard_file.as_posix()}\n")
        else:
            raise ValueError(f"Invalid goal status:{goal_status}")
    elif behavior == PullBehavior.ADD_TO_HOARD:
        reset_local_as_current(hoard, local_uuid, diff.hoard_file, diff.hoard_props, diff.local_props)

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


def _handle_hoard_only_with_behavior(
        diff: FileOnlyInHoardLocalDeleted | FileOnlyInHoardLocalUnknown | FileOnlyInHoardLocalMoved,
        behavior: PullBehavior,
        local_uuid: str,
        out: StringIO):

    assert behavior in (
        PullBehavior.IGNORE, PullBehavior.RESTORE_FROM_HOARD, PullBehavior.ACCEPT_FROM_HOARD,
        PullBehavior.DELETE_FROM_HOARD)
    goal_status = diff.hoard_props.get_status(local_uuid)
    if behavior == PullBehavior.IGNORE:
        logging.info(f"skipping file {diff.hoard_file} not in local.")

        if goal_status not in (HoardFileStatus.CLEANUP, HoardFileStatus.UNKNOWN):
            logging.error(f"File in hoard only, but status in repo is not {HoardFileStatus.CLEANUP}")
            out.write(f"E{diff.hoard_file.as_posix()}\n")

        diff.hoard_props.remove_status(local_uuid)
        if goal_status != HoardFileStatus.UNKNOWN:
            out.write(f"IGNORED {diff.hoard_file.as_posix()}\n")
    elif behavior == PullBehavior.RESTORE_FROM_HOARD:
        if goal_status == HoardFileStatus.AVAILABLE:  # was backed-up here, get it again
            props = diff.hoard_props
            props.mark_to_get([local_uuid])

            out.write(f"g{diff.hoard_file.as_posix()}\n")
        elif goal_status == HoardFileStatus.CLEANUP:  # file already deleted
            diff.hoard_props.remove_status(local_uuid)
        elif goal_status in (HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.MOVE):
            pass
        elif goal_status == HoardFileStatus.UNKNOWN:
            logging.info("File not recognized by this backup, skipping")
        else:
            raise NotImplementedError(f"Unrecognized goal status {goal_status}")
    elif behavior == PullBehavior.DELETE_FROM_HOARD:
        if goal_status == HoardFileStatus.UNKNOWN:
            logging.info(f"file not related to repo, skipping!")
        else:  # file was here, is no longer
            logging.info(f"deleting file {diff.hoard_file} because it was deleted in local")

            diff.hoard_props.mark_to_delete_everywhere()
            diff.hoard_props.remove_status(local_uuid)
            out.write(f"-{diff.hoard_file.as_posix()}\n")
    elif behavior == PullBehavior.ACCEPT_FROM_HOARD:
        if goal_status == HoardFileStatus.CLEANUP:  # file already deleted
            diff.hoard_props.remove_status(local_uuid)
            out.write(f"ACCEPT_CLEANUP {diff.hoard_file.as_posix()}\n")
        else:
            logging.info(f"Ignoring missing file {diff.hoard_file}, desired state={goal_status}")
    else:
        raise ValueError(f"Invalid behavior={behavior}")


def _handle_hoard_only_moved(
        behavior: PullBehavior, local_uuid: str, diff: FileOnlyInHoardLocalMoved, pathing: HoardPathing,
        hoard: HoardContents, config: HoardConfig,
        out: StringIO):
    if behavior == PullBehavior.MOVE_IN_HOARD:
        goal_status = diff.hoard_props.get_status(local_uuid)
        if goal_status == HoardFileStatus.AVAILABLE:
            hoard_new_path = pathing.in_local(
                FastPosixPath(diff.local_props.last_related_fullpath), local_uuid) \
                .at_hoard().as_pure_path
            hoard_new_path_props = hoard.fsobjects[hoard_new_path]
            assert isinstance(hoard_new_path_props, HoardFileProps)
            assert hoard_new_path_props.fasthash == diff.hoard_props.fasthash and \
                   hoard_new_path_props.fasthash == diff.local_props.fasthash

            _move_locally(config, local_uuid, diff, hoard_new_path.as_posix(), hoard_new_path_props, out)
        elif goal_status == HoardFileStatus.UNKNOWN:
            logging.info(f"File {diff.hoard_file} is unknown, can't move!")
        else:
            out.write(f"ERROR_ON_MOVE bad current status = {goal_status}, won't move.\n")
    else:
        assert behavior in (
            PullBehavior.IGNORE, PullBehavior.RESTORE_FROM_HOARD, PullBehavior.ACCEPT_FROM_HOARD,
            PullBehavior.DELETE_FROM_HOARD)
        _handle_hoard_only_with_behavior(diff, behavior, local_uuid, out)


def _move_locally(
        config: HoardConfig, local_uuid: str, diff: FileOnlyInHoardLocalMoved,
        hoard_new_path: str, hoard_new_path_props: HoardFileProps, out: StringIO):
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
    all_diffs = [diff async for diff in
                 compare_local_to_hoard(current_contents, hoard_contents, pathing, progress_tool)]
    diffs_by_type = group_to_dict(all_diffs, key=lambda diff: type(diff))

    for dt, diffs in diffs_by_type.items():
        logging.debug(f"# diffs of class {dt}={len(diffs)}")

    for diff in diffs_by_type.pop(FileIsSame, []):
        assert diff.diff_type == DiffType.FileIsSame
        _handle_file_is_same(
            preferences.on_same_file_is_present, preferences.local_uuid, preferences.content_prefs, diff, out)

    for diff in diffs_by_type.pop(FileOnlyInLocal, []):
        assert diff.diff_type == DiffType.FileOnlyInLocal
        _handle_local_only(
            preferences.on_file_added_or_present, preferences.local_uuid, diff, preferences.content_prefs,
            hoard_contents, out)

    for diff in diffs_by_type.pop(FileContentsDiffer, []):
        assert diff.diff_type == DiffType.FileContentsDiffer
        if diff.local_props.last_status == RepoFileStatus.PRESENT:
            behavior = preferences.on_file_is_different_but_present
        elif diff.local_props.last_status == RepoFileStatus.ADDED:
            behavior = preferences.on_file_is_different_and_added
        elif diff.local_props.last_status == RepoFileStatus.MODIFIED:
            behavior = preferences.on_file_is_different_and_modified
        else:
            raise ValueError(f"Unallowed local_props.last_status={diff.local_props.last_status}")

        _handle_file_contents_differ(
            behavior, preferences.local_uuid, preferences.content_prefs, diff, hoard_contents, out)

    for diff in diffs_by_type.pop(FileOnlyInHoardLocalDeleted, []):
        assert diff.diff_type == DiffType.FileOnlyInHoardLocalDeleted
        if diff.diff_type == DiffType.FileOnlyInHoardLocalMoved:
            behavior = preferences.on_hoard_only_local_moved
        elif diff.diff_type == DiffType.FileOnlyInHoardLocalUnknown:
            behavior = preferences.on_hoard_only_local_unknown
        elif diff.diff_type == DiffType.FileOnlyInHoardLocalDeleted:
            behavior = preferences.on_hoard_only_local_deleted
        else:
            raise ValueError(f"Invalid diff tyoe {type(diff)}")

        _handle_hoard_only_with_behavior(diff, behavior, preferences.local_uuid, out)

    for diff in diffs_by_type.pop(FileOnlyInHoardLocalUnknown, []):
        assert diff.diff_type == DiffType.FileOnlyInHoardLocalUnknown
        if diff.diff_type == DiffType.FileOnlyInHoardLocalMoved:
            behavior = preferences.on_hoard_only_local_moved
        elif diff.diff_type == DiffType.FileOnlyInHoardLocalUnknown:
            behavior = preferences.on_hoard_only_local_unknown
        elif diff.diff_type == DiffType.FileOnlyInHoardLocalDeleted:
            behavior = preferences.on_hoard_only_local_deleted
        else:
            raise ValueError(f"Invalid diff tyoe {type(diff)}")

        _handle_hoard_only_with_behavior(diff, behavior, preferences.local_uuid, out)

    for diff in diffs_by_type.pop(FileOnlyInHoardLocalMoved, []):
        assert diff.diff_type == DiffType.FileOnlyInHoardLocalMoved
        _handle_hoard_only_moved(
            preferences.on_hoard_only_local_moved, preferences.local_uuid, diff, pathing, hoard_contents, config, out)
    for unrecognized_type, unrecognized_diffs in diffs_by_type.items():
        logging.error(f"Unrecognized {len(unrecognized_diffs)} of type: {unrecognized_type}")
    if len(diffs_by_type) > 0:
        raise ValueError(f"Unrecognized diffs of types {list(diffs_by_type.keys())}")


def _incoming__safe_mark_for_cleanup(
        local_uuid: str,
        diff: FileOnlyInLocal | FileContentsDiffer | FileIsSame,
        hoard_file: HoardFileProps, out: StringIO):
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


def reset_local_as_current(
        hoard: HoardContents, remote_uuid: str, hoard_file: FastPosixPath, hoard_props: HoardFileProps,
        local_props: RepoFileProps):
    past_available = hoard_props.by_statuses(HoardFileStatus.AVAILABLE, HoardFileStatus.GET, HoardFileStatus.COPY)

    hoard_props = hoard.fsobjects.add_or_replace_file(hoard_file, local_props)
    hoard_props.mark_to_get(past_available)
    hoard_props.mark_available(remote_uuid)
