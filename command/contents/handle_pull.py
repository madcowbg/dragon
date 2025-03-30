import enum
import logging
from io import StringIO
from typing import List

from command.content_prefs import ContentPrefs
from command.contents.comparisons import compare_local_to_hoard
from command.pathing import HoardPathing
from config import CaveType
from contents.hoard import HoardContents
from contents.hoard_props import HoardFileStatus, HoardFileProps
from contents.repo import RepoContents
from contents.repo_props import RepoFileProps
from contents_diff import FileIsSame, FileOnlyInLocal, FileContentsDiffer, \
    FileOnlyInHoardLocalDeleted, FileOnlyInHoardLocalUnknown, FileOnlyInHoardLocalMoved, DirMissingInHoard, \
    DirMissingInLocal, DirIsSame
from util import group_to_dict


class PullBehavior(enum.Enum):
    ADD = "add"
    IGNORE = "ignore"
    MOVE_AND_CLEANUP = "move_and_cleanup"


class PullPreferences:
    def __init__(
            self, local_uuid: str, content_prefs: ContentPrefs,
            assume_current: bool, force_fetch_local_missing: bool, deprecated_type: CaveType,
            on_same_file_is_present: PullBehavior, on_file_added_or_present: PullBehavior):
        self.local_uuid = local_uuid
        self.content_prefs = content_prefs
        self.deprecated_type = deprecated_type

        self.assume_current = assume_current
        self.force_fetch_local_missing = force_fetch_local_missing

        self.on_same_file_is_present = on_same_file_is_present
        self.on_file_added_or_present = on_file_added_or_present


def _handle_file_is_same(preferences: PullPreferences, diff: "FileIsSame", out: StringIO):
    goal_status = diff.hoard_props.get_status(preferences.local_uuid)

    if preferences.on_same_file_is_present == PullBehavior.MOVE_AND_CLEANUP:  # todo unify with the case when adding
        logging.info(f"incoming file is already recorded in hoard.")

        already_available = diff.hoard_props.by_status(HoardFileStatus.AVAILABLE)
        # content prefs want to add it, and if not in an already available repo
        repos_to_add = [
            uuid for uuid in preferences.content_prefs.repos_to_add(diff.hoard_file, diff.local_props)
            if uuid not in already_available]

        # add status for new repos
        diff.hoard_props.set_status(repos_to_add, HoardFileStatus.GET)
        _incoming__safe_mark_for_cleanup(preferences, diff, diff.hoard_props, out)
        out.write(f"-{diff.hoard_file}\n")
    else:
        assert preferences.on_same_file_is_present == PullBehavior.ADD
        if goal_status in (HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.MOVE, HoardFileStatus.UNKNOWN):
            logging.info(f"mark {diff.hoard_file} as available here!")
            diff.hoard_props.mark_available(preferences.local_uuid)
            out.write(f"={diff.hoard_file}\n")
        elif goal_status == HoardFileStatus.CLEANUP:
            logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
            out.write(f"?{diff.hoard_file}\n")
        elif goal_status == HoardFileStatus.AVAILABLE:
            pass
        else:
            raise ValueError(f"unrecognized hoard state for {diff.hoard_file}: {goal_status}")


def _handle_local_only(
        preferences: PullPreferences, diff: FileOnlyInLocal, hoard: HoardContents,
        out: StringIO):
    if preferences.on_file_added_or_present == PullBehavior.MOVE_AND_CLEANUP:
        props = hoard.fsobjects.add_or_replace_file(diff.hoard_file, diff.local_props)

        # add status for new repos
        props.set_status(
            list(preferences.content_prefs.repos_to_add(diff.hoard_file, diff.local_props)),
            HoardFileStatus.GET)

        _incoming__safe_mark_for_cleanup(preferences, diff, props, out)
        out.write(f"<+{diff.hoard_file}\n")
    elif preferences.on_file_added_or_present == PullBehavior.IGNORE:
        logging.info(f"Ignoring local-only file {diff.hoard_file}")
        out.write(f"?{diff.hoard_file}\n")
    elif preferences.on_file_added_or_present == PullBehavior.ADD:
        hoard_props = hoard.fsobjects.add_or_replace_file(diff.hoard_file, diff.local_props)

        # add status for new repos
        hoard_props.set_status(
            preferences.content_prefs.repos_to_add(diff.hoard_file, diff.local_props),
            HoardFileStatus.GET)

        # set status here
        hoard_props.mark_available(preferences.local_uuid)

        out.write(f"+{diff.hoard_file}\n")
    else:
        raise ValueError(
            f"unrecognized on_file_added_or_present={preferences.on_file_added_or_present} for {diff.hoard_file}")


def _handle_file_contents_differ(
        preferences: PullPreferences, diff: FileContentsDiffer, hoard: HoardContents, out: StringIO):

    goal_status = diff.hoard_props.get_status(preferences.local_uuid)
    if preferences.deprecated_type == CaveType.INCOMING:
        logging.info(f"incoming file has different contents.")
        hoard_props = hoard.fsobjects.add_or_replace_file(diff.hoard_file, diff.local_props)

        # add status for new repos
        hoard_props.set_status(
            list(preferences.content_prefs.repos_to_add(diff.hoard_file, diff.local_props)),
            HoardFileStatus.GET)

        _incoming__safe_mark_for_cleanup(preferences, diff, hoard_props, out)
        out.write(f"u{diff.hoard_file}\n")
    elif preferences.deprecated_type == CaveType.BACKUP:
        if goal_status == HoardFileStatus.AVAILABLE:  # was backed-up here, get it again
            props = diff.hoard_props
            props.mark_to_get([preferences.local_uuid])

            out.write(f"g{diff.hoard_file}\n")
    else:
        assert preferences.deprecated_type == CaveType.PARTIAL
        if goal_status == HoardFileStatus.AVAILABLE:
            # file was changed in-place, but is different now FIXME should that always happen?
            reset_local_as_current(hoard, preferences.local_uuid, diff.hoard_file, diff.hoard_props, diff.local_props)

            out.write(f"u{diff.hoard_file}\n")
        elif goal_status == HoardFileStatus.UNKNOWN:  # fixme this should disappear if we track repository contents
            if preferences.assume_current:
                # file is added as different then what is in the hoard
                reset_local_as_current(
                    hoard, preferences.local_uuid, diff.hoard_file, diff.hoard_props, diff.local_props)

                out.write(f"RESETTING {diff.hoard_file}\n")
            else:
                logging.info(f"Current file is different, but won't be added because --assume-current == False")
                out.write(f"IGNORE_DIFF {diff.hoard_file}\n")
        elif goal_status == HoardFileStatus.CLEANUP:
            if preferences.assume_current:
                reset_local_as_current(
                    hoard, preferences.local_uuid, diff.hoard_file, diff.hoard_props, diff.local_props)
                out.write(f"RESETTING {diff.hoard_file}\n")
            else:
                logging.info(f"skipping {diff.hoard_file} as is marked for deletion")
                out.write(f"?{diff.hoard_file}\n")
        elif goal_status in (HoardFileStatus.GET, HoardFileStatus.COPY):
            if preferences.assume_current:
                reset_local_as_current(
                    hoard, preferences.local_uuid, diff.hoard_file, diff.hoard_props, diff.local_props)
                out.write(f"RESETTING {diff.hoard_file}\n")
            else:
                logging.info(f"current file is out of date and was marked for restore: {diff.hoard_file}")
                out.write(f"g{diff.hoard_file}\n")


def _handle_hoard_only_deleted(
        preferences: PullPreferences,
        diff: FileOnlyInHoardLocalDeleted | FileOnlyInHoardLocalUnknown | FileOnlyInHoardLocalMoved,
        out: StringIO):
    if preferences.deprecated_type == CaveType.INCOMING:
        logging.info(f"skipping file not in local.")
        status = diff.hoard_props.get_status(preferences.local_uuid)
        if status == HoardFileStatus.CLEANUP:
            diff.hoard_props.remove_status(preferences.local_uuid)
        elif status == HoardFileStatus.UNKNOWN:
            pass  # ignore file
        else:
            logging.error(f"File in hoard only, but status is not {HoardFileStatus.CLEANUP}")
            out.write(f"E{diff.hoard_file}\n")
    elif preferences.deprecated_type == CaveType.BACKUP:
        status1 = diff.hoard_props.get_status(preferences.local_uuid)
        if status1 == HoardFileStatus.AVAILABLE:  # was backed-up here, get it again
            props = diff.hoard_props
            props.mark_to_get([preferences.local_uuid])

            out.write(f"g{diff.hoard_file}\n")
        elif status1 == HoardFileStatus.CLEANUP:  # file already deleted
            diff.hoard_props.remove_status(preferences.local_uuid)
        elif status1 == HoardFileStatus.GET or status1 == HoardFileStatus.COPY:
            pass
        elif status1 == HoardFileStatus.UNKNOWN:
            logging.info("File not recognized by this backup, skipping")
        else:
            raise NotImplementedError(f"Unrecognized goal status {status1}")
    else:

        assert preferences.deprecated_type == CaveType.PARTIAL

        goal_status = diff.hoard_props.get_status(preferences.local_uuid)
        if goal_status == HoardFileStatus.CLEANUP:
            logging.info(f"file had been deleted.")
            diff.hoard_props.remove_status(preferences.local_uuid)
        elif goal_status == HoardFileStatus.AVAILABLE:  # file was here, is no longer
            if preferences.force_fetch_local_missing:
                logging.info(f"file {diff.hoard_file} is missing, restoring due to --force-fetch-local-missing")

                diff.hoard_props.mark_to_get([preferences.local_uuid])
                out.write(f"g{diff.hoard_file}\n")
            else:
                logging.info(f"deleting file {diff.hoard_file} as is no longer in local")
                diff.hoard_props.mark_to_delete_everywhere()
                diff.hoard_props.remove_status(preferences.local_uuid)
                out.write(f"-{diff.hoard_file}\n")
        elif goal_status == HoardFileStatus.GET:
            logging.info(f"file fetch had been scheduled already.")
        elif goal_status == HoardFileStatus.UNKNOWN:
            logging.info(f"file not related to repo, skipping!")
        else:
            raise NotImplementedError(f"Unrecognized goal status {goal_status}")


def _handle_hoard_only_unknown(preferences: PullPreferences, diff: FileOnlyInHoardLocalUnknown, out: StringIO):
    # todo implement different case, e.g. do not add or remove
    _handle_hoard_only_deleted(preferences, diff, out)


def _handle_hoard_only_moved(
        preferences: PullPreferences, diff: FileOnlyInHoardLocalMoved, hoard_new_path: str,
        hoard_new_path_props: HoardFileProps,
        other_remotes_wanting_new_file: List[str], out: StringIO):
    if preferences.deprecated_type == CaveType.INCOMING:
        raise NotImplementedError()
    elif preferences.deprecated_type == CaveType.BACKUP:
        raise NotImplementedError()
    else:
        assert preferences.deprecated_type == CaveType.PARTIAL

        out.write(f"MOVE: {diff.hoard_file} to {hoard_new_path}\n")
        for other_remote_uuid in other_remotes_wanting_new_file:
            if diff.hoard_props.get_status(other_remote_uuid) == HoardFileStatus.AVAILABLE:
                logging.info(
                    f"File {diff.hoard_file} is available in {other_remote_uuid}, will mark {hoard_new_path} as move!")

                hoard_new_path_props.set_to_move_from_local(other_remote_uuid, diff.hoard_file)
                out.write(f"MOVE {other_remote_uuid}: {diff.hoard_file} to {hoard_new_path}\n")
            else:
                logging.info(f"File {diff.hoard_file} is not available in {other_remote_uuid}, can't move.")

        # clear from this location
        _handle_hoard_only_deleted(preferences, diff, out)


def pull_repo_contents_to_hoard(
        hoard_contents: HoardContents, pathing: HoardPathing, current_contents: RepoContents,
        preferences: PullPreferences, out: StringIO):
    all_diffs = list(compare_local_to_hoard(current_contents, hoard_contents, pathing))
    diffs_by_type = group_to_dict(all_diffs, key=lambda diff: type(diff))

    for dt, diffs in diffs_by_type.items():
        print(f"{dt}: {len(diffs)}")  # fixme move to log

    for diff in diffs_by_type.pop(FileIsSame, []):
        assert isinstance(diff, FileIsSame)
        _handle_file_is_same(preferences, diff, out)

    for diff in diffs_by_type.pop(FileOnlyInLocal, []):
        assert isinstance(diff, FileOnlyInLocal)
        _handle_local_only(preferences, diff, hoard_contents, out)

    for diff in diffs_by_type.pop(FileContentsDiffer, []):
        assert isinstance(diff, FileContentsDiffer)
        _handle_file_contents_differ(preferences, diff, hoard_contents, out)

    for diff in diffs_by_type.pop(FileOnlyInHoardLocalDeleted, []):
        assert isinstance(diff, FileOnlyInHoardLocalDeleted)
        _handle_hoard_only_deleted(preferences, diff, out)

    for diff in diffs_by_type.pop(FileOnlyInHoardLocalUnknown, []):
        assert isinstance(diff, FileOnlyInHoardLocalUnknown)
        _handle_hoard_only_unknown(preferences, diff, out)

    for diff in diffs_by_type.pop(DirMissingInHoard, []):
        assert isinstance(diff, DirMissingInHoard)
        logging.info(f"new dir found: {diff.local_dir}")
        hoard_contents.fsobjects.add_dir(diff.hoard_dir)

    dir_missing_in_local = diffs_by_type.pop(DirMissingInLocal, [])
    logging.info(f"# dir missing in local = {len(dir_missing_in_local)}")

    dir_is_same = diffs_by_type.pop(DirIsSame, [])
    logging.info(f"# dir missing in local = {len(dir_is_same)}")
    logging.info(f"Handling moves, after the other ops have been done.")

    for diff in diffs_by_type.pop(FileOnlyInHoardLocalMoved, []):
        assert isinstance(diff, FileOnlyInHoardLocalMoved)
        hoard_new_path = pathing.in_local(diff.local_props.last_related_fullpath, current_contents.uuid) \
            .at_hoard().as_posix()
        hoard_new_path_props = hoard_contents.fsobjects[hoard_new_path]
        assert isinstance(hoard_new_path_props, HoardFileProps)
        assert hoard_new_path_props.fasthash == diff.hoard_props.fasthash and \
               hoard_new_path_props.fasthash == diff.local_props.fasthash

        logging.info(f"Marking moving of {diff.hoard_file} to {hoard_new_path}.")
        other_remotes_wanting_file = hoard_new_path_props.by_statuses(
            HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.MOVE)
        _handle_hoard_only_moved(
            preferences, diff, hoard_new_path, hoard_new_path_props, other_remotes_wanting_file, out)
    for unrecognized_type, unrecognized_diffs in diffs_by_type.items():
        logging.error(f"Unrecognized {len(unrecognized_diffs)} of type: {unrecognized_type}")
    if len(diffs_by_type) > 0:
        raise ValueError(f"Unrecognized diffs of types {list(diffs_by_type.keys())}")


def _incoming__safe_mark_for_cleanup(
        preferences: PullPreferences,
        diff: FileOnlyInLocal | FileContentsDiffer | FileIsSame,
        hoard_file: HoardFileProps, out: StringIO):
    logging.info(f"safe marking {diff.hoard_file} for cleanup from {preferences.local_uuid}")

    repos_to_get_file = hoard_file.by_statuses(HoardFileStatus.GET, HoardFileStatus.COPY, HoardFileStatus.AVAILABLE)
    if preferences.local_uuid in repos_to_get_file:
        repos_to_get_file.remove(preferences.local_uuid)
    if len(repos_to_get_file) > 0:
        logging.info(f"marking {diff.hoard_file} for cleanup from {preferences.local_uuid}")
        hoard_file.mark_for_cleanup([preferences.local_uuid])
    else:
        logging.error(f"No place to get {diff.hoard_file}, will NOT cleanup.")
        hoard_file.mark_available(preferences.local_uuid)

        out.write(f"~{diff.hoard_file}\n")


def reset_local_as_current(
        hoard: HoardContents, remote_uuid: str, hoard_file: str, hoard_props: HoardFileProps,
        local_props: RepoFileProps):
    past_available = hoard_props.by_statuses(HoardFileStatus.AVAILABLE, HoardFileStatus.GET, HoardFileStatus.COPY)

    hoard_props = hoard.fsobjects.add_or_replace_file(hoard_file, local_props)
    hoard_props.mark_to_get(past_available)
    hoard_props.mark_available(remote_uuid)


def _backup_handle_hoard_only_deleted(
        preferences: PullPreferences, diff: FileOnlyInHoardLocalDeleted | FileOnlyInHoardLocalUnknown, out: StringIO):
    goal_status = diff.hoard_props.get_status(preferences.local_uuid)
    if goal_status == HoardFileStatus.AVAILABLE:  # was backed-up here, get it again
        props = diff.hoard_props
        props.mark_to_get([preferences.local_uuid])

        out.write(f"g{diff.hoard_file}\n")
    elif goal_status == HoardFileStatus.CLEANUP:  # file already deleted
        diff.hoard_props.remove_status(preferences.local_uuid)
    elif goal_status == HoardFileStatus.GET or goal_status == HoardFileStatus.COPY:
        pass
    elif goal_status == HoardFileStatus.UNKNOWN:
        logging.info("File not recognized by this backup, skipping")
    else:
        raise NotImplementedError(f"Unrecognized goal status {goal_status}")
