import enum


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
            force_fetch_local_missing: bool,
            force_reset_with_local_contents: bool):
        self.local_uuid = local_uuid

        self.on_file_added_or_present = on_file_added_or_present

        self.force_fetch_local_missing = force_fetch_local_missing
        self.force_reset_with_local_contents = force_reset_with_local_contents
