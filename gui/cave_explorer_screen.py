import abc
import enum
import logging
import traceback
from io import StringIO
from sqlite3 import OperationalError
from typing import TypeVar, Dict

from rich.text import Text
from textual import work, on
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical, Grid
from textual.css.query import NoMatches
from textual.message import Message
from textual.reactive import reactive
from textual.screen import Screen
from textual.widget import Widget
from textual.widgets import Tree, Static, Header, Footer, Select, Button, RadioSet, RadioButton, Input
from textual.widgets._tree import TreeNode

from command.command_repo import RepoCommand
from command.content_prefs import ContentPrefs
from command.contents.command import execute_pull, init_pull_preferences, pull_prefs_to_restore_from_hoard, \
    DifferencesCalculator, get_current_file_differences, Difference
from command.contents.comparisons import copy_local_staging_data_to_hoard, \
    commit_local_staging
from command.fast_path import FastPosixPath
from command.files.command import execute_files_push
from command.hoard import Hoard
from command.pathing import HoardPathing
from command.pending_file_ops import FileOp, CleanupFile, GetFile, CopyFile, MoveFile, RetainFile
from config import HoardRemote, latency_order, ConnectionLatency, ConnectionSpeed, CaveType
from contents.hoard import HoardContents
from contents.hoard_connection import ReadonlyHoardContentsConn
from contents.hoard_props import HoardFileProps
from contents.recursive_stats_calc import NodeID, CurrentAndDesiredReader
from contents.repo_props import FileDesc
from exceptions import RepoOpeningFailed, WrongRepo, MissingRepoContents, MissingRepo
from gui.app_config import config, _write_config
from gui.confirm_action_screen import ConfirmActionScreen
from gui.folder_tree import DEPRECATED_FolderNode, DEPRECATED_FolderTree, DEPRECATED_aggregate_on_nodes
from gui.logging import PythonLoggingWidget
from gui.progress_reporting import LongRunningTasks, LongRunningTaskContext
from lmdb_storage.cached_calcs import AppCachedCalculator
from lmdb_storage.tree_object import TreeObject
from util import group_to_dict, format_count, format_size, snake_case, safe_hex


# fixme reimplement with trees
class DiffType(enum.Enum):
    FileOnlyInLocal = enum.auto()
    FileIsSame = enum.auto()
    FileContentsDiffer = enum.auto()
    FileOnlyInHoardLocalDeleted = enum.auto()
    FileOnlyInHoardLocalUnknown = enum.auto()
    FileOnlyInHoardLocalMoved = enum.auto()


# fixme reimplement with trees
class Diff:
    def __init__(
            self, diff_type: DiffType, local_file: FastPosixPath,
            curr_file_hoard_path: FastPosixPath, local_props: FileDesc | None, hoard_props: HoardFileProps | None,
            is_added: bool | None):
        self.diff_type = diff_type

        assert not local_file.is_absolute()
        assert curr_file_hoard_path.is_absolute()
        self.local_file = local_file
        self.hoard_file = curr_file_hoard_path
        self.local_props = local_props
        self.hoard_props = hoard_props

        self.is_added = is_added


# fixme reimplement with trees
class Action(abc.ABC):
    @classmethod
    def action_type(cls):
        return snake_case(cls.__name__)

    diff: Diff

    def __init__(self, diff: Diff):
        self.diff = diff

    @property
    def file_being_acted_on(self): return self.diff.hoard_file

    @abc.abstractmethod
    def execute(self, local_uuid: str, content_prefs: ContentPrefs, hoard: HoardContents, out: StringIO) -> None: pass


class HoardContentsPendingToSyncFile(Tree[NodeID]):
    hoard: Hoard | None = reactive(None)
    remote: HoardRemote | None = reactive(None, recompose=True)

    files_diff_tree_root: NodeID | None

    hoard_conn: ReadonlyHoardContentsConn | None
    hoard_contents: HoardContents | None
    current_and_desired_reader: CurrentAndDesiredReader | None

    pending_ops_calculator: AppCachedCalculator[NodeID, Difference] | None

    def __init__(self, hoard: Hoard, remote: HoardRemote):
        super().__init__('Pending file sync ')
        self.hoard = hoard
        self.remote = remote

        self.hoard_contents = None
        self.current_and_desired_reader = None
        self.pending_ops_calculator = None

        self.files_diff_tree_root = None

        self.show_size = True

        self.expanded = set()

    async def on_mount(self):
        self.hoard_conn = self.hoard.open_contents(create_missing=False)
        self.hoard_contents = self.hoard_conn.__enter__()
        self.current_and_desired_reader = CurrentAndDesiredReader(self.hoard_contents)

        repo_root = self.hoard_contents.env.roots(write=False)[self.remote.uuid]
        self.files_diff_tree_root: NodeID = NodeID(repo_root.current, repo_root.desired)
        self.pending_ops_calculator = AppCachedCalculator(
            DifferencesCalculator(self.hoard_contents, get_current_file_differences),
            Difference)

        self.root.data = self.files_diff_tree_root
        self.root.label = (
            self.root.label.append(f"({self.pending_ops_calculator[self.files_diff_tree_root].count.all})")
            .append(self._pretty_folder_label_descriptor(self.files_diff_tree_root)))
        self.root.expand()

    def on_tree_node_expanded(self, event: Tree[NodeID].NodeExpanded):
        if event.node in self.expanded:
            return

        self.expanded.add(event.node)

        folder_node_id: NodeID = event.node.data
        for child_name, child_id in self.current_and_desired_reader.children(folder_node_id):
            child_obj = self.current_and_desired_reader.convert(child_id)

            if isinstance(child_obj.current, TreeObject) or isinstance(child_obj.desired, TreeObject):
                # is a folder
                folder_label = Text().append(child_name).append(" ") \
                    .append(f"({self.pending_ops_calculator[child_id].count.all})", style="dim")
                cnts_label = self._pretty_folder_label_descriptor(child_id)
                event.node.add(folder_label.append(" ").append(cnts_label), data=child_id)
            else:  # is a file
                child_pending_ops = self.pending_ops_calculator[child_id]
                if child_pending_ops.count.to_obtain > 0:
                    op_type = "GET"
                elif child_pending_ops.count.to_delete > 0:
                    op_type = "CLEANUP"
                elif child_pending_ops.count.to_change > 0:
                    op_type = "CHANGE"
                else:
                    op_type = "UNKNOWN?!"
                event.node.add_leaf(f"{op_type}: {child_name}", data=child_id)

    def _pretty_folder_label_descriptor(self, folder: NodeID) -> Text:
        folder_diffs = self.pending_ops_calculator[folder]

        def format_num(it):
            return format_size(it) if self.show_size else format_count(it)

        cnts_label = Text().append("{")
        if folder_diffs is not None:
            stat = folder_diffs.size if self.show_size else folder_diffs.count
            if folder_diffs.count.to_obtain > 0:
                cnts_label.append("get", style="green") \
                    .append(" ").append(format_num(stat.to_obtain), style="dim").append(",")

            if folder_diffs.count.to_delete > 0:
                cnts_label.append("rm", style="strike dim") \
                    .append(" ").append(format_num(stat.to_delete), style="dim").append(",")

            if folder_diffs.count.to_change > 0:
                cnts_label.append("modify", style="none") \
                    .append(" ").append(format_num(stat.to_change), style="dim").append(",")

        cnts_label.append("}")
        return cnts_label


async def aggregate_counts(op_tree):
    return DEPRECATED_aggregate_on_nodes(
        op_tree,
        lambda op: 1,
        lambda old, new: new if old is None else old + new)


def op_to_str(op: FileOp):
    if isinstance(op, GetFile):
        return "get", 1
    elif isinstance(op, CopyFile):
        return "copy", 2
    elif isinstance(op, MoveFile):
        return "move", 3
    elif isinstance(op, CleanupFile):
        return "cleanup", 4
    elif isinstance(op, RetainFile):
        return "retain", 5
    else:
        raise ValueError(f"Unsupported op: {op}")


T = TypeVar('T')


def sum_dicts(old: Dict[T, any], new: Dict[T, any]) -> Dict[T, any]:
    result = old.copy() if old is not None else dict()
    for k, v in new.items():
        result[k] = result.get(k, 0) + v
    return result


PENDING_TO_PULL = 'Hoard vs Repo contents'


def pull_op_to_str(act: Action):
    raise NotImplementedError()
    # if isinstance(act, MarkIsAvailableBehavior):
    #     return "mark_avail", 25
    # elif isinstance(act, AddToHoardAndCleanupSameBehavior):
    #     return "absorb", 5
    # elif isinstance(act, AddToHoardAndCleanupNewBehavior):
    #     return "absorb", 5
    # elif isinstance(act, AddNewFileBehavior):
    #     return "add", 10
    # elif isinstance(act, MarkToGetBehavior):
    #     return "mark_get", 40
    # elif isinstance(act, MarkForCleanupBehavior):
    #     return "mark_deleted", 92
    # elif isinstance(act, ResetLocalAsCurrentBehavior):
    #     return "reset_to_local", 20
    # elif isinstance(act, RemoveLocalStatusBehavior):
    #     return "unmark", 91
    # elif isinstance(act, DeleteFileFromHoardBehavior):
    #     return "delete", 90
    # elif isinstance(act, MoveFileBehavior):
    #     return "move", 30
    # else:
    #     raise ValueError(f"Unsupported action: {act}")


class HoardContentsPendingToPull(Tree[Action]):
    hoard: Hoard | None = reactive(None)
    remote: HoardRemote | None = reactive(None, recompose=True)

    def __init__(self, hoard: Hoard, remote: HoardRemote):
        super().__init__(PENDING_TO_PULL + " (expand to load)")

        self.hoard = hoard
        self.remote = remote

        self.op_tree = None
        self.counts = None
        self.ops_cnt = None

        self.show_size = False

        self.expanded = set()

    @work(thread=True)
    async def populate_and_expand(self):
        try:
            self.root.label = PENDING_TO_PULL + " (loading...)"
            hoard_config = self.hoard.config()
            pathing = HoardPathing(hoard_config, self.hoard.paths())
            repo = self.hoard.connect_to_repo(self.remote.uuid, True)

            raise NotImplementedError("to be implemented with trees")

            with repo.open_contents(is_readonly=True) as current_contents:
                async with self.hoard.open_contents(create_missing=False) as hoard_contents:
                    preferences = init_pull_preferences(
                        self.remote, assume_current=False, force_fetch_local_missing=False)

                    config = self.hoard.config()
                    abs_staging_root_id = copy_local_staging_data_to_hoard(hoard_contents, current_contents, config)
                    commit_local_staging(hoard_contents, current_contents, abs_staging_root_id)
                    uuid = current_contents.config.uuid

                    # resolutions = await resolution_to_match_repo_and_hoard(
                    #     uuid, hoard_contents, pathing, preferences,
                    #     progress_reporting_it(self, id="hoard-contents-to-pull", max_frequency=10))
                    #
                    # with StringIO() as other_out:
                    #     actions = list(calculate_actions(preferences, resolutions, pathing, hoard_config, other_out))
                    #     logging.debug(other_out.getvalue())

            self.DEPRECATED_op_tree = DEPRECATED_FolderTree[Action](actions, lambda
                action: action.file_being_acted_on.as_posix())

            self.counts = await aggregate_counts(self.DEPRECATED_op_tree)
            self.ops_cnt = DEPRECATED_aggregate_on_nodes(
                self.DEPRECATED_op_tree,
                lambda node: {pull_op_to_str(node.data): node.data.file_obj.size if self.show_size else 1},
                sum_dicts)

            self.root.label = PENDING_TO_PULL + f" ({len(actions)})"
            self.root.data = self.DEPRECATED_op_tree.root

            self.post_message(Tree.NodeExpanded(self.root))

            self._expand_subtree(self.root)

        except RepoOpeningFailed as e:
            traceback.print_exception(e)
            logging.error(f"RepoOpeningFailed : {e}")
            self.root.label = PENDING_TO_PULL + " (FAILED TO OPEN)"
        except OperationalError as e:
            traceback.print_exception(e)
            logging.error(f"OperationalError: {e}")
            self.root.label = PENDING_TO_PULL + " (INVALID CONTENTS)"

    def on_tree_node_expanded(self, event: Tree.NodeExpanded):
        if event.node in self.expanded:
            return
        self.expanded.add(event.node)

        if event.node == self.root:
            self.populate_and_expand()
        else:
            assert self.op_tree is not None
            self._expand_subtree(event.node)

    def _expand_subtree(self, node: TreeNode[DEPRECATED_FolderNode[Action]]):
        for _, folder in node.data.folders.items():
            folder_name = Text().append(folder.name).append(" ").append(f"({self.counts[folder]})", style="dim")
            cnts_label = self._pretty_folder_label_descriptor(folder)
            folder_label = folder_name.append(" ").append(cnts_label)
            node.add(folder_label, data=folder)
        for _, file in node.data.files.items():
            node.add_leaf(f"{type(file.data)}: {file.data.file_being_acted_on}", data=node.data)

    def _pretty_folder_label_descriptor(self, folder: DEPRECATED_FolderNode[FileOp]) -> Text:
        cnts_label = Text().append("{")
        pending = self.ops_cnt[folder]
        if pending is not None:
            for (op_type, order), v in sorted(pending.items(), key=lambda x: x[0][1]):
                cnts_label.append(op_type) \
                    .append(" ").append(format_size(v) if self.show_size else format_count(v), style="dim").append(",")
                # op_type, style="green" if op_type == "get" else "strike dim" if op_type == "cleanup" else "none") \
        cnts_label.append("}")
        return cnts_label


class CaveInfoWidget(Widget):
    class RemoteSettingChanged(Message):
        pass

    hoard: Hoard | None = reactive(None)
    remote: HoardRemote | None = reactive(None, recompose=True)

    def __init__(self, hoard: Hoard, remote: HoardRemote):
        super().__init__()
        self.hoard = hoard
        self.remote = remote

    def compose(self) -> ComposeResult:
        if self.remote is None:
            yield Static("Please choose a cave.")
        else:
            yield Static("UUID: " + self.remote.uuid, classes="repo-setting-group")
            with self.hoard.open_contents(create_missing=False) as hoard_contents:
                repo_root = hoard_contents.env.roots(write=False)[self.remote.uuid]

                yield Static("current: " + safe_hex(repo_root.current), classes="repo-setting-group")
                yield Static("desired: " + safe_hex(repo_root.desired), classes="repo-setting-group")
                yield Static("staging: " + safe_hex(repo_root.staging), classes="repo-setting-group")

            yield Button("`cave refresh`", variant="primary", id="refresh_cave_contents")

            with Grid(classes="repo-setting-grid"):
                yield Static("Name", classes="repo-setting-label")
                yield Input(value=self.remote.name, placeholder="Remote Name", id="repo-name", restrict="..+")

                yield Static("Type", classes="repo-setting-label")
                yield Select[CaveType](
                    value=self.hoard.config().remotes[self.remote.uuid].type, id="repo-type",
                    options=((s.value, s) for s in CaveType), allow_blank=False)

                yield Static("Latency", classes="repo-setting-label")
                yield Select[ConnectionLatency](
                    value=self.hoard.paths()[self.remote.uuid].latency, id="repo-latency",
                    options=((l.value, l) for l in ConnectionLatency), allow_blank=False)

                yield Static("Speed", classes="repo-setting-label")
                yield Select[ConnectionSpeed](
                    value=self.hoard.paths()[self.remote.uuid].speed, id="repo-speed",
                    options=((s.value, s) for s in ConnectionSpeed), allow_blank=False)

                yield Static("Min copies before cleanup", classes="repo-setting-label")
                yield Input(
                    value=str(self.remote.min_copies_before_cleanup), placeholder="min copies",
                    id="repo-min-copies-before-cleanup", type="integer")

            with Horizontal(id="content_trees"):
                with Vertical(id="pane-files-pushed"):
                    with Horizontal():
                        yield Button("`files push`", variant="primary", id="push_files_to_repo")
                        yield Button("`contents reset`", variant="default", id="reset_pending_file_ops")
                    yield HoardContentsPendingToSyncFile(self.hoard, self.remote)

                with Vertical(id="pane-contents-pull"):
                    with Horizontal():
                        yield Button("`contents pull`", variant="primary", id="pull_to_hoard")
                        yield Button("`contents restore`", variant="default", id="restore_from_hoard")
                    yield HoardContentsPendingToPull(self.hoard, self.remote)

    @on(Select.Changed, "#repo-type")
    def repo_type_changed(self, event: Select.Changed):
        assert event.select.id == "repo-type"

        hoard_config = self.hoard.config()
        if hoard_config.remotes[self.remote.uuid].type != CaveType(event.value):
            hoard_config.remotes[self.remote.uuid].type = CaveType(event.value)
            hoard_config.write()

            self.remote = hoard_config.remotes[self.remote.uuid]

    @on(Input.Submitted, "#repo-min-copies-before-cleanup")
    def repo_min_copies_before_cleanup_changed(self, event: Input.Changed):
        assert event.input.id == "repo-min-copies-before-cleanup"

        hoard_config = self.hoard.config()
        if hoard_config.remotes[self.remote.uuid].min_copies_before_cleanup != int(event.value):
            hoard_config.remotes[self.remote.uuid].min_copies_before_cleanup = int(event.value)
            hoard_config.write()

            self.remote = hoard_config.remotes[self.remote.uuid]

    @on(Input.Submitted, "#repo-name")
    def repo_name_changed(self, event: Input.Changed):
        hoard_config = self.hoard.config()
        if hoard_config.remotes[self.remote.uuid].name != event.value:
            hoard_config.remotes[self.remote.uuid].name = event.value
            hoard_config.write()

            self.remote = hoard_config.remotes[self.remote.uuid]

            self.post_message(CaveInfoWidget.RemoteSettingChanged())

    @on(Select.Changed, "#repo-speed")
    def repo_speed_changed(self, event: Select[ConnectionSpeed].Changed):
        paths = self.hoard.paths()
        if paths[self.remote.uuid].speed != event.value:
            paths[self.remote.uuid].speed = event.value
            paths.write()

            self.post_message(CaveInfoWidget.RemoteSettingChanged())

    @on(Select.Changed, "#repo-latency")
    def repo_latency_changed(self, event: Select[ConnectionLatency].Changed):
        paths = self.hoard.paths()
        if paths[self.remote.uuid].latency != event.value:
            paths[self.remote.uuid].latency = event.value
            paths.write()

            self.post_message(CaveInfoWidget.RemoteSettingChanged())

    @on(Button.Pressed, "#push_files_to_repo")
    @work
    async def push_files_to_repo(self):
        if await self.app.push_screen_wait(
                ConfirmActionScreen(
                    f"Are you sure you want to PUSH FILES to the repo: \n"
                    f"{self.remote.name}({self.remote.uuid}\n"
                    f"?")):
            with LongRunningTaskContext(f"Pushing files to {self.remote.name}") as task_logger:
                with StringIO() as out:
                    await execute_files_push(
                        self.hoard.config(),
                        self.hoard, [self.remote.uuid], out,
                        task_logger)
                    logging.info(out.getvalue())

            await self.recompose()

    @on(Button.Pressed, "#refresh_cave_contents")
    @work
    async def refresh_cave_contents(self):
        if await self.app.push_screen_wait(
                ConfirmActionScreen(f"Are you sure you want to REFRESH repo {self.remote.name} ({self.remote.uuid}?")):
            self.run_refresh()

            await self.recompose()

    @work(thread=True)
    async def run_refresh(self):
        repo_cmd = RepoCommand(path=self.hoard.hoardpath, name=self.remote.uuid)
        with LongRunningTaskContext(f"Refreshing {self.remote.name}") as task_context:
            res = await repo_cmd.refresh(show_details=False, task_logger=task_context)

        logging.info(res)
        logging.info(f"Refreshing {self.remote.name}({self.remote.uuid}) completed.")

    @on(Button.Pressed, "#reset_pending_file_ops")
    @work
    async def reset_pending_file_ops(self):
        if await self.app.push_screen_wait(
                ConfirmActionScreen(
                    f"Are you sure you want to RESET PENDING FILE OPS for repo: \n"
                    f"{self.remote.name}({self.remote.uuid}\n"
                    f"?")):
            pathing = HoardPathing(self.hoard.config(), self.hoard.paths())
            with StringIO() as out:
                raise NotImplementedError()
                # await clear_pending_file_ops(self.hoard, self.remote.uuid, out)
                logging.info(out.getvalue())

            await self.recompose()

    @on(Button.Pressed, "#pull_to_hoard")
    @work
    async def pull_to_hoard(self):
        if await self.app.push_screen_wait(
                ConfirmActionScreen(
                    f"Are you sure you want to PULL the repo: \n"
                    f"{self.remote.name}({self.remote.uuid}\n"
                    f"into hoard?")):
            self._execute_pull()

            await self.recompose()

    @work(thread=True)
    async def _execute_pull(self):
        logging.info(f"Loading hoard contents TOML...")

        with StringIO() as out:
            preferences = init_pull_preferences(self.remote, assume_current=False, force_fetch_local_missing=False)
            with self.hoard.open_contents(create_missing=False).writeable() as hoard_contents:
                with LongRunningTaskContext(f"Pulling contents from {self.remote.name}") as task_logger:
                    await execute_pull(
                        self.hoard, hoard_contents, preferences, ignore_epoch=False, out=out, task_logger=task_logger)
            logging.info(out.getvalue())

    @on(Button.Pressed, "#restore_from_hoard")
    @work
    async def restore_from_hoard(self):
        if await self.app.push_screen_wait(
                ConfirmActionScreen(
                    f"Are you sure you want to RESTORE contents from hoard to repo: \n"
                    f"{self.remote.name}({self.remote.uuid}\n"
                    f"")):
            with StringIO() as out:
                preferences = pull_prefs_to_restore_from_hoard(self.remote.uuid, self.remote.type)

                logging.info(f"Loading hoard contents TOML...")
                with self.hoard.open_contents(create_missing=False).writeable() as hoard_contents:
                    with LongRunningTaskContext(f"Pulling contents from {self.remote.name}") as task_logger:
                        await execute_pull(
                            self.hoard, hoard_contents, preferences, ignore_epoch=False, out=out,
                            task_logger=task_logger)
                logging.info(out.getvalue())

            await self.recompose()


class CaveExplorerScreen(Screen):
    CSS_PATH = "cave_exporer_screen.tcss"

    hoard: Hoard | None = reactive(None, recompose=True)
    remote: HoardRemote | None = reactive(None, recompose=True)

    def on_mount(self):
        if self.hoard is not None:
            selected_remote = config().get("cave_exporer_selected_repo", None)
            if selected_remote is not None:
                self.remote = self.hoard.config().remotes[selected_remote]

    def watch_hoard(self):
        try:
            self.query_one(CaveInfoWidget).hoard = self.hoard
        except NoMatches:
            pass

    def watch_remote(self, new_remote: HoardRemote, old_remote: HoardRemote):
        if self.remote is not None:
            config()["cave_exporer_selected_repo"] = self.remote.uuid
            _write_config()
            self.query_one(CaveInfoWidget).remote = self.remote
            self.query_one(f"#uuid-{self.remote.uuid}", RadioButton).toggle()

    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()

        with Vertical():
            with Horizontal(id="selection-pane"):
                with RadioSet(id="choose_remote"):
                    if self.hoard is not None:
                        config = self.hoard.config()

                        latency_to_repos = group_to_dict(
                            config.remotes.all(),
                            key=lambda r: self.hoard.paths()[r.uuid].latency)
                        for latency, repos in sorted(latency_to_repos.items(), key=lambda lr: latency_order(lr[0])):
                            yield Static(f"Latency: {latency.value}", classes="repo-group")
                            for remote in sorted(repos, key=lambda r: r.name):
                                try:
                                    self.hoard.connect_to_repo(remote.uuid, require_contents=True)
                                    style = "green"
                                except MissingRepo as mr:
                                    style = "dim"
                                except MissingRepoContents as mrc:
                                    style = "dim red"
                                except WrongRepo as wr:
                                    style = "dim strike"
                                except Exception as e:
                                    traceback.print_exception(e)
                                    logging.error(e)
                                    style = "red strike"

                                yield RadioButton(
                                    Text().append(remote.name, style),
                                    name=remote.uuid, id=f"uuid-{remote.uuid}", value=remote == self.remote)

                yield CaveInfoWidget(self.hoard, self.remote)

            yield LongRunningTasks()
            yield PythonLoggingWidget()

    @on(CaveInfoWidget.RemoteSettingChanged)
    async def cave_settings_changed(self):
        await self.recompose()

    def on_radio_set_changed(self, event: RadioSet.Changed):
        if self.hoard is None:
            return

        uuid = event.pressed.id[5:]
        if self.hoard.config().remotes[uuid] is None:
            self.notify(f"Invalid repo selected - {event.pressed.name} with uuid {uuid}!", severity="error")
            return

        self.switch_remote(uuid)

    def switch_remote(self, uuid: str):
        self.remote = self.hoard.config().remotes[uuid]
