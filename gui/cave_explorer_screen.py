import logging
import logging
import traceback
from io import StringIO
from sqlite3 import OperationalError

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
from command.content_prefs import ContentPrefs, Presence
from command.contents.command import execute_pull, init_pull_preferences, pull_prefs_to_restore_from_hoard, \
    DifferencesCalculator, get_current_file_differences, Difference, create_single_repo_merge_roots
from command.contents.comparisons import copy_local_staging_data_to_hoard, \
    commit_local_staging
from command.files.command import execute_files_push
from command.hoard import Hoard
from command.pathing import HoardPathing
from command.pending_file_ops import FileOp, CleanupFile, GetFile, CopyFile, MoveFile, RetainFile
from config import HoardRemote, latency_order, ConnectionLatency, ConnectionSpeed, CaveType
from contents.hoard import HoardContents
from contents.hoard_connection import ReadonlyHoardContentsConn
from contents.recursive_stats_calc import NodeID, CurrentAndDesiredReader
from exceptions import RepoOpeningFailed, WrongRepo, MissingRepoContents, MissingRepo
from gui.app_config import config, _write_config
from gui.confirm_action_screen import ConfirmActionScreen
from gui.logging import PythonLoggingWidget
from gui.progress_reporting import LongRunningTasks, LongRunningTaskContext
from lmdb_storage.cached_calcs import AppCachedCalculator
from lmdb_storage.pull_contents import merge_contents
from lmdb_storage.tree_object import TreeObject
from util import group_to_dict, format_count, format_size, safe_hex


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
            self.root.label.append(f" ({self.pending_ops_calculator[self.files_diff_tree_root].count.all})")
            .append(self._pretty_folder_label_descriptor(self.files_diff_tree_root)))
        self.root.expand()

    def on_tree_node_expanded(self, event: Tree[NodeID].NodeExpanded):
        if event.node in self.expanded:
            return

        self.expanded.add(event.node)

        folder_node_id: NodeID = event.node.data
        for child_name, child_id in self.current_and_desired_reader.children(folder_node_id):
            child_obj = self.current_and_desired_reader.convert(child_id)

            child_pending_ops = self.pending_ops_calculator[child_id]
            if isinstance(child_obj.current, TreeObject) or isinstance(child_obj.desired, TreeObject):
                if child_pending_ops.count.all == 0:
                    continue  # # hide not modified

                # is a folder
                folder_label = Text().append(child_name).append(" ") \
                    .append(f"({self.pending_ops_calculator[child_id].count.all})", style="dim")
                cnts_label = self._pretty_folder_label_descriptor(child_id)
                event.node.add(folder_label.append(" ").append(cnts_label), data=child_id)
            else:  # is a file

                if child_pending_ops.count.all == 0:
                    continue  # hide not modified

                if child_pending_ops.count.to_delete:
                    op_label = Text("CLEANUP", style="red").append(": ", style="normal")
                elif child_pending_ops.count.to_change:
                    op_label = Text("CHANGE", style="normal").append(": ", style="normal")
                elif child_pending_ops.count.to_obtain:
                    op_label = Text("GET", style="green").append(": ", style="normal")
                else:
                    op_label = Text("UNRECOGNIZED OP?! ", style="error")

                event.node.add_leaf(op_label.append(child_name), data=child_id)

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


PENDING_TO_PULL = 'Hoard vs Repo contents'


class HoardContentsPendingToPull(Tree[NodeID]):
    hoard: Hoard | None = reactive(None)
    remote: HoardRemote | None = reactive(None, recompose=True)

    def __init__(self, hoard: Hoard, remote: HoardRemote):
        super().__init__(PENDING_TO_PULL + " (expand to load)")

        self.hoard = hoard
        self.remote = remote
        self.hoard_conn: ReadonlyHoardContentsConn | None = None
        self.hoard_contents: HoardContents | None = None

        self.show_size = False

        self.expanded = set()

        self.contents_diff_tree_root = None
        self.pending_ops_calculator: AppCachedCalculator[NodeID, Difference] | None = None

        self.loading = True

    async def on_mount(self):
        self.populate_and_expand()

    @work(thread=True)
    async def populate_and_expand(self):
        try:
            hoard_config = self.hoard.config()
            repo = self.hoard.connect_to_repo(self.remote.uuid, True)

            self.hoard_conn = self.hoard.open_contents(create_missing=False)
            self.hoard_contents = self.hoard_conn.__enter__()

            with repo.open_contents(is_readonly=True) as current_contents:
                preferences = init_pull_preferences(
                    self.remote, assume_current=False, force_fetch_local_missing=False)

                pathing = HoardPathing(hoard_config, self.hoard.paths())
                content_prefs = ContentPrefs(
                    hoard_config, pathing, self.hoard_contents, self.hoard.available_remotes(),
                    Presence(self.hoard_contents))

                abs_staging_root_id = copy_local_staging_data_to_hoard(
                    self.hoard_contents, current_contents, hoard_config)

                commit_local_staging(self.hoard_contents, current_contents, abs_staging_root_id)

                with StringIO() as out:
                    threeway_merge_roots = create_single_repo_merge_roots(
                        current_contents, self.hoard_contents, abs_staging_root_id, out)
                    logging.debug(out.getvalue())

                merged_ids = merge_contents(
                    self.hoard_contents.env,
                    threeway_merge_roots,
                    preferences=preferences, content_prefs=content_prefs,
                    merge_only=[threeway_merge_roots.repo_name])

                desired_root_id = self.hoard_contents.env.roots(False)[self.remote.uuid].desired
                new_desired_root_id = merged_ids.get_if_present(self.remote.uuid)

            self.contents_diff_tree_root: NodeID = NodeID(desired_root_id, new_desired_root_id)
            self.pending_ops_calculator = AppCachedCalculator(
                DifferencesCalculator(self.hoard_contents, get_current_file_differences),
                Difference)

            self.root.label = (
                Text(PENDING_TO_PULL)
                .append(f" ({self.pending_ops_calculator[self.contents_diff_tree_root].count.all})")
                .append(self._pretty_folder_label_descriptor(self.contents_diff_tree_root)))
            self.root.data = self.contents_diff_tree_root

            self.post_message(Tree.NodeExpanded(self.root))

            self.loading = False


        except RepoOpeningFailed as e:
            traceback.print_exception(e)
            logging.error(f"RepoOpeningFailed : {e}")
            self.root.label = PENDING_TO_PULL + " (FAILED TO OPEN)"
        except OperationalError as e:
            traceback.print_exception(e)
            logging.error(f"OperationalError: {e}")
            self.root.label = PENDING_TO_PULL + " (INVALID CONTENTS)"

    def on_unmount(self):
        if self.hoard_contents:
            self.hoard_contents = None

        if self.hoard_conn:
            self.hoard_conn.__exit__(None, None, None)
            self.hoard_conn = None

    def on_tree_node_expanded(self, event: Tree.NodeExpanded):
        if event.node in self.expanded:
            return
        self.expanded.add(event.node)

        self._expand_subtree(event.node)

    def _expand_subtree(self, node: TreeNode[NodeID]):
        with self.hoard_contents.env.objects(write=False) as objects:
            node_obj = node.data.load(objects)

            for child_name, child_id in node_obj.children:
                count_and_sizes = self.pending_ops_calculator[child_id]
                child_obj = child_id.load(objects)

                if child_obj.has_children:
                    if count_and_sizes.count.all == 0:
                        continue  # hide not modified

                    folder_name = Text().append(child_name).append(" ").append(
                        f"({count_and_sizes.count.all})", style="dim")
                    cnts_label = self._pretty_folder_label_descriptor(child_id)
                    folder_label = folder_name.append(" ").append(cnts_label)
                    node.add(folder_label, data=child_id)

                else:
                    if count_and_sizes.count.all == 0:
                        continue  # hide not modified

                    if count_and_sizes.count.to_delete:
                        op_label = Text("DEL", style="red").append(": ", style="normal")
                    elif count_and_sizes.count.to_change:
                        op_label = Text("MOD", style="normal").append(": ", style="normal")
                    elif count_and_sizes.count.to_obtain:
                        op_label = Text("ADD", style="green").append(": ", style="normal")
                    else:
                        op_label = Text("UNRECOGNIZED OP?! ", style="error")
                    node.add_leaf(op_label.append(child_name), data=node.data)

    def _pretty_folder_label_descriptor(self, folder: NodeID) -> Text:
        count_and_sizes = self.pending_ops_calculator[folder]

        def format_num(it):
            return format_size(it) if self.show_size else format_count(it)

        cnts_label = Text().append("{")
        if count_and_sizes is not None:
            stat = count_and_sizes.size if self.show_size else count_and_sizes.count
            if count_and_sizes.count.to_obtain > 0:
                cnts_label.append("get", style="green") \
                    .append(" ").append(format_num(stat.to_obtain), style="dim").append(",")

            if count_and_sizes.count.to_delete > 0:
                cnts_label.append("rm", style="strike dim") \
                    .append(" ").append(format_num(stat.to_delete), style="dim").append(",")

            if count_and_sizes.count.to_change > 0:
                cnts_label.append("modify", style="none") \
                    .append(" ").append(format_num(stat.to_change), style="dim").append(",")

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
