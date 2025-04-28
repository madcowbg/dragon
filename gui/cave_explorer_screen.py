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
from textual.widgets import Tree, Static, Header, Footer, Select, RichLog, Button, RadioSet, RadioButton, Input
from textual.widgets._tree import TreeNode

from command.contents.command import execute_pull, init_pull_preferences, pull_prefs_to_restore_from_hoard, \
    clear_pending_file_ops
from command.contents.comparisons import copy_local_staging_to_hoard, sync_fsobject_to_object_storage
from command.contents.handle_pull import resolution_to_match_repo_and_hoard, calculate_actions, Action, \
    MarkIsAvailableBehavior, AddToHoardAndCleanupSameBehavior, AddToHoardAndCleanupNewBehavior, AddNewFileBehavior, \
    MarkToGetBehavior, MarkForCleanupBehavior, ResetLocalAsCurrentBehavior, RemoveLocalStatusBehavior, \
    DeleteFileFromHoardBehavior, MoveFileBehavior
from command.files.command import execute_files_push
from command.hoard import Hoard
from command.pathing import HoardPathing
from command.pending_file_ops import FileOp, get_pending_operations, CleanupFile, GetFile, CopyFile, MoveFile
from config import HoardRemote, latency_order, ConnectionLatency, ConnectionSpeed, CaveType
from exceptions import RepoOpeningFailed, WrongRepo, MissingRepoContents, MissingRepo
from gui.app_config import config, _write_config
from gui.confirm_action_screen import ConfirmActionScreen
from gui.folder_tree import FolderNode, FolderTree, aggregate_on_nodes
from gui.progress_reporting import StartProgressReporting, MarkProgressReporting, progress_reporting_it, \
    ProgressReporting, progress_reporting_bar
from util import group_to_dict, format_count, format_size


class HoardContentsPendingToSyncFile(Tree[FolderNode[FileOp]]):
    hoard: Hoard | None = reactive(None)
    remote: HoardRemote | None = reactive(None, recompose=True)

    op_tree: FolderTree[FileOp] | None

    def __init__(self, hoard: Hoard, remote: HoardRemote):
        super().__init__('Pending file sync ')
        self.hoard = hoard
        self.remote = remote

        self.op_tree = None
        self.counts = None
        self.ops_cnt = None

        self.show_size = True

        self.expanded = set()

    async def on_mount(self):
        async with self.hoard.open_contents(create_missing=False) as hoard_contents:
            self.op_tree = FolderTree(
                get_pending_operations(hoard_contents, self.remote.uuid),
                lambda op: op.hoard_file.as_posix())

        self.counts = await aggregate_counts(self.op_tree)
        self.ops_cnt = aggregate_on_nodes(
            self.op_tree,
            lambda node: {op_to_str(node.data): node.data.hoard_props.size if self.show_size else 1},
            sum_dicts)

        self.root.data = self.op_tree.root
        self.root.label = self.root.label.append(self._pretty_folder_label_descriptor(self.op_tree.root))
        self.root.expand()

    def on_tree_node_expanded(self, event: Tree[FolderNode[FileOp]].NodeExpanded):
        if event.node in self.expanded:
            return

        self.expanded.add(event.node)

        for _, folder in event.node.data.folders.items():
            folder_label = Text().append(folder.name).append(" ").append(f"({self.counts[folder]})", style="dim")
            cnts_label = self._pretty_folder_label_descriptor(folder)
            event.node.add(folder_label.append(" ").append(cnts_label), data=folder)

        for _, op in event.node.data.files.items():
            event.node.add_leaf(f"{type(op.data)}: {op.data.hoard_file}", data=op)

    def _pretty_folder_label_descriptor(self, folder: FolderNode[FileOp]) -> Text:
        cnts_label = Text().append("{")
        pending = self.ops_cnt[folder]
        if pending is not None:
            for (op_type, order), v in sorted(pending.items(), key=lambda x: x[0][1]):
                cnts_label.append(
                    op_type, style="green" if op_type == "get" else "strike dim" if op_type == "cleanup" else "none") \
                    .append(" ").append(format_size(v) if self.show_size else format_count(v), style="dim").append(",")
        cnts_label.append("}")
        return cnts_label


async def aggregate_counts(op_tree):
    return aggregate_on_nodes(
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
    if isinstance(act, MarkIsAvailableBehavior):
        return "mark_avail", 25
    elif isinstance(act, AddToHoardAndCleanupSameBehavior):
        return "absorb", 5
    elif isinstance(act, AddToHoardAndCleanupNewBehavior):
        return "absorb", 5
    elif isinstance(act, AddNewFileBehavior):
        return "add", 10
    elif isinstance(act, MarkToGetBehavior):
        return "mark_get", 40
    elif isinstance(act, MarkForCleanupBehavior):
        return "mark_deleted", 92
    elif isinstance(act, ResetLocalAsCurrentBehavior):
        return "reset_to_local", 20
    elif isinstance(act, RemoveLocalStatusBehavior):
        return "unmark", 91
    elif isinstance(act, DeleteFileFromHoardBehavior):
        return "delete", 90
    elif isinstance(act, MoveFileBehavior):
        return "move", 30
    else:
        raise ValueError(f"Unsupported action: {act}")


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
            with repo.open_contents(is_readonly=True) as current_contents:
                async with self.hoard.open_contents(create_missing=False) as hoard_contents:
                    preferences = init_pull_preferences(
                        self.remote, assume_current=False, force_fetch_local_missing=False)

                    copy_local_staging_to_hoard(hoard_contents, current_contents)
                    uuid = current_contents.config.uuid
                    await sync_fsobject_to_object_storage(hoard_contents.env, hoard_contents.fsobjects, hoard_config)

                    resolutions = await resolution_to_match_repo_and_hoard(
                        uuid, hoard_contents, pathing, preferences,
                        progress_reporting_it(self, id="hoard-contents-to-pull", max_frequency=10))

                    with StringIO() as other_out:
                        actions = list(calculate_actions(preferences, resolutions, pathing, hoard_config, other_out))
                        logging.debug(other_out.getvalue())

            self.op_tree = FolderTree[Action](actions, lambda action: action.file_being_acted_on.as_posix())

            self.counts = await aggregate_counts(self.op_tree)
            self.ops_cnt = aggregate_on_nodes(
                self.op_tree,
                lambda node: {pull_op_to_str(node.data): node.data.hoard_props.size if self.show_size else 1},
                sum_dicts)

            self.root.label = PENDING_TO_PULL + f" ({len(actions)})"
            self.root.data = self.op_tree.root

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

    def _expand_subtree(self, node: TreeNode[FolderNode[Action]]):
        for _, folder in node.data.folders.items():
            folder_name = Text().append(folder.name).append(" ").append(f"({self.counts[folder]})", style="dim")
            cnts_label = self._pretty_folder_label_descriptor(folder)
            folder_label = folder_name.append(" ").append(cnts_label)
            node.add(folder_label, data=folder)
        for _, file in node.data.files.items():
            node.add_leaf(f"{type(file.data)}: {file.data.file_being_acted_on}", data=node.data)

    def _pretty_folder_label_descriptor(self, folder: FolderNode[FileOp]) -> Text:
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

            self.run_worker(self.recompose())

    @on(Input.Submitted, "#repo-min-copies-before-cleanup")
    def repo_min_copies_before_cleanup_changed(self, event: Input.Changed):
        assert event.input.id == "repo-min-copies-before-cleanup"

        hoard_config = self.hoard.config()
        if hoard_config.remotes[self.remote.uuid].min_copies_before_cleanup != int(event.value):
            hoard_config.remotes[self.remote.uuid].min_copies_before_cleanup = int(event.value)
            hoard_config.write()

            self.remote = hoard_config.remotes[self.remote.uuid]

            self.run_worker(self.recompose())

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
            with StringIO() as out:
                await execute_files_push(
                    self.hoard.config(),
                    self.hoard, [self.remote.uuid], out,
                    progress_reporting_bar(self, "push-to-files-operation", 10))
                logging.info(out.getvalue())

            await self.recompose()

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
                await clear_pending_file_ops(self.hoard, self.remote.uuid, out)
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
            with StringIO() as out:
                preferences = init_pull_preferences(self.remote, assume_current=False, force_fetch_local_missing=False)
                await execute_pull(
                    self.hoard, preferences, ignore_epoch=False, out=out,
                    progress_bar=progress_reporting_it(self, "pull-to-hoard-operation", 10))
                logging.info(out.getvalue())

            await self.recompose()

    @on(Button.Pressed, "#restore_from_hoard")
    @work
    async def restore_from_hoard(self):
        if await self.app.push_screen_wait(
                ConfirmActionScreen(
                    f"Are you sure you want to RESTORE contents from hoard to repo: \n"
                    f"{self.remote.name}({self.remote.uuid}\n"
                    f"")):
            with StringIO() as out:
                preferences = pull_prefs_to_restore_from_hoard(self.remote.uuid)
                await execute_pull(
                    self.hoard, preferences, ignore_epoch=False, out=out,
                    progress_bar=progress_reporting_it(self, "restore-from-hoard-operation", 10))
                logging.info(out.getvalue())

            await self.recompose()


class CaveExplorerScreen(Screen):
    CSS_PATH = "cave_exporer_screen.tcss"

    hoard: Hoard | None = reactive(None, recompose=True)
    remote: HoardRemote | None = reactive(None, recompose=True)

    def on_mount(self):
        if self.hoard is not None:
            selected_remote = config.get("cave_exporer_selected_repo", None)
            if selected_remote is not None:
                self.remote = self.hoard.config().remotes[selected_remote]

    def watch_hoard(self):
        try:
            self.query_one(CaveInfoWidget).hoard = self.hoard
        except NoMatches:
            pass

    def watch_remote(self, new_remote: HoardRemote, old_remote: HoardRemote):
        if self.remote is not None:
            config["cave_exporer_selected_repo"] = self.remote.uuid
            _write_config()
            self.query_one(CaveInfoWidget).remote = self.remote
            self.query_one(f"#uuid-{self.remote.uuid}", RadioButton).toggle()

    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()

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

        yield ProgressReporting()
        yield RichLog(id="cave_explorer_log")

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

        self.remote = self.hoard.config().remotes[uuid]

    async def on_start_progress_reporting(self, event: StartProgressReporting):
        await self.query_one(ProgressReporting).on_start_progress_reporting(event)

    async def on_mark_progress_reporting(self, event: MarkProgressReporting):
        await self.query_one(ProgressReporting).on_mark_progress_reporting(event)
