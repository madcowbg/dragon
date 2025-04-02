import os
from pathlib import PurePosixPath

import humanize
from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Label, DataTable

from command.contents.command import augment_statuses
from command.pathing import HoardPathing
from config import HoardConfig
from contents.hoard import HoardFile, HoardDir, HoardContents
from contents.hoard_props import HoardFileProps, HoardFileStatus
from util import pretty_truncate, format_size, group_to_dict, format_count


class NodeDescription(Widget):
    hoard_item: HoardFile | HoardDir | None = reactive(None, recompose=True)

    def __init__(self, hoard_contents: HoardContents, hoard_config: HoardConfig, hoard_pathing: HoardPathing):
        super().__init__()
        self.hoard_contents = hoard_contents
        self.hoard_config = hoard_config
        self.hoard_pathing = hoard_pathing

    def compose(self) -> ComposeResult:
        if self.hoard_item is None:
            yield Label("Please select an item on the left")
        elif isinstance(self.hoard_item, HoardDir):
            hoard_dir = self.hoard_item
            yield Label(f"Folder name: {hoard_dir.name}")
            yield Label(f"Hoard path: {hoard_dir.fullname}")

            yield Label(f"Addressable on repos", classes="desc_section")
            for hoard_remote in self.hoard_config.remotes.all():
                local_path = self.hoard_pathing.in_hoard(hoard_dir.fullname).at_local(hoard_remote.uuid)
                if local_path is not None:
                    availability_status_class = "status_available" \
                        if os.path.isdir(local_path.on_device_path()) else "status_not_available"
                    yield Horizontal(
                        Label(
                            hoard_remote.name,
                            classes=" ".join([
                                "repo_name",
                                availability_status_class])),
                        Label(
                            f"[@click=app.open_cave_dir('{local_path.on_device_path()}')]{pretty_truncate(hoard_remote.uuid, 15)}[/]",
                            classes="repo_uuid"),
                        Label(Text(self.hoard_pathing.in_local("", hoard_remote.uuid).on_device_path()),
                              classes=f"remote_location {availability_status_class}"),
                        Label(Text(local_path.as_pure_path.as_posix()), classes="local_path"),
                        classes="desc_status_line")

            yield Label(f"Storage on repos {hoard_dir.fullname}", classes="desc_section")
            statuses = self.hoard_contents.fsobjects.status_by_uuid(PurePosixPath(hoard_dir.fullname))
            available_states, statuses_sorted = augment_statuses(
                self.hoard_config, self.hoard_contents, False, statuses)
            all_stats = [
                "total",
                HoardFileStatus.AVAILABLE.value, HoardFileStatus.GET.value, HoardFileStatus.COPY.value,
                HoardFileStatus.MOVE.value, HoardFileStatus.CLEANUP.value]

            data_table = DataTable()
            yield data_table
            data_table.add_columns("name",  *all_stats)
            for name, uuid, updated_maybe, uuid_stats in statuses_sorted:
                data_table.add_row(
                    name,
                    *(format_count(uuid_stats[stat]["nfiles"]) if stat in uuid_stats else "" for stat in all_stats))

            data_table.add_row()
            for name, uuid, updated_maybe, uuid_stats in statuses_sorted:
                data_table.add_row(
                    name,
                    *(format_size(uuid_stats[stat]["size"]) if stat in uuid_stats else "" for stat in all_stats))

        elif isinstance(self.hoard_item, HoardFile):
            hoard_file = self.hoard_item
            yield Label(f"File name: {hoard_file.name}")
            yield Label(f"Hoard path: {hoard_file.fullname}")

            hoard_props = self.hoard_contents.fsobjects[PurePosixPath(hoard_file.fullname)]
            assert isinstance(hoard_props, HoardFileProps)

            yield Label(f"size = {format_size(hoard_props.size)}", classes="desc_line")
            yield Label(f"fasthash = {hoard_props.fasthash}", classes="desc_line")

            presence = hoard_props.presence
            by_presence = group_to_dict(presence.keys(), key=lambda uuid: presence[uuid])

            yield Label("Statuses per repo", classes="desc_section")
            for status, repos in by_presence.items():
                yield Label(f"Repos where status = {status.value.upper()}")
                for repo_uuid in repos:
                    hoard_remote = self.hoard_config.remotes[repo_uuid]
                    full_local_path = self.hoard_pathing.in_hoard(hoard_file.fullname) \
                        .at_local(repo_uuid).on_device_path()
                    yield Horizontal(
                        Label(
                            hoard_remote.name,
                            classes=" ".join([
                                "repo_name",
                                "status_available" if os.path.isfile(full_local_path) else "status_not_available"])),
                        Label(
                            f"[@click=app.open_cave_file('{full_local_path}')]{repo_uuid}[/]",
                            classes="repo_uuid"),
                        classes="desc_status_line")

        else:
            raise ValueError(f"unknown hoard item type: {type(self.hoard_item)}")
