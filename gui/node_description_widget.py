import logging
import os

from rich.text import Text
from textual.app import ComposeResult
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Label, DataTable

from command.contents.command import augment_statuses
from command.fast_path import FastPosixPath
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
            yield from self._compose_dir(self.hoard_item)
        elif isinstance(self.hoard_item, HoardFile):
            yield from self._compose_file(self.hoard_item)
        else:
            raise ValueError(f"unknown hoard item type: {type(self.hoard_item)}")

    def _compose_file(self, hoard_file):
        yield Label(f"File name: {hoard_file.name}")
        yield Label(f"Hoard path: {hoard_file.fullname}")
        hoard_props = self.hoard_contents.fsobjects[FastPosixPath(hoard_file.fullname)]
        assert isinstance(hoard_props, HoardFileProps)
        yield Label(f"size = {format_size(hoard_props.size)}", classes="desc_line")
        yield Label(f"fasthash = {hoard_props.fasthash}", classes="desc_line")
        presence = hoard_props.presence
        by_presence = group_to_dict(presence.keys(), key=lambda uuid: presence[uuid])
        yield Label("Statuses per repo", classes="desc_section")
        data_table = DataTable()
        yield data_table
        data_table.add_columns("repo", "status", "uuid", "path")
        for status, repos in sorted(by_presence.items(), key=lambda kv: file_status_order(kv[0])):
            for repo_uuid in repos:
                hoard_remote = self.hoard_config.remotes[repo_uuid]
                local_path = self.hoard_pathing.in_hoard(FastPosixPath(hoard_file.fullname)).at_local(repo_uuid)
                on_device_path = local_path.on_device_path()
                is_file_present = os.path.isfile(on_device_path)

                data_table.add_row(
                    Text().append(hoard_remote.name, "green" if is_file_present else "dim strike"),
                    Text().append(status.value),
                    Text.from_markup(
                        f"[@click=app.open_cave_file('{on_device_path}')]{pretty_truncate(hoard_remote.uuid, 15)}[/]", style='u'),
                    Text(
                        self.hoard_pathing.in_local(FastPosixPath(""), hoard_remote.uuid).on_device_path().as_posix(),
                        style="green" if is_file_present else "strike").append(
                        local_path.as_pure_path.simple, style="normal")
                )

    def _compose_dir(self, hoard_dir: HoardDir):
        yield Label(f"Folder name: {hoard_dir.name}")
        yield Label(f"Hoard path: {hoard_dir.fullname}")
        yield Label(f"Addressable on repos", classes="desc_section")
        data_table = DataTable()
        yield data_table
        data_table.add_columns("repo", "uuid", "path")
        for hoard_remote in self.hoard_config.remotes.all():
            local_path = self.hoard_pathing.in_hoard(FastPosixPath(hoard_dir.fullname)).at_local(hoard_remote.uuid)
            if local_path is not None:
                path_on_device = local_path.on_device_path()
                is_dir_present = os.path.isdir(path_on_device.as_posix())
                data_table.add_row(
                    Text(
                        hoard_remote.name,
                        style="green" if is_dir_present else "strike"),
                    Text.from_markup(
                        f"[@click=app.open_cave_dir('{path_on_device.as_posix()}')]{pretty_truncate(hoard_remote.uuid, 15)}[/]",
                        style="u"),
                    Text(
                        self.hoard_pathing.in_local(FastPosixPath(""), hoard_remote.uuid).on_device_path().as_posix(),
                        style="green" if is_dir_present else "strike").append(
                        local_path.as_pure_path.simple, style="normal"))
        yield Label(f"Storage on repos {hoard_dir.fullname}", classes="desc_section")
        statuses = self.hoard_contents.fsobjects.status_by_uuid(FastPosixPath(hoard_dir.fullname))
        available_states, statuses_sorted = augment_statuses(
            self.hoard_config, self.hoard_contents, False, statuses)
        all_stats = [
            "total",
            HoardFileStatus.AVAILABLE.value, HoardFileStatus.GET.value, HoardFileStatus.COPY.value,
            HoardFileStatus.MOVE.value, HoardFileStatus.CLEANUP.value]
        data_table = DataTable()
        yield data_table
        data_table.add_columns("name", *all_stats)
        for name, uuid, updated_maybe, uuid_stats in statuses_sorted:
            data_table.add_row(
                name,
                *(Text(format_count(uuid_stats[stat]["nfiles"]), justify="right") if stat in uuid_stats else "" for stat
                  in all_stats))
        data_table.add_row()
        for name, uuid, updated_maybe, uuid_stats in statuses_sorted:
            data_table.add_row(
                name,
                *(Text(format_size(uuid_stats[stat]["size"]), justify="right") if stat in uuid_stats else "" for stat in
                  all_stats))


def file_status_order(status: HoardFileStatus):
    if status == HoardFileStatus.AVAILABLE:
        return 1
    elif status == HoardFileStatus.GET:
        return 2
    elif status == HoardFileStatus.MOVE:
        return 3
    elif status == HoardFileStatus.COPY:
        return 4
    elif status == HoardFileStatus.CLEANUP:
        return 10
    else:
        assert status == HoardFileStatus.UNKNOWN
        return 100
