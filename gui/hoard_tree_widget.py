from command.fast_path import FastPosixPath
from typing import Dict, List, Tuple

from rich.text import Text
from textual.reactive import reactive, var
from textual.widgets import Tree
from textual.widgets._tree import TreeNode

from config import HoardConfig, HoardRemote
from contents.hoard import HoardContents, HoardDir, HoardFile
from util import group_to_dict, format_size, format_count

TreeData = HoardDir | HoardFile


class HoardTreeWidget(Tree):
    contents: HoardContents = reactive(None)
    loaded_offset: dict[TreeData, int] = var(dict())

    def __init__(self, contents: HoardContents, config: HoardConfig):
        self.file_nodes: dict[str, Tuple[TreeNode[TreeData], int]] = dict()
        self.mounts: Dict[FastPosixPath, List[HoardRemote]] = group_to_dict(
            config.remotes.all(), key=lambda r: r.mounted_at)

        super().__init__("Hoard (loading...)", data=None, id="hoard_tree")
        self.guide_depth = 2
        self.auto_expand = False
        self.contents = contents

    async def on_mount(self):
        self.root.expand()
        self.run_worker(self._expand_root())

    async def _expand_root(self):
        hoard_tree = await self.contents.fsobjects.tree
        hoard_root = self.root.add(
            self._create_pretty_folder_label("/", FastPosixPath("/"), 45), data=hoard_tree.root, expand=True)

        hoard_root.expand()
        self.root.set_label("Hoard")

    def _expand_hoard_dir(self, widget_node: TreeNode[TreeData], hoard_dir: HoardDir, parent_offset: int):
        label_max_width = 45 - parent_offset * widget_node.tree.guide_depth
        for folder in hoard_dir.dirs.values():
            folder_label = self._create_pretty_folder_label(folder.name, FastPosixPath(folder.fullname),
                                                            label_max_width)
            widget_node.add(folder_label, allow_expand=True, data=folder)

        for file in hoard_dir.files.values():
            size = file.props.size
            file_label = self._pretty_file_label(file, label_max_width, size)
            file_node = widget_node.add_leaf(file_label, data=file)
            self.file_nodes[file.fullname] = file_node, label_max_width

    def _pretty_file_label(self, file, label_max_width, size: int):
        file_label = Text().append(file.name, self.file_name_style(FastPosixPath(file.fullname)))
        file_label.align("left", label_max_width + 2)
        file_label.append(f"{format_size(size):>16}", "none")
        return file_label

    def _create_pretty_folder_label(self, name: str, fullname: FastPosixPath, max_width: int):
        name_style = self.folder_name_style(fullname)
        count, size = self.contents.fsobjects.stats_in_folder(fullname)
        folder_name = Text().append(name, name_style).append(self._pretty_count_attached(fullname))
        folder_name.align("left", max_width)
        folder_label = folder_name \
            .append(f"{format_count(count):>8}", "dim") \
            .append(f"{format_size(size):>8}", "none")
        return folder_label

    def folder_name_style(self, folder_name: FastPosixPath) -> str:
        if self.contents.fsobjects.query.count_non_deleted(folder_name) == 0:
            return "strike dim"
        elif self.contents.fsobjects.query.num_without_source(folder_name) > 0:
            return "red"
        else:
            return "bold green"

    def file_name_style(self, file_name: FastPosixPath) -> str:
        if self.contents.fsobjects.query.is_deleted(file_name):
            return "strike dim"
        else:
            return "none"

    def _pretty_count_attached(self, fullname: FastPosixPath) -> str:
        return f" ✅{len(self.mounts.get(fullname))}" if self.mounts.get(fullname) is not None else ""

    def on_tree_node_expanded(self, event: Tree[TreeData].NodeExpanded):
        if event.node.parent is not None and event.node.data not in self.loaded_offset:
            self.loaded_offset[event.node.data] = 1 + (
                self.loaded_offset[event.node.parent.data] if event.node.parent.data is not None else 0)
            self._expand_hoard_dir(event.node, event.node.data, self.loaded_offset[event.node.data])

    def refresh_file_label(self, hoard_file: HoardFile):
        file_node, label_max_width = self.file_nodes[hoard_file.fullname]
        file_node.set_label(self._pretty_file_label(hoard_file, label_max_width, hoard_file.props.size))

        # TODO also update the file's parents
