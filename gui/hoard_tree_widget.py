from command.fast_path import FastPosixPath
from typing import Dict, List

from rich.text import Text
from textual.reactive import reactive, var
from textual.widgets import Tree
from textual.widgets._tree import TreeNode

from config import HoardConfig, HoardRemote
from contents.hoard import HoardContents, HoardDir, HoardFile
from util import group_to_dict, format_size, format_count


class HoardTreeWidget(Tree):
    contents: HoardContents = reactive(None)
    loaded_offset: dict[HoardDir | HoardFile, int] = var(dict())

    def __init__(self, contents: HoardContents, config: HoardConfig):
        super().__init__("Hoard", data=contents.fsobjects.tree.root, id="hoard_tree")
        self.guide_depth = 2
        self.auto_expand = False
        self.contents = contents
        self.select_node(self.root)
        self.root.expand()

        self.mounts: Dict[FastPosixPath, List[HoardRemote]] = group_to_dict(
            config.remotes.all(), key=lambda r: r.mounted_at)
        self.root.set_label(self._create_pretty_folder_label("/", FastPosixPath("/"), 45))

    def _expand_hoard_dir(self, widget_node: TreeNode[HoardDir | HoardFile], hoard_dir: HoardDir, parent_offset: int):
        label_max_width = 45 - parent_offset * widget_node.tree.guide_depth
        for folder in hoard_dir.dirs.values():
            folder_label = self._create_pretty_folder_label(folder.name, FastPosixPath(folder.fullname), label_max_width)
            widget_node.add(folder_label, allow_expand=True, data=folder)

        for file in hoard_dir.files.values():
            size = self.contents.fsobjects[FastPosixPath(file.fullname)].size
            file_label = Text().append(file.name.ljust(label_max_width + 2)).append(f"{format_size(size):>13}", "none")
            widget_node.add(file_label, allow_expand=False, data=file)

    def _create_pretty_folder_label(self, name: str, fullname: FastPosixPath, max_width: int, name_style: str = "bold green"):
        count, size = self.contents.fsobjects.stats_in_folder(fullname)
        folder_name = Text().append(name, name_style).append(self._pretty_count_attached(fullname))
        folder_name.align("left", max_width)
        folder_label = folder_name \
            .append(f"{format_count(count):>6}", "dim") \
            .append(f"{format_size(size):>7}", "none")
        return folder_label

    def _pretty_count_attached(self, fullname: FastPosixPath) -> str:
        return f" ✅{len(self.mounts.get(fullname))}" if self.mounts.get(fullname) is not None else ""

    def on_tree_node_expanded(self, event: Tree[HoardDir | HoardFile].NodeExpanded):
        if event.node.data not in self.loaded_offset:
            self.loaded_offset[event.node.data] = 1 + (
                self.loaded_offset[event.node.parent.data] if event.node.parent is not None else 0)
            self._expand_hoard_dir(event.node, event.node.data, self.loaded_offset[event.node.data])
