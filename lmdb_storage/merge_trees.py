import abc
from typing import List, Dict, Callable

from lmdb_storage.tree_structure import Objects, TreeObject, ObjectID
from util import group_to_dict

type ObjectsByRoot = Dict[str, ObjectID]


class Merge[F]:
    objects: Objects[F]

    @abc.abstractmethod
    def combine(self, path: List[str], children: Dict[str, ObjectsByRoot], files: ObjectsByRoot) -> ObjectsByRoot:
        """Calculates values for the combined path by working on trees and files that are attached to this path."""
        pass

    @abc.abstractmethod
    def should_drill_down(self, path: List[str], trees: ObjectsByRoot, files: ObjectsByRoot) -> bool:
        pass


class TakeOneFile[F](Merge[F]):
    def __init__(self, objects: Objects[F]):
        self.objects = objects

    def combine(self, path: List[str], children: Dict[str, ObjectsByRoot], files: ObjectsByRoot) -> ObjectsByRoot:
        """Take the first value that is a file object as the resolved combined value."""
        if len(files) > 0:  # prioritize taking the first file
            file_iter = files.values().__iter__()  # fixme take with priority
            return {"MERGED": file_iter.__next__()}
        else:  # we are merging a tree
            assert len(children) > 0, f"Should not have drilled down as we already have files: {files}"

            result = TreeObject(dict())
            for child_name, merged_child in children.items():
                result.children[child_name] = merged_child["MERGED"]

            result_id = result.id
            self.objects[result_id] = result
            return {"MERGED": result_id}

    def should_drill_down(self, path: List[str], trees: ObjectsByRoot, files: ObjectsByRoot) -> bool:
        return len(files) == 0  # as we prioritize taking the first file


def split_by_object_type[F](objects: Objects[F], obj_ids: ObjectsByRoot) -> (ObjectsByRoot, ObjectsByRoot):
    files = dict((name, obj_id) for name, obj_id in obj_ids.items() if type(objects[obj_id]) is not TreeObject)
    trees = dict((name, obj_id) for name, obj_id in obj_ids.items() if type(objects[obj_id]) is TreeObject)
    return trees, files


def remap[A, B, C](dictionary: Dict[A, B], key: Callable[[B], C]) -> Dict[A, C]:
    return dict((k, key(v)) for k, v in dictionary.items())


def merge_trees_recursively[F](
        merge: Merge[F], path: List[str], trees: ObjectsByRoot, files: ObjectsByRoot) -> ObjectsByRoot:
    trees_objects = remap(trees, lambda obj_id: merge.objects[obj_id])

    # merging child folders first
    all_children = [
        (child_name, tree_root, child_obj_id)
        for tree_root, tree_obj in trees_objects.items()
        for child_name, child_obj_id in tree_obj.children.items()]

    # group by child name first
    merged_children: Dict[str, ObjectsByRoot] = dict()
    child_name_to_tree_root_and_obj = group_to_dict(all_children, lambda cto: cto[0], map_to=lambda cto: cto[1:])
    for child_name, tree_root_to_obj_id in child_name_to_tree_root_and_obj.items():
        all_objects_in_name: ObjectsByRoot = dict(tree_root_to_obj_id)

        if merge.should_drill_down(path, trees, files):
            sub_trees, sub_files = split_by_object_type(merge.objects, all_objects_in_name)
            merged_children[child_name] = merge_trees_recursively(merge, path + [child_name], sub_trees, sub_files)
        else:
            merged_children[child_name] = all_objects_in_name

    return merge.combine(path, merged_children, files)


def merge_trees[F](obj_ids: ObjectsByRoot, merge: Merge[F]) -> ObjectsByRoot:
    assert isinstance(obj_ids, Dict)

    trees, files = split_by_object_type(merge.objects, obj_ids)

    return merge_trees_recursively(merge, [], trees, files)
