import abc
import binascii
import pathlib
from typing import List, Dict, Callable
from unittest import IsolatedAsyncioTestCase

from command.test_command_file_changing_flows import populate
from command.test_hoard_command import populate_repotypes
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.test_experiment_lmdb import dump_tree
from lmdb_storage.tree_iteration import zip_dfs
from lmdb_storage.tree_structure import Objects, ObjectID, TreeObject
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


class TestingMergingOfTrees(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = "./tests"
        self.obj_storage_path = f"{self.tmpdir}/test/example.lmdb"

        pathlib.Path(self.obj_storage_path).parent.mkdir(parents=True, exist_ok=True)

        populate(self.tmpdir)
        populate_repotypes(self.tmpdir)

    def test_merge_combining(self):
        env = ObjectStorage(self.obj_storage_path)

        root_ids = sorted(env.roots(write=False).all_live)
        self.assertEqual([
            b'89527b0fa576e127d04089d9cb5aab0e5619696d',
            b'9fbdcfe094f258f954ba6f65c4a3641d25b32e06',
            b'a80f91bc48850a1fb3459bb76b9f6308d4d35710',
            b'd995800c80add686a027bac8628ca610418c64b6',
            b'f6a74030fa0a826b18e424d44f8aca9be8c657f3'], [binascii.hexlify(r) for r in root_ids])

        root_left_id = binascii.unhexlify(b'f6a74030fa0a826b18e424d44f8aca9be8c657f3')
        root_right_id = binascii.unhexlify(b'9fbdcfe094f258f954ba6f65c4a3641d25b32e06')
        root_third_id = binascii.unhexlify(b'89527b0fa576e127d04089d9cb5aab0e5619696d')

        with env.objects(write=True) as objects:
            diffs = [
                (path, diff_type.value)
                for path, diff_type, left_id, right_id, should_skip
                in zip_dfs(objects, "", root_left_id, root_right_id)]

            self.assertEqual([
                ('', 'different'),
                ('/test.me.1', 'same'),
                ('/wat', 'different'),
                ('/wat/test.me.2', 'right_missing'),
                ('/wat/test.me.3', 'left_missing')], diffs)

            merged = merge_trees(
                dict((binascii.hexlify(it).decode(), it) for it in [root_left_id, root_right_id]),
                TakeOneFile(objects))
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, '1881f6f9784fb08bf6690e9763b76ac3'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, 'd6dcdb1bc4677aab619798004537c4e3'),
                ('$ROOT/wat/test.me.3', 2, '7c589c09e2754a164ba2e8f06feac897')],
                dump_tree(objects, merged["MERGED"], show_fasthash=True))

            merged = merge_trees(dict((binascii.hexlify(it).decode(), it) for it in root_ids), TakeOneFile(objects))
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2, '1881f6f9784fb08bf6690e9763b76ac3'),
                ('$ROOT/test.me.4', 2, '6228a39ea262e9797f8efef82cd0eeba'),
                ('$ROOT/test.me.5', 2, 'ac8419ee7f30e5ba4da89914da71b299'),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2, 'd6dcdb1bc4677aab619798004537c4e3'),
                ('$ROOT/wat/test.me.3', 2, '7c589c09e2754a164ba2e8f06feac897'),
                ('$ROOT/wat/test.me.6', 2, 'c907b68b6a1f18c6135c112be53c978b')],
                dump_tree(objects, merged["MERGED"], show_fasthash=True))
