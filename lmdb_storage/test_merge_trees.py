import abc
import binascii
import pathlib
from typing import Set, List
from unittest import IsolatedAsyncioTestCase

from command.test_command_file_changing_flows import populate
from command.test_hoard_command import populate_repotypes
from lmdb_storage.file_object import FileObject
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.test_experiment_lmdb import dump_tree
from lmdb_storage.tree_iteration import zip_dfs
from lmdb_storage.tree_structure import Objects, ObjectID, TreeObject

type RootSet = List[ObjectID]


class Merge[F, R]:
    objects: Objects[F]

    @abc.abstractmethod
    def combine(self, obj_ids: RootSet) -> R:  pass

    @abc.abstractmethod
    def should_drill_down(self, obj_ids: RootSet) -> bool: pass


class Sum[F](Merge[F, ObjectID]):
    def __init__(self, objects: Objects[F]):
        self.objects = objects

    def combine(self, obj_ids: RootSet) -> ObjectID:
        """Take the first value that is a file object as the resolved combined value."""
        for obj_id in obj_ids:
            assert not isinstance(obj_id, TreeObject), f"{obj_id} should not be a TreeObject"

            if isinstance(self.objects[obj_id], FileObject):
                return obj_id
        raise ValueError("Can't combine, all seem to be null?!")

    def should_drill_down(self, obj_ids: RootSet) -> bool:
        return any(isinstance(self.objects[obj_id], TreeObject) for obj_id in obj_ids)


def merge_trees[F, R](obj_ids: RootSet, merge: Merge[F, R]) -> R:
    if merge.should_drill_down(obj_ids):
        all_objects = dict((obj_id, merge.objects[obj_id]) for obj_id in obj_ids)
        all_children_names = set(
            sum((list(obj.children.keys()) for obj in all_objects.values() if isinstance(obj, TreeObject)), []))

        if len(all_children_names) == 0:  # none of the folders have any children
            return merge.combine([obj_id for obj_id, obj in all_objects.values() if isinstance(obj, FileObject)])

        new_tree = TreeObject(dict())
        # merging folder names, ignoring files
        for child_name in all_children_names:
            child_obj_ids = [
                all_objects[obj_id].children.get(child_name, None)
                for obj_id in obj_ids if isinstance(all_objects[obj_id], TreeObject)]
            child_obj_ids = [child for child in child_obj_ids if child is not None]
            new_tree.children[child_name] = merge_trees(child_obj_ids, merge)

        new_tree_id = new_tree.id
        merge.objects[new_tree_id] = new_tree

        return new_tree_id
    else:  # should combine on this level
        return merge.combine(obj_ids)


class TestingMergingOfTrees(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = "./tests"
        self.obj_storage_path = f"{self.tmpdir}/test/example.lmdb"

        pathlib.Path(self.obj_storage_path).parent.mkdir(parents=True, exist_ok=True)

        populate(self.tmpdir)
        populate_repotypes(self.tmpdir)

    def test_merge_combining(self):
        env = ObjectStorage(self.obj_storage_path)

        root_ids = sorted(env.all_roots)
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

            merged_id = merge_trees([root_left_id, root_right_id], Sum(objects))
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2),
                ('$ROOT/wat/test.me.3', 2)], dump_tree(objects, merged_id))

            merged_id = merge_trees(root_ids, Sum(objects))
            self.assertEqual([
                ('$ROOT', 1),
                ('$ROOT/test.me.1', 2),
                ('$ROOT/test.me.4', 2),
                ('$ROOT/test.me.5', 2),
                ('$ROOT/wat', 1),
                ('$ROOT/wat/test.me.2', 2),
                ('$ROOT/wat/test.me.3', 2),
                ('$ROOT/wat/test.me.6', 2)], dump_tree(objects, merged_id))
