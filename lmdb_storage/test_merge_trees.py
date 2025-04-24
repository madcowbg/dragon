import binascii
import pathlib
from unittest import IsolatedAsyncioTestCase

from command.test_command_file_changing_flows import populate
from command.test_hoard_command import populate_repotypes
from lmdb_storage.merge_trees import TakeOneFile, merge_trees
from lmdb_storage.object_store import ObjectStorage
from lmdb_storage.test_experiment_lmdb import dump_tree
from lmdb_storage.tree_iteration import zip_dfs


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
