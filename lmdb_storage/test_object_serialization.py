import unittest

from lmdb_storage.file_object import BlobObject
from lmdb_storage.object_serialization import write_stored_object, read_stored_object, construct_tree_object, \
    find_object_data_version, BlobStorageFormat
from lmdb_storage.tree_object import TreeObject


class TestObjectSerialization(unittest.TestCase):
    def test_serialization_version_v0(self):
        self.assertEqual(
            BlobStorageFormat.V0,
            find_object_data_version(b'\x92\x02\x93\xa7asdasda\x01\xc0'))

        self.assertEqual(
            BlobStorageFormat.V0,
            find_object_data_version(b'\x92\x02\x93\xa7asdasda\x01\xa6asdfas'))

        self.assertEqual(
            BlobStorageFormat.V0,
            find_object_data_version(b'\x92\x01\x92\x92\xa4none\xc4\x08yeeeeah!\x92\xa4rock\xc4\x08and roll'))

        self.assertEqual(
            BlobStorageFormat.V0,
            find_object_data_version(b'\x92\x01\x92\x92\xa1a\xc4\x08and roll\x92\xa1z\xc4\x08yeeeeah!'))

    def test_serialize_files_v0(self):
        blob_obj = BlobObject.create("asdasda", 1, None)
        self.assertEqual(b'\x92\x02\x93\xa7asdasda\x01\xc0', write_stored_object(blob_obj))
        self.assertEqual(b'\n\x94\xfb\x8bt*c}B3S-"\xeb8\xe2\x80\x90\x00\xbd', blob_obj.id)

        blob_obj = BlobObject.create("asdasda", 1, "asdfas")
        self.assertEqual(b'\x92\x02\x93\xa7asdasda\x01\xa6asdfas', write_stored_object(blob_obj))
        self.assertEqual(b'\xc3c\x01\n\xd7\xe1\xe1\x8e\x8f\r\xb9\x0b&\x01C\x16\xe6\xcf\xa5\t', blob_obj.id)

        blob_obj = BlobObject.create("", -5, "asdfas")
        self.assertEqual(b'\x92\x02\x93\xa0\xfb\xa6asdfas', write_stored_object(blob_obj))
        self.assertEqual(b'\x9b\x97fcCm\xbeS&\x15\x85\rBA:\xbee\xc8]\x96', blob_obj.id)

        blob_obj = BlobObject.create("asdfas", -5, "")
        self.assertEqual(b'\x92\x02\x93\xa6asdfas\xfb\xa0', write_stored_object(blob_obj))
        self.assertEqual(b'Pq\x81\x17\xff\x11\xa7Y7\xedX\xa0.\xa8\xbd\xb5\xaaK\xa0\xb5', blob_obj.id)

        blob_obj = BlobObject.create("asdfas", 756181684685, "")
        self.assertEqual(
            b'\x92\x02\x93\xa6asdfas\xcf\x00\x00\x00\xb0\x0f\xf0\xd1\xcd\xa0', write_stored_object(blob_obj))
        self.assertEqual(b"H\xc4C\xbd\x0b\xf3'\xf7\xcdp\xdeO\x05\x8f\xf0%\xec\t\xc9f", blob_obj.id)

    def test_deserialize_files_pairs_v0(self):
        blob_obj = read_stored_object(
            b'\n\x94\xfb\x8bt*c}B3S-"\xeb8\xe2\x80\x90\x00\xbd',
            b'\x92\x02\x93\xa7asdasda\x01\xc0')
        self.assertEqual(BlobObject.create("asdasda", 1, None), blob_obj)

        blob_obj = read_stored_object(
            b'\xc3c\x01\n\xd7\xe1\xe1\x8e\x8f\r\xb9\x0b&\x01C\x16\xe6\xcf\xa5\t',
            b'\x92\x02\x93\xa7asdasda\x01\xa6asdfas')
        self.assertEqual(BlobObject.create("asdasda", 1, "asdfas"), blob_obj)

        blob_obj = read_stored_object(
            b'\x9b\x97fcCm\xbeS&\x15\x85\rBA:\xbee\xc8]\x96',
            b'\x92\x02\x93\xa0\xfb\xa6asdfas')
        self.assertEqual(BlobObject.create("", -5, "asdfas"), blob_obj)

        blob_obj = read_stored_object(
            b"H\xc4C\xbd\x0b\xf3'\xf7\xcdp\xdeO\x05\x8f\xf0%\xec\t\xc9f",
            b'\x92\x02\x93\xa6asdfas\xcf\x00\x00\x00\xb0\x0f\xf0\xd1\xcd\xa0')
        self.assertEqual(BlobObject.create("asdfas", 756181684685, ""), blob_obj)

    def test_deserialize_files_hacked_ids_v0(self):
        blob_obj = read_stored_object(
            b'alabala',
            b'\x92\x02\x93\xa7asdasda\x01\xc0')
        self.assertEqual(BlobObject(b'alabala', ("asdasda", 1, None)), blob_obj)

        blob_obj = read_stored_object(
            b'sweet',
            b'\x92\x02\x93\xa7asdasda\x01\xa6asdfas')
        self.assertEqual(BlobObject(b"dude", ("asdasda", 1, "asdfas")), blob_obj)

    def test_serialize_trees_v0(self):
        tree_obj = construct_tree_object({})
        self.assertEqual(b'\x92\x01\x90', write_stored_object(tree_obj))
        self.assertEqual(b'\xa8\x0f\x91\xbcH\x85\n\x1f\xb3E\x9b\xb7k\x9fc\x08\xd4\xd3W\x10', tree_obj.id)

        tree_obj = construct_tree_object({"some": b'whatthehell'})
        self.assertEqual(b'\x92\x01\x91\x92\xa4some\xc4\x0bwhatthehell', write_stored_object(tree_obj))
        self.assertEqual(b'\x89?a\xde\x13\x96\xaa\xce\xc88XC\xc7\xad!B!\x90\xca\x1f', tree_obj.id)

        tree_obj = construct_tree_object({"none": b'yeeeeah!', 'rock': b'and roll'})
        self.assertEqual(b'\x92\x01\x92\x92\xa4none\xc4\x08yeeeeah!\x92\xa4rock\xc4\x08and roll',
                         write_stored_object(tree_obj))
        self.assertEqual(b'\xb7|\x1e"\x1a\x99f\x06Q\x8f\xdf\xa3\x80\xd8yj\xedN\xeb\xfa', tree_obj.id)

        tree_obj = construct_tree_object({"z": b'yeeeeah!', 'a': b'and roll'})
        self.assertEqual(
            b'\x92\x01\x92\x92\xa1a\xc4\x08and roll\x92\xa1z\xc4\x08yeeeeah!',
            write_stored_object(tree_obj))
        self.assertEqual(b'Xh\xbcZ\xfb\xee\x04\x1b\xf6\x13QI\x19u\xd0L\x0f\x12\x14\x01', tree_obj.id)

    def test_deserialize_trees_v0(self):
        tree_obj = read_stored_object(
            b'\xa8\x0f\x91\xbcH\x85\n\x1f\xb3E\x9b\xb7k\x9fc\x08\xd4\xd3W\x10',
            b'\x92\x01\x90')
        self.assertEqual(construct_tree_object({}), tree_obj)

        tree_obj = read_stored_object(
            b'\x89?a\xde\x13\x96\xaa\xce\xc88XC\xc7\xad!B!\x90\xca\x1f',
            b'\x92\x01\x91\x92\xa4some\xc4\x0bwhatthehell')
        self.assertEqual(construct_tree_object({"some": b'whatthehell'}), tree_obj)

        tree_obj = read_stored_object(
            b'\xb7|\x1e"\x1a\x99f\x06Q\x8f\xdf\xa3\x80\xd8yj\xedN\xeb\xfa',
            b'\x92\x01\x92\x92\xa4none\xc4\x08yeeeeah!\x92\xa4rock\xc4\x08and roll')
        self.assertEqual(construct_tree_object({"none": b'yeeeeah!', 'rock': b'and roll'}), tree_obj)

        tree_obj = read_stored_object(
            b'Xh\xbcZ\xfb\xee\x04\x1b\xf6\x13QI\x19u\xd0L\x0f\x12\x14\x01',
            b'\x92\x01\x92\x92\xa1a\xc4\x08and roll\x92\xa1z\xc4\x08yeeeeah!')
        self.assertEqual(construct_tree_object({"z": b'yeeeeah!', 'a': b'and roll'}), tree_obj)

    def test_deserialize_trees_hacked_ids_v0(self):
        tree_obj = read_stored_object(
            b'yeah',
            b'\x92\x01\x90')
        self.assertEqual(TreeObject(b'yeah', {}), tree_obj)

        tree_obj = read_stored_object(
            b'oooh baby',
            b'\x92\x01\x91\x92\xa4some\xc4\x0bwhatthehell')
        self.assertEqual(TreeObject(b'oooh baby', {"some": b'whatthehell'}), tree_obj)

        tree_obj = read_stored_object(
            b'taka nidei',
            b'\x92\x01\x92\x92\xa4none\xc4\x08yeeeeah!\x92\xa4rock\xc4\x08and roll')
        self.assertEqual(TreeObject(b'taka nidei', {"none": b'yeeeeah!', 'rock': b'and roll'}), tree_obj)

        tree_obj = read_stored_object(
            b'',
            b'\x92\x01\x92\x92\xa1a\xc4\x08and roll\x92\xa1z\xc4\x08yeeeeah!')
        self.assertEqual(TreeObject(b'', {"z": b'yeeeeah!', 'a': b'and roll'}), tree_obj)


if __name__ == '__main__':
    unittest.main()
