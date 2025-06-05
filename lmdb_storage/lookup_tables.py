from typing import Iterable, Tuple, List, Callable, Dict

from varint import decode_buffer

type CompressedPath = List[int]


def _read_packed[LookupData](packed_lookup_data) -> Tuple[Dict[bytes, List[int] | List[LookupData]], bytes | None]:
    if len(packed_lookup_data) == 0:
        return dict(), None

    root_id = bytes(packed_lookup_data[:20])
    idx = 20
    lookup_table: Dict[bytes, List[int] | List[LookupData]] = dict()
    while idx < len(packed_lookup_data):
        prefix = bytes(packed_lookup_data[idx:idx + 20])
        assert type(prefix) is bytes
        idx += 20

        if prefix not in lookup_table:
            lookup_table[prefix] = [idx]
        else:
            lookup_table[prefix].append(idx)

        cnt, idx = decode_buffer(packed_lookup_data, idx)  # find size of path
        idx += cnt
    return lookup_table, root_id


class LookupTable[LookupData]:
    def __init__(self, packed_lookup_data: bytes, reader: Callable[[bytes, int], LookupData]):
        lookup_table, root_id = _read_packed(packed_lookup_data)

        self.root_id = root_id
        self._lookup_table = lookup_table
        self._packed_lookup_data = packed_lookup_data

        self._decoded_lookup_data = dict()
        self._reader = reader

    def __str__(self):
        return f"LookupTable[{len(self)}, root_id={self.root_id.hex() if self.root_id else None}]"

    def __len__(self) -> int:
        return len(self._lookup_table)

    def __getitem__(self, obj_id: bytearray | bytes) -> List[LookupData]:
        hash_prefix = bytes(obj_id) if isinstance(obj_id, bytearray) else obj_id
        if hash_prefix not in self._lookup_table:
            return []

        idxs = self._lookup_table[hash_prefix]
        if isinstance(idxs[0], int):  # convert the list of ints to the list of unpacked paths
            idxs = [self._reader(self._packed_lookup_data, idx)[1] for idx in idxs]
            self._lookup_table[hash_prefix] = idxs
        return idxs

    def __contains__(self, obj_id: bytes) -> bool:
        return bytes(obj_id) in self._lookup_table

    def keys(self) -> Iterable[bytes]:
        return self._lookup_table.keys()
