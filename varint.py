from typing import Tuple


def encode(number):
    """Pack `number` into varint bytes"""
    buf = bytearray()
    while True:
        towrite = number & 0x7f
        number >>= 7
        if number:
            buf.append(towrite | 0x80)
        else:
            buf.append(towrite)
            break
    return buf


def decode_buffer(buffer: bytes, offset: int) -> Tuple[int, int]:
    """Read a varint from `stream`"""
    shift = 0
    result = 0
    while True:
        i = buffer[offset]
        offset += 1
        result |= (i & 0x7f) << shift
        shift += 7
        if not (i & 0x80):
            break

    return result, offset
