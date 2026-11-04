#!/usr/bin/env python3
"""Repair one stopped test store's duplicate shard metadata after saving a backup.

This is only for the isolated chaos cluster. It preserves all protobuf fields
except LogStore.Shards entries not explicitly listed on the command line.
"""

import argparse
import hashlib
from pathlib import Path


def varint(buf: bytes, pos: int) -> tuple[int, int]:
    value = shift = 0
    while pos < len(buf):
        byte = buf[pos]
        pos += 1
        value |= (byte & 127) << shift
        if byte < 128:
            return value, pos
        shift += 7
    raise ValueError("incomplete varint")


def fields(buf: bytes):
    pos = 0
    while pos < len(buf):
        start = pos
        tag, pos = varint(buf, pos)
        field, wire = tag >> 3, tag & 7
        if wire == 0:
            value, pos = varint(buf, pos)
        elif wire == 2:
            size, pos = varint(buf, pos)
            value = buf[pos:pos + size]
            pos += size
            if len(value) != size:
                raise ValueError("truncated field")
        else:
            raise ValueError(f"unsupported wire type {wire}")
        yield field, wire, value, buf[start:pos]


def shard_identity(raw: bytes) -> tuple[int, int]:
    shard_id = 0  # Proto3 omits the zero-valued HAKeeper ShardID.
    replica_id = None
    for field, wire, value, _ in fields(raw):
        if field == 1 and wire == 2:
            for child_field, child_wire, child_value, _ in fields(value):
                if child_field == 1 and child_wire == 0:
                    shard_id = child_value
        elif field == 2 and wire == 0:
            replica_id = value
    if replica_id is None:
        raise ValueError("missing shard identity")
    return shard_id, replica_id


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("metadata", type=Path)
    parser.add_argument("--keep", action="append", required=True,
                        help="shard:replica from the pre-shutdown member table")
    args = parser.parse_args()
    wanted = {tuple(map(int, item.split(":"))) for item in args.keep}
    if len(wanted) != len(args.keep):
        raise ValueError("duplicate keep entry")
    path = args.metadata
    backup = path.with_name(path.name + ".before-cold-recovery")
    if backup.exists():
        raise FileExistsError(backup)
    data = path.read_bytes()
    if len(data) < 8 or data[:8] != hashlib.md5(data[8:]).digest()[8:]:
        raise ValueError("metadata checksum mismatch")
    retained = []
    removed = []
    output = bytearray()
    for field, wire, value, raw in fields(data[8:]):
        if field == 2 and wire == 2:
            identity = shard_identity(value)
            (retained if identity in wanted else removed).append(identity)
            if identity not in wanted:
                continue
        output.extend(raw)
    if set(retained) != wanted or len(retained) != len(wanted):
        raise ValueError(f"expected {wanted}, retained {retained}")
    backup.write_bytes(data)
    new = hashlib.md5(output).digest()[8:] + output
    temp = path.with_name(path.name + ".cold-recovery.tmp")
    temp.write_bytes(new)
    temp.replace(path)
    print(f"backup={backup} retained={retained} removed={removed}")


if __name__ == "__main__":
    main()
