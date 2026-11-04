#!/usr/bin/env python3
"""Repeat an old-directory HAKeeper bootstrap zombie reproduction."""

import argparse
import json
import re
import subprocess
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--target", type=int, default=0)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--old-id", type=int, default=131072)
    parser.add_argument("--raft-marker", default="[00000:31072]",
                        help="Dragonboat's log prefix for the old replica")
    parser.add_argument("--peer-addresses", required=True,
                        help="three current voting stores' Logservice RPC addresses")
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    root = args.root.resolve()
    node = root / "node.sh"
    client = root / "chaos-client"
    logfile = root / f"node-{args.target}" / "stdout.log"
    metadata = next((root / f"node-{args.target}").rglob("mo-logservice.metadata"))

    def varint(data: bytes, offset: int) -> tuple[int, int]:
        value = shift = 0
        while True:
            byte = data[offset]
            offset += 1
            value |= (byte & 0x7f) << shift
            if byte < 0x80:
                return value, offset
            shift += 7

    def fields(data: bytes):
        offset = 0
        while offset < len(data):
            tag, offset = varint(data, offset)
            field, wire = tag >> 3, tag & 7
            if wire == 0:
                value, offset = varint(data, offset)
            elif wire == 2:
                size, offset = varint(data, offset)
                value = data[offset:offset + size]
                offset += size
            else:
                raise ValueError(f"unexpected metadata wire type {wire}")
            yield field, wire, value

    def metadata_ids() -> set[int]:
        # The first eight bytes are the checksum. LogStore.Shards is field 2;
        # each LogShard.ReplicaID is also field 2.
        data = metadata.read_bytes()[8:]
        return {value for field, _, shard in fields(data) if field == 2
                for field, wire, value in fields(shard) if field == 2 and wire == 0}

    def call(*argv: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(argv, cwd=root, text=True, capture_output=True, timeout=15)

    def status() -> list[str]:
        result = call("bash", str(node), str(root), "status", str(args.target))
        if result.returncode:
            raise RuntimeError(result.stderr)
        return result.stdout.splitlines()

    def peer_views(shard: int) -> list[str]:
        result = call(str(client), "-addresses", args.peer_addresses,
                      "-op", "status", "-shard", str(shard))
        if result.returncode:
            raise RuntimeError(result.stderr)
        lines = [line for line in result.stdout.splitlines() if line.startswith("address=")]
        if len(lines) != 3:
            raise RuntimeError(f"shard {shard}: expected three peer replies: {lines}")
        for line in lines:
            if "ok=true" not in line or f"{args.old_id}:" in line or len(re.findall(r"\d+:[\d.]+:\d+", line)) != 3:
                raise RuntimeError(f"shard {shard}: invalid current membership: {line}")
        return lines

    if "stopped" not in " ".join(status()):
        raise RuntimeError("target must be stopped before the experiment")
    if args.old_id in metadata_ids():
        raise RuntimeError("old ID is still in local metadata; precondition not met")
    for shard in (0, 1):
        peer_views(shard)

    for round_no in range(args.rounds):
        if args.old_id in metadata_ids():
            raise RuntimeError(f"round {round_no}: old ID still in metadata")
        offset = logfile.stat().st_size
        result = call("bash", str(node), str(root), "start", str(args.target))
        if result.returncode:
            raise RuntimeError(result.stderr)
        print(f"round={round_no} {result.stdout.strip()}", flush=True)
        replay = prevote = killed = cleaned = False
        first = last = ""
        try:
            deadline = time.monotonic() + args.timeout
            while time.monotonic() < deadline:
                with logfile.open("rb") as stream:
                    stream.seek(offset)
                    chunk = stream.read()
                    offset = stream.tell()
                for line in chunk.decode(errors="replace").splitlines():
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    msg = event.get("msg", "")
                    if args.raft_marker in msg:
                        replay |= "replaying raft logs" in msg
                        prevote |= "PreVote candidate" in msg
                        first = first or event.get("time", "")
                        last = event.get("time", "")
                    if f":0:{args.old_id}:0" in msg and "applying cmd: L/Kill" in msg:
                        killed = True
                        last = event.get("time", "")
                if replay and killed and args.old_id not in metadata_ids():
                    cleaned = True
                    break
                time.sleep(0.25)
            for shard in (0, 1):
                peer_views(shard)
            print(f"round={round_no} replay={replay} prevote={prevote} "
                  f"kill={killed} metadata_clean={cleaned} first={first} last={last}", flush=True)
            if not replay or not killed or not cleaned:
                raise RuntimeError(f"round {round_no}: did not reproduce startup and cleanup")
        finally:
            result = call("bash", str(node), str(root), "kill", str(args.target))
            if result.returncode:
                raise RuntimeError(result.stderr)
            print(f"round={round_no} {result.stdout.strip()}", flush=True)
        time.sleep(2)


if __name__ == "__main__":
    main()
