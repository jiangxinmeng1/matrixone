#!/usr/bin/env python3
"""Kill one isolated LogService node while an exported snapshot is generating."""

import argparse
import ctypes
import glob
import os
import select
import signal
import struct
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("node", type=int, choices=(0, 1, 2))
    parser.add_argument("--hakeeper", action="store_true", help="watch the internal HAKeeper snapshot instead of shard 1 export")
    parser.add_argument("--timeout", type=float, default=90)
    args = parser.parse_args()
    root = args.root.resolve()
    base = root / f"node-{args.node}" / "logservice-data" / "*" / "host-*" / "*"
    if args.hakeeper:
        pattern = base / "snapshot-part-0" / "snapshot-0-*"
    else:
        pattern = base / "exported-snapshot" / "shard-1" / "replica-*"
    matches = glob.glob(str(pattern))
    if len(matches) != 1:
        raise RuntimeError(f"expected one snapshot parent, found {matches}")
    parent = matches[0]
    pid = int((root / f"node-{args.node}" / "pid").read_text())
    expected = os.path.realpath(root / "mo-service")
    actual = os.path.realpath(f"/proc/{pid}/exe")
    cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode()
    if actual != expected or f"-cfg {root}/log-{args.node}.toml" not in cmdline:
        raise RuntimeError(f"refusing to signal unexpected process {pid}: {actual} {cmdline}")

    libc = ctypes.CDLL(None, use_errno=True)
    fd = libc.inotify_init1(os.O_NONBLOCK)
    if fd < 0:
        raise OSError(ctypes.get_errno(), "inotify_init1")
    try:
        if libc.inotify_add_watch(fd, os.fsencode(parent), 0x100) < 0:
            raise OSError(ctypes.get_errno(), "inotify_add_watch")
        print(f"watching={parent} pid={pid}", flush=True)
        until = time.monotonic() + args.timeout
        while time.monotonic() < until:
            if not select.select([fd], [], [], 1)[0]:
                continue
            data = os.read(fd, 65536)
            offset = 0
            while offset < len(data):
                _, mask, _, name_len = struct.unpack_from("iIII", data, offset)
                name = data[offset + 16:offset + 16 + name_len].split(b"\0", 1)[0].decode()
                offset += 16 + name_len
                if not (mask & 0x100 and name.startswith("snapshot-") and
                        name.endswith(".generating")):
                    continue
                directory = os.path.join(parent, name)
                os.kill(pid, signal.SIGSTOP)
                killed = False
                try:
                    time.sleep(0.02)
                    if os.path.isdir(directory):
                        print(f"hit={name} files={os.listdir(directory)}", flush=True)
                        os.kill(pid, signal.SIGKILL)
                        killed = True
                        print(f"killed={pid}", flush=True)
                        return
                    print(f"missed={name}", flush=True)
                finally:
                    if not killed:
                        try:
                            os.kill(pid, signal.SIGCONT)
                        except ProcessLookupError:
                            pass
        raise TimeoutError("no generating export observed")
    finally:
        os.close(fd)


if __name__ == "__main__":
    main()
