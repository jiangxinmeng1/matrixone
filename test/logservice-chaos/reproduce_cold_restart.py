#!/usr/bin/env python3
"""Repeat the duplicate-shard cold-start failure in the isolated 51 cluster."""

import argparse
import hashlib
import shutil
import subprocess
import time
from pathlib import Path


UUIDS = {
    1: "58e9b83a-b015-5d34-8004-a79c4f063e37",
    2: "a6c44d9f-12f8-591e-beb6-5a869f699ae8",
}
ADDRESSES = "127.0.0.1:56411,127.0.0.1:56421,127.0.0.1:56431"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--first", type=int, default=9451)
    args = parser.parse_args()
    root = args.root.resolve()
    if root.name != "logservice-chaos-20261103-d61a7206":
        raise ValueError("refusing to operate outside the dedicated test cluster")
    node_script = root / "node.sh"
    client = root / "chaos-client"
    metadata = {
        i: root / f"node-{i}/logservice-data/{uuid}/mo-logservice.metadata"
        for i, uuid in UUIDS.items()
    }
    stale = {i: path.with_name(path.name + ".before-cold-recovery")
             for i, path in metadata.items()}
    clean = {i: root / f"cold-repeat-node{i}.clean-metadata"
             for i in metadata}

    def command(*argv: str, timeout: int = 20) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(argv, cwd=root, capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError(f"{argv}: rc={result.returncode} {result.stdout[-200:]} {result.stderr[-300:]}")
        return result

    def node(action: str, i: int) -> str:
        if action == "start":
            pid_file = root / f"node-{i}/pid"
            if pid_file.exists():
                pid = int(pid_file.read_text())
                if not Path(f"/proc/{pid}/exe").exists():
                    pid_file.unlink()
        return command("bash", str(node_script), str(root), action, str(i)).stdout.strip()

    def running(i: int) -> bool:
        return f"node {i} running" in node("status", i)

    def stop_all() -> None:
        for i in (1, 2, 3):
            if running(i):
                print(node("kill", i), flush=True)
        deadline = time.monotonic() + 10
        while any(running(i) for i in (1, 2, 3)):
            if time.monotonic() > deadline:
                raise RuntimeError("node failed to stop")
            time.sleep(0.1)
        # Dragonboat's advisory data-dir locks can outlive /proc/PID/exe briefly.
        time.sleep(2)

    def start_clean() -> None:
        for i in (1, 2):
            shutil.copy2(clean[i], metadata[i])
            cfg = root / f"log-{i}.cold-recovery.toml"
            if "bootstrap-cluster = false" not in cfg.read_text():
                raise RuntimeError(f"missing recovery config {cfg}")
            node_dir = root / f"node-{i}"
            with (node_dir / "stdout.log").open("ab") as out:
                proc = subprocess.Popen([str(root / "mo-service"), "-cfg", str(cfg)],
                                        cwd=node_dir, stdin=subprocess.DEVNULL,
                                        stdout=out, stderr=subprocess.STDOUT,
                                        start_new_session=True)
            (node_dir / "pid").write_text(str(proc.pid))
        print(node("start", 3), flush=True)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if all(running(i) for i in (1, 2, 3)):
                result = command(str(client), "-addresses", ADDRESSES, "-op", "status")
                if result.stdout.count("ok=true") == 3:
                    return
            time.sleep(1)
        raise RuntimeError("clean recovery did not restore three members")

    if not all(running(i) for i in (1, 2, 3)):
        raise RuntimeError("all three test nodes must initially be running")
    for i in metadata:
        if not stale[i].is_file():
            raise FileNotFoundError(stale[i])
        shutil.copy2(metadata[i], clean[i])
    before = command(str(client), "-addresses", ADDRESSES, "-op", "read",
                     "-first", str(args.first)).stdout
    digest = hashlib.sha256(before.encode()).hexdigest()
    print(f"baseline sha256={digest} rows={before.count('read lsn=')}", flush=True)

    for round_number in range(args.rounds):
        stop_all()
        for i in metadata:
            shutil.copy2(stale[i], metadata[i])
        offsets = {i: (root / f"node-{i}/stdout.log").stat().st_size for i in (1, 2)}
        for i in (1, 2, 3):
            print(f"round={round_number} {node('start', i)}", flush=True)
        time.sleep(3)
        panics = {}
        for i in (1, 2):
            with (root / f"node-{i}/stdout.log").open("rb") as stream:
                stream.seek(offsets[i])
                chunk = stream.read().decode(errors="replace")
            panics[i] = "panic: shard already exist" in chunk and not running(i)
        print(f"round={round_number} panic_node1={panics[1]} panic_node2={panics[2]} "
              f"node3_running={running(3)}", flush=True)
        if panics != {1: True, 2: True} or not running(3):
            raise RuntimeError(f"round {round_number}: expected failure did not recur")
        stop_all()
        start_clean()
        after = command(str(client), "-addresses", ADDRESSES, "-op", "read",
                        "-first", str(args.first)).stdout
        after_digest = hashlib.sha256(after.encode()).hexdigest()
        print(f"round={round_number} recovered_sha256={after_digest} "
              f"same={after_digest == digest}", flush=True)
        if after_digest != digest:
            raise RuntimeError(f"round {round_number}: readback changed")


if __name__ == "__main__":
    main()
