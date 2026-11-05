#!/usr/bin/env python3
"""Reproduce replay failure when a late duplicate outlives its confirmed copy.

Run only against an isolated three-node LogService test cluster. The probe
binary must be built from this directory and placed in the cluster root.
"""

import argparse
import os
import re
import signal
import subprocess
import time
from pathlib import Path


def run_probe(root, binary, addresses, path, *options):
    with path.open("w") as output:
        return subprocess.run(
            [str(binary), "-addresses", addresses, "-max-client", "1", *options],
            cwd=root,
            env=dict(os.environ, LD_LIBRARY_PATH=str(root / "lib")),
            stdout=output,
            stderr=subprocess.STDOUT,
            timeout=120,
        )


def wait_for(path, process, timeout):
    deadline = time.monotonic() + timeout
    while not path.exists():
        if process.poll() is not None:
            raise RuntimeError(f"probe exited before {path}: {process.returncode}")
        if time.monotonic() >= deadline:
            raise RuntimeError(f"timeout waiting for {path}")
        time.sleep(0.05)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--addresses", required=True)
    parser.add_argument("--tag", required=True, help="unique name for retained evidence")
    parser.add_argument("--probe", default="tn-driver-probe")
    args = parser.parse_args()
    root = args.root.resolve()
    if not root.name.startswith("logservice-driver-"):
        raise RuntimeError("refusing a non-test LogService directory")
    binary = (root / args.probe).resolve()
    if not binary.is_file() or not (root / "chaos-client").is_file():
        raise RuntimeError("test probe or chaos-client is missing")
    address_ports = []
    for address in args.addresses.split(","):
        host, port = address.rsplit(":", 1)
        if host != "127.0.0.1":
            raise RuntimeError("only loopback test addresses are allowed")
        address_ports.append(int(port))
    if len(address_ports) != 3 or any(
        address_ports[i + 1] != address_ports[i] + 10 for i in range(2)
    ):
        raise RuntimeError("expected three sequential test ports")

    old = new = None
    paused = None
    old_prefix = None
    old_dsn = None
    for attempt in range(1, 11):
        old_prefix = f"{args.tag}-attempt{attempt}-old"
        ready = root / f"{old_prefix}.ready"
        release = root / f"{old_prefix}.release"
        progress = root / f"{old_prefix}.progress"
        old_log = root / f"{old_prefix}.load"
        if any(path.exists() for path in (ready, release, progress, old_log)):
            raise RuntimeError(f"tag already used: {old_prefix}")

        with old_log.open("w") as output:
            try:
                old = subprocess.Popen(
                    [str(binary), "-addresses", args.addresses,
                     "-mode", "cycle", "-max-client", "1", "-prefix", old_prefix,
                     "-progress-file", str(progress),
                     "-pause-before-seq", "31",
                     "-pause-ready-file", str(ready),
                     "-pause-release-file", str(release)],
                    cwd=root,
                    env=dict(os.environ, LD_LIBRARY_PATH=str(root / "lib")),
                    stdout=output,
                    stderr=subprocess.STDOUT,
                )
                wait_for(ready, old, 60)
                old_dsn = int(ready.read_text().split()[1])
                sockets = subprocess.run(
                    ["ss", "-tnp"], text=True, capture_output=True, check=True
                ).stdout.splitlines()
                ports = []
                for line in sockets:
                    if f"pid={old.pid}," not in line:
                        continue
                    ports.extend(
                        int(port) for port in re.findall(r"127\.0\.0\.1:(\d+)", line)
                        if int(port) in address_ports
                    )
                status = subprocess.run(
                    [str(root / "chaos-client"), "-op", "status",
                     "-addresses", args.addresses],
                    cwd=root, text=True, capture_output=True, check=True,
                ).stdout
                if sum("ok=true" in line for line in status.splitlines()) != 3:
                    raise RuntimeError(f"three test replicas are not reachable: {status}")
                good = next(line for line in status.splitlines() if "ok=true" in line)
                match = re.search(r"leader=(\d+).*replicas=map\[(.*?)\]", good)
                replicas = {int(replica): int(port) for replica, port in
                            re.findall(r"(\d+):127\.0\.0\.1:(\d+)", match[2])}
                if len(replicas) != 3:
                    raise RuntimeError(f"expected three voting replicas: {good}")
                leader_port = replicas[int(match[1])]
                if len(ports) != 1 or ports[0] == leader_port:
                    print(f"attempt {attempt}: no follower connection, ports={ports}", flush=True)
                    continue

                port = ports[0]
                if port not in address_ports:
                    raise RuntimeError(f"unexpected connection {port}")
                index = address_ports.index(port)
                pid = int((root / f"node-{index}" / "pid").read_text())
                if os.path.realpath(f"/proc/{pid}/exe") != str(root / "mo-service"):
                    raise RuntimeError("target process is not the test LogService")
                print(f"attempt {attempt}: pause follower node{index}, old DSN={old_dsn}", flush=True)
                subprocess.Popen(
                    ["bash", "-c", f"sleep 100; kill -CONT {pid} 2>/dev/null || true"],
                    start_new_session=True,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                os.kill(pid, signal.SIGSTOP)
                paused = pid
                release.touch()
                time.sleep(5)
                if int(progress.read_text().split()[1]) != old_dsn:
                    raise RuntimeError("old Append unexpectedly acknowledged while paused")
                old.send_signal(signal.SIGKILL)
                old.wait(timeout=10)

                new_prefix = f"{args.tag}-new"
                new_log = root / f"{new_prefix}.load"
                if new_log.exists():
                    raise RuntimeError(f"tag already used: {new_prefix}")
                result = run_probe(
                    root, binary, args.addresses, new_log,
                    "-mode", "cycle", "-count", "30", "-prefix", new_prefix,
                )
                if result.returncode:
                    raise RuntimeError(f"new driver failed: {new_log}")
                os.kill(pid, signal.SIGCONT)
                paused = None
                time.sleep(8)
                break
            finally:
                if paused is not None:
                    os.kill(paused, signal.SIGCONT)
                    paused = None
                if old is not None and old.poll() is None:
                    old.send_signal(signal.SIGKILL)
                    old.wait(timeout=10)
    else:
        raise RuntimeError("could not connect the test driver to a follower")

    first = old_dsn + 1
    before = root / f"{args.tag}.before-cut.replay"
    if run_probe(root, binary, args.addresses, before, "-mode", "replay").returncode:
        raise RuntimeError("pre-truncate replay failed")
    text = before.read_text(errors="replace")
    if ("Wal-Replay-Duplicate-Entry" not in text or
            f"replay dsn={first} payload={new_prefix}/000000" not in text):
        raise RuntimeError("late duplicate window was not reached; no truncate issued")

    cut = root / f"{args.tag}.cut.load"
    if run_probe(
        root, binary, args.addresses, cut,
        "-mode", "cycle", "-count", "10", "-prefix", f"{args.tag}-cut",
        "-truncate-every", "10", "-truncate-lag", "20",
    ).returncode:
        raise RuntimeError("cut driver failed")
    if "Wal-Truncate-Execute" not in cut.read_text(errors="replace"):
        raise RuntimeError("remote truncate was not confirmed")

    after = root / f"{args.tag}.after-cut.replay"
    result = run_probe(root, binary, args.addresses, after, "-mode", "replay")
    text = after.read_text(errors="replace")
    old_payload = f"replay dsn={first} payload={old_prefix}/000031"
    missing = f"dsn {first + 1} not found"
    print(f"truncate completed; replay exit={result.returncode}", flush=True)
    print(f"expected old payload: {old_payload}", flush=True)
    print(f"expected error: {missing}", flush=True)
    if result.returncode == 0 or old_payload not in text or missing not in text:
        raise RuntimeError(f"bug signature was not reproduced; inspect {after}")
    print(f"REPRODUCED: {after}", flush=True)


if __name__ == "__main__":
    main()
