#!/usr/bin/env python3
"""Keep a three-address client busy across one long isolated-leader interval."""

import argparse
import re
import subprocess
import threading
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--seconds", type=int, default=340)
    parser.add_argument("--prefix", default="long-partition")
    args = parser.parse_args()
    root = args.root.resolve()
    addresses = (root / "addresses.txt").read_text().strip()
    targets = addresses.split(",")
    client = root / "chaos-client"
    netns = root / "netns.sh"
    node = root / "node.sh"
    progress = root / f"{args.prefix}.progress"
    stop = root / f"{args.prefix}.stop"
    load = root / f"{args.prefix}.load"
    if progress.exists() or load.exists():
        raise FileExistsError("prefix already used")
    stop.unlink(missing_ok=True)

    def run(*argv: str, timeout: int = 20) -> subprocess.CompletedProcess[str]:
        return subprocess.run(argv, cwd=root, capture_output=True, text=True, timeout=timeout)

    def op(name: str, *extra: str, timeout: int = 20) -> subprocess.CompletedProcess[str]:
        return run(str(client), "-addresses", addresses, "-op", name, *extra, timeout=timeout)

    def verified() -> int:
        try:
            return int(progress.read_text().split()[1])
        except (FileNotFoundError, IndexError, ValueError):
            return 0

    def live(index: int) -> bool:
        return f"node {index} running" in run("bash", str(node), str(root), "status", str(index)).stdout

    def print_event(message: str) -> None:
        print(f"{time.strftime('%Y-%m-%dT%H:%M:%S%z')} {message}", flush=True)

    initial = op("truncated")
    match = re.search(r"truncated=(\d+)", initial.stdout)
    if initial.returncode or not match:
        raise RuntimeError(f"initial truncated query failed: {initial.stdout} {initial.stderr}")
    last_truncate = int(match.group(1))
    done = threading.Event()
    state = {"truncates": 0, "errors": 0}
    output = load.open("w")
    process = subprocess.Popen(
        [str(client), "-addresses", addresses, "-op", "cycle", "-count", "0",
         "-prefix", args.prefix, "-progress-file", str(progress), "-stop-file", str(stop)],
        cwd=root, stdout=output, stderr=subprocess.STDOUT,
    )

    def truncate_loop() -> None:
        nonlocal last_truncate
        while not done.wait(4):
            target = verified() - 100
            if target <= last_truncate + 50:
                continue
            try:
                result = op("truncate", "-lsn", str(target), timeout=25)
                if result.returncode == 0 and f"truncate lsn={target} err=<nil>" in result.stdout:
                    last_truncate = target
                    state["truncates"] += 1
                else:
                    state["errors"] += 1
                    print_event(f"truncate retry target={target} rc={result.returncode} {result.stderr[-150:]}")
            except subprocess.TimeoutExpired:
                state["errors"] += 1
                print_event(f"truncate timeout target={target}")

    worker = threading.Thread(target=truncate_loop, daemon=True)
    down = False
    try:
        deadline = time.monotonic() + 45
        while verified() < last_truncate + 50 and time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"cycle exited during warmup: {process.returncode}")
            time.sleep(.25)
        if verified() < last_truncate + 50:
            raise RuntimeError("cycle did not warm up")
        worker.start()
        status = op("status", "-shard", "0").stdout
        match = re.search(r"leader=(\d+).*replicas=map\[([^]]+)\]", status)
        if not match:
            raise RuntimeError(f"HAKeeper status unavailable: {status}")
        leader_id = match.group(1)
        mapping = dict(re.findall(r"(\d+):([^ ]+)", match.group(2)))
        leader_address = mapping[leader_id]
        target = targets.index(leader_address)
        survivors = [i for i in range(3) if i != target]
        print_event(f"isolate HAKeeper leader node={target} id={leader_id} lsn={verified()} survivors={survivors}")
        result = run("bash", str(netns), str(root), "down", str(target))
        if result.returncode:
            raise RuntimeError(f"network down failed: {result.stderr}")
        down = True
        start = time.monotonic()
        while time.monotonic() - start < args.seconds:
            if process.poll() is not None:
                raise RuntimeError(f"cycle exited during isolation: {process.returncode}")
            if not all(live(i) for i in survivors):
                raise RuntimeError("surviving process exited")
            print_event(f"isolated_elapsed={time.monotonic()-start:.1f}s verified_lsn={verified()} truncates={state['truncates']} errors={state['errors']}")
            time.sleep(min(10, max(0, args.seconds - (time.monotonic() - start))))
        result = run("bash", str(netns), str(root), "up", str(target))
        if result.returncode:
            raise RuntimeError(f"network up failed: {result.stderr}")
        down = False
        print_event(f"network restored node={target} lsn={verified()}")
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            status = op("status").stdout
            views = re.findall(r"ok=true leader=(\d+) replicas=map\[([^]]+)\]", status)
            if len(views) == 3 and all(
                leader != "0" and
                len(re.findall(r"\d+:[^ ]+", members)) == 3 and
                all(address in members for address in targets)
                for leader, members in views
            ):
                break
            time.sleep(1)
        else:
            raise RuntimeError(f"three-address status did not recover: {status}")
        print_event(f"three-address status restored lsn={verified()} truncates={state['truncates']} errors={state['errors']}")
    finally:
        done.set()
        if down:
            run("bash", str(netns), str(root), "up", str(target))
        if worker.is_alive():
            worker.join(timeout=30)
        stop.touch()
        try:
            process.wait(timeout=190)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=10)
        output.close()
        print_event(f"cycle_exit={process.returncode} verified_lsn={verified()} truncates={state['truncates']} errors={state['errors']}")
    if process.returncode:
        raise RuntimeError(f"cycle failed: {process.returncode}")


if __name__ == "__main__":
    main()
