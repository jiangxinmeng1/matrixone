#!/usr/bin/env python3
"""Run one continuous read/write client while rotating isolated node failures."""

import argparse
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path


def stamp() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds")


def log(message: str) -> None:
    print(f"{stamp()} {message}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--prefix", default="cycle-chaos")
    args = parser.parse_args()
    root = args.root.resolve()
    addresses = (root / "addresses.txt").read_text().strip()
    client = root / "chaos-client"
    node_tool = root / "node.sh"
    progress = root / f"{args.prefix}.progress"
    stop_file = root / f"{args.prefix}.stop"
    output = root / f"{args.prefix}.load"
    if stop_file.exists():
        stop_file.unlink()
    if progress.exists():
        progress.unlink()

    def command(*parts: str, timeout: float = 30) -> subprocess.CompletedProcess[str]:
        return subprocess.run(parts, cwd=root, text=True, capture_output=True, timeout=timeout)

    def client_op(op: str, *extra: str, timeout: float = 30) -> subprocess.CompletedProcess[str]:
        return command(str(client), "-addresses", addresses, "-op", op, *extra, timeout=timeout)

    def running(index: int) -> bool:
        result = command(str(node_tool), str(root), "status", str(index))
        return result.returncode == 0 and f"node {index} running" in result.stdout

    def membership() -> tuple[int, dict[int, int]]:
        result = client_op("status", timeout=15)
        for line in result.stdout.splitlines():
            if "ok=true" not in line:
                continue
            leader = re.search(r"leader=(\d+)", line)
            members = {int(replica): int(port) for replica, port in re.findall(r"(\d+):127\.0\.0\.1:(\d+)", line)}
            if leader:
                return int(leader.group(1)), members
        raise RuntimeError(f"no shard status: {result.stdout} {result.stderr}")

    def verified_lsn() -> int:
        if not progress.exists():
            return 0
        return int(progress.read_text().split()[1])

    def wait_for_progress(minimum: int, timeout: float) -> None:
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            if verified_lsn() >= minimum:
                return
            if process.poll() is not None:
                raise RuntimeError(f"cycle client exited early: {process.returncode}")
            time.sleep(0.2)
        raise RuntimeError(f"cycle client did not reach LSN {minimum}")

    def wait_for_members(timeout: float = 75) -> tuple[int, dict[int, int]]:
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            if all(running(index) for index in range(3)):
                try:
                    leader, members = membership()
                    if len(members) == 3 and leader in members:
                        return leader, members
                except Exception:
                    pass
            if process.poll() is not None:
                raise RuntimeError(f"cycle client exited early: {process.returncode}")
            time.sleep(0.5)
        raise RuntimeError("three-member recovery timed out")

    def wait_for_snapshot(index: int, event: str, timeout: float = 18) -> bool:
        path = root / f"node-{index}" / "stdout.log"
        until = time.monotonic() + timeout
        with path.open(errors="replace") as stream:
            stream.seek(0, 2)
            while time.monotonic() < until:
                line = stream.readline()
                if not line:
                    time.sleep(0.05)
                    continue
                if f'"msg":"{event} snapshot success"' in line and '"shard":1' in line:
                    log(f"node={index} observed {event} snapshot")
                    return True
        log(f"node={index} no new {event} snapshot within {timeout}s; injecting on timer")
        return False

    initial = client_op("truncated")
    match = re.search(r"truncated=(\d+)", initial.stdout)
    if initial.returncode != 0 or not match:
        raise RuntimeError(f"cannot get initial truncated LSN: {initial.stdout} {initial.stderr}")
    last_truncate = int(match.group(1))
    state = {"truncate_success": 0, "truncate_errors": 0, "last_success": time.monotonic(), "fatal": ""}
    shutdown = threading.Event()

    def truncate_loop() -> None:
        nonlocal last_truncate
        while not shutdown.is_set():
            safe = verified_lsn()
            target = safe - 100 if safe > 100 else 0
            if target > last_truncate + 40:
                try:
                    result = client_op("truncate", "-lsn", str(target), timeout=25)
                    if result.returncode == 0 and f"truncate lsn={target} err=<nil>" in result.stdout:
                        last_truncate = target
                        state["truncate_success"] += 1
                        state["last_success"] = time.monotonic()
                        log(f"truncate ok lsn={target}")
                    else:
                        state["truncate_errors"] += 1
                        log(f"truncate retry lsn={target} rc={result.returncode} out={result.stdout[-180:]} err={result.stderr[-180:]}")
                except subprocess.TimeoutExpired:
                    state["truncate_errors"] += 1
                    log(f"truncate retry lsn={target} timeout")
                if time.monotonic() - state["last_success"] > 120:
                    state["fatal"] = "truncate made no progress for 120s"
                    return
            shutdown.wait(2)

    output_stream = output.open("w")
    process = subprocess.Popen(
        [str(client), "-addresses", addresses, "-op", "cycle", "-count", "0",
         "-prefix", args.prefix, "-progress-file", str(progress), "-stop-file", str(stop_file)],
        cwd=root, stdout=output_stream, stderr=subprocess.STDOUT,
    )
    log(f"cycle client pid={process.pid} prefix={args.prefix}")
    down_node = None
    thread = threading.Thread(target=truncate_loop, daemon=True)
    try:
        wait_for_progress(last_truncate + 40, 30)
        thread.start()
        # Each node is killed once near each kind of event. One round per node
        # exceeds HAKeeper's 30-second expiry to exercise replica replacement.
        for round_number in range(9):
            index = round_number % 3
            leader, members = wait_for_members()
            if state["fatal"]:
                raise RuntimeError(state["fatal"])
            role = "leader" if members.get(leader) == 56401 + 10 * index else "follower"
            trigger = ("export", "import", "truncate")[round_number // 3]
            log(f"round={round_number} node={index} role={role} trigger={trigger} leader={leader} members={members}")
            if trigger in ("export", "import"):
                wait_for_snapshot(index, trigger)
            else:
                safe = verified_lsn()
                target = safe - 100
                if target > last_truncate:
                    # This request overlaps with the crash on a best-effort basis.
                    subprocess.Popen(
                        [str(client), "-addresses", addresses, "-op", "truncate", "-lsn", str(target)],
                        cwd=root, stdout=(root / f"{args.prefix}.round-{round_number}.truncate").open("w"),
                        stderr=subprocess.STDOUT,
                    )
                    time.sleep(0.02)
            result = command(str(node_tool), str(root), "kill", str(index))
            if result.returncode != 0:
                raise RuntimeError(f"kill failed: {result.stdout} {result.stderr}")
            down_node = index
            if sum(running(i) for i in range(3)) != 2:
                raise RuntimeError("not exactly two test nodes alive after kill")
            log(f"round={round_number} node={index} killed verified_lsn={verified_lsn()}")
            hold = 38 if round_number in (3, 4, 5) else 8
            until = time.monotonic() + hold
            while time.monotonic() < until:
                if process.poll() is not None:
                    raise RuntimeError(f"cycle client exited during node outage: {process.returncode}")
                if state["fatal"]:
                    raise RuntimeError(state["fatal"])
                time.sleep(0.5)
            result = command(str(node_tool), str(root), "start", str(index))
            if result.returncode != 0:
                raise RuntimeError(f"restart failed: {result.stdout} {result.stderr}")
            down_node = None
            log(f"round={round_number} node={index} restarted verified_lsn={verified_lsn()}")
            recovered_leader, recovered_members = wait_for_members()
            log(f"round={round_number} recovered leader={recovered_leader} members={recovered_members}")
        log(f"rounds complete truncate_success={state['truncate_success']} truncate_errors={state['truncate_errors']}")
    except Exception as err:
        log(f"FAIL {err}")
        raise
    finally:
        shutdown.set()
        if thread.is_alive():
            thread.join(timeout=30)
        if down_node is not None:
            result = command(str(node_tool), str(root), "start", str(down_node))
            log(f"recovery restart node={down_node} rc={result.returncode} {result.stdout.strip()} {result.stderr.strip()}")
        stop_file.touch()
        try:
            process.wait(timeout=190)
        except subprocess.TimeoutExpired:
            log("FAIL cycle client did not stop within 190s")
            process.terminate()
            process.wait(timeout=10)
        output_stream.close()
        log(f"cycle exit={process.returncode} verified_lsn={verified_lsn()} truncate_success={state['truncate_success']} truncate_errors={state['truncate_errors']}")
    if process.returncode != 0 or state["fatal"]:
        raise RuntimeError(f"cycle failed: rc={process.returncode} fatal={state['fatal']}")


if __name__ == "__main__":
    main()
