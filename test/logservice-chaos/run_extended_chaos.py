#!/usr/bin/env python3
"""Long, seeded Logservice fault campaign with empty stores and link partitions."""

import argparse
import random
import re
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path


def stamp() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def log(message: str) -> None:
    print(f"{stamp()} {message}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--prefix", default="extended-chaos")
    parser.add_argument("--seed", type=int, default=20261103)
    args = parser.parse_args()
    root = args.root.resolve()
    rng = random.Random(args.seed)
    client = root / "chaos-client"
    node = root / "node.sh"
    netns = root / "netns.sh"
    addresses = {i: f"198.18.51.{10+i}:{56401+10*i}" for i in range(7)}
    initial_addresses = ",".join(addresses[i] for i in range(3))
    progress = root / f"{args.prefix}.progress"
    stop = root / f"{args.prefix}.stop"
    output = root / f"{args.prefix}.load"
    if output.exists() or progress.exists():
        raise FileExistsError("campaign prefix already used")
    stop.unlink(missing_ok=True)
    spare_next = 3
    partitioned = None
    killed = None
    process = None
    output_stream = None
    shutdown = threading.Event()
    state = {"truncations": 0, "truncate_errors": 0, "last_truncate": 0,
             "last_success": time.monotonic(), "fatal": ""}

    def command(*argv: str, timeout: int = 30) -> subprocess.CompletedProcess[str]:
        return subprocess.run(argv, cwd=root, capture_output=True, text=True, timeout=timeout)

    def client_op(op: str, *extra: str, timeout: int = 30) -> subprocess.CompletedProcess[str]:
        return command(str(client), "-addresses", initial_addresses, "-op", op, *extra, timeout=timeout)

    def node_op(op: str, index: int) -> subprocess.CompletedProcess[str]:
        return command("bash", str(node), str(root), op, str(index))

    def net_op(op: str, index: int) -> subprocess.CompletedProcess[str]:
        return command("bash", str(netns), str(root), op, str(index))

    def running(index: int) -> bool:
        result = node_op("status", index)
        return f"node {index} running" in result.stdout

    def checked(result: subprocess.CompletedProcess[str], label: str) -> None:
        if result.returncode:
            raise RuntimeError(f"{label} rc={result.returncode}: {result.stdout[-300:]} {result.stderr[-300:]}")

    def verified_lsn() -> int:
        return int(progress.read_text().split()[1]) if progress.exists() else 0

    def healthy_client() -> None:
        if process.poll() is not None:
            raise RuntimeError(f"cycle client exited early rc={process.returncode}")
        if state["fatal"]:
            raise RuntimeError(state["fatal"])

    def wait(seconds: float) -> None:
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            healthy_client()
            time.sleep(min(0.5, until - time.monotonic()))

    def get_membership() -> tuple[int, dict[int, int]]:
        live = [addresses[i] for i in range(spare_next) if running(i)]
        result = command(str(client), "-addresses", ",".join(live), "-op", "status", timeout=20)
        views = []
        for line in result.stdout.splitlines():
            if "ok=true" not in line:
                continue
            leader = re.search(r"leader=(\d+)", line)
            members = {
                int(replica): next((i for i, address in addresses.items() if address == endpoint), -1)
                for replica, endpoint in re.findall(r"(\d+):((?:\d{1,3}\.){3}\d{1,3}:\d+)", line)
            }
            if leader and len(members) >= 2 and -1 not in members.values():
                views.append((int(leader.group(1)), members))
        if not views:
            raise RuntimeError(f"no usable shard status: {result.stdout[-400:]} {result.stderr[-200:]}")
        # At a topology change, a peer can briefly return a stale gossip view.
        views.sort(key=lambda item: sum(item == other for other in views), reverse=True)
        return views[0]

    def wait_members(expected: set[int], timeout: int = 150) -> tuple[int, dict[int, int]]:
        until = time.monotonic() + timeout
        last = ""
        while time.monotonic() < until:
            healthy_client()
            try:
                leader, members = get_membership()
                actual = set(members.values())
                if len(members) == 3 and actual == expected and leader in members and all(running(i) for i in expected):
                    return leader, members
                last = f"leader={leader} members={members}"
            except Exception as error:
                last = str(error)
            time.sleep(1)
        raise RuntimeError(f"member convergence timeout expected={expected} last={last}")

    def wait_snapshot(index: int, event: str, timeout: int = 12) -> None:
        path = root / f"node-{index}" / "stdout.log"
        until = time.monotonic() + timeout
        with path.open(errors="replace") as stream:
            stream.seek(0, 2)
            while time.monotonic() < until:
                healthy_client()
                line = stream.readline()
                if f'"msg":"{event} snapshot success"' in line and '"shard":1' in line:
                    log(f"snapshot trigger node={index} event={event}")
                    return
                if not line:
                    time.sleep(0.05)
        log(f"snapshot trigger node={index} event={event} timed out; using timer")

    def truncate_loop() -> None:
        while not shutdown.is_set():
            safe = verified_lsn()
            target = safe - 100
            if target > state["last_truncate"] + 40:
                try:
                    result = client_op("truncate", "-lsn", str(target), timeout=25)
                    if result.returncode == 0 and f"truncate lsn={target} err=<nil>" in result.stdout:
                        state["last_truncate"] = target
                        state["truncations"] += 1
                        state["last_success"] = time.monotonic()
                        log(f"truncate ok lsn={target}")
                    else:
                        state["truncate_errors"] += 1
                        log(f"truncate retry lsn={target} rc={result.returncode} {result.stderr[-120:]}")
                except subprocess.TimeoutExpired:
                    state["truncate_errors"] += 1
                    log(f"truncate retry lsn={target} timeout")
                if time.monotonic() - state["last_success"] > 150:
                    state["fatal"] = "truncate made no progress for 150s"
                    return
            shutdown.wait(2)

    initial = client_op("truncated")
    found = re.search(r"truncated=(\d+)", initial.stdout)
    checked(initial, "initial truncate query")
    if not found:
        raise RuntimeError(initial.stdout)
    state["last_truncate"] = int(found.group(1))
    output_stream = output.open("w")
    process = subprocess.Popen(
        [str(client), "-addresses", initial_addresses, "-op", "cycle", "-count", "0",
         "-prefix", args.prefix, "-progress-file", str(progress), "-stop-file", str(stop)],
        cwd=root, stdout=output_stream, stderr=subprocess.STDOUT,
    )
    thread = threading.Thread(target=truncate_loop, daemon=True)
    log(f"seed={args.seed} cycle_pid={process.pid} initial_addresses={initial_addresses}")
    try:
        start_lsn = state["last_truncate"]
        until = time.monotonic() + 45
        while verified_lsn() < start_lsn + 40 and time.monotonic() < until:
            healthy_client()
            time.sleep(0.25)
        if verified_lsn() < start_lsn + 40:
            raise RuntimeError("client did not warm up")
        thread.start()
        # Every outage uses a fresh random choice from the *current* three voters.
        # Long outages add an empty store, then restart the removed old store.
        plan = ["short", "partition", "long", "short", "long", "partition",
                "short", "long", "partition", "short", "partition", "short"]
        for round_number, kind in enumerate(plan):
            leader, members = get_membership()
            active = set(members.values())
            if len(active) != 3 or not all(running(i) for i in active):
                raise RuntimeError(f"round={round_number} expected three live voters: {members}")
            if kind == "partition":
                target = members[leader]
            else:
                target = rng.choice(sorted(active))
            role = "leader" if members[leader] == target else "follower"
            trigger = ("export", "truncate", "import")[round_number % 3]
            log(f"round={round_number} kind={kind} node={target} role={role} trigger={trigger} members={members} lsn={verified_lsn()}")
            if trigger in ("export", "import"):
                wait_snapshot(target, trigger)
            else:
                target_lsn = verified_lsn() - 100
                if target_lsn > state["last_truncate"]:
                    with (root / f"{args.prefix}.round-{round_number}.truncate").open("w") as stream:
                        subprocess.Popen([str(client), "-addresses", initial_addresses, "-op", "truncate",
                                          "-lsn", str(target_lsn)], cwd=root, stdout=stream, stderr=subprocess.STDOUT)
                    time.sleep(0.02)
            survivors = active - {target}
            if len(survivors) != 2 or not all(running(i) for i in survivors):
                raise RuntimeError("would violate two live voter invariant")
            if kind == "partition":
                checked(net_op("down", target), "partition leader")
                partitioned = target
                log(f"round={round_number} leader link down node={target}")
                wait(rng.randint(18, 30))
                checked(net_op("up", target), "heal leader link")
                partitioned = None
                log(f"round={round_number} leader link restored node={target}")
                new_leader, new_members = wait_members(active)
            elif kind == "short":
                checked(node_op("kill", target), "kill node")
                killed = target
                log(f"round={round_number} killed node={target}")
                wait(rng.randint(8, 22))
                checked(node_op("start", target), "restart old directory")
                killed = None
                new_leader, new_members = wait_members(active)
            else:
                spare = spare_next
                if spare >= len(addresses):
                    raise RuntimeError("spare namespace capacity exceeded")
                # HAKeeper may select *any* idle store. Keep only the fresh
                # empty store eligible so this round tests that exact path.
                for idle in range(spare_next):
                    if idle not in active and running(idle):
                        checked(node_op("kill", idle), "stop idle old store")
                        log(f"round={round_number} idle old store node={idle} stopped")
                checked(command("python3", str(root / "add_spare.py"), str(root), "--index", str(spare),
                                "--host", f"198.18.51.{10+spare}"), "create empty store")
                checked(node_op("start", spare), "start empty store")
                spare_next += 1
                log(f"round={round_number} empty node={spare} started")
                checked(node_op("kill", target), "kill old voter")
                killed = target
                log(f"round={round_number} old voter node={target} killed")
                new_leader, new_members = wait_members(survivors | {spare}, timeout=210)
                if set(members).intersection(new_members) != set(members) - {next(replica for replica, i in members.items() if i == target)}:
                    raise RuntimeError(f"unexpected old replica after replacement: before={members} after={new_members}")
                log(f"round={round_number} replacement converged members={new_members} lsn={verified_lsn()}")
                wait(25)
                checked(node_op("start", target), "restart old directory")
                killed = None
                log(f"round={round_number} old directory node={target} restarted")
                wait(18)
                new_leader, new_members = wait_members(survivors | {spare})
                if target in new_members.values():
                    raise RuntimeError(f"zombie rejoined membership node={target} members={new_members}")
                checked(node_op("kill", target), "stop returned old store")
                log(f"round={round_number} old directory node={target} stopped after zombie check")
                # Let HAKeeper expire the stopped spare before another long
                # outage, otherwise it may schedule onto the old directory.
                wait(35)
            log(f"round={round_number} recovered leader={new_leader} members={new_members} lsn={verified_lsn()}")
            wait(5)
        log(f"rounds complete truncations={state['truncations']} truncate_errors={state['truncate_errors']}")
    except Exception as error:
        log(f"FAIL {error}")
        raise
    finally:
        shutdown.set()
        if thread.is_alive():
            thread.join(timeout=30)
        if partitioned is not None:
            result = net_op("up", partitioned)
            log(f"emergency heal node={partitioned} rc={result.returncode}")
        if killed is not None:
            result = node_op("start", killed)
            log(f"emergency restart node={killed} rc={result.returncode}")
        stop.touch()
        try:
            process.wait(timeout=190)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=10)
        output_stream.close()
        log(f"cycle_exit={process.returncode} verified_lsn={verified_lsn()} truncations={state['truncations']} truncate_errors={state['truncate_errors']}")
    if process.returncode or state["fatal"]:
        raise RuntimeError(f"campaign failed: client={process.returncode} fatal={state['fatal']}")


if __name__ == "__main__":
    main()
