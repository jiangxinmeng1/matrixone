#!/usr/bin/env python3
"""Reproduce future-watermark truncation of acknowledged LogService appends."""

import argparse
import json
import re
import subprocess
import time
from pathlib import Path


ADDRESSES = [f"127.0.0.1:{56401 + i * 10}" for i in (1, 2, 3)]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--prefix", default="future-repeat-20261104")
    args = parser.parse_args()
    root = args.root.resolve()
    if root.name != "logservice-chaos-20261103-d61a7206":
        raise ValueError("use only the dedicated LogService test cluster")
    client = root / "chaos-client"
    evidence = root / args.prefix
    evidence.mkdir(exist_ok=False)

    def call(op: str, addresses=ADDRESSES, **flags):
        argv = [str(client), "-addresses", ",".join(addresses), "-op", op]
        for name, value in flags.items():
            argv += [f"-{name.replace('_', '-')}", str(value)]
        return subprocess.run(argv, cwd=root, capture_output=True, text=True, timeout=20)

    def value(op: str, key: str) -> int:
        result = call(op)
        match = re.search(rf"^{key}=(\d+) err=<nil>$", result.stdout, re.M)
        if result.returncode or not match:
            raise RuntimeError(f"{op}: {result.stdout} {result.stderr}")
        return int(match.group(1))

    if call("status").stdout.count("ok=true") != 3:
        raise RuntimeError("three LogService addresses are required")
    for round_no in range(args.rounds):
        before = value("latest", "latest")
        target = before + 18
        result = call("truncate", lsn=target)
        (evidence / f"round-{round_no}.truncate").write_text(result.stdout + result.stderr)
        if result.returncode or f"truncate lsn={target} err=<nil>" not in result.stdout:
            raise RuntimeError(f"future truncate rejected: {result.stdout} {result.stderr}")
        if value("truncated", "truncated") != target:
            raise RuntimeError("truncate watermark did not advance")

        acks = []
        for seq in range(30):
            payload = f"{args.prefix}-r{round_no}-s{seq}/000000"
            result = call("append", prefix=payload.rsplit("/", 1)[0], count=1)
            (evidence / f"round-{round_no}.append").open("a").write(result.stdout + result.stderr)
            match = re.search(r"lsn=(\d+) err=<nil>", result.stdout)
            if result.returncode or not match:
                raise RuntimeError(f"append failed: {result.stdout} {result.stderr}")
            ack = int(match.group(1))
            acks.append((ack, payload))
            if seq < 8:
                time.sleep(1)
            if seq >= 8 and ack > target:
                break
        low = next(((lsn, payload) for lsn, payload in acks if lsn <= target), None)
        high = next(((lsn, payload) for lsn, payload in acks if lsn > target), None)
        if low is None or high is None:
            raise RuntimeError(f"did not cover both sides of watermark {target}: {acks}")

        deadline = time.monotonic() + 20
        while True:
            old_results = [call("read", addresses=[a], first=low[0]) for a in ADDRESSES]
            if all(r.returncode != 0 and "invalid log range" in r.stderr for r in old_results):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"old append still readable at {low[0]}: "
                                   f"{[(r.returncode, r.stdout[-150:], r.stderr[-150:]) for r in old_results]}")
            time.sleep(1)
        high_results = [call("read", addresses=[a], first=high[0]) for a in ADDRESSES]
        if any(r.returncode or high[1] not in r.stdout for r in high_results):
            raise RuntimeError(f"post-watermark append not readable: "
                               f"{[(r.returncode, r.stdout[-150:], r.stderr[-150:]) for r in high_results]}")
        for index, result in enumerate(old_results):
            (evidence / f"round-{round_no}.old-node-{index+1}").write_text(result.stdout + result.stderr)
        for index, result in enumerate(high_results):
            (evidence / f"round-{round_no}.new-node-{index+1}").write_text(result.stdout + result.stderr)
        summary = {"round": round_no, "latest_before": before, "truncate_to": target,
                   "first_acked_then_unreadable": low, "post_watermark_readable": high,
                   "acked_count": len(acks), "latest_after": value("latest", "latest")}
        print(json.dumps(summary), flush=True)
        (evidence / f"round-{round_no}.summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
