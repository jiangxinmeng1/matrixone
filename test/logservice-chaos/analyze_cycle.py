#!/usr/bin/env python3
"""Summarize continuous write/read fault testing and check retained records."""

import argparse
import json
import math
import re
from collections import Counter
from pathlib import Path


CYCLE = re.compile(
    r"^cycle time=\S+ seq=(\d+) lsn=(\d+) append_attempts=(\d+) read_attempts=(\d+) "
    r"append_ms=([\d.]+) read_ms=([\d.]+) total_ms=([\d.]+)$"
)
READ = re.compile(r"^read lsn=(\d+) payload=(\S+)$")


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * fraction) - 1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("cycle_log", type=Path)
    parser.add_argument("--read-log", type=Path)
    parser.add_argument("--min-lsn", type=int, default=1)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--node-root", type=Path)
    parser.add_argument("--since", help="node log timestamp lower bound, YYYY/MM/DD HH:MM:SS")
    parser.add_argument("--until", help="node log timestamp upper bound")
    args = parser.parse_args()

    cycles: list[tuple[int, int]] = []
    append_ms: list[float] = []
    read_ms: list[float] = []
    total_ms: list[float] = []
    retry_errors: Counter[str] = Counter()
    final_errors = []
    summary = None
    for line in args.cycle_log.read_text().splitlines():
        if match := CYCLE.fullmatch(line):
            seq, lsn = int(match.group(1)), int(match.group(2))
            if seq != len(cycles):
                raise AssertionError(f"missing or duplicate cycle seq={seq}, expected={len(cycles)}")
            cycles.append((seq, lsn))
            append_ms.append(float(match.group(5)))
            read_ms.append(float(match.group(6)))
            total_ms.append(float(match.group(7)))
        elif line.startswith("retry "):
            kind = re.search(r"op=(\w+).*err=(.*)$", line)
            retry_errors[f"{kind.group(1)}: {kind.group(2)}" if kind else line] += 1
        elif line.startswith("final_error "):
            final_errors.append(line)
        elif line.startswith("summary "):
            summary = line

    if not cycles or summary is None:
        raise AssertionError("missing cycle or clean summary")
    declared = re.search(r"cycles=(\d+)", summary)
    if not declared or int(declared.group(1)) != len(cycles):
        raise AssertionError(f"summary mismatch: {summary}, observed={len(cycles)}")
    print(summary)
    print(f"retry_errors={dict(retry_errors)} final_errors={len(final_errors)}")
    if final_errors:
        raise AssertionError(final_errors[0])
    for name, values in (("append_ms", append_ms), ("read_ms", read_ms), ("cycle_ms", total_ms)):
        maximum = max(values)
        print(
            f"{name}: p50={percentile(values, .50):.3f} "
            f"p95={percentile(values, .95):.3f} "
            f"p99={percentile(values, .99):.3f} max={maximum:.3f} "
            f"max_seq={values.index(maximum)} over_1000ms={sum(v > 1000 for v in values)}"
        )

    if args.read_log:
        observed: dict[str, list[int]] = {}
        for line in args.read_log.read_text().splitlines():
            if match := READ.fullmatch(line):
                lsn, payload = int(match.group(1)), match.group(2)
                if payload.startswith(args.prefix + "/"):
                    observed.setdefault(payload, []).append(lsn)
        retained = [(seq, lsn) for seq, lsn in cycles if lsn >= args.min_lsn]
        bad = [
            (seq, lsn, observed.get(f"{args.prefix}/{seq:06d}"))
            for seq, lsn in retained
            if lsn not in observed.get(f"{args.prefix}/{seq:06d}", [])
        ]
        if bad:
            raise AssertionError(f"{len(bad)} retained records missing/mismatched: {bad[:5]}")
        duplicates = [(payload, lsns) for payload, lsns in observed.items() if len(lsns) > 1]
        print(f"retained_verified={len(retained)} duplicate_payloads_in_retained_range={len(duplicates)}")
        if duplicates:
            print(f"duplicate_examples={duplicates[:5]}")

    if args.node_root:
        for index in range(3):
            counts: Counter[str] = Counter()
            path = args.node_root / f"node-{index}" / "stdout.log"
            for line in path.open(errors="replace"):
                if 'snapshot success' not in line or '"shard":1' not in line:
                    continue
                record = json.loads(line)
                at = record.get("time", "")
                if args.since and at < args.since:
                    continue
                if args.until and at > args.until:
                    continue
                if record.get("msg") in ("export snapshot success", "import snapshot success"):
                    counts[record["msg"]] += 1
            print(f"node={index} export={counts['export snapshot success']} import={counts['import snapshot success']}")


if __name__ == "__main__":
    main()
