#!/usr/bin/env python3
"""Summarize direct Logservice load attempts and reconcile acknowledged records."""

import argparse
import math
import re
from collections import Counter
from datetime import datetime
from pathlib import Path


TRY = re.compile(r'^try time=\S+ payload=(\S+) attempt=(\d+) latency_ms=([\d.]+) lsn=(\d+) err="(.*)"$')
ACK = re.compile(r'^ack time=(\S+) payload=(\S+) lsn=(\d+) attempts=(\d+) total_ms=([\d.]+)$')
READ = re.compile(r'^read lsn=(\d+) payload=(\S+)$')


def percentile(values: list[float], p: float) -> float:
    values = sorted(values)
    return values[math.ceil(len(values) * p) - 1] if values else float("nan")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("load_log", type=Path)
    parser.add_argument("--read-log", type=Path)
    parser.add_argument("--prefix", help="limit read-side duplicate check to this payload prefix")
    parser.add_argument("--assert-unique", action="store_true", help="fail if a payload appears at multiple LSNs")
    args = parser.parse_args()

    tries: list[float] = []
    totals: list[float] = []
    ack_times: list[datetime] = []
    errors: Counter[str] = Counter()
    acknowledged: dict[str, int] = {}
    retry_count = 0
    final_errors = []
    for line in args.load_log.read_text().splitlines():
        if match := TRY.fullmatch(line):
            tries.append(float(match.group(3)))
            if match.group(5) != "<nil>":
                errors[match.group(5)] += 1
        elif match := ACK.fullmatch(line):
            ack_times.append(datetime.fromisoformat(match.group(1)))
            payload, lsn = match.group(2), int(match.group(3))
            if payload in acknowledged:
                raise AssertionError(f"duplicate acknowledgement for {payload}")
            acknowledged[payload] = lsn
            totals.append(float(match.group(5)))
            retry_count += int(match.group(4)) > 1
        elif line.startswith("final_error "):
            final_errors.append(line)

    print(f"acks={len(acknowledged)} tries={len(tries)} retried={retry_count} final_errors={len(final_errors)}")
    print(f"attempt_errors={dict(errors)}")
    for name, values in (("attempt_ms", tries), ("logical_ms", totals)):
        print(
            f"{name}: p50={percentile(values, .50):.3f} "
            f"p95={percentile(values, .95):.3f} "
            f"p99={percentile(values, .99):.3f} "
            f"max={max(values, default=float('nan')):.3f} "
            f"over_1000ms={sum(v > 1000 for v in values)}"
        )
    if len(ack_times) > 1:
        gaps = [(b - a).total_seconds() * 1000 for a, b in zip(ack_times, ack_times[1:])]
        print(f"max_ack_gap_ms={max(gaps):.3f}")
    if final_errors:
        print("\n".join(final_errors))
        raise AssertionError("load returned a final error")

    if args.read_log:
        observed: dict[str, list[int]] = {}
        for line in args.read_log.read_text().splitlines():
            if match := READ.fullmatch(line):
                lsn, payload = int(match.group(1)), match.group(2)
                if args.prefix is None or payload.startswith(args.prefix):
                    observed.setdefault(payload, []).append(lsn)
        mismatches = [
            (payload, lsn, observed.get(payload))
            for payload, lsn in acknowledged.items()
            if lsn not in observed.get(payload, [])
        ]
        if mismatches:
            raise AssertionError(f"{len(mismatches)} missing acknowledged LSNs: {mismatches[:5]}")
        duplicates = [(payload, lsns) for payload, lsns in observed.items() if len(lsns) > 1]
        print(f"read_verified={len(acknowledged)} duplicate_payloads={len(duplicates)}")
        if duplicates:
            print(f"duplicate_examples={duplicates[:5]}")
            if args.assert_unique:
                raise AssertionError("a payload appears at multiple LSNs")


if __name__ == "__main__":
    main()
