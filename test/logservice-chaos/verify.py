#!/usr/bin/env python3
"""Check every acknowledged direct Logservice append against a later read."""

import argparse
import re
from pathlib import Path


APPEND = re.compile(r"^append payload=(\S+) lsn=(\d+) err=<nil>$")
READ = re.compile(r"^read lsn=(\d+) payload=(\S+)$")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("read_log", type=Path)
    parser.add_argument("append_logs", nargs="+", type=Path)
    parser.add_argument("--min-lsn", type=int, default=1, help="first LSN still required after truncate")
    args = parser.parse_args()
    expected: dict[int, str] = {}
    for path in args.append_logs:
        entries = 0
        for line in path.read_text().splitlines():
            match = APPEND.fullmatch(line)
            if match:
                payload, index = match.group(1), int(match.group(2))
                if index in expected:
                    raise AssertionError(f"duplicate acknowledged LSN {index}")
                if index >= args.min_lsn:
                    expected[index] = payload
                    entries += 1
            elif line.startswith("append payload="):
                raise AssertionError(f"unconfirmed append in {path}: {line}")
        print(f"{path.name}: {entries} acknowledged appends")
    observed: dict[int, str] = {}
    for line in args.read_log.read_text().splitlines():
        match = READ.fullmatch(line)
        if match:
            index, payload = int(match.group(1)), match.group(2)
            if index in observed:
                raise AssertionError(f"duplicate read LSN {index}")
            observed[index] = payload
    mismatches = [
        (index, payload, observed.get(index))
        for index, payload in sorted(expected.items())
        if observed.get(index) != payload
    ]
    if mismatches:
        raise AssertionError(f"{len(mismatches)} missing/mismatched records: {mismatches[:5]}")
    print(f"PASS: {len(expected)} acknowledged records match read LSN and payload")


if __name__ == "__main__":
    main()
