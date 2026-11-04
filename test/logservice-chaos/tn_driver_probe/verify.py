#!/usr/bin/env python3
"""Compare acknowledged TN WAL entries with a fresh driver's replay output."""

import argparse
import re
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay", type=Path, required=True)
    parser.add_argument("loads", nargs="+", type=Path)
    args = parser.parse_args()

    acknowledgements = {}
    latencies = []
    for path in args.loads:
        for line in path.read_text(errors="replace").splitlines():
            match = re.match(
                r"ack seq=\d+ dsn=(\d+) payload=(\S+) elapsed_ms=([\d.]+)", line
            )
            if not match:
                continue
            dsn, payload, elapsed = int(match[1]), match[2], float(match[3])
            if dsn in acknowledgements and acknowledgements[dsn] != payload:
                raise AssertionError(f"conflicting acknowledgement at DSN {dsn}")
            acknowledgements[dsn] = payload
            latencies.append(elapsed)

    replay = []
    for line in args.replay.read_text(errors="replace").splitlines():
        match = re.match(r"replay dsn=(\d+) payload=(\S+)", line)
        if match:
            replay.append((int(match[1]), match[2]))
    if not replay:
        raise AssertionError("empty replay")
    if any(right[0] != left[0] + 1 for left, right in zip(replay, replay[1:])):
        raise AssertionError("replay DSNs have gaps or duplicates")
    for dsn, payload in replay:
        if acknowledgements.get(dsn) != payload:
            raise AssertionError(f"replay mismatch or unacknowledged DSN {dsn}: {payload}")
    if max(acknowledgements) != replay[-1][0]:
        raise AssertionError("latest acknowledged DSN is absent from replay")

    print(
        f"ack_count={len(acknowledgements)} "
        f"replay_count={len(replay)} "
        f"replay_range={replay[0][0]}..{replay[-1][0]} "
        f"max_ack_ms={max(latencies):.3f}"
    )


if __name__ == "__main__":
    main()
