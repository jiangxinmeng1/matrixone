#!/usr/bin/env python3
"""Choose a legal checkpoint whose physical PSN prefix leaves a DSN hole.

Input is sequence_probe output from a fresh, user-only WAL shard. This predicts
the truncation boundary; the real WAL and LogService must still be exercised.
"""

import argparse
from collections import defaultdict
from pathlib import Path
import re

parser = argparse.ArgumentParser()
parser.add_argument('mapping', type=Path)
parser.add_argument('--minimum-retained', type=int, default=50)
parser.add_argument('--minimum-holes', type=int, default=3)
args = parser.parse_args()
by_psn = defaultdict(set)
lsns = set()
for line in args.mapping.read_text(errors='replace').splitlines():
    match = re.match(r'^record psn=(\d+) dsn=(\d+) lsn=(\d+) ', line)
    if match:
        psn, dsn, lsn = map(int, match.groups())
        if dsn != lsn:
            raise ValueError('this selector requires a fresh shard with DSN == user LSN')
        by_psn[psn].add(dsn)
        lsns.add(lsn)
if not lsns or lsns != set(range(1, max(lsns) + 1)):
    raise ValueError('mapping must contain every user LSN from 1 through latest')

psns = sorted(by_psn)
latest = max(lsns)
for cutoff in range(latest - args.minimum_retained, 0, -1):
    first_uncheckpointed_psn = next(
        (psn for psn in psns if max(by_psn[psn]) > cutoff), None)
    if first_uncheckpointed_psn is None:
        continue
    retained = {dsn for psn in psns if psn >= first_uncheckpointed_psn
                for dsn in by_psn[psn]}
    first = min(retained)
    holes = [dsn for dsn in range(first, cutoff + 1) if dsn not in retained]
    if len(holes) >= args.minimum_holes:
        # The WAL cycle probe appends one user entry before its checkpoint.
        print(f'latest={latest} cutoff={cutoff} checkpoint_lag={latest + 1 - cutoff} '
              f'first_retained_dsn={first} first_hole={holes[0]} holes={len(holes)}')
        break
else:
    raise SystemExit('no predicted hole at a legal checkpoint boundary')
