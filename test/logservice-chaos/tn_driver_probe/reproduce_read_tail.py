#!/usr/bin/env python3
"""Pause one follower between WAL replay's truncate query and first Read."""
import argparse
from pathlib import Path
import os
import random
import re
import signal
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument('--root', type=Path, default=Path('/home/mo/jxm/logservice-tnwal-lifecycle-20261105'))
parser.add_argument('--probe', type=Path, default=Path('/home/mo/jxm/tn-wal-probe-lifecycle'))
parser.add_argument('--tag', default='read-tail-repeat')
parser.add_argument('--attempts', type=int, default=8)
args = parser.parse_args()
root = args.root
addresses = '127.0.0.1:58201,127.0.0.1:58211,127.0.0.1:58221'
env = dict(os.environ, LD_LIBRARY_PATH=str(root / 'lib'))

for attempt in range(1, args.attempts + 1):
    tag = f'{args.tag}-{attempt}'
    ready = root / f'{tag}.ready'
    release = root / f'{tag}.release'
    output = root / f'{tag}.log'
    if any(path.exists() for path in (ready, release, output)):
        raise RuntimeError(f'refusing to overwrite evidence for {tag}')
    with output.open('w') as log:
        probe = subprocess.Popen([str(args.probe), '-addresses', addresses,
                                  '-mode', 'replay', '-replay-timeout', '6m',
                                  '-read-ready-file', str(ready),
                                  '-read-release-file', str(release)],
                                 cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
    paused = None
    try:
        deadline = time.monotonic() + 30
        while not ready.exists():
            if probe.poll() is not None:
                raise RuntimeError(f'probe died before ready: {probe.returncode}')
            if time.monotonic() > deadline:
                raise TimeoutError('probe ready')
            time.sleep(.05)
        status = subprocess.run([str(root / 'chaos-client'), '-op', 'status',
                                 '-addresses', addresses], text=True,
                                capture_output=True, check=True).stdout
        match = re.search(r'leader=(\d+).*replicas=map\[(.*?)\]', status)
        ports = {int(replica): int(port) for replica, port in
                 re.findall(r'(\d+):127\.0\.0\.1:(\d+)', match[2])}
        leader = (ports[int(match[1])] - 58201) // 10
        node = random.choice([i for i in range(3) if i != leader])
        pid = int((root / f'node-{node}' / 'pid').read_text())
        if os.path.realpath(f'/proc/{pid}/exe') != str(root / 'mo-service'):
            raise RuntimeError('wrong executable')
        subprocess.Popen(['bash', '-c', f'sleep 95; kill -CONT {pid} 2>/dev/null || true'],
                         start_new_session=True, stdin=subprocess.DEVNULL,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        os.kill(pid, signal.SIGSTOP)
        paused = pid
        start = time.monotonic()
        release.touch()
        time.sleep(4)
        if probe.poll() is not None:
            print(f'{tag}: follower node{node}, replay finished in {time.monotonic()-start:.2f}s', flush=True)
            continue
        print(f'{tag}: follower node{node}, replay Read stalled', flush=True)
        survivor_addresses = ','.join(f'127.0.0.1:{58201+10*i}' for i in range(3) if i != node)
        healthy = subprocess.run([str(root / 'chaos-client'), '-op', 'latest',
                                  '-addresses', survivor_addresses], text=True,
                                 capture_output=True, check=True).stdout
        print(healthy.strip(), flush=True)
        try:
            probe.wait(timeout=80)
        except subprocess.TimeoutExpired:
            print(f'{tag}: still blocked after {time.monotonic()-start:.2f}s', flush=True)
        else:
            print(f'{tag}: exit={probe.returncode} after {time.monotonic()-start:.2f}s', flush=True)
        break
    finally:
        if paused is not None:
            os.kill(paused, signal.SIGCONT)
        if probe.poll() is None:
            try:
                probe.wait(timeout=8)
            except subprocess.TimeoutExpired:
                probe.kill()
                probe.wait()
