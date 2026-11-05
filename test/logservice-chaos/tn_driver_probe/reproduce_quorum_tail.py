#!/usr/bin/env python3
"""Cut two voters during a live TN WAL workload, then restore after six seconds."""

import argparse
import os
from pathlib import Path
import re
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument('--root', type=Path, default=Path('/home/mo/jxm/logservice-tail-quorum-20261105'))
parser.add_argument('--probe', type=Path, default=Path('/home/mo/jxm/tn-wal-concurrent'))
parser.add_argument('--tag', required=True)
parser.add_argument('--down-seconds', type=float, default=6)
args = parser.parse_args()
if args.down_seconds <= 0 or args.down_seconds >= 30:
    raise ValueError('down-seconds must be between 0 and 30')

root = args.root
addresses = '127.0.0.1:58201,127.0.0.1:58211,127.0.0.1:58221'
output = root / f'{args.tag}.log'
timeline = root / f'{args.tag}.timeline'
if output.exists() or timeline.exists():
    raise FileExistsError(f'refusing to overwrite evidence for {args.tag}')
nodecmd = [str(root / 'node.sh'), str(root)]
env = dict(os.environ, LD_LIBRARY_PATH=str(root / 'lib'))


def emit(message):
    with timeline.open('a') as stream:
        stream.write(f'{time.strftime("%H:%M:%S")} {message}\n')
    print(message, flush=True)


def ack_count():
    return sum(line.startswith('ack ') for line in output.open(errors='replace'))


def stopped(node):
    result = subprocess.run(nodecmd + ['status', str(node)], capture_output=True,
                            text=True, check=True)
    return 'stopped' in result.stdout


def await_stopped(node):
    deadline = time.monotonic() + 15
    while not stopped(node):
        if time.monotonic() >= deadline:
            raise TimeoutError(f'node{node} did not stop')
        time.sleep(.1)


with output.open('w') as log:
    probe = subprocess.Popen([
        str(args.probe), '-addresses', addresses, '-prefix', args.tag,
        '-workers', '32', '-per-worker', '60', '-delay', '300ms',
        '-checkpoint-every-acks', '300', '-checkpoint-lag', '100',
        '-checkpoint-files', '-trace-backend-append',
    ], cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)

still_down = []
try:
    deadline = time.monotonic() + 60
    while ack_count() < 250:
        if probe.poll() is not None:
            raise RuntimeError(f'probe exited early: {probe.returncode}')
        if time.monotonic() >= deadline:
            raise TimeoutError('250 ACKs')
        time.sleep(.1)

    status = subprocess.run([str(root / 'chaos-client'), '-op', 'status',
                             '-addresses', addresses], capture_output=True,
                            text=True, check=True).stdout
    match = re.search(r'leader=(\d+) replicas=map\[(.*?)\]', status)
    if not match:
        raise RuntimeError(f'cannot identify data leader: {status}')
    ports = {int(replica): int(port) for replica, port in
             re.findall(r'(\d+):127\.0\.0\.1:(\d+)', match[2])}
    leader = (ports[int(match[1])] - 58201) // 10
    follower = next(node for node in range(3) if node != leader)
    still_down = [leader, follower]
    emit(f'kill leader node{leader} and follower node{follower} at ack={ack_count()}')
    for node in still_down:
        subprocess.run(nodecmd + ['kill', str(node)], check=True)
    for node in still_down:
        await_stopped(node)
    time.sleep(args.down_seconds)
    emit(f'restore at ack={ack_count()}')
    for node in still_down:
        subprocess.run(nodecmd + ['start', str(node)], check=True)
    still_down = []

    probe.wait(timeout=180)
    emit(f'probe exit={probe.returncode}, ack={ack_count()}')
    if probe.returncode != 0:
        raise RuntimeError(f'probe failed: {probe.returncode}')
finally:
    for node in still_down:
        if stopped(node):
            subprocess.run(nodecmd + ['start', str(node)], check=False)
    if probe.poll() is None:
        probe.terminate()
        try:
            probe.wait(timeout=5)
        except subprocess.TimeoutExpired:
            probe.kill()
            probe.wait()
