#!/usr/bin/env python3
"""Drop only one dedicated test RPC port during WAL replay initialization."""
import argparse
from pathlib import Path
import os
import random
import re
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument('--root', type=Path, default=Path('/home/mo/jxm/logservice-tnwal-lifecycle-20261105'))
parser.add_argument('--probe', type=Path, default=Path('/home/mo/jxm/tn-wal-probe-lifecycle'))
parser.add_argument('--tag', default='replay-net-repeat')
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
                                  '-replay-ready-file', str(ready),
                                  '-replay-release-file', str(release)],
                                 cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
    drop = None
    try:
        deadline = time.monotonic() + 30
        while not ready.exists():
            if probe.poll() is not None:
                raise RuntimeError(f'probe exited {probe.returncode} before ready')
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
        port = str(58201 + 10 * node)
        pid = int((root / f'node-{node}' / 'pid').read_text())
        if os.path.realpath(f'/proc/{pid}/exe') != str(root / 'mo-service'):
            raise RuntimeError('wrong node executable')
        comment = f'JXM_TN_WAL_NET_{os.getpid()}_{attempt}'
        drop = ['sudo', '-n', '/sbin/iptables', '-w', '-p', 'tcp', '-d', '127.0.0.1',
                '--dport', port, '-m', 'comment', '--comment', comment, '-j', 'DROP']
        subprocess.run(drop[:4] + ['-I', 'OUTPUT', '1'] + drop[4:], check=True)
        # A detached safety cleanup protects the host if the controller exits.
        delete_rule = drop[:4] + ['-D', 'OUTPUT'] + drop[4:]
        subprocess.Popen(['python3', '-c',
                          'import subprocess,sys,time; time.sleep(220); subprocess.run(sys.argv[1:])',
                          *delete_rule],
                         start_new_session=True, stdin=subprocess.DEVNULL,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        start = time.monotonic()
        release.touch()
        time.sleep(4)
        if probe.poll() is not None:
            print(f'{tag}: follower node{node} port={port}, replay finished in {time.monotonic()-start:.2f}s', flush=True)
            continue
        print(f'{tag}: follower node{node} port={port}, replay stalled', flush=True)
        survivor_addresses = ','.join(f'127.0.0.1:{58201+10*i}' for i in range(3) if i != node)
        healthy = subprocess.run([str(root / 'chaos-client'), '-op', 'truncated',
                                  '-addresses', survivor_addresses], text=True,
                                 capture_output=True, check=True).stdout
        print(healthy.strip(), flush=True)
        try:
            probe.wait(timeout=190)
        except subprocess.TimeoutExpired:
            print(f'{tag}: still blocked after {time.monotonic()-start:.2f}s', flush=True)
        else:
            print(f'{tag}: exit={probe.returncode} after {time.monotonic()-start:.2f}s', flush=True)
        break
    finally:
        if drop is not None:
            subprocess.run(drop[:4] + ['-D', 'OUTPUT'] + drop[4:], check=False)
        if probe.poll() is None:
            try:
                probe.wait(timeout=8)
            except subprocess.TimeoutExpired:
                probe.kill()
                probe.wait()
