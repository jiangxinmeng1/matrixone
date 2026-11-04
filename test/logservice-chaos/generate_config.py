#!/usr/bin/env python3
"""Generate an isolated three-process Logservice deployment for fault tests."""

import argparse
from pathlib import Path
from uuid import uuid5, NAMESPACE_URL


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--port-base", type=int, default=56400)
    parser.add_argument("--hosts", default="127.0.0.1,127.0.0.1,127.0.0.1",
                        help="comma-separated advertised hosts for nodes 0, 1, and 2")
    args = parser.parse_args()
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    hosts = args.hosts.split(",")
    if len(hosts) != 3 or any(not host for host in hosts):
        parser.error("--hosts requires three nonempty addresses")
    ids = [str(uuid5(NAMESPACE_URL, f"{root}/log-{i}")) for i in range(3)]
    addresses = [f"{hosts[i]}:{args.port_base + 10*i + 1}" for i in range(3)]
    seeds = [f"{hosts[i]}:{args.port_base + 10*i + 2}" for i in range(3)]
    members = [f"{131072 + i}:{uid}" for i, uid in enumerate(ids)]
    array = lambda values: "[" + ", ".join(f'"{value}"' for value in values) + "]"
    for i, uid in enumerate(ids):
        node = root / f"node-{i}"
        node.mkdir(exist_ok=True)
        config = f'''service-type = "LOG"
data-dir = "{node}"

[log]
level = "info"

[limit]
memory = "2GB"

[hakeeper-client]
service-addresses = {array(addresses)}

[[fileservice]]
name = "LOCAL"
backend = "DISK-V2"
data-dir = "{node / 'local'}"

[[fileservice]]
name = "SHARED"
backend = "DISK-V2"
data-dir = "{root / 'shared'}"

[[fileservice]]
name = "ETL"
backend = "DISK-ETL"

[observability]
disableTrace = true
disableMetric = true
status-port = {args.port_base + 100 + i}

[logservice]
uuid = "{uid}"
service-host = "{hosts[i]}"
deployment-id = 32043
logservice-port = {args.port_base + 10*i + 1}
raft-port = {args.port_base + 10*i}
gossip-port = {args.port_base + 10*i + 2}
gossip-seed-addresses = {array(seeds)}
gossip-allow-self-as-seed = true
truncate-interval = "1s"
hakeeper-truncate-interval = "1m"
max-exported-snapshot = 4

[logservice.HAKeeperConfig]
log-store-timeout = "30s"

[logservice.BootstrapConfig]
bootstrap-cluster = true
num-of-log-shards = 1
num-of-tn-shards = 1
num-of-log-shard-replicas = 3
init-hakeeper-members = {array(members)}
'''
        (root / f"log-{i}.toml").write_text(config)
    (root / "addresses.txt").write_text(",".join(addresses) + "\n")
    (root / "identity.txt").write_text("\n".join(f"{i} {uid} {addresses[i]}" for i, uid in enumerate(ids)) + "\n")


if __name__ == "__main__":
    main()
