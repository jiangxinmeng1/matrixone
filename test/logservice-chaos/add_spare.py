#!/usr/bin/env python3
"""Add a fourth, empty Logservice store to an existing isolated deployment."""

import argparse
import re
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--index", type=int, default=3, help="new node index")
    parser.add_argument("--host", default="127.0.0.1", help="advertised host for the new node")
    args = parser.parse_args()
    root = args.root.resolve()
    old_config = (root / "log-0.toml").read_text()
    old_dir = str(root / "node-0")
    if args.index < 3:
        parser.error("--index must be at least 3")
    new_dir = str(root / f"node-{args.index}")
    new_uuid = str(uuid5(NAMESPACE_URL, f"{root}/log-{args.index}"))
    old_uuid = re.search(r'(?m)^uuid = "([^"]+)"$', old_config)
    raft_base = re.search(r"(?m)^raft-port = (\d+)$", old_config)
    status_base = re.search(r"(?m)^status-port = (\d+)$", old_config)
    if old_uuid is None or raft_base is None or status_base is None or old_dir not in old_config:
        raise ValueError("log-0.toml does not match the expected isolated configuration")
    if (root / f"log-{args.index}.toml").exists() or (root / f"node-{args.index}").exists():
        raise FileExistsError("spare config or data directory already exists")

    replacements = {
        "raft-port": int(raft_base.group(1)) + args.index * 10,
        "logservice-port": int(raft_base.group(1)) + args.index * 10 + 1,
        "gossip-port": int(raft_base.group(1)) + args.index * 10 + 2,
        "status-port": int(status_base.group(1)) + args.index,
    }

    def replace_port(match: re.Match[str]) -> str:
        return f"{match.group(1)} = {replacements[match.group(1)]}"

    config = old_config.replace(old_dir, new_dir)
    config = config.replace(f'uuid = "{old_uuid.group(1)}"', f'uuid = "{new_uuid}"')
    if re.search(r'(?m)^service-host = "[^"]+"$', config):
        config = re.sub(r'(?m)^service-host = "[^"]+"$',
                        f'service-host = "{args.host}"', config)
    else:
        config = config.replace(f'uuid = "{new_uuid}"',
                                f'uuid = "{new_uuid}"\nservice-host = "{args.host}"')
    config = config.replace("bootstrap-cluster = true", "bootstrap-cluster = false")
    config = re.sub(
        r"(?m)^(raft-port|logservice-port|gossip-port|status-port) = \d+$",
        replace_port,
        config,
    )
    for key, port in replacements.items():
        if f"{key} = {port}" not in config:
            raise ValueError(f"failed to set {key}")
    if "bootstrap-cluster = false" not in config:
        raise ValueError("spare must not bootstrap the cluster")

    (root / f"node-{args.index}").mkdir()
    (root / f"log-{args.index}.toml").write_text(config)
    print(f"new empty store uuid={new_uuid} rpc={args.host}:{replacements['logservice-port']} data={new_dir}")


if __name__ == "__main__":
    main()
