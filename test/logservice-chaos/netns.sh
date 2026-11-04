#!/usr/bin/env bash
set -euo pipefail

root_dir=$1
operation=$2
argument=${3:-}
bridge=mocxbr
prefix=mocxn
host_prefix=mocxh
subnet=198.18.51

case "$operation" in
  setup)
    [[ "$argument" =~ ^[0-9]+$ ]] || { echo "setup requires node count" >&2; exit 2; }
    [[ ! -f "$root_dir/netns-prefix" ]] || { echo "network already configured" >&2; exit 1; }
    sudo -n ip link show "$bridge" >/dev/null 2>&1 && { echo "bridge already exists" >&2; exit 1; }
    existing_namespaces=$(sudo -n ip netns list | cut -d ' ' -f 1)
    for ((i=0; i<argument; i++)); do
      if grep -Fxq "$prefix$i" <<< "$existing_namespaces" || sudo -n ip link show "$host_prefix$i" >/dev/null 2>&1; then
        echo "test network name already exists for node $i" >&2
        exit 1
      fi
    done
    complete=false
    cleanup_partial() {
      if [[ "$complete" == false ]]; then
        for ((j=0; j<argument; j++)); do
          sudo -n ip netns del "$prefix$j" 2>/dev/null || true
          sudo -n ip link del "$host_prefix$j" 2>/dev/null || true
        done
        sudo -n ip link del "$bridge" 2>/dev/null || true
      fi
    }
    trap cleanup_partial EXIT
    sudo -n ip link add "$bridge" type bridge
    sudo -n ip addr add "$subnet.1/24" dev "$bridge"
    sudo -n ip link set "$bridge" up
    for ((i=0; i<argument; i++)); do
      sudo -n ip netns add "$prefix$i"
      sudo -n ip link add "$host_prefix$i" type veth peer name "mocxp$i"
      sudo -n ip link set "mocxp$i" netns "$prefix$i"
      sudo -n ip link set "$host_prefix$i" master "$bridge"
      sudo -n ip link set "$host_prefix$i" up
      sudo -n ip -n "$prefix$i" link set lo up
      sudo -n ip -n "$prefix$i" link set "mocxp$i" name eth0
      sudo -n ip -n "$prefix$i" addr add "$subnet.$((10+i))/24" dev eth0
      sudo -n ip -n "$prefix$i" link set eth0 up
    done
    echo "$prefix" > "$root_dir/netns-prefix"
    complete=true
    trap - EXIT
    ;;
  down|up)
    [[ "$argument" =~ ^[0-9]+$ ]] || { echo "$operation requires node index" >&2; exit 2; }
    [[ -f "$root_dir/netns-prefix" && "$(cat "$root_dir/netns-prefix")" == "$prefix" ]] || {
      echo "this root does not own the test network" >&2
      exit 1
    }
    sudo -n ip link set "$host_prefix$argument" "$operation"
    ;;
  teardown)
    [[ "$argument" =~ ^[0-9]+$ ]] || { echo "teardown requires node count" >&2; exit 2; }
    [[ -f "$root_dir/netns-prefix" && "$(cat "$root_dir/netns-prefix")" == "$prefix" ]] || {
      echo "this root does not own the test network" >&2
      exit 1
    }
    for ((i=0; i<argument; i++)); do
      sudo -n ip netns del "$prefix$i" 2>/dev/null || true
      sudo -n ip link del "$host_prefix$i" 2>/dev/null || true
    done
    sudo -n ip link del "$bridge" 2>/dev/null || true
    rm -f "$root_dir/netns-prefix"
    ;;
  *)
    echo "usage: $0 ROOT setup|down|up|teardown COUNT_OR_INDEX" >&2
    exit 2
    ;;
esac
