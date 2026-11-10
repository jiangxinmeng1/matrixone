#!/usr/bin/env bash
set -euo pipefail

root_dir=$1
operation=$2
node_index=$3
node_dir="$root_dir/node-$node_index"
pid_file="$node_dir/pid"
binary="$root_dir/mo-service"

case "$operation" in
  start)
    if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
      echo "node $node_index already running: $(cat "$pid_file")" >&2
      exit 1
    fi
    if [[ -f "$root_dir/netns-prefix" ]]; then
      namespace="$(cat "$root_dir/netns-prefix")$node_index"
      sudo -n ip netns exec "$namespace" runuser -u "$(id -un)" -- \
        bash -c 'cd "$1"; export LD_LIBRARY_PATH="$2/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"; nohup "$2/mo-service" -cfg "$3" >> "$4" 2>&1 < /dev/null & echo $! > "$5"' \
        _ "$node_dir" "$root_dir" "$root_dir/log-$node_index.toml" "$node_dir/stdout.log" "$pid_file"
    else
      (
        cd "$node_dir"
        nohup "$binary" -cfg "$root_dir/log-$node_index.toml" \
          >> "$node_dir/stdout.log" 2>&1 < /dev/null &
        echo $! > "$pid_file"
      )
    fi
    echo "started node $node_index pid=$(cat "$pid_file")"
    ;;
  kill|term)
    [[ -f "$pid_file" ]] || { echo "missing pid file for node $node_index" >&2; exit 1; }
    pid=$(cat "$pid_file")
    [[ -e "/proc/$pid/exe" ]] || { echo "node $node_index already stopped"; exit 0; }
    actual=$(readlink -f "/proc/$pid/exe")
    [[ "$actual" == "$(readlink -f "$binary")" ]] || {
      echo "refusing to signal pid $pid: executable is $actual" >&2
      exit 1
    }
    if [[ "$operation" == "kill" ]]; then
      kill -KILL "$pid"
    else
      kill -TERM "$pid"
    fi
    echo "signaled node $node_index pid=$pid with $operation"
    ;;
  status)
    if [[ -f "$pid_file" ]]; then
      pid=$(cat "$pid_file")
      if [[ -e "/proc/$pid/exe" ]]; then
        echo "node $node_index running pid=$pid exe=$(readlink -f "/proc/$pid/exe")"
      else
        echo "node $node_index stopped pid=$pid"
      fi
    else
      echo "node $node_index not started"
    fi
    ;;
  *)
    echo "usage: $0 ROOT start|kill|term|status NODE_INDEX" >&2
    exit 2
    ;;
esac
