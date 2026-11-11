#!/usr/bin/env bash
set -euo pipefail

root=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
package=pkg/vm/engine/tae/logstore/driver/logservicedriver
target="$root/$package/replay_matrix_diagnostic_test.go"
fixture="$root/test/logservice-chaos/tn_driver_probe/replay_matrix/replay_matrix_diagnostic_test.go.txt"

if [[ -e "$target" ]]; then
  echo "refusing to replace $target" >&2
  exit 1
fi

cp "$fixture" "$target"
trap 'rm -f "$target"' EXIT
cd "$root"
.agents/skills/mo-dev/scripts/mo-cgo-test -v -run '^TestDiagnosticFixedReplayMatrix$' -count=1 -timeout=120s "./$package"
