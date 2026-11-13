# HAKeeper bootstrap A→B→A deadline reproduction

Tracked as [matrixorigin/matrixone#32667](https://github.com/matrixorigin/matrixone/issues/32667).

This diagnostic uses three real HAKeeper Raft voters and one fake TN heartbeat. It submits bootstrap commands but intentionally does not execute the Log shard start commands, so the replicated state remains `HAKeeperBootstrapCommandsReceived` while the configured timeout elapses. No HAKeeper voter is stopped.

To run it, copy `repro_test.go.txt` into `pkg/logservice/bootstrap_retake_elapsed_diagnostic_test.go`, run the tests below, then remove the copied file. Set the repository's usual CGo headers and libraries before `go test`.

```sh
GOWORK=off go test -mod=readonly ./pkg/logservice -run '^TestDiagnosticBootstrapLeaderRetakeRealElapsed$/^short$' -count=3 -timeout=2m
GOWORK=off go test -mod=readonly ./pkg/logservice -run '^TestDiagnosticBootstrapLeaderRetakeRealElapsed$/^default$' -count=1 -v -timeout=8m
```

The `short` case uses a 30 ms HAKeeper check interval, giving a 3 second bootstrap window. It passed three consecutive repetitions. A separate verbose run passed with A's deadline at `12:07:55.073`, B's at `12:07:56.096`, and the replicated state becoming `BootstrapFailed` after A retook leadership at `12:07:55.147`.

The `default` case uses the normal 3 second interval and waits for the actual 5 minute window. It passed once: A's deadline was `2026-11-13 12:05:48.846 +08:00`, B's deadline was `12:06:50.028`, and A retook leadership and committed `BootstrapFailed` at `12:05:53.923`. All three HAKeeper replicas logged the failed state. The test does not directly edit `bootstrapCheckDeadline`; A sets it in `bootstrap()`, and B sets it on its first `checkBootstrap()`.

Full default-run output was saved locally as `/tmp/mo-bootstrap-retake-default.log` (about 5,244 lines); the timestamps above are the concise evidence. The test's bounded 10 second leadership-transfer assertions completed successfully.
