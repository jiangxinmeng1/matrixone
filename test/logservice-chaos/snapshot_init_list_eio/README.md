# Snapshot directory read error crashes LogService during replica restart

`repro_test.go.txt` starts a LogService replica, closes it, and restarts the
service with the same data and filesystem. A filesystem wrapper allows the
first read of the exported snapshot directory (interrupted-export cleanup) and
returns `syscall.EIO` on the second read (snapshot manager initialization).

Run in a separate process by copying the source to
`pkg/logservice/snapshot_init_list_eio_test.go` and executing:

```sh
GOWORK=off go test -mod=readonly ./pkg/logservice \
  -run '^TestDiagnosticRestartSnapshotDirectoryListEIO$' -count=1
```

Use the repository's normal CGo include, library and `LD_LIBRARY_PATH` setup.
The current result is `panic: input/output error` at `pkg/logservice/store.go:674`.
It reproduced in three independent test processes. The diagnostic source is
kept outside the normal test suite because it exits the test process.

The injected error is transient and occurs only while starting a retained Log
replica. This test does not measure how often a real filesystem returns EIO or
what an external service supervisor does after the crash.

Related issue: [matrixone#32836](https://github.com/matrixorigin/matrixone/issues/32836).
