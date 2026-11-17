# Exported snapshot commit error crashes LogService

The diagnostic test in `repro_test.go.txt` injects `syscall.EIO` into the
filesystem `Rename` that commits a Log shard **exported** snapshot. It uses a
single LogService process and a single logical TN lease; no datasync path is
involved.

To run it, copy the file to `pkg/logservice/snapshot_export_rename_failure_test.go`
and run:

```sh
GOWORK=off go test -mod=readonly ./pkg/logservice \
  -run '^TestDiagnosticExportSnapshotCommitEIOPanics$' -count=1
```

The repository's normal CGo include, library, and `LD_LIBRARY_PATH` setup is
needed. Run in a separate process: the expected current behavior is a process
crash, not a Go test failure. Its final `require.ErrorIs` is unreachable.

Observed in three independent test processes: `panic: [00001:00001] commit
snapshot failed: input/output error`. Stack: Dragonboat `node.doSave`
(`node.go:797`) -> `ssWorker.workerMain` (`engine.go:266`) -> `panicNow`
(`nodehost.go:2333`). The LogService caller uses `SnapshotOption{Exported: true}`
in `pkg/logservice/truncation.go`, so this snapshot does not compact the active
Raft log. The injected failure is transient and occurs after snapshot data was
written but before the exported directory was committed. The process exits
before `SyncRequestSnapshot` can report an error to the truncation loop.

This test does not show that ordinary filesystems frequently return EIO, nor
does it test how the node recovers after a service manager restarts it.
