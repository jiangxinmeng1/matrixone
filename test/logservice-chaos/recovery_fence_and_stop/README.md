# WAL recovery lease fence and stopped replica snapshots

Copy `repro_test.go.txt` to `pkg/logservice/recovery_fence_diagnostic_test.go`. With the repository's CGo headers and libraries configured, run:

```sh
GOWORK=off go test -mod=readonly ./pkg/logservice -run '^TestDiagnostic(ConnectDuringWALRecoveryFence|StopReplicaRetainsExportedSnapshot)$' -count=3 -v
```

The first test establishes TN lease ID 100, begins WAL recovery with zero entries, and calls both `CONNECT(100)` and `UPDATE_LEASEHOLDER_ID(100)`. Both return success even though the recovery fence retains lease ID `MaxUint64`; the next `APPEND_WAL(100)` returns `ErrNotLeaseHolder` (20419). After the recovery end marker, `CONNECT(100)` and `APPEND_WAL(100)` succeed. This is a misleading connection result during an intentionally write-blocked recovery window. The test does not establish a separate data-loss defect.

The second test exports a snapshot, stops the replica, and runs one truncation tick. The exported snapshot remains on disk because the stopped replica's metadata remains available for a later restart. This differs from `KillZombie`, which permanently deletes the metadata and leaves a snapshot directory without that cleanup entry point (issue #32678).

Both tests passed three consecutive runs on 2026-11-13. Remove the copied Go file after running.
