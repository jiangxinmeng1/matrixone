# KillZombie exported snapshot orphan

Tracked as [matrixorigin/matrixone#32678](https://github.com/matrixorigin/matrixone/issues/32678). This diagnostic uses one Log shard replica and one TN ID; the `KillZombie` cleanup path is local to the LogService store.

Copy `repro_test.go.txt` into `pkg/logservice/kill_zombie_snapshot_diagnostic_test.go`. With the repository's CGo headers and libraries configured, run:

```sh
GOWORK=off go test -mod=readonly ./pkg/logservice -run '^TestDiagnosticKillZombieLeavesExportedSnapshot$' -count=3 -v -timeout=2m
```

All three repetitions passed on 2026-11-13. Each exported snapshot index 14, ran `KillZombie`, verified the replica and metadata disappeared, then ran `processTruncateLog`. The directory `exported-snapshot/shard-1/replica-1/snapshot-000000000000000E` remained on disk.

Remove the copied Go file after running. It asserts the current incorrect behavior and is kept outside the normal suite so a cleanup fix can change the expected result.
