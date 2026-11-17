# Corrupt newest exported snapshot blocks truncation

`repro_test.go.txt` is a diagnostic Go test for a three-replica Log shard and
HAKeeper fixture using one logical TN lease. It exports two snapshots, replaces
the newer `.gbsnap` file with an empty file, advances `TRUNCATE_WAL` to the newer
index, and runs the normal LogService truncation tick twice. Both ticks choose
the corrupt candidate and return `invalid file size`. The older candidate can
be imported directly, but the next normal tick still chooses the corrupt one.

Run by copying the source to `pkg/logservice/snapshot_corrupt_candidate_test.go`
and executing:

```sh
GOWORK=off go test -mod=readonly ./pkg/logservice \
  -run '^TestDiagnosticCorruptNewestSnapshotBlocksOlderCandidate$' -count=5 -v
```

The repository's normal CGo include, library, and `LD_LIBRARY_PATH` setup is
needed. Five ordinary runs and three `-race` runs passed. The diagnostic source
is outside the normal test suite because it asserts current faulty behavior.

Related issue: [matrixone#32842](https://github.com/matrixorigin/matrixone/issues/32842).
