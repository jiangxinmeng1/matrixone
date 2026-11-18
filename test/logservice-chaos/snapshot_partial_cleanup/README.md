# Partial cleanup after snapshot import leaves an unusable candidate

`repro_test.go.txt` is a diagnostic test for one Log shard, one HAKeeper replica,
and one TN lease. After a successful exported snapshot import, the filesystem
wrapper deletes the external `.gbsnap` file and returns an error from
`RemoveAll`. This models a partial directory removal. The snapshot is already
committed in Dragonboat, but LogService retains the failed external candidate
in memory. Two ordinary truncation ticks select it again and return
`snapshot is incomplete`. Confirmed WAL remains readable and new WAL can be
appended. Recreating the LogService with its old data directory skips the
incomplete candidate, and appending continues.

Copy the source to `pkg/logservice/snapshot_partial_cleanup_repro_test.go`, then run:

```sh
GOWORK=off go test -mod=readonly ./pkg/logservice \
  -run '^TestPartialSnapshotCleanupLeavesStaleImportCandidate$' -count=3 -v
```

Use the repository's normal CGo include and library settings. With those
settings, five ordinary runs of the pre-restart variant and three ordinary
plus three `-race` runs of the final restart variant passed. The source stays
outside the regular suite because it asserts the current faulty behavior.

Related issue: [matrixone#32842](https://github.com/matrixorigin/matrixone/issues/32842).

`orphan_repro_test.go.txt` tests the symmetric partial deletion order: the FS
wrapper removes `snapshot.metadata` and then reports the `RemoveAll` error.
Subsequent ticks fail while the process remains up. After restart, the manager
skips the incomplete directory, but its `.gbsnap` file still exists after a
normal truncation tick. This diagnostic produced a 1,074-byte orphan in the
small test fixture. It passed five ordinary and three `-race` runs. Copy it to
`pkg/logservice/snapshot_orphan_file_repro_test.go` and run:

```sh
GOWORK=off go test -mod=readonly ./pkg/logservice \
  -run '^TestPartialSnapshotCleanupLeavesOrphanSnapshotData$' -count=5 -v
```

The test uses fault injection; it does not establish how often a real
filesystem operation can fail after deleting only one file.
