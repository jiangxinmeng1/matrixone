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
