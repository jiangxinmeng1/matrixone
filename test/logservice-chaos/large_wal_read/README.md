# Large WAL read across exported-snapshot import

This diagnostic tests a one-replica LogService with the production default 100 MiB RPC message limit. Copy `repro_test.go.txt` to `pkg/logservice/large_wal_read_diagnostic_test.go` and run with the repository's CGo dependencies configured:

```sh
GOWORK=off go test -mod=readonly ./pkg/logservice -run '^TestDiagnosticReadWALLargerThanDriverBatch$' -count=1 -timeout=120s
```

The test exports a snapshot, appends a 99 MiB WAL record and a small following record, then reads with the TN driver's 64 MiB batch budget. It verifies that the oversized record is returned whole, the next cursor points to the small record, and both records remain readable after `TRUNCATE_WAL` and importing the earlier snapshot. The combined snapshot test passed twice on `upstream/main` `e765c5fd44`; a simpler read-only version also passed with 65 MiB and 99 MiB records.

The first attempt used the standard test fixture, whose service RPC limit is only 1000 bytes, and correctly received a connection reset. The diagnostic explicitly configures 100 MiB on both client and server. It calls `importSnapshot` directly after advancing the waterline and does not exercise HAKeeper's import permission check. Remove the copied Go file after running.
