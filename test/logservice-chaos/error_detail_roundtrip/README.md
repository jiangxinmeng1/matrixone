# LogService RPC error detail round trip

Tracked as [matrixorigin/matrixone#32683](https://github.com/matrixorigin/matrixone/issues/32683).

Copy `repro_test.go.txt` to `pkg/logservice/error_detail_diagnostic_test.go`. With the repository's CGo headers and libraries configured, run:

```sh
GOWORK=off go test -mod=readonly ./pkg/logservice -run '^TestDiagnosticClientErrorDetails$' -count=3 -v
```

The test uses the real LogService RPC client. A rejected `TRUNCATE(3)` has server message `invalid truncate lsn for shard 1 at boundary 4`, while the client reports `invalid truncate lsn for shard 0 at boundary 0`. After the lease moves from TN ID 2 to 42, a rejected APPEND has server message `not lease holder, current lease holder ID 42`, while the client reports ID 3735928559. Both client errors retain their correct `moerr` codes. Three consecutive runs passed on 2026-11-13.

Remove the copied Go file after running. It asserts the current incorrect error text and is kept outside the normal test suite.
