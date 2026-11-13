# Stale TN lease advances truncate boundaries

Tracked as [matrixorigin/matrixone#32679](https://github.com/matrixorigin/matrixone/issues/32679).

Copy `repro_test.go.txt` to `pkg/logservice/truncate_lease_diagnostic_test.go`. With the repository's CGo headers and libraries configured, run:

```sh
GOWORK=off go test -mod=readonly ./pkg/logservice -run '^TestDiagnosticTruncateWithoutCurrentLease$' -count=3 -v
```

The diagnostic test connects one logical TN first as lease ID 100 and then as ID 101. After ten accepted appends with ID 101, a request bearing old ID 100 successfully advances the legacy truncate watermark from 0 to 8. A subsequent `TRUNCATE_WAL` bearing ID 100 advances the pair to PSN 9 and checkpointed DSN 1. An over-end `TRUNCATE` with LSN 1,000,000 is rejected with code 20418, and the watermark stays unchanged.

All three runs passed on 2026-11-13. The test asserts current buggy behavior and is kept outside the normal suite; remove the copied Go file after running.
