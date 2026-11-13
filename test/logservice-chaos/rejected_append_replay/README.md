# Rejected WAL append returned as a replayable UserRecord

Tracked as [matrixorigin/matrixone#32677](https://github.com/matrixorigin/matrixone/issues/32677). These diagnostic tests use one logical TN and a sequential lease change from replica ID 100 to 101. Both tests use the same fixed, valid TAE WAL payload containing DSN 1.

Copy `rejected_read_test.go.txt` into `pkg/logservice/rejected_read_diagnostic_test.go` and `driver_replay_test.go.txt` into `pkg/vm/engine/tae/logstore/driver/logservicedriver/rejected_record_diagnostic_test.go`. With the repository's CGo headers and libraries configured, run:

```sh
GOWORK=off go test -mod=readonly ./pkg/logservice -run '^TestDiagnosticRejectedAppendVisibleAsUserRecord$' -count=3 -v -timeout=2m
GOWORK=off go test -mod=readonly ./pkg/vm/engine/tae/logstore/driver/logservicedriver -run '^TestDiagnosticRejectedUserRecordIsAppliedByTNReplay$' -count=3 -v -timeout=2m
```

The first test calls LogService's `CONNECT` with IDs 100 then 101, sends `APPEND_WAL` with ID 100, verifies error code 20419 and response LSN 0, then reads the identical command bytes at Raft LSN 5 with type `UserRecord`. The second gives the actual `logservicedriver` replayer the matching record sequence: internal entries at LSN 1–2, lease updates at 3–4, and that same WAL payload as `UserRecord` at LSN 5. Its apply callback receives DSN 1. Both tests passed three consecutive repetitions on 2026-11-13.

Remove the copied Go files after running. They assert the current incorrect behavior and are kept outside the normal test suite so a future fix can change the expected result.
