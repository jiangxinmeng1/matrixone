# TN driver Append cancellation diagnostic

This probes `wrappedClient.Append` in isolation. It does **not** establish a production TN cancellation timeline: `groupCommitter.Commit` currently passes `context.Background()` to this helper.

Copy `repro_test.go.txt` to `pkg/vm/engine/tae/logstore/driver/logservicedriver/append_cancel_diagnostic_test.go`, then run with this repository's CGo dependencies configured:

```sh
GOWORK=off go test -mod=readonly ./pkg/vm/engine/tae/logstore/driver/logservicedriver -run '^TestDiagnosticAppendRetriesAfterCallerCancel$' -count=3 -v
```

On `upstream/main` `e765c5fd44`, all three runs made three `AppendWAL` attempts and returned after about 602 ms even though the context was canceled during the first attempt. The diagnostic uses a 250 ms retry budget. It is kept outside the regular test suite because it asserts the current helper behavior without proving a product defect.
