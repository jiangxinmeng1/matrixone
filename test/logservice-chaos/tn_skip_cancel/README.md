# TN replay skip append ignores cancellation during retry backoff

Copy `repro_test.go.txt` into `pkg/vm/engine/tae/logstore/driver/logservicedriver/append_skip_cancel_diagnostic_test.go`, then run:

```sh
GOWORK=off go test -mod=readonly -v -count=3 -timeout=120s \
  -run '^TestReplaySkipAppendCanceledContextStillWaitsRetryBudget$' \
  ./pkg/vm/engine/tae/logstore/driver/logservicedriver
```

This uses the real `replayer.AppendSkipCmd` and `wrappedClient.Append` code with a mock LogService client that returns `context.Canceled`. The context is canceled before the call. With `MaxTimeout=350ms`, each run took about 600ms, because the retry loop sleeps for 200ms and then 400ms before checking its elapsed budget. The default `MaxTimeout` is three minutes. The diagnostic assertion records the current bug; it is not a desired regression contract.

Production reachability: when a single TN replays out-of-order WAL for write, `tryScheduleApply` calls `appendSkipCmd(ctx, skipMap)` to persist a skip command. If that replay context is canceled while LogService requests are failing, the retry loop ignores cancellation until its elapsed budget. Ordinary commit Append uses a background context, so this reproduction does not establish an ordinary write cancellation issue or an observed three-minute TN shutdown delay.
