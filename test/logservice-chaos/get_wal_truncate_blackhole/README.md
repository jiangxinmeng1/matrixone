# GetWALTruncate on an isolated established RPC connection

Copy `repro_test.go.txt` to `pkg/logservice/get_wal_truncate_blackhole_diagnostic_test.go` and run with the repository's CGo dependencies configured:

```sh
GOWORK=off go test -mod=readonly ./pkg/logservice -run '^TestDiagnosticGetWALTruncateBlackholedConnection$' -count=1 -v -timeout=60s
```

The client connects through a proxy that later stops forwarding responses. Its first `GetWALTruncate` on the existing connection returns an error after about 8.002 seconds. After the client resets and uses a direct healthy address, the next call succeeds in about 1.3 ms and returns the same PSN and checkpointed DSN. This is a one-service connection test, not a three-voter network partition or full TN driver retry test. The observed 8-second wait matches `defaultLogServiceBackendReadTimeout`; no new issue was filed from this observation.
