# Deterministic WAL replay matrix

Run `bash test/logservice-chaos/tn_driver_probe/replay_matrix/run.sh` from a MatrixOne checkout with the native test artifacts prepared. The runner temporarily installs the fixture as a package test, runs only `TestDiagnosticFixedReplayMatrix`, then removes it.

The fixture fixes the logical WAL order and skip target, then crosses two replacement shapes, seven physical gap configurations (no gap, or a one- or two-PSN gap at each of three positions), two physical truncate PSNs, and six read batch sizes: 168 cases. It asserts that replay returns the replacement WAL payload once, in logical LSN order. On the tested `9381db5dde` code, 48 cases fail: 24 report DSN overlap and 24 succeed but deliver the obsolete payload. The runner intentionally exits nonzero while that defect remains. It is a diagnostic fixture rather than a normal package test because the defect is open.

The fixture uses the production replayer with an injected backend Read result. The backend supplies an authoritative physical `LogRecord.Lsn` and omits one physical PSN at a chosen position, as observed on the three-replica fault disk. See the [test report](../../../../docs/design/logservice_three_node_failure_test.md) and [issue #32534](https://github.com/matrixorigin/matrixone/issues/32534) for live-cluster evidence and limitations.
