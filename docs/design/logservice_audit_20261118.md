# LogService 单 TN 路径补测（2026-11-18）

基线：`upstream/main` `517e169538`。延续[上一轮报告](https://github.com/jiangxinmeng1/matrixone/blob/codex/logservice-audit-20261117/docs/design/logservice_audit_20261117.md)，仅检查单个逻辑 TN 使用的 LogService 写入、截断、快照和读取路径；未运行 datasync，也未启动真实 TN。

## 快照导入、后续截断与重启

最近的读取修复增加了 `userEntryStateQuery` 和 `LeaseRejected` 分类。将上一轮的 `TestImportedSnapshotRetainsLaterWALAcrossServiceRecreation` 移到最新代码，保留同一 TN 租约：导出快照后写入 WAL，提交 `TRUNCATE_WAL`，并发导入快照和推进到较新的 PSN/DSN，关闭服务后使用原目录重建。重建后核对水位、截断水位之后每条已确认 WAL 的字节和 `UserRecord` 类型，再继续追加。

最初沿用旧断言，从快照位置读取已经被较新 checkpoint 覆盖的日志；最新实现返回 `invalid log range`，符合已提交的截断水位。把读取起点改成 `laterPSN+1` 后，普通模式连续 10 轮、`-race` 连续 3 轮通过。这个测试只覆盖单副本、同进程服务重建；直接调用快照导入，不覆盖 HAKeeper 导入许可。

本轮未确认新的产品缺陷。

## 三副本快照、切主与旧目录恢复

将上一轮的 `TestSnapshotImportAfterThreeReplicaLeaderFailure` 移到最新代码：三副本 Log shard 和 HAKeeper，使用含三个服务地址的同一个 TN 客户端写入；由普通副本导出快照后停止 leader，原有客户端先从断开的旧连接读取，再由剩余两票完成写入、`TRUNCATE_WAL` 和正常截断循环中的快照导入。最后按旧数据目录重建被停止的副本，核对所有已确认 WAL 与截断水位。普通模式连续 3 轮、`-race` 连续 2 轮通过。

此测试关闭的是整个服务进程，旧连接会直接报错；未模拟网络黑洞造成的超时。三副本集群仍是测试进程内的服务实例，没有启动真实 TN。未发现新缺陷。
