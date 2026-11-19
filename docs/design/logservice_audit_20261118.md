# LogService 单 TN 路径补测（2026-11-18）

基线：`upstream/main` `517e169538`；随后获取的 `035af99680` 未修改本轮涉及的 LogService 和 TN driver 文件。延续[上一轮报告](https://github.com/jiangxinmeng1/matrixone/blob/codex/logservice-audit-20261117/docs/design/logservice_audit_20261117.md)，仅检查单个逻辑 TN 使用的 LogService 写入、截断、快照和读取路径；未运行 datasync，也未启动真实 TN。

## 快照导入、后续截断与重启

最近的读取修复增加了 `userEntryStateQuery` 和 `LeaseRejected` 分类。将上一轮的 `TestImportedSnapshotRetainsLaterWALAcrossServiceRecreation` 移到最新代码，保留同一 TN 租约：导出快照后写入 WAL，提交 `TRUNCATE_WAL`，并发导入快照和推进到较新的 PSN/DSN，关闭服务后使用原目录重建。重建后核对水位、截断水位之后每条已确认 WAL 的字节和 `UserRecord` 类型，再继续追加。

最初沿用旧断言，从快照位置读取已经被较新 checkpoint 覆盖的日志；最新实现返回 `invalid log range`，符合已提交的截断水位。把读取起点改成 `laterPSN+1` 后，普通模式连续 10 轮、`-race` 连续 3 轮通过。这个测试只覆盖单副本、同进程服务重建；直接调用快照导入，不覆盖 HAKeeper 导入许可。

本轮未确认新的产品缺陷。

## 三副本快照、切主与旧目录恢复

将上一轮的 `TestSnapshotImportAfterThreeReplicaLeaderFailure` 移到最新代码：三副本 Log shard 和 HAKeeper，使用含三个服务地址的同一个 TN 客户端写入；由普通副本导出快照后停止 leader，原有客户端先从断开的旧连接读取，再由剩余两票完成写入、`TRUNCATE_WAL` 和正常截断循环中的快照导入。最后按旧数据目录重建被停止的副本，核对所有已确认 WAL 与截断水位。普通模式连续 3 轮、`-race` 连续 2 轮通过。

此测试关闭的是整个服务进程，旧连接会直接报错；未模拟网络黑洞造成的超时。三副本集群仍是测试进程内的服务实例，没有启动真实 TN。未发现新缺陷。

## 过期 TN 记录与快照导入判定

静态检查发现 `shouldDoImport` 会检查 HAKeeper 中所有 TN store 的非零 `ReplayedLsn`，不区分 store 是否过期。一次白盒测试通过真实 HAKeeper 状态机确认：若人为给已经过期的旧 TN 设置较低的非零水位，当前 TN 即使水位足够，导入仍被阻止，三轮均重现。随后核对当前 TN 心跳实现：`pkg/tnservice/store_heartbeat.go` 始终发送 `ReplayedLsn=0`，而该导入检查会跳过 0。因此这个人为构造的非零旧水位不属于当前单 TN 实际路径；诊断测试已撤销，未作为产品缺陷。

## 超过 TN 读取批量预算的 WAL

将上一轮的 99 MiB WAL 诊断移到最新代码，并补充 `UserRecord` 分类断言。单条记录超过 TN driver 的 64 MiB 读取预算；在快照导入前后，读取均返回完整记录，下一游标指向后续小记录，内容和类型均正确。普通模式连续 2 轮、`-race` 1 轮通过。此测试为单副本，直接调用快照导入；没有启动真实 TN。

## 三副本 RPC 黑洞与不确定截断结果

新增 `TestThreeReplicaClientRecoversFromBlackholedConnection`：三投票副本保持运行，同一个 TN 客户端的已有 RPC 连接固定到一台的代理，代理在 CONNECT 后丢弃响应。`Read` 三轮均在约 7.00 秒后重新连接并取回已确认 WAL；`GetWALTruncate` 单次约 8.00 秒报错，下一次调用在约 0.8–5.9 毫秒内成功。两票在整个黑洞阶段保持可用，没有一分钟级等待。

同一测试随后让 `TruncateWAL` 经黑洞连接提交，直接从另一个副本确认多数派已有 PSN/DSN，再停止原 leader。三轮中首次截断请求均收到 EOF；新 leader 的 `GetWALTruncate` 返回已提交水位，重复截断返回 `ErrInvalidTruncateLsn`，符合 TN driver 已有的查询水位后确认逻辑。扩展后普通模式连续 3 轮、`-race` 1 轮通过。这里调用了真实 LogService 客户端，未启动 TN driver 实例；它验证了 driver 重试所依赖的服务端状态，但不单独验证 driver 的完整生命周期。

## 新触发方式：导入完成后外部快照目录部分清理失败

`importSnapshot` 先让 Dragonboat 导入，再调用 `snapshotMgr.Remove` 删除外部快照。若 `RemoveAll` 已删掉 `.gbsnap` 文件后报错，管理器因错误仍保留内存候选；之后正常 `processShardTruncateLog` 再次挑中该不完整目录，返回 `snapshot is incomplete`。故障注入使用包装文件系统模拟“部分删除后报错”，不是实测真实磁盘发生该错误。普通模式先以五轮确认连续两个截断 tick 均失败；增加保留目录重启检查后，普通模式三轮、`-race` 三轮仍稳定复现。首次导入已提交，后续已确认 WAL 可读，继续写入也成功。重启后管理器跳过不完整候选并能继续写入；未验证长期运行时磁盘容量增长幅度。

诊断代码与运行方法见 [snapshot_partial_cleanup](../../test/logservice-chaos/snapshot_partial_cleanup/README.md)。这个时间线与 [matrixone#32842](https://github.com/matrixorigin/matrixone/issues/32842) 的“不可用快照候选反复挡住正常截断”机制相同，作为该缺陷的补充复现，不另建重复 issue。

进一步测试对称的部分删除顺序：先删除 `snapshot.metadata`，再从 `RemoveAll` 返回错误。运行中的进程仍会反复选择坏候选；保留目录重启后，管理器会跳过该不完整目录，但其中的 `.gbsnap` 文件仍在，后续正常截断 tick 也未清除。小测试快照留下 1,074 字节；真实快照大小可能不同，未测量长期累计占用。普通模式五轮、`-race` 三轮通过。复现源文件见同目录的 `orphan_repro_test.go.txt`；这同样属于故障注入，尚未在真实磁盘上观察到部分删除。

## 快照导入期间读取仍需保留的 WAL

检查 `queryLog` 先读取状态机最新位置、再查询 Dragonboat 物理日志、最后按租约历史分类的三段操作。新增 `TestReadRetainedWALWhileImportingSnapshot`：单 TN 租约下写入第一批 WAL，导出快照；写入第二批、提交覆盖第一批的截断水位，同时启动快照导入和 100 次第二批读取，每次核对所有已确认 WAL 的 LSN、字节及 `UserRecord` 分类。普通模式连续 3 轮、`-race` 连续 2 轮通过，未发现新的读取错误。测试调用服务端 `queryLog` 和快照导入方法，没有运行 TN driver；并发启动不保证读请求一定落在导入内部的特定阶段，不能据此排除更窄的竞态窗口。

## 新发现：TN replay 写入 skip command 时忽略取消

`replayer.tryScheduleApply` 在单个 TN 的乱序 WAL 回放中调用 `AppendSkipCmd(ctx, skipMap)`；该函数进入 `wrappedClient.Append`。后者每次失败后直接 `time.Sleep(RetryInterval)`，没有检查 `ctx.Done()`。白盒复现使用真实 replayer 和包装客户端，后端在已取消 context 下返回 `context.Canceled`：将 `MaxTimeout` 缩短为 350ms，连续 3 轮均在约 600ms 后才返回。生产默认 `MaxTimeout` 为 3 分钟；这里没有等待三分钟或启动完整 TN，因此实际 TN 进程级延迟尚未测量。普通事务提交的 Append 使用 background context，本证据只指向 replay skip 写入。复现源文件和命令见 [tn_skip_cancel](../../test/logservice-chaos/tn_skip_cancel/README.md)，缺陷记录为 [matrixone#32912](https://github.com/matrixorigin/matrixone/issues/32912)。
