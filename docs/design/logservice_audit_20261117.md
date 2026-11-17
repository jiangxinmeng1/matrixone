# LogService 单 TN 路径检查与测试（2026-11-17）

基线：`upstream/main` `e765c5fd44`；已核对后续 `47957ef450` 未修改本轮涉及的 LogService 和 TN driver 代码。只检查单个逻辑 TN 使用的 LogService 写入、截断、快照路径；未运行 datasync，也未启动真实 TN。后续测试在本机启动了三副本 LogService/HAKeeper 测试集群。

## 本轮检查

| 路径 | 可疑交错 | 结果 |
|---|---|---|
| Log shard 导出快照、WAL 写入、快照导入 | 导入旧快照可能覆盖导出后已提交的写入，或物理截断后读不到已确认的 WAL | 新增 `TestImportExportedSnapshotWhileAppendingWAL`：导出 index 14 的快照，继续写入 10 条，把截断水位推进至快照位置，再在导入期间并发写入 100 条。逐条核对导出后共 110 条已确认记录的 LSN 和完整字节；连跑 10 次通过。测试直接调用 `importSnapshot`，未覆盖 HAKeeper 的导入许可判断。 |
| 导入快照后重建 LogService | 完成快照导入请求后重建服务，可能读不到快照之后已确认的 WAL | 新增 `TestImportedSnapshotRetainsLaterWALAcrossServiceRecreation`：在同一测试进程中关闭整个服务，保留同一 FS 与数据目录并创建新服务，核对快照后的 10 条 WAL 字节，再继续写入一条。连跑 3 次通过；这模拟服务重启，不等同于真实操作系统进程重启，且未单独证明重建时一定从该快照文件恢复。 |
| TN `logservicedriver` 追加失败后的重试 | `wrappedClient.Append` 退避使用 `time.Sleep`，不会因传入的 context 取消而立即退出 | [诊断测试](../../test/logservice-chaos/append_cancel/README.md)将重试窗口缩为 250 毫秒，并在首个 `AppendWAL` 内取消 context；三轮均继续尝试 3 次，约 602 毫秒才返回。真实 TN 的 `groupCommitter.Commit` 当前给该方法传入 `context.Background()`，因此本轮没有证明真实 TN 的普通提交会走到“调用方取消”时间线；未单列产品缺陷。 |
| 同一 NodeHost 上直接停止并重启同一副本 | 用于模拟快照导入后的冷重启 | 该诊断步骤得到 `shard already exist`，与真正关闭并重建服务进程不同。已从回归测试删除，不作为 LogService 缺陷。 |

## 后续值得测的时间线

1. 将上述并发写入与快照导入扩展到三投票副本，在导入开始前后切换 leader，并核对所有成功返回的 LSN。

本轮没有确认新的产品缺陷。

## 验证

- `GOWORK=off go list/build/vet -mod=readonly ./pkg/logservice`：通过。
- 新增快照并发写入测试连跑 10 次，服务重建测试连跑 3 次：通过。
- 两个新增测试以 `-race -count=3` 运行：通过；`pkg/logservice` 完整包普通测试及 `-race` 测试各跑 1 次：通过。
- 新工作树没有预编译的 `thirdparties/install`；本轮测试使用现存 `matrixone-upstream-main-20261111` 工作树的原生头文件和库。首次尝试使用主工作树旧版 USearch 头文件时编译失败，换成匹配的新版依赖后通过。没有把依赖不匹配当作产品缺陷。

## 读取游标与 RPC 故障补测（2026-11-17）

1. **超出 TN 读取批量预算的单条 WAL。** TN driver 每批以 64 MiB 读取，单条 WAL 默认上限为 99 MiB。使用生产默认 100 MiB RPC 上限，真实客户端写入 65 MiB、99 MiB 记录后，以 64 MiB 预算读取，均得到完整单条记录且游标正确指向后续小记录。进一步在导出快照后写入 99 MiB 记录，推进 `TRUNCATE_WAL` 水位并导入旧快照；导入前后记录内容和游标均正确，两轮通过。诊断代码见 [large_wal_read](../../test/logservice-chaos/large_wal_read/README.md)。该测试是单副本，直接调用导入函数；未覆盖 HAKeeper 导入许可或真实 TN WAL 编码。
2. **已有连接被黑洞后的 `GetWALTruncate`。** 实际 RPC 客户端经代理建立连接，然后让代理停止转发响应。首次查询约 8.002 秒后报错；客户端重置连接并改用健康直连地址，下一次查询约 1.3 毫秒成功，返回相同 PSN/DSN。此行为与当前 8 秒后端读取超时一致；外层 TN driver 会在错误后重试。本测试只覆盖单服务连接切换，尚未在三副本网络隔离下测量完整 TN driver 尾延迟。复现见 [get_wal_truncate_blackhole](../../test/logservice-chaos/get_wal_truncate_blackhole/README.md)。未据此建立新缺陷。

## 快照导入与后续截断并发（2026-11-17）

审查 `importSnapshot` 与 `handleWALTruncate` 的交错：旧快照导入期间，单个 TN 可以继续推进 checkpointed DSN 与物理截断 PSN。扩展 `TestImportedSnapshotRetainsLaterWALAcrossServiceRecreation`，在导出快照后写入 10 条 WAL，先截断到快照位置，再并发导入旧快照和推进至一条较新 WAL 的 PSN/DSN。等待两操作成功后关闭并重建 LogService，核对较新的截断状态、全部 10 条已确认 WAL 字节，并继续写入。普通模式连跑 20 次、`-race` 连跑 5 次通过。此测试在单投票副本上运行，服务重建发生于同一测试进程；没有覆盖三副本切主、HAKeeper 导入许可或实际 TN checkpoint 生成。未发现新缺陷。

## 导出快照后、TN 截断前重启（2026-11-17）

检查 `startReplica` 重新加载导出快照的窗口：快照已落在磁盘，TN 仍未推进 checkpointed DSN。新增 `TestExportedSnapshotSurvivesRestartBeforeWALTruncate`，先导出快照并写入后续 10 条 WAL，然后关闭并重建 LogService；确认快照管理器重新发现原快照且 checkpointed DSN 尚未前进。之后提交 `TRUNCATE_WAL`、导入原快照、再次重建服务，逐条核对后续 WAL、PSN/DSN 水位并继续写入。普通模式连跑 10 次、`-race` 连跑 3 次通过。测试为单副本、同进程服务重建，并直接调用快照导入；未覆盖三副本切主及 HAKeeper 导入许可。未发现新缺陷。

## 导入快照时发生一次 I/O 错误（2026-11-17）

检查 `importSnapshot` 失败后是否保留可重试的候选。新增 `TestImportSnapshotRetriesAfterCopyFailure`：导出快照、继续写入 10 条 WAL 并推进 `TRUNCATE_WAL` 后，在 Dragonboat 复制快照到内部临时目录时注入一次 `Create` 错误。首次导入按预期报错，快照管理器仍保留原候选；使用同一份快照立即重试成功，候选被删除，后续 WAL 字节和继续写入均正确。普通模式连跑 3 次、`-race` 连跑 5 次通过。该测试覆盖单副本和一次临时文件错误，不覆盖持续磁盘故障或三副本导入。审查 Dragonboat 实现还发现，导入请求的 context 只在入口检查，后续复制与提交不会因为调用方的 deadline 自动中断；因此本轮没有把“导入已完成但返回超时”作为已证实的路径。2026-11-17 再次获取 `upstream/main` 为 `47957ef450`，相对本分支基线 `e765c5fd44` 没有 LogService 或 TN logservicedriver 代码变化。未发现新缺陷。

## `TRUNCATE_WAL` 已提交但回包丢失（2026-11-17）

新增 `TestTruncateWALBlackholedResponseCanBeReconciled`：真实 RPC 客户端先经代理连到单副本 LogService，再丢弃服务端回包。服务端已经提交 PSN/DSN 水位，客户端约 16 秒后报错；客户端重连健康地址后，重复截断返回 `ErrInvalidTruncateLsn`，`GetWALTruncate` 查到原先提交的 PSN/DSN。普通模式共跑 3 次、`-race` 跑 1 次通过；单次 `-race` 测得等待 15.994 秒。另在 TN logservicedriver 新增 `TestTruncateReconcilesCommittedRequestWithLostReply`，模拟首次截断已提交但回包丢失：driver 重试并核对水位后正确返回成功；若 checkpointed DSN 小于请求值则拒绝确认。该测试普通模式连跑 10 次，新增不足水位断言后以 `-race` 连跑 10 次通过。约 16 秒是本测试连接被黑洞时的观测值，不能推断三副本中两票可用时的尾延迟；未发现新缺陷。

## 三副本快照、切主与成员变更（2026-11-17）

- `TestSnapshotImportAfterThreeReplicaLeaderFailure`：三副本 Log shard 和 HAKeeper 中先写入、由 follower 导出快照，再关闭 leader；客户端重试同一 WAL，提交 `TRUNCATE_WAL`，运行带 HAKeeper 检查的正常截断循环，核对两存活副本的已确认 WAL。最后用旧目录重建被关闭的服务并核对追赶。普通模式 10 轮、`-race` 3 轮通过。最初的 `Eventually` 回调可能独自阻塞到超时，曾产生误导性失败；改成每次读取独立 2 秒期限后未复现数据问题。
- `TestImportSnapshotExportedBeforeReplicaRemoval`：三成员快照导出后，停止并正式移除原 leader，再在剩余副本写入、导入旧快照；成员关系没有回退，快照后的 WAL 保留。普通模式 10 轮、`-race` 5 轮通过。
- `TestImportSnapshotExportedBeforeReplicaAddition`：两成员时导出快照，随后增加第三个投票副本并追赶，再导入旧快照；关闭原成员后，新成员与另一原成员仍可写入并读到所有已确认 WAL。普通模式 10 轮、`-race` 3 轮通过。
- `TestImportSnapshotRetriesAfterExportCleanupFailure`：快照已经成功导入 Dragonboat 后，清理外部快照目录时注入一次错误；候选仍可重试，重试后 WAL 完整。普通模式 1 轮、`-race` 5 轮通过。

以上测试使用本机测试服务和一个逻辑 TN 租约，没有启动真实 TN 进程，也没有覆盖 datasync。正常路径未发现新缺陷。

## 新缺陷：导出快照提交 I/O 错误导致整个 LogService 进程退出

独立诊断测试在外部快照目录从 `snapshot-*.generating` 重命名为最终目录时注入一次 `syscall.EIO`。3 个独立 Go 测试进程均直接 panic：`commit snapshot failed: input/output error`。LogService 通过 `SnapshotOption{Exported: true}` 请求该快照，本次导出不会压缩活动 Raft 日志；Dragonboat 快照 worker 却把提交错误统一传给 `panicNow`，调用方无法取得普通错误并在下一轮重试。此为单节点 I/O 故障注入，未实测三副本集群在此故障下的业务延迟。

复现源文件及单独运行方法见 [snapshot_export_commit_eio](../../test/logservice-chaos/snapshot_export_commit_eio/README.md)。该源文件以 `.go.txt` 存放，避免正常测试套件意外退出。缺陷记录：[matrixone#32836](https://github.com/matrixorigin/matrixone/issues/32836)，已分配给 `jiangxinmeng1`。

新增四个正常测试以 `-race -count=3` 联合运行通过；完整 `pkg/logservice` 包以 `-race -count=1` 运行通过（155.612 秒）。故障注入诊断在独立进程中运行，不包含在正常包测试内。
