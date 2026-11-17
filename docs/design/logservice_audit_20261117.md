# LogService 单 TN 路径检查与测试（2026-11-17）

基线：`upstream/main` `e765c5fd44`。只检查单个逻辑 TN 使用的 LogService 写入、截断、快照路径；未运行 datasync，也未启动真实 TN 或三节点故障集群。

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
