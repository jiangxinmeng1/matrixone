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
