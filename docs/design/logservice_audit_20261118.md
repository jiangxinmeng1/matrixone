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

继续扩展同一测试：第一次导入后立刻通过正常截断循环导出第二份快照，再写入、提交新的 PSN/DSN 水位、导入第二份快照，最后用原数据目录重启服务。第二次导入前后和重启后，核对水位之后每条已确认 WAL 的内容和 `UserRecord` 类型。普通模式连续 5 轮、`-race` 连续 2 轮通过；这覆盖了候选快照消费后重新导出的状态更新，未发现新缺陷。测试仍为单副本，快照导入由测试直接触发，没有运行真实 TN。

曾尝试从 `latest LSN + 1` 直接调用 `queryLog`，稳定返回 Dragonboat 的 `invalid log range`。核对 TN replayer 的游标推进后，正常回放不会自行发出这个越界请求；该断言已撤销，未计为产品缺陷。

## 新发现：TN replay 写入 skip command 时忽略取消

`replayer.tryScheduleApply` 在单个 TN 的乱序 WAL 回放中调用 `AppendSkipCmd(ctx, skipMap)`；该函数进入 `wrappedClient.Append`。后者每次失败后直接 `time.Sleep(RetryInterval)`，没有检查 `ctx.Done()`。白盒复现使用真实 replayer 和包装客户端，后端在已取消 context 下返回 `context.Canceled`：将 `MaxTimeout` 缩短为 350ms，连续 3 轮均在约 600ms 后才返回。生产默认 `MaxTimeout` 为 3 分钟；这里没有等待三分钟或启动完整 TN，因此实际 TN 进程级延迟尚未测量。普通事务提交的 Append 使用 background context，本证据只指向 replay skip 写入。复现源文件和命令见 [tn_skip_cancel](../../test/logservice-chaos/tn_skip_cancel/README.md)，缺陷记录为 [matrixone#32912](https://github.com/matrixorigin/matrixone/issues/32912)。

## TSO 命令造成物理 PSN 间隔时的 TN 回放和截断

`getNextValidPSN` 会逐一检查两个已记录 WAL PSN 之间的物理索引；LogService 的 TSO 命令也提交在第一个 Log shard 上。微测量中，100 万个合成空位约需 8ms，1000 万个约需 76–83ms，尚无足够证据将其归为产品长尾缺陷。随后新增 `TestReplayAcrossTSOEntriesOnSameLogShard` 使用真实 LogService 和单个 TN driver：写入 DSN 1，经同一 shard 提交 200 次 TSO 更新，再写入 DSN 2。测试确认两个 WAL PSN 相隔至少 200；重建 driver 后按顺序回放 1、2，提交 DSN 1 的 `TruncateWAL` 后再重建，只回放 2。普通模式连续 3 轮、`-race` 连续 2 轮通过。该测试是单副本，没有启动完整 TN，也没有强制物理快照导入。

## 三副本切主时跨 TSO 间隔读取、截断和继续写入

检查了三地址客户端在 leader 下线后沿用旧连接的恢复路径。新增 `TestThreeReplicaClientReadsAcrossTSOGapAfterLeaderFailure`：三投票副本、同一个 TN ID 的客户端先写两条 WAL，中间在同一 Log shard 提交 100 条 TSO 命令；核实 WAL 的物理 LSN 间隔至少 100 后停止 leader。剩余两票选主后，原客户端读回两条 WAL，提交 `TruncateWAL` 并核对 PSN/DSN 水位，再读取保留记录、继续追加第三条并读回。普通模式连续 3 轮、`-race` 连续 2 轮通过，未发现新缺陷。测试关闭整个 leader 服务而不是注入网络黑洞；未启动真实 TN driver，也未触发物理快照导入。

## 停副本期间导出迟到完成，再用旧目录重启

新增 `TestStoppedReplicaRecoversWALAfterLateSnapshotExport`：单副本先确认 12 条 WAL；包装文件系统在导出快照的最终 rename 处暂停，随后停止副本，使导出请求返回 `ErrShardClosed`；再放行 rename，明确确认完整快照目录已经出现。关闭 store、用同一数据目录重建后，先逐条核对原 WAL，再确认快照管理器加载了迟到候选；继续写入新 WAL、提交旧快照位置的 `TruncateWAL`，通过正常截断循环导入该候选，最终读回新 WAL。普通模式连续 3 轮、`-race` 连续 2 轮通过，未发现新缺陷。测试用文件系统钩子固定时序，关闭后台 worker 并直接调用 store 接口；不代表真实磁盘故障或完整 TN 进程测试。首次诊断因夹具重建时传入空 task service getter 却启动 HAKeeper worker 而 panic；按现有手动测试配置关闭后台 worker 后，目标时序通过，未将该夹具 panic 计为产品缺陷。

## 同一 TN 反复 CONNECT 后的租约历史快照与读取分类

`handleSetLeaseHolderID` 对重复的同一 TN ID 仍记录新的租约位置，因此补测了重连导致的租约历史增多。扩展 `TestImportedSnapshotRetainsLaterWALAcrossServiceRecreation`：快照导出前连续 100 次、导出后再连续 100 次用同一个 TN ID CONNECT；之后写入、提交 `TRUNCATE_WAL`、导入快照并用旧目录重启，核对水位之后 WAL 的字节和 `UserRecord` 分类。普通模式连续 5 轮、`-race` 连续 2 轮通过，未见租约误分类或数据丢失。测试通过服务端 CONNECT 入口构造重复连接，没有模拟 200 次真实 TCP 重建；不证明极大规模历史下的内存上界。

## 新发现：发现地址无响应时客户端建连忽略取消约 1 秒

TN 的 WAL 客户端工厂通过 `logservice.NewClient` 使用 `DiscoveryAddress`；建连进入 `connectToLogServiceByReverseProxy` 后，`GetShardInfo` 用 `context.Background()` 发起独立的 1 秒发现 RPC，没有继承建连请求的 context。新增 `TestNewClientDiscoveryIgnoresContextDeadline`：真实 RPC 客户端连到只接受连接、不返回发现结果的 TCP 端点，外层 context 在 20ms 过期。普通模式 3 轮的返回时间约 1.0009、1.0006、1.0012 秒；`-race` 2 轮约 1.0016、1.0005 秒，均在外层期限之后才返回 `context deadline exceeded`。测试代码位于 [shardinfo_test.go](../../pkg/logservice/shardinfo_test.go)，问题记录为 [matrixone#32916](https://github.com/matrixorigin/matrixone/issues/32916)，已指派 `jiangxinmeng1`。

这是单次发现 RPC 最多约 1 秒的取消长尾；没有实测完整 TN 关闭、事务写入延迟，也没有证据表明它会单独造成分钟级等待。检查 `SaveSnapshot` 时还发现状态机未显式检查 `io.Writer` 的短写；实际 Dragonboat 快照路径使用文件写入，未构造出真实磁盘上的短写无错误场景，因此未计为产品缺陷。

## 三副本切主后经发现地址重新连接

原切主测试用 `ServiceAddresses` 直连三个副本；TN 生产工厂优先使用 `DiscoveryAddress`，所以新增 `TestThreeReplicaDiscoveryClientReadsAcrossTSOGapAfterLeaderFailure`。测试先等待存活节点的 gossip 发现视图就绪，再确保单 TN 客户端实际连到当前 leader；写入两条由 100 次 TSO 命令隔开的 WAL 后停掉该 leader。原客户端经存活节点的发现地址重新连接，读取并核对两条 WAL，提交 `TruncateWAL`、核对水位、读取保留 WAL，继续追加并读回第三条。普通模式连续 5 轮、`-race` 连续 2 轮通过，未见新缺陷。

首次测试在发现视图尚未同步时直接创建客户端，三轮均返回 `shard not found`；这是启动发现窗口，TN 工厂有建连重试，因此改为等待发现就绪后测试切主。该测试关闭整个 leader 服务，发现端点选用存活副本地址；没有模拟负载均衡器或网络黑洞，也没有启动完整 TN。

## HAKeeper 暂停期间保留候选快照，恢复后继续导入

检查 `shouldDoImport` 在读取 HAKeeper 失败时跳过物理导入的路径，新增 `TestSnapshotImportResumesAfterHAKeeperReplicaRestarts`。单 TN 先写入 WAL、导出快照、写入较新 WAL 并提交覆盖旧快照位置的 `TruncateWAL`；随后只停止 HAKeeper shard 0 的本地副本，保持 Log shard 1 可用。停机期间继续写入并读取 WAL，截断循环没有导入候选快照。重启同一个 HAKeeper 副本后，截断循环导入原候选；检查原候选不再可选、PSN/DSN 水位正确、停机期间及之前的较新 WAL 内容未变，并可继续写入。普通模式连续 5 轮、`-race` 连续 3 轮通过，未发现新缺陷。

测试期间因为 Log shard 已继续前进，HAKeeper 不可用时还可能导出一份更高索引快照。因此快照管理器在导入旧候选后仍有候选属于预期，不能以列表必须清空作为断言。该测试是单 Log 副本、单 HAKeeper 副本的进程内场景；没有模拟三副本网络分区或长期磁盘增长。

## 同一 TN 频繁重连后的小分页读取与快照导入

`queryLog` 先查询 Raft 日志页，再按租约历史分类；频繁 CONNECT 会插入大量 `LeaseUpdate` 记录，分页可能在任意租约更新处断开。新增 `TestPaginatedWALReadAcrossSameTNReconnectsAndSnapshotImport`：同一个 TN ID 在两个阶段各重建 20 次真实 RPC 连接，每四次连接写一条 WAL；用 `Read(..., maxSize=1)` 逐页检查游标严格前进，核对所有已确认 WAL 的 LSN、字节和 `UserRecord` 类型。两阶段之间导出快照，第二阶段后提交 `TruncateWAL` 并确认物理导入，再从保留水位之后重新逐页核对。普通模式连续 5 轮、`-race` 连续 3 轮通过，未见新缺陷。

测试只有一个 TN ID、一个 Log 副本，没有并发调用同一个客户端；1 字节是读取批次上限，Dragonboat 仍会返回至少一条完整记录。它验证记录分类和分页前进，不代表大规模租约历史的内存上界。

## 新缺陷：单个旧 gossip 答复可让合法 Log 副本启动时被跳过

`checkZombieReplicas` 在重启时逐个探测 peer 的 gossip 成员视图，只要至少一个 peer 成功答复且所有成功答复都不包含本地副本 ID，就将其标成 zombie；未要求其他配置的 peer 可达。新增 `TestRestartSkipsLiveReplicaWhenOnlyStaleGossipPeerAnswers`：真实三投票副本 Log shard 已提交成员 `{1,2,3}`，关闭副本 3 并保留本地数据；在它重启时，让 peer 1 的成员发现答复暂时只包含 `{1,2}`，peer 2 的发现 RPC 不可达。启动自检将仍在 Raft 成员表中的副本 3 标成 zombie，`startReplicas` 跳过它。同一数据目录再次重启，在使用真实且已同步的 peer 发现结果后，副本 3 正常启动。普通模式 3 轮、`-race` 3 轮稳定重现。缺陷已登记为 [matrixone#32919](https://github.com/matrixorigin/matrixone/issues/32919)，指派 `jiangxinmeng1`。

错误影响是 Log shard 虽仍有剩余两票，但失去一票冗余；再损失任一存活投票副本可能失去多数派。测试注入了旧 gossip 答复与另一个 peer 不可达，证明该故障状态下的真实启动决策；尚未捕获自然发生的 gossip 延迟时间线，也没有在本轮测试中停第二个存活投票副本。代码里的 gossip 视图本身是最终一致的，因此单个成功答复的“缺席”不能证明 Raft 已移除本地副本。

## 快照导入成功后、候选目录删除前重启

另新增 `TestSnapshotImportCrashBeforeCandidateRemoval`，将服务关闭点固定在 Dragonboat 已完成 `SyncRequestImportSnapshot`，但 LogService 尚未执行 `snapshotMgr.Remove` 和 `forwardTruncate` 之间；原数据目录重启后，候选被重新加载并再次导入。首轮截断 tick 清除旧候选并推进本地物理水位，第二轮仍可继续处理，截断水位之后的已确认 WAL 字节保持不变。普通模式 5 轮、`-race` 2 轮通过，未发现此窗口的新缺陷；这是在两步之间有序关闭服务，未模拟进程被强制杀死或文件系统写入错误。
