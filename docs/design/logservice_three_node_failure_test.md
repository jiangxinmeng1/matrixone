# Logservice 三节点单节点下线测试记录

## 基线与状态

- 代码基线：`upstream/main` `d61a7206c16b30baa4efc525eb6067c4364035aa`（2026-11-03），工作分支 `codex/logservice-chaos`。
- 测试机：`mo@10.222.1.51`，隔离目录 `/home/mo/jxm/logservice-chaos-20261103-d61a7206`。使用本提交构建的 `mo-service` 与直接 Logservice 客户端，三个独立进程、目录和端口（56401、56411、56421），一个数据 shard 与三个投票副本，另有三个 HAKeeper 副本。没有启动 TN/CN；其他工作负载未被注入故障。
- 注入方式：只向上述隔离目录中 `node.sh` 验证过二进制路径的 PID 发送 `SIGKILL`，再用相同配置启动；下文“下线”均指进程下线，同机其他进程继续运行。
- 证据保存在测试机上述目录，主要文件为 `*.append`、`*.read`、`*.out`、`*.killtime`、`*.restarttime`、`node-*/stdout.log`。本仓库 `test/logservice-chaos/` 提供配置生成、启停、直接调用和逐条核对工具。

## 模型与统一判据

固定一个数据 log shard、三个**投票**副本，三个独立数据目录及端口；另记录 HAKeeper shard 的成员和 leader。测试前后采集每个节点的二进制 SHA、配置、shard/replica ID、store incarnation、Raft leader、成员关系、日志及快照目录。所有服务与数据只由本测试创建和清理。

工作负载使用带唯一序号的记录：维护“客户端收到成功响应”的 `(seq, LSN, digest)` 清单；请求超时或连接中断的记录另列为**结果未知**，恢复后按内容对账，不能把它们直接记为失败或成功。每个场景至少检查：

1. 故障前成功确认的记录恢复后仍可读取，内容和 LSN 不变；故障后成功确认的记录也一致。每次读取从有效的未截断 LSN 开始，并排除 Raft 内部记录。
2. 两副本仍构成多数派时，允许选主/路由的短暂不可用，最终读写恢复；故障期间的请求必须在明确超时内结束。记录不可用窗口，但不使用固定毫秒数作为正确性断言。
3. `Truncate(L)` 成功后，`GetTruncatedLsn() >= L`；读 `L+1` 及之后仍正确。`L` 及之前允许已清理，不能要求一直可读。若测试物理日志清理，另核对 snapshot 导入成功与 WAL 范围收缩，不能仅凭截断标记判断。
4. snapshot 失败或节点重启后，后续导出/导入可以继续推进；不得清理尚未满足 TN 回放门槛的日志。下线节点恢复或被替换后，成员关系与本地副本身份收敛，不复活 zombie。

## 注入时机优先级

| 优先级 | 下线窗口与目标 | 代码依据 / 可疑点 | 主要判据 |
|---|---|---|---|
| P0 | 持续写入时下线数据 shard leader，分别在请求发出后、成功响应后；随后下线普通副本作对照 | `store.append` 经 `SyncPropose` 返回 LSN；客户端错误后重建连接，但只自动重试 shard-not-found。响应丢失时提交结果可能未知。`store.queryLog` 先读状态机 index，再读 Raft log，期间可遇到截断或 leader 变化。 | 已确认写不丢；未知写按 payload 对账；新 leader 可继续读写；请求有界结束。 |
| P0 | `Truncate(L)` 提议前、提议已提交但客户端未收到响应时下线 leader | 截断位点是 Raft 状态机的单调更新；相同/更小 LSN 返回 invalid-truncate。响应丢失后的重试不能只以 RPC 结果判断状态。 | 恢复后查询位点；仍有效的记录可读；重复截断结果与实际位点一致。 |
| P0 | snapshot **导出成功至本地 snapshotManager.Add 之间**下线执行节点，重启后重复；leader 和普通副本各做一次 | `exportSnapshot` 先 `SyncRequestSnapshot(Exported=true)`，再登记本地目录并推进内存 index；`snapshotManager.Init` 重启时扫描磁盘目录。中间崩溃可能留下未登记快照。 | 重启后可重新发现或安全处理导出文件；后续导出/导入继续推进，无永久 quota 卡死或误清理。 |
| P0 | snapshot **导入成功至 Remove/forwardTruncate 之间**下线执行节点 | `importSnapshot` 先调用 Dragonboat 导入，再删除导出快照、更新内存截断位点；两者不是一个原子步骤。重启后可能重试同一导入。 | 数据完整；重试或恢复不永久卡住；物理日志范围最终符合已确认截断位点。 |
| P1 | TN 截断位点已推进、但其余 TN 回放 LSN 尚未达到候选快照 index 时下线节点；回放追上后再恢复 | `shouldDoImport` 读取 HAKeeper TN 状态并检查 `ReplayedLsn`，读状态失败也跳过导入；这一控制面依赖容易与节点下线叠加。 | 回放落后时不导入/清理；条件满足后能继续导入。 |
| P1 | `getTruncatedLsn` 读取超时且导出快照 quota 已满时下线节点 | 超时路径会 `DropNewest` 并重新对齐内存 snapshot index；只有一个候选时刻意不丢。 | 快照数量有界；恢复后仍有可导入候选，清理最终推进；无持续 WAL 增长。 |
| P1 | 普通副本下线超过 HAKeeper 判定过期与修复时间，再恢复旧进程；随后对 leader 重复 | HAKeeper 可能移除旧 replica 并新增 ID；启动时 zombie 检查和 store incarnation 防止旧副本复活。 | 旧 ID 不重新参与 Raft；新副本追上；已确认记录不丢。 |
| P1 | 同一节点恢复后**尚未追上或成员关系尚未稳定**时再次下线，最多三轮；另做稳定后再下线的对照 | Raft 追赶、客户端发现视图和 HAKeeper 调度可能处于不同代际；这是单次下线无法覆盖的边界。每轮先确认剩余两投票副本仍健康。 | 已确认写不丢；请求有界；副本不会反复以旧身份启动或产生无法清理的快照。 |
| P2 | HAKeeper leader 下线时执行数据 shard snapshot/truncate；与“仅数据 shard leader 下线”分开测 | 数据 shard 的 `shouldDoImport` 依赖 HAKeeper state；HAKeeper 自身 snapshot/truncate 走另一条流程。 | 数据写读和控制面行为分别记录；HAKeeper 恢复后导入/调度继续推进。 |

上表的两个“成功至下一步之间”窗口不能靠 `sleep` 精确命中。先用日志事件做粗粒度故障实验；若要验证该窗口本身，应在测试专用构建中通过明确 barrier/failpoint 暂停对应调用，再杀**测试自有**节点。每次故障均记录触发事件、进程 PID、shard leader、请求 ID、LSN、snapshot index 和时间戳。

## 时长与顺序

- **短下线**：在尚未发生 HAKeeper 副本替换时重启原节点，以观察 Raft 追赶、客户端重连和本地 snapshot 状态恢复。以实际成员关系确认“尚未替换”，不靠固定秒数推定。
- **短时间重复下线**：优先在同一节点重启后、尚未追上或调度尚未收敛时再次下线；以“追赶中/调度中”这一可观测状态作为触发条件，最多三轮。另在 leader/成员关系稳定后重复一次作对照。每轮重新记录实际 leader，并保存独立成功清单，避免把第一次的未知写混入第二次。
- **长下线**：保持一个节点下线，直到 HAKeeper 已将旧副本判为过期并发出或完成修复，再启动旧进程。若没有空闲第四个 store，无法验证“补足三副本”，只验证剩余两副本可用及旧副本安全回归；需要第四个备用 store 才能验证替换。
- 每个时机都要区分**数据 shard leader**、**普通副本**和**HAKeeper leader**；它们可能同机，但必须记录实际角色，不能按机器编号假定。

## 执行记录

| 日期 / 场景 | 环境与提交 | 故障事件 | 写 / 读 / 截断 / 快照结果 | 证据与结论 |
|---|---|---|---|---|
| 2026-11-03 远端只读预检 | `10.222.1.51`；运行中的容器为 4.2.6，未确认本任务三节点集群 | 无 | 未执行 | SSH 可登录；主机资源和现有工作负载已检查。 |
| 2026-11-03 11:53 再次预检 | `10.222.1.51`；约 10 GiB 可用内存、104 GiB 可用磁盘；只有 Kafka 容器运行 | 无 | 未执行 | 资源已宽裕，可建立隔离环境。远端现有 `mo-service` 源码提交为 `8f33d3c4d6`，默认 Go 为 1.19.8；仍需准备本测试基线 `d61a7206c1` 的二进制与三节点配置。 |
| 12:10 三节点基线 | 本提交的隔离 Logservice；shard 1 初始 leader node0 | 无 | 20 条 `baseline` 写入 LSN 6–25，读取一致 | `baseline.append`、`baseline.read`。 |
| 12:13 数据 leader 长下线、持续写入 | node0，旧副本 262147 | 写入中杀进程，约 55 秒后重启；超过 HAKeeper 30 秒过期配置 | `leader-write1` 1000 条均确认，恢复后与基线共 1020 条按 LSN 和内容核对通过 | 新 leader node1；HAKeeper 移除旧 262147，并在 node0 建立新副本 272386。`leader-write1.*`。 |
| 12:14 普通副本短时重复下线 | node0，新副本 272386 | 约 2 秒下线两轮，间隔约 3 秒 | 成员保持三副本；前述 1020 条核对通过 | `follower-flap.events`。 |
| 12:16 普通副本导出 snapshot 附近下线 | node2 | 观察到 `export snapshot success` 后杀进程并重启 | `Truncate(800)` 成功；从 801 读取的 231 条已确认记录一致 | 日志触发是粗粒度时机，不能证明命中 `SyncRequestSnapshot` 和 `snapshotMgr.Add` 之间。`follower-snapshot.killtime`、`after-truncate1.read`。 |
| 12:17 数据 leader 的 truncate 请求中下线 | node1 | 发起 `Truncate(1100)` 约 20 毫秒后杀进程 | 该请求 `connect: timeout`，位点仍为 800；801 起 351 条已确认记录一致。新 leader node2，重启后再执行 `Truncate(1100)` 成功 | 本次故障落在连接阶段，未命中“提议已提交但响应丢失”窗口。`leader-truncate.out`、`truncate2.out`。 |
| 12:17 普通副本导入 snapshot 附近下线 | node1 | 观察到 `import snapshot success` index 1039 后杀进程并重启 | 截断位点 1100；1101 起 58 条已确认记录一致，后续三节点继续导出 | 不能证明命中导入与本地清理之间的精确窗口。`follower-import.killtime`、`after-import.read`。 |
| 12:19 数据 leader 短时连续下线 | node2、接替的 node0、再接替的 node1 | 写入进程运行时每轮下线约 2 秒，全部恢复后才杀下一位 leader | 前两次写入继续；第三次 `leader-short/000102` 返回 `EOF`，读取发现它已提交于 LSN 1272。102 条成功响应的记录全部按 LSN、内容核对通过 | 这是实际出现的“客户端结果未知但服务端已提交”；不能把 `EOF` 当作未写入。`leader-short.*`、`after-leader-short.read`。 |
| 12:20 普通副本在 truncate 期间下线 | node2 | 发起 `Truncate(1200)` 约 20 毫秒后杀进程，2 秒后重启 | truncate 返回成功，位点查询为 1200 | `follower-truncate.*`。 |
| 12:20 数据 leader 导出 snapshot 附近下线 | node0 | 本机记录 `export snapshot success` index 1283 后约 42 毫秒杀进程，2 秒后重启 | `leader-snapshot` 300 条均确认；`Truncate(1300)` 后三节点均导入 snapshot，1301 起 282 条已确认记录一致 | 日志触发不能证明精确窗口。`leader-snapshot.*`、`truncate-after-leader-snapshot.out`、`after-truncate1300.read`。 |
| 12:23–12:25 HAKeeper leader 下线时写入、truncate、snapshot | HAKeeper leader node1，后为 node0；数据 shard leader 为 node2 | 三轮控制面 leader 下线约 2.6、5.6、9.8 秒 | `Truncate(1450/1600/1796)` 均成功，写入继续；最后一轮 node0 离线期间，node1、node2 在 12:24:59/12:24:58 完成 index 1796 导入。1797 起 20 条确认写入一致 | `hakeeper-leader.*`、`hakeeper-snapshot.*`、`hakeeper-import.*`、`final.read`。控制面失去 leader 后会重新选主。 |
| 12:25–12:27 普通副本长下线、持续写入 | node1，旧数据副本 262146、旧 HAKeeper 副本 131073 | 下线约 59 秒；恢复前 shard 1 仅剩两个投票副本 | 下线期间 `follower-long` 150 条均确认并读回一致；重启后新副本 374785/374786 加入。再写 20 条、`Truncate(1900)` 成功，1901 起 96 条确认记录一致；新副本导出 index 1988/1998 snapshot。随后 `Truncate(1988)` 成功，新副本导入 index 1988，1989 起 32 条确认记录一致 | `follower-long.*`、`after-follower-long.read`、`post-rejoin.*`、`after-rejoin-snapshot.*`。最终三个进程均运行，两个 shard 均为三副本。 |

## 结论与边界

- 本轮覆盖了数据 shard leader 和普通副本在持续写入、snapshot 导出/导入附近、truncate 请求附近的单节点下线，以及短时重复下线和超过 HAKeeper 过期时间的长下线；还覆盖 HAKeeper leader 下线时数据操作。所有**已收到成功响应且未被截断**的记录在每次核对时均按 LSN 和 payload 一致，最终截断位点 1988，三节点恢复为三副本。
- 观察到一个明确的未知结果请求：`leader-short/000102` 返回 `EOF`，但恢复后读取到 LSN 1272。调用方在超时/断连后必须按业务 payload 或幂等键对账，不能仅凭 RPC 错误断定未提交。此次没有发现已确认记录丢失或最终不可用。
- 日志驱动的进程终止未严格命中 `exportSnapshot` 成功与 `snapshotMgr.Add` 之间、或 `importSnapshot` 成功与 `snapshotMgr.Remove/forwardTruncate` 之间；leader `Truncate(1100)` 的失败也发生在连接阶段。要给这些**精确代码窗口**作结论，需在测试专用构建中加入 barrier/failpoint，然后重跑。本文只将它们记为“附近下线”。
- 本轮没有 TN/CN，因此没有验证 TN 回放落后时禁止导入、追上后恢复导入；也没有人为制造 `getTruncatedLsn` 超时与 snapshot quota 满的同时发生。测试日志中有短暂超时与跳过 tick，但不能等同于已覆盖这两个组合窗口。物理 WAL 范围没有单独核对；`GetTruncatedLsn` 和 snapshot 导入日志仅证明逻辑位点与导入动作。

## 复现

本地执行 `python3 test/logservice-chaos/generate_config.py <隔离目录>` 生成三份配置；用 `test/logservice-chaos/node.sh <隔离目录> start|kill|status <0|1|2>` 管理测试进程。直接客户端源码见 `test/logservice-chaos/client/main.go`，`-addresses` 传三个 Logservice RPC 地址；`-op status -shard 0` 查询 HAKeeper，默认 shard 1 查询数据副本。`verify.py --min-lsn <截断位点+1> <read-output> <append-output>...` 仅核对明确成功的写入；包含 `EOF` 等未知结果的文件需先提取成功行，未知请求另行按 payload 查找。

## 代码定位

- `pkg/logservice/store.go`：`append`、`truncateLog`、`queryLog`，以及 Raft 配置和副本启动。
- `pkg/logservice/truncation.go`：`processShardTruncateLogWithReplica`、`exportSnapshot`、`importSnapshot`、`shouldDoImport`、超时配额处理。
- `pkg/logservice/snapshot.go`：导出文件扫描、登记、删除和重启恢复。
- `pkg/logservice/client.go` 与 `shardinfo.go`：客户端重连、错误重试和 leader 地址发现。
- `pkg/hakeeper/checkers/logservice/`：心跳过期、副本修复、zombie 与 store incarnation。

## 追加实验：三地址客户端持续负载与延迟（2026-11-03 15:04–15:12）

### 测量口径

复用 51 上同一隔离集群和 shard 1，未重置已有数据；实验开始前截断位点为 1988。测试客户端的 `-addresses` 固定为 `127.0.0.1:56401,127.0.0.1:56411,127.0.0.1:56421`，通过与 MO WAL 相同的 `logservice.NewClient` 建连，并复用一个 managed client。`-op load` 对每个唯一 payload 逐次记录底层 Append 的错误和耗时；错误后使用 MO WAL 的默认单次 1 分钟超时、3 分钟继续重试窗口及 100 毫秒起、最多 4 秒的指数退避，重试同一份 record。`ack` 表示逻辑请求最终得到成功响应，`final_error` 表示重试窗口后仍向上返回错误。p99 和最大值均按**每个逻辑写入从首次尝试到成功的耗时**计算，单位毫秒，样本为单个顺序写入客户端；不是集群吞吐或 SQL 延迟。

这条路径复用了 MO 的 Logservice 客户端与重试参数，但测试 payload 是普通 UserRecord，没有启动 TN、编码 TN WAL 的 DSN，也没有执行 TN 回放。因而可以观察底层连接、重试、物理日志和向该模拟 WAL 边界返回的结果，不能把它当作端到端 SQL 测试。

| 场景 | 成功 / 尝试 / 重试 | 底层错误；最终错误 | 逻辑写入 p50 / p95 / p99 / 最大 | 读回与连接证据 |
|---|---:|---|---:|---|
| 三节点健康基线 | 100 / 100 / 0 | 无；0 | 150 / 225 / 404 / 463 | 100 个 payload 各出现一次。`mo-path-baseline2.load`。 |
| 已连接 node1 时杀普通副本 node1，离线约 25 秒 | 700 / 701 / 1 | `EOF` 1 次；0 | 141 / 217 / 315 / 1428 | 700 个已确认 payload 各出现一次。故障前 TCP 连接到 56411，重试后到 56421；`mo-path-follower.*`。 |
| 已连接数据 leader node2 时杀 node2，离线约 28 秒 | 700 / 702 / 1 | `EOF`、`context deadline exceeded` 各 1 次；0 | 150 / 220 / 314 / 6249 | 700 个确认 LSN 均可读，但 `mo-path-leader/000010` 在 LSN 2900 和 2903 各有一条物理记录。故障前连接 56421，选主后连接 56411；`mo-path-leader.*`。 |
| 已连接新 leader node1 时杀未被连接的普通副本 node0，离线约 18 秒 | 300 / 300 / 0 | 无；0 | 141 / 208 / 302 / 869 | 300 个 payload 各出现一次，TCP 连接保持 56411；`mo-path-other-follower.*`。 |

leader 故障的长尾集中在 `mo-path-leader/000010`：首次尝试 156 毫秒后 `EOF`；第二次尝试 5301 毫秒后 `context deadline exceeded`；第三次 190 毫秒成功，逻辑总耗时 6249 毫秒。源码中单地址建连超时为 5 秒、备用地址延迟为 300 毫秒，和此次第二次尝试的量级相符，但本次没有逐段 RPC trace，不能将 5301 毫秒全部归因于某一段。`mo-path-leader.load` 还记录了故障地址连接失败的 `ERROR` 日志；这是可见日志，并未成为该笔逻辑写入的最终错误。

**物理重复确实发生。** 首次 `EOF` 对应的写入已在 LSN 2900 提交，重试同一 payload 又在 LSN 2903 成功；Logservice 的 Append 接口本身没有按 payload 去重。TN WAL 的真实记录含 DSN，`logservicedriver/replayer.go` 对完整重复 DSN 范围有跳过逻辑，并有 `Test_ReplayerIgnoresDuplicateDSNAcrossBatches` 等单测。本轮没有运行真实 TN 回放，不能据此宣称 SQL 层会重复执行，也不能将现有单测当作 51 上的端到端验证。

复算命令：在远端隔离目录执行 `python3 analyze_load.py mo-path-leader.load --read-log mo-path-leader.read --prefix mo-path-leader/`；其他场景改为对应前缀。工具会分别统计底层错误、最终错误、逻辑延迟、已确认 LSN 是否可读及同 payload 的物理重复。

## 追加实验：超过 HAKeeper 过期时间的持续负载（2026-11-03 16:07–16:16）

沿用上述三地址客户端和逐笔耗时口径，每轮顺序写入 1000 个唯一 payload。每轮开始时均为三投票副本；故障前通过 `ss -tnp` 记录客户端实际连接的节点。保持一个节点下线直到 HAKeeper 已移除其旧副本，再重启相同进程配置；每轮重启后均观察到新副本 ID 加入，并从未截断的 LSN 1989 开始读取核对。这里的“长下线”以实际杀进程和重启时间计算，均超过配置的 30 秒 HAKeeper 过期时间。

| 下线对象与客户端原连接 | 实际下线；恢复后的新数据副本 | 成功 / 尝试 / 重试；底层错误；最终错误 | 逻辑写入 p50 / p95 / p99 / 最大（毫秒） | 读回 |
|---|---|---|---:|---|
| 普通副本 node0；客户端连接 node2（56421） | 约 75 秒；node0 从 272386 改为 385025 | 1000 / 1000 / 0；无；0 | 135 / 200 / 267 / 893 | 1000 个确认 LSN 均可读，payload 无重复。`mo-path-long-follower.*`。 |
| 数据 leader node1；客户端连接存活的普通副本 node0（56401） | 约 84 秒；node1 从 374785 改为 385027 | 1000 / 1000 / 0；无；0 | 134 / 198 / 272 / **4118** | 1000 个确认 LSN 均可读，payload 无重复。新 leader 为 node2。`mo-path-long-leader.*`。 |
| 数据 leader node2；客户端直接连接旧 leader（56421） | 约 79 秒；node2 从 262145 改为 395266 | 1000 / 1002 / 1；`EOF`、`timeout` 各一次；0 | 134 / 209 / 274 / **6647** | 1000 个确认 LSN 均可读；`mo-path-long-leader-direct-1/000067` 在 LSN 5988 和 5991 各有一条物理记录。新 leader 为 node0。`mo-path-long-leader-direct-1.*`。 |

第二轮虽然没有错误，选主期间仍有一笔成功写入耗时 4.12 秒，故“已连接存活副本”也不能保证写入无停顿。第三轮故障后的第 67 个逻辑 payload 首次尝试 248 毫秒后 `EOF`，第二次 5575 毫秒后 `timeout`，第三次 223 毫秒成功；从首次尝试到成功共 6647 毫秒。首次尝试对应的物理记录已提交于 LSN 5988，第三次成功响应为 LSN 5991。客户端重试把这两次底层错误挡在模拟 WAL 边界内，但留下了可见的错误日志和两份物理记录。TN DSN 回放与 SQL 层行为仍受上一节所述测试边界约束。

## 追加实验：持续写读、snapshot/truncate 与九轮轮换下线（2026-11-03 16:25–16:29）

运行 `run_cycle_faults.py`，启动**一个贯穿所有故障的客户端**，反复用三个地址写入唯一 payload，并立即从返回的 LSN 读取、校验类型与内容。每条读回成功后才把 LSN 发布到进度文件；后台只截断这个位点之前至少 100 个 LSN，避免测试自身截掉未验证记录。写、读各按单次 1 分钟、继续重试窗口 3 分钟及指数退避处理；用尽窗口或内容不符会打印 `final_error` 并以非零状态退出。脚本在杀下一台前检查上一台已重启且 shard 恢复三成员；每次注入后确认只有两台测试节点运行。

| 轮次 | 下线节点 / 当时角色 | 触发窗口 | 下线时长 | 结果 |
|---:|---|---|---:|---|
| 0–2 | node0 leader、node1 follower、node2 leader | node0/node2 观察到新导出 snapshot 后；node1 18 秒内无新事件，定时注入 | 各约 8 秒 | 均恢复三成员；继续写读。 |
| 3–5 | node0 leader、node1 leader、node2 leader | node1 观察到 `import snapshot success` 后约 10 毫秒；node0/node2 等待 18 秒无新导入，定时注入 | 各约 38 秒 | 均恢复三成员；轮 4 出现最长读取停顿。 |
| 6–8 | node0 follower、node1 leader、node2 leader | 发起一次 truncate 约 20 毫秒后杀节点，同时后台持续推进 truncate | 各约 8 秒 | 均恢复三成员；轮 7 的写入重试产生两条物理日志。 |

客户端完整结束，`summary cycles=735 append_retries=3 read_retries=4 max_cycle_ms=62250.316`，**没有最终写入/读取失败，也没有内容不匹配**。单轮写读耗时 p50/p95/p99 为 149/422/896 毫秒；最大 62.25 秒。单独的读取阶段最大 61.95 秒。第 204 笔读取先经历 25.87 秒 `EOF` 和 5.60 秒 `context deadline exceeded`，第三次成功，整轮 32.94 秒。第 466 笔在轮 4 的 leader 下线期间，第一次读取约 56.10 秒后 `EOF`，第二次约 5.16 秒后 `timeout`，第三次成功，整轮 62.25 秒。这两个长尾都没有耗尽 3 分钟窗口，但对实时请求有明显影响。

后台 truncate 成功 **17 次**，无后台最终错误，最终 `GetTruncatedLsn()` 为 7621。故障交叠的三次一次性 truncate 探针中，轮 6 成功，轮 7、8 在建连阶段返回 `context deadline exceeded`；这些探针本身不重试，不能记为通过，后台后续请求则继续成功。测试窗口内 node0/1/2 的数据 shard snapshot 分别**导出 21/23/18 次、导入 7/9/5 次**。最终三节点运行且 shard 为三成员。

从 7622 起重读保留区间，**104 条已确认记录**按 LSN 和 payload 一致；同一 payload `cycle-rotating-20261103/000690` 出现在 **LSN 7687 和 7690**，其中 7690 是第三次 Append 的成功响应。该笔先发生 `EOF`，第二次约 4.81 秒后 `timeout`，第三次成功。较早且已截断的区间无法用最终读取统计物理重复，但每条记录在产生时都经过即时读回校验。本轮仍未运行 TN DSN 回放，不能将物理重复直接解释为 SQL 重复执行。

远端证据位于隔离目录下 `cycle-rotating-20261103.campaign.log`、`.load`、`.progress`、`.final.read` 及 `round-*.truncate`。复算：`python3 analyze_cycle.py cycle-rotating-20261103.load --read-log cycle-rotating-20261103.final.read --min-lsn 7622 --prefix cycle-rotating-20261103 --node-root . --since '2026/11/03 16:25:39' --until '2026/11/03 16:29:36'`。

## 追加实验：空目录替补、旧目录节点回归（2026-11-03 17:06–17:12）

在同一隔离集群中，用 `python3 test/logservice-chaos/add_spare.py <隔离目录>` 从 node0 配置生成第四个 store 的配置和**全新空目录** `node-3`。第四节点使用独立 UUID，RPC `56431`，Raft `56430`，gossip `56432`，且 `bootstrap-cluster=false`。先启动 node3，再启动一个贯穿故障的四地址客户端，持续执行唯一 payload 的 Append→Read 内容核对。17:07:35 杀旧目录 node0，当时它是数据 shard 1 和 HAKeeper shard 0 的 leader；node1、node2 保持存活。HAKeeper 约在 17:09:05 向 node3 下发 `L/Start`，随后移除 node0 的旧 ID，并在 node3 建立数据副本 **477185**、HAKeeper 副本 **477186**。17:10:36，替补副本运行约 90 秒后，用**原 node0 数据目录及原配置**重启 node0；客户端继续写读至 17:11:43。

客户端清洁退出：`summary cycles=1535 append_retries=0 read_retries=0 max_cycle_ms=2650.045`，无最终错误。完整保留区间从截断位点 7621 之后重读，1535 笔已确认记录全部按 LSN 和 payload 核对通过，保留区间内没有重复 payload。整轮耗时 p50/p95/p99 为 **161/229/306 毫秒**，最大 2.65 秒；此最大值发生在替补完成之前。测试中未执行新的用户主动 truncate；node1、node2 各有一次数据 shard snapshot 导出，HAKeeper shard 自身还有常规 snapshot/truncate 活动。

旧目录重启后，四个 RPC 地址对两个 shard 返回一致的成员表：数据 shard `{385027:node1, 395266:node2, 477185:node3}`，HAKeeper shard `{385028:node1, 395265:node2, 477186:node3}`。node0 旧数据 ID **385025**、旧 HAKeeper ID **385026** 都未重新进入 Raft 成员表。node0 启动日志对这两个 ID 及更早的 272385 打印 `skip starting zombie replica`，并清理 385025 的本地元数据。

远端证据保存在 `zombie-spare-20261103.load`、`.progress`、`.final.read` 和 `node-0`/`node-3` 的 `stdout.log`。复算：`python3 analyze_cycle.py zombie-spare-20261103.load --read-log zombie-spare-20261103.final.read --min-lsn 7622 --prefix zombie-spare-20261103 --node-root . --since '2026/11/03 17:07:20' --until '2026/11/03 17:11:45'`。
完成观察后停止没有投票副本且持续打印错误的旧目录 node0；node1、node2、node3 保持运行，两个 shard 在三个存活地址查询均为上述三副本成员表。

### 缺陷记录：旧目录可短暂启动已移除的初始 HAKeeper 副本

**状态与触发前提。** 已在上述 51 隔离集群复现，尚未修复。node0 原配置仍为 `bootstrap-cluster=true`，`init-hakeeper-members` 指定初始 HAKeeper ID **131072**；旧磁盘保留该 ID 的 Dragonboat Raft 文件。HAKeeper 曾对 131072 下发 `L/Kill`，使其不再出现在 node0 的 `mo-logservice.metadata` 中；其后 node0 的新 HAKeeper 副本也被移至空目录 node3。启动前，当前 HAKeeper shard 0 的三成员为 node1 `385028`、node2 `395265`、node3 `477186`，均不含 node0 或 131072；node0 处于停止状态。**仅有原始 bootstrap 配置还在，不足以复现；必须同时满足“初始 ID 已从本地 metadata 删除、当前 shard 已迁出、旧 Raft 文件仍在”这组条件。**

**复现步骤。** 在独立三副本测试集群中先使 node0 长下线，等 HAKeeper 将其原始 ID 131072 从当前成员表移除；恢复 node0 后等待 `L/Kill ...:0:131072:0` 的本地 metadata 清理完成。再次让 node0 长下线，启动空目录替补并等它获得新的 HAKeeper 副本，使 node0 不再承载当前 shard 0。保留 node0 原配置和原数据目录，在另外三个投票节点正常运行时重启 node0。对本次已形成上述前提的隔离目录，直接执行：

```sh
root=/home/mo/jxm/logservice-chaos-20261103-d61a7206
python3 "$root/reproduce_bootstrap_zombie.py" "$root" \
  --rounds 3 --old-id 131072 \
  --peer-addresses 127.0.0.1:56411,127.0.0.1:56421,127.0.0.1:56431
```

复现脚本在每轮启动前读取磁盘 `mo-logservice.metadata`，要求 131072 已不存在；在 `L/Kill` 后等该 ID 再次从 metadata 中消失，才停止 node0 并开始下一轮。**不能只看到 `applying cmd: L/Kill` 就立即杀进程**：该日志先于 `stopReplica`/`removeMetadata`，过早杀进程会打断清理，使下次启动正常跳过旧 ID。2026-11-04 的第一次试跑因此出现 2/3 次短暂启动；纠正等待条件后，连续 **3/3 次**都观察到旧 Raft 实例重放、PreVote 和后续 `L/Kill`，对应的重放→`L/Kill` 日志时间分别约为 **5.9、3.0、3.2 秒**。证据在远端 `zombie-repeat-verified-20261104.log` 及 node0 的 `stdout.log`；三轮后 node0 已停止，两个 shard 的成员表仍为 node1、node2、node3。

**实际行为与预期行为。** 2026-11-03 首次复现时，node0 在 17:10:37–17:10:45 恢复旧 HAKeeper Raft 实例并反复 PreVote/ReadIndex，随后收到 `L/Kill`；旧 ID 从未进入当前成员表，1535 笔确认写入和读回无最终错误。预期是：在已有集群的权威成员关系确认 131072 已移除时，node0 **不启动**该旧副本，也不向 HAKeeper 客户端宣称本机承载它；如需让 node0 再承载 HAKeeper，应由 HAKeeper 按当前成员关系分配新 ID。真正首次建群、尚无权威成员关系时，初始 bootstrap 仍应正常工作。

**可能影响。** 已观察到旧实例的预投票、ReadIndex 无 leader 日志，以及旧目录 node0 的 `failed to get HAKeeper Leader ID: shard not found`、`failed to get cluster details: cannot locate ha keeper` 等服务端错误；没有观察到旧 ID 成为当前 leader、确认数据丢失或客户端最终失败。代码表明，旧实例启动后 `haKeeperReplicaID` 非零，而 `CHECK_HAKEEPER` 仅按这个值回答，因此它在被清理前**可能被发现为 HAKeeper 地址**，引入请求错误或长尾；占用本机 shard 0 也可能推迟同 store 新 ID 的启动。若足够多仍保留旧配置和 Raft 文件的初始节点同时回归，旧成员组形成多数派并产生过期控制面视图是需要防范的风险，**本实验没有复现这一组合**。`L/Kill` 是事后清理，若当前 HAKeeper 不可用，其到达时间没有本次几秒的保证。

**代码原因与回归缺口。** `pkg/logservice/store.go` 的 `startReplicas` 只把本地 metadata 中的副本交给 `checkZombieReplicas`；已删除的 131072 不会进入 `skippedZombies`。`pkg/logservice/service_bootstrap.go` 的 `BootstrapHAKeeper` 从原配置取 131072，只检查该集合，随后调用 `startHAKeeperReplica`，又把旧 ID 写回 metadata。HAKeeper checker 最终将其识别为 zombie，`pkg/logservice/service_commands.go` 的 `handleKillZombie` 调用 `stopReplica` 和 `removeMetadata`。现有 `TestServiceBootstrap/skips zombie HAKeeper replica` 手工预置 `skippedZombies`，没有覆盖“配置保留旧 ID、metadata 已删除”的路径。修复应在 bootstrap 启动前核对权威成员关系，并增加这一前提下“旧 ID 从未本地启动”的回归测试。

## 追加实验：随机进程下线、空目录替补与 leader 网络隔离长测（2026-11-03 17:40–17:55）

为验证真正的双向网络分区，在 51 上建立另一套独立集群，三个初始节点及后续空目录节点分别运行于自己的 Linux 网络命名空间，测试网段为 `198.18.51.0/24`。断开某个节点的宿主机 veth 接口会同时切断其入站与出站流量。预检实测隔离节点与另一节点双向 ping 失败，而其余两个节点之间仍可 ping；恢复接口后通信恢复。测试脚本 `netns.sh` 仅管理这套隔离测试接口，`run_extended_chaos.py` 使用固定随机种子 `20261103`，每轮从**当时三个投票副本**中随机选进程故障目标，网络故障则选当时的数据 shard leader；杀节点前检查其余两票的进程存活。连续客户端沿用上一轮的 Append→Read 精确内容核对、单次 1 分钟/继续重试 3 分钟口径；后台只在已验证 LSN 之前截断。每次故障前等待导出/导入 snapshot 事件或并发发起 truncate，等不到事件时记录定时注入。

共 **12 轮、约 14 分 24 秒**：5 次短进程下线（普通副本 3、leader 2），4 次当前 leader 的双向网络隔离（约 20、20、26、29 秒），3 次普通副本长下线后由**全新空目录** node3、node4、node5 依次替补。长下线每轮都先等旧 ID 从成员表移除、空目录获得新 ID，再运行约 25 秒、用旧目录重启原节点约 18 秒核查；旧节点不再是投票成员后停用，避免影响下一轮替补目标。三次数据副本替换依次为 node0→node3（新 ID **262149**）、node3→node4（**282626**）、node2→node5（**292865**）。每轮恢复后都重新核对三成员，故障期间始终至少有两台当前投票节点的进程运行。snapshot 触发中，轮 2/5/11 观察到导入成功，轮 3/6/9 观察到导出成功；其余相应轮次按超时定时注入。它们是日志附近的故障，不能证明精确命中导入/导出函数内部窗口。

持续客户端正常退出：`summary cycles=3174 append_retries=1 read_retries=0 max_cycle_ms=27165.576`，**没有最终写入/读取错误，也没有内容不匹配**。一次底层 Append `EOF` 出现在轮 7 杀 node2 时，重试后成功；是否造成已截断区间的物理重复无法从最终保留区间判定。单轮写读耗时 p50/p95/p99 为 **198/634/1349 毫秒**，最大 **27.17 秒**（轮 5 leader 网络隔离后的 seq 1963）。读取阶段最大 940 毫秒；本次最大长尾来自写入阶段。长尾反映这次客户端和故障安排，不能视为生产 SQL 延迟承诺。

后台 truncate **71 次成功、0 次失败**，最终 `GetTruncatedLsn()` 为 **3229**。从 3230 重读保留区间，**115 条已确认记录**全部按 LSN 与 payload 一致，保留区间没有重复 payload；分别以 node1、node4、node5 的单地址建立 Logservice 客户端读取，三份输出逐行相同。本次窗口中数据 shard snapshot 导出/导入次数分别为 node0 `4/1`、node1 `93/38`、node2 `61/14`、node3 `12/2`、node4 `40/12`、node5 `20/6`。结束时仅 node1、node4、node5 运行；三地址看到的数据 shard 成员是 `{262147:node1, 282626:node4, 292865:node5}`，HAKeeper shard 是 `{131073:node1, 282625:node4, 292866:node5}`，旧目录 ID 均未回到成员表。新集群的旧目录 node0、node2 重启时，启动日志跳过了旧副本和初始 HAKeeper 副本；未观察到上一节的初始 ID 短暂运行。但上一节的独立复现仍表明：当该初始 ID 已从本地 metadata 消失时，现有启动路径存在缺口。

为确定候选 store 调度行为，正式长测前还运行了约 8 分钟的试跑：`extended-20261103.load` 完成 1666 轮、一次 Append `EOF` 和一次 Read `timeout` 重试、无最终错误；后台 truncate 37 次成功。网络隔离期间的读取单次尝试约 59.9 秒才 `timeout`，最大整轮 **61.78 秒**；最终保留区间 112 条确认记录一致。试跑在第二次空目录替补等待时停止：HAKeeper 将新副本 **272387** 调度到仍在线的旧目录 node0，而不是空目录 node4。这是多个空闲 store 中的合法调度选择，导致原脚本的“必定落在 node4”断言超时，并非服务写读失败。正式长测在旧目录回归检查后停掉无投票副本的旧 store，等其从候选中失效，再进行下一次空目录替补。

正式长测的远端证据位于 `/home/mo/jxm/logservice-chaos-netns-long-20261103-d61a7206/`：`integrated-20261103.campaign.log`、`.load`、`.progress`、`.truncated`、`.final.read`、`.node-{1,4,5}.read` 和各 `node-*/stdout.log`。复算：`python3 analyze_cycle.py integrated-20261103.load --read-log integrated-20261103.final.read --min-lsn 3230 --prefix integrated-20261103`。试跑证据保存在相邻的 `/home/mo/jxm/logservice-chaos-netns-20261103-d61a7206/`。最终读回与状态确认后，已停止本轮隔离进程、撤销测试网络命名空间；原有隔离目录的 node1、node2、node3 仍运行，日志留存。
