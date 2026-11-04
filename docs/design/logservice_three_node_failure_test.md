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

跟踪：[matrixorigin/matrixone#32135](https://github.com/matrixorigin/matrixone/issues/32135)。

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

## 追加实验：两票存活时的 1 分钟长尾与控制面心跳（2026-11-04）

在同一隔离网络目录重新启动 node1、node4、node5；测试前数据与 HAKeeper shard 均为三个投票成员，两个 shard 的 leader 都是 node1。持续客户端仅连接这三个测试地址，对唯一 payload 顺序执行 Append→Read，单次尝试超时 1 分钟、失败后按模拟 MO WAL 规则重试。先将当前 leader node1 的双向网络切断 20 秒：node4、node5 选出新 leader，能单独返回 `latest`，但客户端仍通过故障前的 node1 连接等待，`seq=25` 的 Append 在链路恢复后才成功，整轮 **27.17 秒**，没有底层重试。链路恢复后用 `ss` 确认同一客户端仍连接 node1；此时 node1 是普通副本。再隔离 node1 **70 秒**，全程保持 node4、node5 存活；存活地址可立即读取 `seq=183` 的首次写入（LSN **3544**），但原客户端的该次 Append 等满 **60000.604 毫秒**才返回 `context deadline exceeded`。它随后重连到 node5，第二次尝试成功返回 LSN **3548**，整轮 **61.321 秒**。这证明超过 1 分钟的耗时并非两票无法提交，而是在途 RPC 未及时失效、调用方给每次尝试设置 1 分钟超时，再加重连/退避造成的长尾。代码上，Logservice 数据客户端对 RPC 使用 `WithBackendReadTimeout(0)`，`future.Get()` 可一直等到请求 context 结束；`managedClient` 收到错误后才关闭连接并重新发现地址。这里是可用性与延迟问题，缩短单次超时可能改善等待，但 Append 超时结果仍须按“可能已提交”对账，不能盲目当作失败。

本轮 **800/800** 次循环最终写读成功，底层 Append 重试 3 次、Read 重试 0 次，单轮 p50/p95/p99 为 **181/269/566 毫秒**，最大 **61.321 秒**。从截断位点 3230 之后，800 笔已确认记录在 node1、node4 的单地址读取结果逐行一致。保留区间发现两个重复 payload：`seq=183` 在 LSN 3544、3548；`seq=334` 在 LSN 3699、3702，后者首次尝试遇到 `EOF`、第二次遇到 `context deadline exceeded` 后成功。故这次重试拦住了最终错误，却没有保证一份 payload 只占一个物理 LSN。随后仅 node1、node4 运行时，`Truncate(4000)` 成功且查询位点为 4000，新写入 10 条均确认并从 4001 起读回一致；两个节点随后均记录 shard 1 的 index 3700 snapshot 导入和 index 4185 导出。

**控制面附带发现。** 第二次隔离前，node4 已于 10:36:19 当选 HAKeeper leader；第二次隔离期间，node4 日志在 10:37:20–10:38:03 连续记录 **18 次** LogService 心跳失败，仍反复尝试隔离中的 node1 HAKeeper 地址。尽管 node4、node5 的进程及数据 Raft 两票在运行，HAKeeper 的 `syshealth.Check` 于 10:38:03 将含副本的三个 store 都列为过期，并为六个曾出现的 store 生成 `L/shutdown`。node5 在 10:38:42 收到命令，10:38:47 开始主动退出；客户端 `seq=334` 的 `EOF` 与此时间重合。node1、node4 未收到退出命令，仍继续提供两票写读；10:41:33 主动重启 node5 后，三地址再次返回原三成员表。代码中 HAKeeper 客户端在调用方 `context.DeadlineExceeded` 时不会重置共享连接；结合这次持续发往 node1 的日志，心跳路由未在过期阈值内稳定切到存活 leader 是合理解释，仍需用连接级跟踪确认全部重试路径。**本测试把 log store 过期阈值设为 30 秒，生产默认是 5 分钟**；本轮只证明这一加速配置下的行为，尚未证明默认阈值也会触发关停。

证据保存在 `/home/mo/jxm/logservice-chaos-netns-long-20261103-d61a7206/` 的 `latency-20261104-a.load`、`.progress`、`.node1.read`、`.node4.read`、`latency-20261104-post.append`、`.read` 与 `node-{1,4,5}/stdout.log`。复算：`python3 analyze_cycle.py latency-20261104-a.load --read-log latency-20261104-a.node4.read --min-lsn 3230 --prefix latency-20261104-a`。测试后已停止这套隔离进程并撤销其网络命名空间；日志与数据目录保留。

## 追加实验：两副本短时下线，再恢复到只缺一票（2026-11-04 10:45–10:49）

重新启动隔离集群 node1、node4、node5，故障前数据 shard 1 与 HAKeeper shard 0 都有三个投票副本、leader 均在 node1。每轮启动持续 Append→Read 客户端后，同时 `SIGKILL` 两个测试节点，约 **5 秒**后仅恢复其中一个；在另一节点仍下线、只剩两票期间继续写读并执行 truncate。单副本继续下线阶段分别持续约 **46 秒、39 秒**，随后才恢复最后一个进程。用前一轮相同的每次 1 分钟超时和 3 分钟继续重试口径；两轮均为各 350 笔顺序写读，实际下线/恢复时间由 `node.sh` 与远端日志记录。

| 场景 | 失去多数派 → 恢复两票 | 客户端与数据核对 | 两票上的 truncate、snapshot |
|---|---|---|---|
| 两个普通副本 node4、node5 下线，恢复 node4，node5 继续下线 | 10:45:58 两台下线，10:46:03 恢复 node4；node1 仍是 leader | 350/350 次最终成功；故障边缘 `seq=17` 先 `EOF`，再约 4.85 秒 `timeout`，第三次成功；最大整轮 **8.059 秒**，无最终错误。三个地址恢复后读取逐行一致，350 笔确认记录按 LSN/payload 通过；同一 payload 在 LSN **4205、4209** 各有一条物理记录。 | node5 继续下线时 `Truncate(4150)` 成功，查询位点 4150；node5 稍后恢复，三个地址均返回原三成员表。 |
| 数据与 HAKeeper leader node1 加普通副本 node4 下线，恢复 node4，node1 继续下线 | 10:47:56 两台下线，10:48:01 恢复 node4；数据 shard 改由 node4 领导，HAKeeper shard 改由 node5 领导 | 350/350 次最终成功，底层重试 **0** 次；受影响的 `seq=27` Append 等待 **8.350 秒**后直接成功，最大整轮 **8.352 秒**。node4、node5 单地址读取逐行一致，350 笔确认记录按 LSN/payload 通过，保留区间无重复 payload。 | node1 继续下线时 `Truncate(4500)` 成功，查询位点 4500；node4、node5 随后均记录数据 shard snapshot 导出。 |

这两轮说明短时两票下线时请求可能报错或等待，恢复一票形成 2/3 多数派后持续写读、截断和 snapshot 能继续；`EOF` 后重试的写入仍要按内容对账，不能假定第一次没有提交。第二轮 node1 约 39 秒后重启，观察窗口内它的两个 shard 查询均为 `ok=false`，node4/node5 的查询视图只列出两票；该视图可能因 gossip 过期而暂时遗漏 node1，不能据此断言 Raft 成员变更已提交。本轮没有验证 node1 最终重新出现在三地址的查询视图。

**再次观察到控制面风险。** 第二轮 node4、node5 虽已组成两票并继续写读，10:49:02 的 `syshealth.Check` 仍将三个含副本的 store 都列为过期，并生成对三个 store 的 `L/shutdown`。node4、node5 在该窗口分别记录 12、20 次 LogService 心跳失败。由于测试配置将 log store 过期阈值设为 **30 秒**（生产默认 5 分钟），这再次复现的是加速阈值下的控制面问题；本轮在客户端完成数据核对后停止测试进程，未等待这些关停命令全部投递，不能声称三节点都实际退出。

证据保存在同一远端目录的 `dual-follower-20261104.*`、`dual-leader-20261104.*` 和 `node-{1,4,5}/stdout.log`。复算两轮分别用 `python3 analyze_cycle.py dual-follower-20261104.load --read-log dual-follower-20261104.node1.read --min-lsn 4151 --prefix dual-follower-20261104`、`python3 analyze_cycle.py dual-leader-20261104.load --read-log dual-leader-20261104.node4.read --min-lsn 4501 --prefix dual-leader-20261104`。已停止本轮隔离进程并撤销其网络命名空间；旧目录与日志保留。

## 追加实验：全新三副本集群的故障组合与默认过期阈值（2026-11-04）

另建完全独立的隔离集群 `/home/mo/jxm/logservice-chaos-fresh-20261104/`，三个空数据目录和三个网络命名空间分别使用 `198.18.51.10:56501`、`.11:56511`、`.12:56521`；`log-store-timeout` 使用默认的 **5 分钟**。数据 shard 1 的三投票 ID 为 node0 `262147`、node1 `262146`、node2 `262145`；HAKeeper shard 0 对应 `131072`、`131073`、`131074`。客户端按 MO WAL 的单次 1 分钟超时、3 分钟继续重试口径，连续执行唯一 payload 的 Append→Read 精确核对。

第一轮先将当时的 HAKeeper leader node2 **双向隔离 70 秒**，数据与控制面均由 node0、node1 保持两票；隔离期间 `Truncate(50)`、`Truncate(200)` 成功。恢复网络后，再同时杀数据 leader node1 与 HAKeeper leader node0，**8 秒后仅恢复 node0**，node1 继续下线约 70 秒；两票期间 `Truncate(400)` 成功，数据 shard 出现 snapshot 导出。node1 最后恢复，三地址均返回原始三成员表。持续客户端 **901/901** 轮成功，Append 底层重试 3 次（`EOF`、`context deadline exceeded`、`timeout` 各一次），Read 重试 0 次，最大整轮 **11.779 秒**，无最终错误。截断位点 400 后，三地址单独读回逐行一致，514 条仍保留的已确认记录按 LSN/payload 核对通过；`seq=497` 的同一 payload 出现在 LSN 511、514，说明重试后可能产生物理重复。

第二轮在持续写读时，并发发起 `Truncate(700)`，随即同时杀 node0、node1，**25 秒只留 node2 一票**；先恢复 node1，使 node1、node2 两票持续运行约 30 秒，再恢复 node0。并发 truncate 返回连接超时，位点仍为 400，因此不能视为已提交；恢复两票后的再次 `Truncate(700)` 成功，查询位点为 700。客户端 **464/464** 轮最终成功，Append 底层重试 5 次（一次 `EOF`、四次 `timeout`），Read 重试 0 次，最大整轮 **29.373 秒**，无最终错误。三地址从 LSN 701 读回逐行一致，464 条已确认记录按 LSN/payload 核对通过；故障边缘 `seq=58` 的同一 payload 占 LSN 985、990。所有节点恢复后，三地址均返回原始三成员表。这一轮验证了暂失多数派时重试会等待、恢复两票后写读与截断可继续，并再次观察到“已提交但回包不确定”的重复物理记录。

两轮没有观察到 `L/shutdown`；这是 5 分钟过期阈值下最长 70 秒故障的结果，不能推断超过阈值后的行为。证据为远端该目录的 `fresh-default-20261104.*`、`quorum-gap-20261104.*`、`node-{0,1,2}/stdout.log`。复算分别执行 `python3 analyze_cycle.py fresh-default-20261104.load --read-log fresh-default-20261104.node0.read --min-lsn 401 --prefix fresh-default-20261104` 和 `python3 analyze_cycle.py quorum-gap-20261104.load --read-log quorum-gap-20261104.node0.read --min-lsn 701 --prefix quorum-gap-20261104`。

第三轮在全新持续客户端运行时，将当时**同时担任数据与 HAKeeper leader** 的 node2 双向网络隔离 **330 秒**（11:03:19–11:08:49），跨越 5 分钟的 log store 过期阈值；node0、node1 始终保持网络互通并成为两个 shard 的新 leader/投票成员。隔离期间 `Truncate(1000)` 成功且查询位点为 1000。约两分钟时，node0、node1 的地址发现查询暂时只列出两成员；仅凭该视图不能认定 Raft 已提交成员移除。恢复网络后，三地址的数据与 HAKeeper shard 查询**均重新列出原始三个 ID**，没有看到新 ID 或旧 ID 的额外投票成员。

持续客户端 **1765/1765** 轮成功，Append/Read 底层重试均为 0，最大整轮 **2.612 秒**，p50/p95/p99 为 **179/317/645 毫秒**；从 LSN 1001 开始的三地址读回逐行一致，1765 条仍保留的已确认记录按 LSN/payload 核对通过，保留区间没有重复 payload。node0、node1 在整个隔离期间持续写读，日志未见 `L/shutdown` 或 LogService 心跳失败；最终三个节点日志均未见 `L/shutdown`。这次**没有复现**前述 30 秒阈值下的错误关停，但只覆盖这一种全新集群、一个 leader 隔离时序，不能排除其他长故障时触发。隔离节点恢复后，其单地址读回与另外两台完全一致。

第三轮证据为同一远端目录的 `expiry-default-20261104.load`、`.progress`、`.network`、`.restored`、`.node{0,1,2}.read` 及三个节点日志。复算：`python3 analyze_cycle.py expiry-default-20261104.load --read-log expiry-default-20261104.node0.read --min-lsn 1001 --prefix expiry-default-20261104`。读回与状态核对后，已停止这套新集群的三个进程并撤销网络命名空间；测试日志和数据目录保留。

## 缺陷记录：存活两票时在途 Append 仍等待单次 1 分钟超时（2026-11-04）

跟踪：[matrixorigin/matrixone#32133](https://github.com/matrixorigin/matrixone/issues/32133)。

**复现条件与步骤。** 使用上一节的全新集群、默认 5 分钟 store 过期阈值，三个数据与 HAKeeper 投票副本均正常。以三个 RPC 地址启动 `chaos-client -op cycle -count 0 -try-timeout 1m -retry-window 3m`，对唯一 payload 连续执行 Append→Read；用宿主机 `ss -tnp` 找到该客户端当前保持的 TCP 连接，确认目标是三副本中的一台后，用 `netns.sh <目录> down <节点号>` 双向隔离**这个已连接节点** 75 秒，另外两节点保持互通。恢复网络并等三地址再次显示原三成员表后，重新读取当前客户端连接，轮换目标重复。若隔离的是无客户端连接的节点，不能据此判定是否复现本问题。网络隔离脚本的 `down`/`up` 只操作本轮命名空间的 veth；没有停止其余节点。

**三次连续结果。** 同一个客户端依次连接 node1、node2、node0 时隔离对应节点，受影响的 `seq=79/235/378` 第一次 Append 分别在 **60001.127/60000.663/60001.011 毫秒**后返回 `context deadline exceeded`；重连后同一逻辑循环总耗时分别 **60.505/60.520/61.325 秒**，均最终写读成功。隔离期间另两台的 `latest` 继续推进。全程 491 次循环最终成功、Append 底层重试 3 次、Read 重试 0 次；三地址最终读回逐行一致，491 条已确认记录按 LSN/payload 核对通过。三次未知结果的首次写入实际也已提交：同一 payload 分别占 LSN **3263/3267、3423/3427、3570/3573**。因此这里是已确认的可用性长尾与未知结果重试导致物理重复；本轮没有 TN/CN，不能直接断言 SQL 重复执行或 SQL 层同等延迟。

**机制与预期。** Logservice 客户端复用现有 MORPC 连接；数据客户端设置 `WithBackendReadTimeout(0)`，在网络黑洞中可等到本次调用的 1 分钟 context 超时才关闭连接并重新发现其他副本。两票可用且已能推进 LSN 时，请求不应被旧连接无条件拖到调用方的整段超时；同时必须保留 Append “超时但可能已提交”的语义，修复后的重试不能仅凭超时判为未写入。证据在 51 的 `/home/mo/jxm/logservice-chaos-fresh-20261104/`：`tail-repeat-a-20261104.load`、`.progress`、`.node{0,1,2}.read`、`tail-repeat-{a,b,c}-20261104.restored` 和节点日志。复算：`python3 analyze_cycle.py tail-repeat-a-20261104.load --read-log tail-repeat-a-20261104.node0.read --min-lsn 1001 --prefix tail-repeat-a-20261104`。这次**3/3 个已连接目标**复现长尾；未测试所有网络故障形式。

## 缺陷记录：30 秒阈值下健康 Raft 节点被误判过期并收到关停命令（2026-11-04）

跟踪：[matrixorigin/matrixone#32134](https://github.com/matrixorigin/matrixone/issues/32134)。

**环境和复现步骤。** 三套互相独立的空目录三节点集群分别保存在 51 的 `logservice-chaos-heartbeat-repeat{,2,3}-20261104/`，端口基数依次为 56600、56700、56800；各配置 `log-store-timeout = "30s"`，每节点独立网络命名空间。先启动持续三地址 Append→Read 客户端，确认 HAKeeper shard 有三票。隔离当前 HAKeeper leader 约 90 秒、恢复并等待三地址重新看到三成员；记录新的 leader 和各节点在其命名空间内 `ss -tnp` 的 HAKeeper RPC 连接。再隔离**新 leader 或其 HAKeeper 客户端仍连接的节点**约 80–90 秒，始终保留另两台进程及相互网络；每轮记录健康节点的 `failed to send log service heartbeat`、`syshealth/check.go` 的 `Expired logStore info`、HAKeeper 的 `L/shutdown` 生成与服务进程实际退出。第二次隔离前确认存活节点的客户端确实连着将被隔离的地址，是这组复现的关键前提。

| 独立目录 | 第一次隔离（单独未误关停） | 第二阶段命中的连接与故障 | 健康节点误判和结果 |
|---|---|---|---|
| `heartbeat-repeat-20261104` | 原 HAKeeper leader node1 90 秒；恢复后另隔离已成普通副本的 node1 80 秒，均未误关停 | HAKeeper leader node2；确认 node0 的客户端 TCP 连接 node2，再隔离 node2 90 秒 | node0 心跳连续失败；11:29:32 过期检查列出**存活 node0** 和隔离 node2，并为三个 store 生成 `L/shutdown`；node1 收到命令并于 11:29:40 完成进程关停。 |
| `heartbeat-repeat2-20261104` | 原 leader node1 90 秒，node0/node2 仅各两次心跳失败，未误关停 | 新 leader node2；确认 node0/node1 的客户端 TCP 均连接 node2，再隔离 node2 90 秒 | node0/node1 心跳连续失败；11:34:11 过期检查列出**存活 node0** 和隔离 node2，为三 store 生成 `L/shutdown`；node1 于 11:34:19 完成进程关停。 |
| `heartbeat-repeat3-20261104` | 原 leader node2 90 秒，node0/node1 仅 2/3 次心跳失败，未误关停 | 新 leader node0；确认 node0 的客户端仍连接普通副本 node1，再隔离 node1 90 秒 | node0/node2 心跳连续失败；11:39:20 过期检查列出**存活 node0** 和隔离 node1，为三 store 生成 `L/shutdown`；**一直未被隔离的 node2** 于 11:39:30 完成进程关停。 |

**稳定性与影响。** 三套独立集群在记录到持续指向故障地址的客户端连接并进行第二阶段隔离后 **3/3 复现**；单次 leader 隔离的对照均未复现，故“任意一票断网 90 秒”并不是充分的复现描述。HAKeeper 的 Raft 两票与数据写读在误判前仍能推进，但共享心跳客户端的请求持续超时，健康 store 的心跳在 30 秒阈值内没有送达新 leader；`syshealth.Check` 随后可错误关停健康节点，进一步减少可用票数。`shouldResetHAKeeperClient` 对调用方 `context.DeadlineExceeded` 不重置共享客户端，与观察到的持续旧连接相符；还需要连接级追踪确认每次请求的完整路由。三轮均使用**加速的 30 秒配置**；前一节的 5 分钟阈值、330 秒隔离没有复现关停，不能声称生产默认阈值必然触发。

远端三目录分别保留 `heartbeat-repeat*.isolated`、`.restored`、`.network`、`.load`、`node-*/stdout.log`；第三套还保留 `heartbeat-repeat3b-20261104.preconnection`。可用 `grep 'failed to send log service heartbeat\|Expired logStore info\|L/shutdown\|Shutdown complete' node-*/stdout.log` 复核时间线。测试结束后已停止这三套隔离集群的测试进程并撤销网络命名空间，证据目录保留。

## 追加实验：读取边界、不完整快照目录与配额超时（2026-11-04）

另建 5 分钟过期阈值的独立三副本集群 `/home/mo/jxm/logservice-chaos-boundary-20261104/`，三个 RPC 地址为 `198.18.51.10:56901`、`.11:56911`、`.12:56921`。确认三地址均为数据 shard 三成员后，顺序写入 100 条，最后一条用户记录 LSN 105，随后 `latest=106`。三地址从 105 读取均返回该条，从 106、107、110 读取均正常返回空结果；从远超尾部的 1000 读取均返回 `invalid log range`。这只是本次直接客户端的接口边界观察，不能据此断定远未来 LSN 请求必须成功。

在 `Truncate(50)` 后，三节点各导出 index 123 的快照。杀普通副本 node1，在其 `exported-snapshot/shard-1/replica-262146/` 下人为创建空目录 `snapshot-000000000000007C`（index 124），保留真正的 index 123 快照，再用原数据目录重启 node1。node1 仍回到原三成员表，但在静默 shard 上从 12:00:42 起，每次截断 tick 的 `SyncRequestSnapshot` 都返回 `request rejected`；另外两节点继续成功导出。测试专用空目录于 12:01:20 移除后，node1 于 12:01:22 成功导出 index 124。第二次同法创建空 index 125 目录：重启后再次出现拒绝；**保留**该空目录并继续写入 200 条时，快照导出绕过该 index，在更高 index 成功，空目录仍留在磁盘。`Truncate(200)` 后三节点均完成 snapshot 导入，三地址从 201 读回逐行一致，125 条仍保留的确认记录按 LSN/payload 通过。这个注入证明最终导出目标目录的残留物可阻塞静默 shard 的同 index 重试；**尚未证明正常进程崩溃会形成相同的空最终目录**，不能直接归因于已测的真实 crash 窗口。`snapshotManager.Init` 对扫描到的无效目录忽略 `snapshots.add` 的错误，因而不会在启动时清理或显式拒绝这类残留。

在同一集群导出配额为 4、node1 恰有四个有效快照 index 328–331 时，将 node1 双向隔离 **30 秒**，保留 node0、node2 两票。node1 的 `getTruncatedLsn` 读取超时一次，配额逃生分支删除最新的 index **331**，保留 328–330；恢复网络后继续写入 100 条并 `Truncate(350)`，三节点均再次导出并导入快照。从 351 的三地址读回逐行一致，83 条仍保留的确认记录按 LSN/payload 通过，查询截断位点为 350。

为覆盖源码中 `MaxExportedSnapshot=1` 的独立分支，又建 `/home/mo/jxm/logservice-chaos-quota1-20261104/`（端口 `57001/57011/57021`，5 分钟 store 过期阈值）。三节点写入 100 条并 `Truncate(50)` 后各有唯一 index 107 导出快照；隔离 node1 30 秒，触发一次 `getTruncatedLsn` 超时，日志**没有** `DropNewest`，磁盘仍保留唯一快照。恢复网络、再写 100 条和 `Truncate(120)` 后三节点均完成导入，从 121 起三地址读回一致，88 条仍保留的确认记录按 LSN/payload 通过。本轮未测在隔离持续数小时且只有一个快照时 WAL 的空间增长上限。

上述两目录留有 `read-boundary-*`、`partial-snapshot*`、`partial-load-*`、`quota-partition-*`、`quota1-partition-*`、`quota-recovery*` 及各节点日志；可用 `verify.py --min-lsn <截断位点+1> <单地址读回> <写入日志...>` 重做确认记录核对。`boundary` 集群核对后已停进程并撤销网络命名空间，日志和数据目录保留。

## 追加实验：默认 5 分钟阈值的定向 HAKeeper 连接隔离（2026-11-04）

沿用上述 `max-exported-snapshot=1` 的隔离集群，但此轮只验证控制面心跳；`log-store-timeout` 为 **5 分钟**。启动持续三地址 Append→Read 客户端后，先双向隔离 HAKeeper leader node0 **90 秒**（12:09:08–12:10:38），node1、node2 当选新 leader 并保持两票。恢复 node0 和三成员视图后，确认新 leader node2 的 HAKeeper 客户端 TCP 仍连接普通副本 node1；再隔离 node1 **390 秒**（12:11:08–12:17:38），始终保持 node0、node2 进程及相互网络在线。这次满足此前 30 秒阈值误关停测试的“新 leader 客户端仍连接故障地址”前提，而不仅是简单隔离 leader。

第二阶段结束时，node0、node2 的节点日志分别累计记录 **139、102 次** LogService 心跳失败，但未生成 `L/shutdown`。约 6 分钟时 node1 仍隔离，`ss` 显示 node0 的 HAKeeper 客户端已改连存活的 node2；两台的失败计数随后停止增加。全轮持续客户端 **2800/2800** 次写读成功，Append/Read 底层重试均为 0，最大整轮 **3.945 秒**。恢复 node1 后三地址的数据与 HAKeeper shard 均重新列出原始三个 ID；从截断位点 120 后分别读回逐行一致，2800 条已确认记录按 LSN/payload 通过，保留区间无重复 payload。此轮**没有复现** 30 秒加速阈值下的误关停；只能说明这一组 5 分钟阈值、两阶段时序中，客户端最终切换到存活地址。不能据此推断任意默认配置网络故障都安全。

证据保存在 `/home/mo/jxm/logservice-chaos-quota1-20261104/` 的 `default-expiry-stage{1,2}.{isolated,restored,network}`、`default-expiry-stage2.preconnection`、`default-expiry-targeted-20261104.load`、`default-expiry-targeted.node{0,1,2}.read` 以及节点日志。复算：`python3 analyze_cycle.py default-expiry-targeted-20261104.load --read-log default-expiry-targeted.node0.read --min-lsn 121 --prefix default-expiry-targeted-20261104`。核对后已停这套集群的三个进程并撤销网络命名空间，证据目录保留。

## 追加实验：真实导出崩溃形成无效 snapshot 目录（2026-11-04）

跟踪：[matrixorigin/matrixone#32144](https://github.com/matrixorigin/matrixone/issues/32144)。

**复现方式。** 重新启动前述 `/home/mo/jxm/logservice-chaos-boundary-20261104/` 三副本集群，保持 node0、node2 两票在线。在 node1 的 `exported-snapshot/shard-1/replica-262146/` 目录上用 Linux `inotify` 监听创建和移入事件，收到 `snapshot-<index>-262146.generating` 或其重命名为正式 `snapshot-<index>` 的事件后，立即对 node1 的 `mo-service` 发送 `SIGKILL`。监听脚本保存在远端同目录的 `snapshot-crash-watch.py`；通过把截断位点推进到已有导出快照的 index，触发下一次导出。这是进程崩溃注入，没有人工创建或修改 snapshot 内容，且从头到尾至少两票存活。

**两种中断窗口。** 第一次在 `snapshot-00000000000001C2-262146.generating`（index 450）创建时杀掉 node1，留下**空的临时目录**；原数据目录重启后该临时目录被清理，node1 成功重新导出 index 450，未出现同 index 持续拒绝。这符合 Dragonboat 先写 `*.generating`、完成后才重命名的实现，因此不能把之前人工创建的**空正式目录**视作已由普通进程崩溃复现。

随后两次将中断点改为正式目录的 `IN_MOVED_TO` 事件，分别命中 index **453**（`snapshot-00000000000001C5`）和 **569**（`snapshot-0000000000000239`）。两次杀进程后正式目录都实际含三个文件：`snapshot-<index>.gbsnap`、`snapshot.metadata`、`dragonboat.snapshot.message`。最后一个是 Dragonboat 在提交后尚未来得及删除的标记文件；LogService 的 `snapshotItem.Valid` 要求恰好两个文件，`snapshotManager.Init` 又忽略加入无效快照时的错误。原目录重启后，坏目录仍在，node1 的同 index 导出连续返回 `request rejected`：第一次 **26 次**（14:04:19–14:04:48），第二次 **18 次**（14:06:28–14:06:48）。这是两次独立导出窗口的 **2/2 正式目录残留与重启后持续拒绝**；监听式杀进程控制了具体时机，不代表随机重启也有同样概率。

**影响与恢复。** 第一次正式目录残留时，在没有新业务写入的约 29 秒内 node1 未成功导出；继续写入 100 条后，应用 index 前进，node1 从 index 455 起重新成功导出，坏目录仍留在磁盘。三地址从 LSN 451 读回逐行一致，100 条确认写入按 LSN/payload 核对通过；随后 `Truncate(500)` 成功，node1 导入 index 459 快照，三地址从 501 读回一致，54 条仍保留的确认写入核对通过。第二次重启后仍可在三地址查询到原三成员表。已证实的是静默 shard 的同 index 导出会持续拒绝、错误日志和坏目录不会自愈；**没有观察到数据丢失或本次有写入后的截断失败**。更长时间静默时的磁盘占用影响未量化。

证据在 51 的 `/home/mo/jxm/logservice-chaos-boundary-20261104/`：监听脚本、`node-1/stdout.log`、`real-snapshot-crash-20261104.append`、`real-snapshot-crash.node{0,1,2}.read`、`real-snapshot-crash-after-truncate.node{0,1,2}.read`，以及 node1 磁盘上的两个带标记文件的正式目录。复算：`python3 /home/mo/jxm/logservice-chaos-fresh-20261104/verify.py --min-lsn 451 real-snapshot-crash.node0.read real-snapshot-crash-20261104.append`，再以 `--min-lsn 501` 核对截断后读回。测试后已停三进程并撤销测试网络命名空间，保留磁盘现场。

## 追加实验：数据 leader 导出崩溃与独立 HAKeeper leader（2026-11-04）

另建独立隔离集群 `/home/mo/jxm/logservice-chaos-snapshot-leader-20261104/`，三个数据和 HAKeeper 投票副本，默认 5 分钟 store 过期阈值，`max-exported-snapshot=1`，RPC 端口 57101/57111/57121。先短时重启 node2，使 **HAKeeper shard 0 的 leader 在 node0**、**数据 shard 1 的 leader 在 node1**，三地址均确认两个 shard 仍有三成员。写入 100 条 seed 后启动持续三地址 Append→Read 客户端；对数据 leader node1 的导出父目录监听 `IN_MOVED_TO`，在 index **157** 的正式目录刚由 `*.generating` 重命名时给 node1 发 `SIGKILL`。node0、node2 一直存活且互通；node1 的目录确实留下 `dragonboat.snapshot.message`、`snapshot.metadata` 和 `.gbsnap` 三文件，与上一轮普通副本故障的残留形态相同。

node1 下线后，数据 shard 由 node2 领导，HAKeeper 仍由 node0 领导；两票期间 `Truncate(150)` 成功。另起仅连接 node0/node2 的客户端完成 **100/100** 次 Append→Read，零重试，最大整轮 **0.550 秒**。原三地址客户端在故障边缘 `seq=46` 已成功 Append 到 LSN 158，随后的 **Read 等待约 60.001 秒**才返回 `EOF`，重试后核对成功；这一轮从 Append 到读回共 **61.623 秒**。这把已记录的旧连接 1 分钟长尾扩展到 Read 路径；不是两票失去提交能力，也没有最终客户端错误。恢复 node1 后两个 shard 的三地址视图均重新列出原三个 ID，坏的 index 157 目录仍留在磁盘；持续写入使应用 index 前进，node1 后来成功导出 index 426。

本轮还观察到导出请求超时后的**迟到完成**：node1 恢复追赶时，14:26:55 与 14:26:58 的 `SyncRequestSnapshot` 分别返回 `timeout`，但紧接着磁盘上出现了内容各为 `.gbsnap` 和 `snapshot.metadata` 两文件的有效正式目录 index **400**、**414**；LogService 没有对这两次记录“导出成功”，随后只登记了成功响应的 index 426。执行 `Truncate(426)` 后，node1 导入并删除 index 426，但 index 400/414 仍在磁盘，说明本轮运行中它们未随已登记快照清理。再次重启 node1 后，启动扫描拾取这些目录并导入 index 414，index 400/414 随即被清理；带标记文件的无效 index 157 目录仍保留。这个**单次恢复窗口**表明超时后迟到完成可能暂时绕过 `max-exported-snapshot=1` 的内存配额并留下未登记目录；尚未测量长期不重启时能否持续累积，暂不单列新缺陷。

持续三地址客户端最终 **555/555** 次写读成功，Append 重试 0、Read 重试 1、最大整轮 61.623 秒；另一个两地址客户端 100/100 次成功。最终 `Truncate(426)` 位点确认，三地址从 LSN 427 读回逐行一致，前者仍保留的 **347 条**确认记录按 LSN/payload 核对通过，保留区间无重复 payload。两地址客户端的 100 条均早于截断位点，由其运行中的逐次 Append→Read 校验覆盖，最终读回已不能再次核对这些被截断记录。没有观察到数据丢失、最终读写错误或三成员恢复失败。

原始证据在该目录的 `leader-snapshot-crash.load`、`.progress`、`leader-snapshot-survivors.load`、`final-hakeeper.status`、`final-data.status`、`final.node{0,1,2}.read` 及三个节点的 `stdout.log`；监听脚本为 `snapshot-crash-watch.py`。复算：`python3 analyze_cycle.py leader-snapshot-crash.load --read-log final.node0.read --min-lsn 427 --prefix leader-snapshot-crash-20261104`。测试后已停三进程并撤销测试网络命名空间，保留目录和日志。

## 追加实验：Read 旧连接黑洞中的快速重试对照（2026-11-04）

跟踪：[Read 长尾缺陷 matrixorigin/matrixone#32148](https://github.com/matrixorigin/matrixone/issues/32148)；同类 Append 长尾见 #32133。

**代码路径。** `pkg/logservice/service.go` 的 `handleRead` 直接调用 `store.queryLog`；它不决定客户端换地址。`pkg/logservice/client.go` 的 `managedClient.Read` 在 RPC 出错后 `resetClient`，但仅对 shard-not-found 错误在该层自行重试；普通 `EOF`、调用超时会交给调用方。建连时给 MORPC 的 `WithBackendReadTimeout` 传 **0**，没有额外的 backend 读超时来提前判定旧 TCP 黑洞。TN WAL 回放的 `logservicedriver.readFromBackend` 对每次 Read 使用 `DefaultOneTryTimeout = 1m`，失败后指数退避重试，总时限按 `MaxTimeout` 在每次失败后检查，默认 3 分钟并非严格墙钟截止。因此黑洞连接在没有 TCP/服务端早期错误时等到调用方的 1 分钟 deadline 才切换，是**当前实现的结果**；它不说明两票没有服务能力。

**受控注入。** 在 `test/logservice-chaos/client` 增加仅供测试的 `-pause-read-seq`、`-read-ready-file`、`-read-release-file` 参数，使同一 `logservice.NewClient` 在 Append 已成功并拿到 LSN 后、Read 尚未发出时暂停。用宿主机 `ss -tnp` 确认该客户端持有的 TCP 目标，双向断开该目标节点的测试 veth，再放行 Read。每次只隔离一台，另外两台始终存活且互通；完成后恢复网络。这个实验的 2 秒设置是**直接测试客户端参数**，没有修改 MO/TN 的生产默认值。此前真实 leader 崩溃时的一次 Read 等待 60 秒的结果见上一节。

| 客户端单次超时 | 已连接且被隔离的地址 | 首次 Read 的结果 | Read 阶段从放行到核对成功 | 结果 |
|---|---|---|---|---|
| 2 秒，第一轮 | node0 `198.18.51.10:57101` | 2000.734 ms 后 `context deadline exceeded` | **3204.537 ms** | 第二次 Read 成功，LSN 789 核对正确。 |
| 2 秒，第二轮 | node2 `198.18.51.12:57121` | 2000.961 ms 后 `context deadline exceeded` | **3330.323 ms** | 第二次 Read 成功，LSN 945 核对正确。 |
| 默认 1 分钟对照 | node2 `198.18.51.12:57121` | 60000.428 ms 后 `context deadline exceeded` | **60291.560 ms** | 第二次 Read 成功，LSN 949 核对正确。 |

三轮 Append 均在断网前成功，Read 均在断网后发起并仅重试一次，无最终错误。客户端的整轮 `total_ms` **包含人工暂停**，不能用它估算故障恢复耗时；上表的 `read_ms` 从真正开始 Read 才计时。在无故障网络上，同一 2 秒配置的另外 **150/150** 次 Append→Read 均零重试，最大整轮 **638.310 ms**；这只是本测试负载下的阴性对照，不能证明 2 秒适合所有生产负载。恢复三节点后，从 LSN 789 的三地址读回逐行一致，三条受控请求的 payload 分别在 LSN 789、945、949 核对通过。

证据保存在 `/home/mo/jxm/logservice-chaos-snapshot-leader-20261104/`：`fast-read-{2s,2s-b,60s}.{ready,release,fault,load,stderr}`、`fast-read-baseline.load`、`fast-read-final.node{0,1,2}.read` 和 `chaos-client-fast`。测试脚本修改在 `test/logservice-chaos/client/{main,cycle}.go`。再次核对后已停测试节点并撤销网络命名空间，日志和数据保留。

## 追加实验：单副本恢复与三副本整组冷重启（2026-11-04 15:09–15:15）

**目的与前提。** 在原独立测试集群 `/home/mo/jxm/logservice-chaos-20261103-d61a7206/` 中核实单副本、整组三副本重启分别如何使用已导入的正式 snapshot。起始时 node1、node2、node3 均为两个 shard 的投票成员；数据 shard 1 的成员分别为 `385027`、`395266`、`477185`，leader 在 node2；HAKeeper shard 0 的成员分别为 `385028`、`395265`、`477186`。起始 `TruncatedLsn=7621`，`latest=9285`。本轮没有清空任何 Raft 目录。

**单副本恢复。** 三地址客户端以唯一 payload 连续做 500 次 Append→Read，期间将普通副本 node3 `SIGKILL` 约 10 秒，再从原目录启动。node1、node2 保持在线。恢复后三个地址仍返回原数据成员表；负载最终 `500/500` 次成功、Append 底层 `EOF` 重试 1 次、Read 重试 0 次，无最终错误。整轮 p50/p95/p99 为 **162/245/587 ms**，最大 **1.102 s**。负载期间 `Truncate(9450)` 成功，查询水位为 9450。此段测试说明单副本从旧目录重启后可继续追赶，但没有证明它必须走 snapshot 安装而非保留日志回放。

**整组冷重启发现的启动故障。** 500 次结束后，从 9451 读取保留区间作基线（338 条用户记录），同时 `SIGKILL` 三个进程；确认三进程全停后，以原配置和原目录依次启动。node3 留在运行状态，但 node1 和 node2 启动后不到一秒均退出，两个 RPC 地址拒绝连接，node3 单票不能组成多数派；客户端连接超时。这是本轮**未被正常请求重试掩盖的集群不可用错误**，不是数据快照损坏。

node1/node2 的 `mo-logservice.metadata` 各自保留了旧 HAKeeper 副本：node1 为 `131073`、`374786`、当前 `385028`；node2 为 `131074`、当前 `395265`。冷启动时其他地址尚未提供成员表，`checkZombieReplicas` 的自检无权威回复，按当前实现将这些历史记录都暂当有效；`startReplicas` 先启动旧 shard 0 ID，再尝试在同一 NodeHost 启动当前 shard 0 ID，返回 `shard already exist`，`cmd/mo-service` 在 `NewService` 错误处 panic。两台都记录了上述 panic，且各自 HAKeeper 旧 ID 曾在退出前从本地 Raft 文件回放。**仅把配置中的 `bootstrap-cluster` 设为 `false` 再试，仍然发生同样 panic**；故障发生在 `NewService/startReplicas`，早于后续 bootstrap 路径。对应代码在 `pkg/logservice/store.go` 的 `checkZombieReplicas`、`startReplicas`。该实验只有这一次整组冷重启，尚未在全新集群复测概率。

**隔离环境恢复与校验。** 为避免丢失原证据，先将 node1/node2 的 metadata 各备份为 `mo-logservice.metadata.before-cold-recovery`，用 `test/logservice-chaos/recover_metadata.py` 仅保留停机前已核实的当前 HAKeeper 与数据副本 ID；没有更改 Raft 日志或 snapshot 文件。使用 `bootstrap-cluster=false` 的临时配置重启 node1/node2 后，三个进程均存活，两个 shard 的三地址成员表恢复一致，截断水位仍为 9450。Dragonboat 日志明确显示数据副本从**已导入的正式 snapshot** 恢复：node1 index **7826**、node2 **7825**、node3 **8464**；之后重放日志。三个单地址从 9451 的读回均与停机前 338 条基线逐字节相同（SHA256 `d4818258a14706d89e791699c4a5b0609f125787f7e92cdf64acd664d0c322bd`），338 条仍保留的确认记录按 LSN/payload 全部通过，没有重复 payload。恢复后新写入 10 条（LSN 9801–9810）均成功并读回正确。

**对照冷重启。** 保留原 metadata 备份，再将过滤后的三个测试节点全部停下并按临时配置冷启动第二轮。这次三个进程均保持运行，数据与 HAKeeper shard 的三地址成员表均一致；从 9451 读回的三份结果与第二次停机前基线完全相同（SHA256 `de4113707e5da6b657c937fb0e952c38be179febb11e17a4561b0078816c4cb7`），又成功追加 3 条（LSN 9819–9821）。这支持“历史 ID 与启动自检不可用的组合导致第一次失败”的判断；但第二轮还使用了临时 `bootstrap-cluster=false`，不能单独量化配置因素对其他启动路径的影响。第一次只把这个值改成 `false`、而未过滤 metadata 时仍 panic。

**影响与边界。** 已证实的缺陷是：当历史副本 ID 仍在本地 metadata，且整个控制面同停导致启动自检暂时不可用时，多个 LogStore 可因同 shard 重复启动而退出，从而无法自行形成多数派。单节点重启、旧目录替补测试不能覆盖这一冷启动前提。手工过滤 metadata 只用于这个隔离测试集群的恢复，不是建议的线上修复方式；需要在启动逻辑中解决无权威成员表时同 shard 多 ID 的选择，并保护真正首次建群的恢复路径。

远端证据：`restart-20261104.load`、`.progress`、`.before.read`、`.node{1,2,3}.read`、`.clean-before.read`、`.clean-node{1,2,3}.read`、对应 `node-{1,2,3}/stdout.log`，以及两份 `mo-logservice.metadata.before-cold-recovery`。复算：`python3 analyze_cycle.py restart-20261104.load --read-log restart-20261104.node1.read --min-lsn 9451 --prefix restart-20261104`。测试结束时 node1、node2、node3 均运行；node1/node2 目前使用临时 `bootstrap-cluster=false` 配置，原始 TOML 未修改。

### 稳定复现：三轮整组冷启动（2026-11-04 15:26–15:28）

跟踪：[matrixorigin/matrixone#32151](https://github.com/matrixorigin/matrixone/issues/32151)。

使用 `test/logservice-chaos/reproduce_cold_restart.py` 在上述隔离目录重复三轮。每轮先确认三进程均运行并保存 9451 起的读回基线，再同时停掉 node1、node2、node3，等待 Dragonboat 目录锁释放，将先前备份的**原始** `mo-logservice.metadata.before-cold-recovery` 复制回 node1/node2（保留 Raft 文件与原配置不变），随后以原配置启动三个节点。每轮 node1/node2 都在启动阶段记录 `panic: shard already exist` 并退出，node3 保持运行：**3/3 轮、两台各 3/3 次**。这组重复排除了前述测试脚本过早启动时遇到的 `failed to lock data directory` 干扰；该干扰轮不计入复现。随后每轮停 node3、恢复事先保存的仅含当前 ID 的 metadata，使用临时 `bootstrap-cluster=false` 配置重启三节点，三地址恢复可用；从 9451 起的 351 条读回与基线 SHA256 `bb28f629f8e5fa5378904365d1c64fab8f1cc3e806df9b06b4db64e2a733c5ac` 在三轮均一致。最后三进程运行、两个 shard 的多数派恢复。原始失败 metadata 及代码流程证据均保留。

完整输出在远端 `cold-repeat-verified2-20261104.log` 和 node1/node2 的 `stdout.log`；重复命令：`python3 /home/mo/jxm/logservice-chaos-20261103-d61a7206/reproduce_cold_restart.py /home/mo/jxm/logservice-chaos-20261103-d61a7206 --rounds 3`。脚本只适用于这个隔离目录，运行前要求三节点都在线，并依赖原始 metadata 备份。

### 补充测试：历史 ID 数量与冷启动顺序（2026-11-04 15:44–15:48）

为明确 #32151 的触发边界，在同一隔离集群追加三组；每次停机前读取 LSN 9451 起的保留区间作基线，测试后用三地址核对成员表。所有手工 metadata 替换只发生在已经停止的测试进程上，保留原备份及 Raft 文件。

| 本地 metadata 与启动顺序 | 实测结果 |
|---|---|
| node1、node2 都只含当前 ID；三节点全部停止，然后三台都用**原始 `bootstrap-cluster=true` 配置**启动 | 三台均保持运行，数据与 HAKeeper shard 的三地址成员表一致。LSN 9451 起读回与停机前 SHA256 均为 `bb28f629f8e5fa5378904365d1c64fab8f1cc3e806df9b06b4db64e2a733c5ac`；新增 3 条写入成功（LSN 9838–9840）。这排除了“仅因为原始 bootstrap 配置就无法冷启动”的解释。 |
| **只有 node1** 恢复历史 ID `131073`、`374786`；全停后先启动 node1，约 4 秒后再启动干净的 node2、node3 | node1 首次启动 `panic: shard already exist`；node2/node3 后续组成两票，数据与 HAKeeper shard 均可查询，两票上新增 3 条写入（LSN 9844–9846）并读回正确。再用**未手工清理的同一份 node1 metadata**重启 node1 时，它从在线同伴识别并跳过两个历史 ID，原三成员表恢复；启动日志还显示 `BootstrapHAKeeper` 跳过初始旧 ID。说明有两票在线时原目录可自恢复，但冷启动期间单个失败进程仍会退出。 |
| node1、node2 都恢复历史 ID；全停后**先启动干净 node3，等 5 秒**，再启动 node1/node2 | node3 单票查询数据 shard 返回 `shard not found`；node1、node2 随后均 `panic: shard already exist`，集群无法形成多数派。只让一台干净节点先上线，不足以提供这条启动自检所需的可用成员视图。恢复 node1/node2 的干净 metadata 后，三节点再次正常；LSN 9451 起读回与本轮停机前 SHA256 同为 `ddd3c72e073de64a5a4793dc1f229392e44f4f006caa60b39e9731a38d5f3850`，又新增 3 条写入成功（LSN 9852–9854）。 |

最终三个 RPC 地址对两个 shard 均返回原三投票成员；从 LSN 9838 起分别经 node1/node2/node3 单地址读回的 SHA256 全为 `bccf299444cd5a8911e76f72948ed86ce283310b854521854b98042352b3f815`。远端证据为 `cold-matrix-20261104.*`、各节点 `stdout.log`、原始 `mo-logservice.metadata.before-cold-recovery`。这个矩阵强化了 #32151 的根因边界：关键是本地同 shard 多 ID 与启动时缺少可用同伴视图的组合；它不说明所有正常三节点冷启动都会失败。

## 追加实验：落后副本通过 Raft snapshot 追赶（2026-11-04 16:13–16:15）

在原隔离三副本集群，启动三地址持续 Append→Read 客户端，普通副本 node1 下线时最后确认 LSN 约 9882。node2、node3 两票持续写读，先 `Truncate(9850)` 释放旧导出快照配额，再将已核对写入之后的水位推进到 9910；两票分别导入数据 shard snapshot index **9901**、**9900**，均已越过 node1 下线前位置。随后在 node1 尚未超过 30 秒测试过期阈值时按原目录重启，Dragonboat 日志记录它从本地 snapshot 7826 恢复、收到并恢复 leader 传来的 snapshot **9900**。它的本地截断 worker 随后还记录了一次旧导出快照 index 9589 的“导入成功”；这条日志并未证明活动恢复点倒退：再杀并启动 node1 后，Dragonboat 仍从 index **9900** 恢复。

客户端 **350/350** 次 Append→Read 最终成功，Append/Read 重试均为 0，最大单轮约 **1.114 秒**。截断水位 9910 后，从 9911 读取的 node1 数据与 node2 在 node1 第二次重启前的基线 SHA256 同为 `dae4dcff2fcfd2ff0b23d7ba631cb58c9bf799378122ba2da2e7ebe23f975907`；三个 RPC 地址仍返回原三成员。证据在远端 `catchup-20261104.load`、`.progress`、`.before-restart.read`、`.after-restart.read` 与 node1–3 日志。这个用例证明本轮真实跨越物理压缩点的副本追赶成功，没有发现新的数据错误。

## 新缺陷：Truncate 接受未来 LSN，之后确认的写入会被快照压缩（2026-11-04 16:16–16:18）

跟踪：[matrixorigin/matrixone#32159](https://github.com/matrixorigin/matrixone/issues/32159)。

**前提与 API 边界。** 仍在上述仅供测试的三副本数据 shard 1 上，用直接 LogService 客户端先查询 `GetLatestLsn()`，然后提交大于 latest 的 `Truncate(target)`。这是刻意构造的越界调用；本次没有 TN/CN，尚未证明正常 TN 会发出未来水位。客户端接口称 truncate 指定 LSN（含该位置）以释放空间；服务端 `store.truncateLog` 只提交命令，`stateMachine.handleTruncateLsn` 只验证相对旧水位单调递增，没有检查当前已应用 index。随后合法的 Append 仍成功返回 `lsn <= target`。snapshot worker 看到已提交的未来水位，就可以导入覆盖这些新记录的 snapshot，并物理压缩它们。直接读取早期记录在快照导入之前曾成功，导入之后三个地址都返回 `invalid log range`。

| 轮次 | Truncate 前 latest → 请求水位 | 已确认但随后三地址不可读的记录 | 水位外仍可读的确认记录 | 结果 |
|---|---:|---:|---:|---|
| 初次手动注入 | 10245 → 10263 | 第一条 10249；另有 10251、10255、10258、10260、10262 均已成功 Append | 10265、10268 | 三节点分别导入 index 10255–10263 附近的正式 snapshot；从 10249 读取三地址均报 `invalid log range`。 |
| 独立重复 1 | 10276 → 10294 | 10281 | 10297 | 9 次 Append 均成功；三地址从 10281 均报 `invalid log range`，从 10297 均能读到对应 payload。 |
| 独立重复 2 | 10313 → 10331 | 10318 | 10333 | 9 次 Append 均成功；三地址从 10318 均报 `invalid log range`，从 10333 均能读到对应 payload。 |

这是**3/3 轮**可控复现的已确认写入不可读。直接风险是错误的未来水位一旦被接受，就会提前删除随后成功返回的 WAL；即使上层调用方应保证水位正确，LogService 不应在当前日志尾部之外接受会影响未来写入的截断。建议在 Raft 状态机应用截断命令时使用同一条命令的 `e.Index` 验证 `requestedLSN <= e.Index`，并返回可识别的参数错误；仅在客户端先查 latest 再检查有并发竞态，不能作为唯一保护。精确允许边界还应覆盖 `requestedLSN == e.Index` 与当前数据日志尾部的区别。

远端证据在 `future-truncate-20261104.{before,plan,truncate,watermark,append,from-first.read,from-first.err,after-watermark.read}`、`future-truncate-paced-20261104.*`、`future-repeat-20261104.log` 和目录 `future-repeat-20261104/`，以及三个节点的 `stdout.log`。重复脚本为 `test/logservice-chaos/reproduce_future_truncate.py`；它限定只能作用于这套隔离目录。测试结束时最新 LSN 已超过故障注入水位，三个节点仍运行；此前被提前压缩的测试 payload 无法恢复。

## 追加实验：落后副本恢复与下一台故障交叠（2026-11-04 16:50–16:59）

这组只使用已确认写入以内的 Truncate 水位。在 node1/2/3 三投票成员的隔离集群持续运行同一个 Append→Read 客户端；每次只杀一台，待它原目录启动后立即杀另一台，始终保留至少两台进程，并在过程中按已读回的进度推进截断。用单地址客户端分别读回三个副本、比较 SHA256，并核对截断后尚保留的客户端确认记录。首次脚本试跑在首个下线后等待“新增超过 100 条”的 20 秒条件上提前结束，自动恢复节点；该轮没有执行交叠，不计入以下结果。另一轮精确 leader 注入第一次因 `SIGKILL` 发出后马上检查进程数，撞到目标进程尚未退出而提前结束；已恢复节点并改成等待 `/proc` 中进程实际退出，此轮也不计入完成的 leader 窗口。

| 时间线 | 客户端与截断 | 最终读回 |
|---|---|---|
| 三轮连续交叠：node1→node2、node2→node3、node3→node1；前一台刚启动约 0.3 秒即杀后一台，各保持约 12 秒 | **544/544** 次写读成功；Append/Read 各重试 1 次；7 次合法 Truncate 成功。第三轮在已成功 Append 的 Read 上等待约 **59.891 秒**后 `timeout`，重试成功，最大整轮 **61.310 秒**，属于 #32148 已记录的旧连接长尾。 | 水位 10923 后，86 条保留的确认记录按 LSN/payload 核对通过，三地址 SHA256 同为 `bed717760d6e023e14b1589238ced8ea2d0d88554a65e78be166e7c008544ad6`；保留区间无重复 payload。 |
| node1 下线期间继续写入和截断；重启后观察到数据 shard 开始恢复 Raft snapshot，再杀 node2；**这轮未校验 node2 是 leader** | **230/230** 次写读成功；Append 重试 1 次，Read 重试 0；最大整轮 **4.116 秒**，7 次 Truncate 成功。 | 水位 11217 后，49 条保留记录核对通过，三地址 SHA256 同为 `da97088d287f264e517e276b2b53c42d1ec16322599c6539a964cd96f707dbe6`。 |
| node3 下线约 27 秒内持续写入和截断，重启后在 **16:58:18.393** 观察到数据 shard 开始恢复 Raft snapshot index **11513**；随即确认 node2 仍是数据 leader，再杀 node2，约 12 秒后恢复 | **218/218** 次写读成功；故障边缘 Append 出现两次 EOF、一次约 **5.301 秒**的 `context deadline exceeded`，重试后成功；Read 重试 0；最大整轮 **6.777 秒**，7 次 Truncate 成功。 | 水位 11630 后，42 条保留记录核对通过，三地址 SHA256 同为 `b2b17ae2207b5c388a59bf0e85fb28ac14869b2fb98e4d18f01f3f228d87f8a5`；保留区间无重复 payload。数据与 HAKeeper 恢复三成员。 |

最后一轮在 leader 下线时，刚恢复的 node3 正开始安装新 snapshot，观察到客户端短暂重试，但未出现最终错误或已确认记录丢失。这里的“两台存活”是进程数保证；刚重启的副本尚未追上时，不能把两进程等同于持续可提交的两票。最终三节点运行，原始证据在 51 的同一目录：`overlap2-20261104.{load,progress,node{1,2,3}.read}`、`restore-cut-20261104.*`、`restore-leader-cut2-20261104.*`、三台 `stdout.log`，以及对应注入脚本 `overlap2-20261104.py`、`restore-cut-20261104.py`、`restore-leader-cut2-20261104.py`。这组未发现新的独立缺陷；约一分钟 Read 长尾已由 #32148 跟踪。
