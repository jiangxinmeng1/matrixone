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

## 新缺陷：导出请求超时后完成，正式 snapshot 目录绕过运行中清理（2026-11-04 17:02–17:05）

跟踪：[matrixorigin/matrixone#32161](https://github.com/matrixorigin/matrixone/issues/32161)。

**真实时序窗口。** 三副本均在线，仅对 node3 普通副本注入约 5 秒 `SIGSTOP`/`SIGCONT`，其他两台继续服务。外部 `inotify` 监听数据 shard 的 `snapshot-<index>-<replica>.generating` 创建事件，在 Dragonboat 刚开始导出时暂停 node3。并行客户端只做正常 Append→Read 和**不超过已确认 LSN** 的 Truncate；没有手工创建或修改 snapshot 目录内容。重现脚本 `test/logservice-chaos/pause_snapshot_export.py` 固定只检查并暂停这套隔离集群的 node3 `mo-service`。每轮先启动监听，再通过合法 Truncate 腾出导出配额并运行写读负载。

| 轮次 | 暂停与返回 | 磁盘及后续清理 |
|---|---|---|
| 1 | 监听到 `snapshot-0000000000002DB1-477185.generating`（index **11697**）后暂停 5 秒；恢复时 `SyncRequestSnapshot` 返回 `timeout`，node3 日志 **17:02:08.764** 记录 `request export snapshot failed`，没有 index 11697 的 `export snapshot success`。 | Dragonboat 随后生成恰含 `.gbsnap` 和 `snapshot.metadata` 的**有效正式目录** index 11697。`Truncate(11800)` 后 node3 正常导入 index **11785**，该更旧的目录仍在。 |
| 2 | 再次监听到 index **11993** 的 `.generating`，暂停 5 秒；node3 日志 **17:04:07.264** 再次报 `request export snapshot failed: timeout`，没有 index 11993 的成功登记。 | 第二个有效正式目录 index 11993 出现。随后 `Truncate(12150)`，node3 正常导入 index **12048**；index 11697、11993 两目录**均仍留在磁盘**。 |

这是**2/2 次**受控暂停后“超时却迟到完成、未被运行中 snapshot manager 登记”的复现。最后 node3 数据 shard 的正式导出目录有 **7 个**，超过该测试配置的 `max-exported-snapshot=4`；其中至少上述两个目录在更高 index 的正常导入后仍存在。`store.exportSnapshot` 在 `SyncRequestSnapshot` 返回错误时直接退出，只有成功响应才调用 `snapshotMgr.Add`；`snapshotMgr.Remove` 只遍历内存登记的项目。运行中没有对目录做重新扫描；`snapshotMgr.Init` 在副本启动时会扫描，因此**尚不能断言跨重启仍泄漏**。已经证实的是，进程持续运行且不断出现此类超时/迟到完成时，磁盘正式目录数可绕过配置配额并随事件累积；本测试的 snapshot 很小，未量化生产数据量下的磁盘增长，也未发现读写错误或 WAL 丢失。

两轮正常客户端分别完成 **300/300**、**200/200** 次 Append→Read，重试均为 0。最终水位 12150 后，第二轮保留的 42 条确认记录按 LSN/payload 核对通过，三地址读回 SHA256 均为 `d3aa32f0ee28946f1815bdbc0fbbf35ace81f077569f0c80242261ef43609042`，保留区间无重复 payload；数据 shard 三成员恢复正常。原始证据在 51 同一隔离目录：`pause-snapshot-round{1,2}.log`、`late-export-pause{,2}-20261104.load`、`late-export-pause2-20261104.node{1,2,3}.read`、node3 `stdout.log`，以及磁盘上两个有效正式目录。该问题不同于 #32144 的**带额外标记文件的无效正式目录**；这里两个目录内容有效，问题是请求超时导致运行中管理器没有登记它们。

## 追加实验：复用真实 TN WAL driver，未启动 TN/CN（2026-11-04 17:24–17:35）

**环境与入口。** 另建独立三投票副本集群 `/home/mo/jxm/logservice-driver-chaos-20261104/`，节点 RPC 分别为 `127.0.0.1:57401/57411/57421`，HAKeeper store 过期阈值采用默认 5 分钟。只启动三个 LogService 进程；没有启动 TN 或 CN。测试程序 `test/logservice-chaos/tn_driver_probe/main.go` 直接构造 `logservicedriver.NewLogServiceDriver`，使用实际 `logservice.NewClient` 配置、`driver.Replay`、`driver.Append`、`Entry.WaitDone` 和 `driver.Truncate`；每次写入一条带唯一 payload 的正常编码 WAL entry。每个新写入进程先执行 `ReplayForWrite`，独立核对进程执行 `ReplayForRead`。这覆盖 TN 的 WAL driver 到 LogService 的网络、重试和 DSN 回放路径，不模拟 TN 上层事务或 checkpoint。

| 时间线 | 实际 driver 结果 |
|---|---|
| 三节点健康基线，写入后关闭 driver，再起新 driver 回放 | 20 条确认写入、20 条顺序回放，DSN 1–20。 |
| 持续写入并由 driver 每 25 条提出合法截断，普通副本 node1 下线后恢复；恢复约 0.3 秒又杀数据 leader node0，约 12 秒后恢复 | 所有写入最终确认，两个阶段共推进到 DSN 462；一次 `WAL-Commit-Append` 内部重试 2 次，耗时 **6.280 秒**。driver 的异步截断推进至物理 PSN 454，三地址从 PSN 455 的直接读回 SHA256 相同。新 driver 回放保留的 DSN 392–462，共 71 条，与确认 payload 逐条一致。两个节点进程始终在运行，但刚恢复节点是否已追上不能仅由进程数保证。 |
| 持续写入时用 `ss -tnp` 确认 driver 进程实际连到数据 leader node2，再将 node2 `SIGSTOP` 70 秒，node0/node1 继续运行；90 秒自动 `SIGCONT` 作为安全兜底 | 暂停前确认到 DSN 525。下一次 `Append` 经默认单次约 **1 分钟**超时后，由 driver 重连并成功确认 DSN 526，单条 `WaitDone` **60.629 秒**；在 node2 仍暂停时确认进度已到 DSN 566。恢复 node2 后继续写至 DSN 617，driver 无最终错误。新进程回放 DSN 392–617 共 226 条，连续且与确认 payload 一致。 |

对 `driver-baseline.load`、`driver-fault-a-20261104.load`、`driver-fault-b-20261104.load`、`driver-pause-connected-20261104.load`、`driver-pause-connected2-20261104.load` 的确认清单去重核对，累计 **617 个 DSN**；最新截断水位之后的 **226 条**全部与独立回放一致，无 DSN 缺口或 payload 不一致。`driver-pause-connected-20261104` 是首次暂停脚本发现进程同时打开两个地址连接后安全退出的预备轮，**未注入故障**，其中已确认的写入仍计入清单。真实暂停轮在注入前按当前数据 leader 选择已连接的地址。`replayer.go` 的 `context canceled` 收尾日志发生在独立回放完成且关闭 driver 时，`replay_done` 正常结束，并非本轮回放失败。

**结论与限制。** 本轮在 driver 层验证了普通副本下线、刚恢复节点与 leader 下线交叠、合法截断及 snapshot 导出/导入、已连接 leader 停顿超过单次请求超时、关闭后重新回放。没有发现最终 WAL 写入或回放错误；**60.629 秒写入长尾仍然存在**，与已记录的 1 分钟默认尝试超时一致，说明不能把“有两票可服务”等同于客户端低延迟。`Truncate` 的正常路径未发出越过已确认 DSN 的未来水位；这个实验不能证明真实 TN 的 checkpoint 水位选择、事务可见性、HAKeeper TN 心跳或 SQL 行为。故障注入为进程 `SIGKILL`/`SIGSTOP`，不包含这套 driver 集群上的网络分区。

证据在 51 的上述目录：`driver-*.load`、`driver-*.replay`、`driver-*.progress`、`driver-fault-run-b.py`、`driver-pause-connected2.py`、`verify-tn-driver.py` 和三个 `node-*/stdout.log`。核对程序为 `test/logservice-chaos/tn_driver_probe/verify.py`。所有暂停进程已恢复，三个测试 LogService 节点在本轮结束时运行。

### 已连接副本暂停的独立复测（2026-11-04 17:43–17:47）

为确认上述 60 秒长尾不是单次偶发，在同一独立集群再次分别启动全新的 TN WAL driver 写入进程。每轮先写入至少 31 条，用 `ss -tnp` 找到**该 driver 进程实际持有的 LogService TCP 连接**，再将连接目标 node1 `SIGSTOP` 70 秒；另两台继续运行。脚本安排 90 秒后的自动 `SIGCONT`，并在 `finally` 中再次保证恢复。第二轮注入前查询数据 shard 三成员表，明确 node1 为**普通副本**，leader 为 node2。

| 轮次 | 暂停前确认进度 | 受阻的下一条写入 | 恢复目标节点前的进度 | 进程结束 |
|---|---:|---|---:|---:|
| A：已连接 node1 | DSN 680 | DSN 681，driver 内部重试 1 次，`WaitDone` **60.587 秒** | DSN 724 | DSN 775，正常退出 |
| B：已连接普通副本 node1，数据 leader 为 node2 | DSN 806 | DSN 807，driver 内部重试 1 次，`WaitDone` **60.648 秒** | DSN 848 | DSN 899，正常退出 |

两轮均在 node1 仍暂停时重新推进写入，说明另两台能够服务；长尾来自这个 driver 对**已连接但不响应的地址**等待完整单次超时，并非整个 Raft shard 连续 60 秒不能提交。最终全新 driver 回放连续 DSN **392–899** 共 508 条；其中本次两轮保留完整确认清单的 **DSN 650–899 共 250 条**逐条匹配 payload、无缺口。A 轮第一次脚本预检查发现所选 leader 地址没有该进程的连接后安全退出，未注入故障；它已确认的 DSN 618–649 后被同名前缀第二次运行覆盖了日志，故不纳入 250 条清单核对，也不将其当作故障轮。三台 LogService 进程最终均运行。

**判断。** 当前代码在 `pkg/logservice/client.go` 建连时传给 MORPC `WithBackendReadTimeout(0)`；`managedClient.Append` 要等请求返回错误才 `resetClient`，TN driver 的 `wrappedClient.Append` 则给每次尝试 `DefaultOneTryTimeout = 1m`。对不响应但 TCP 未断开的地址，等约一分钟才重试符合当前超时配置；但在另外两票已可继续写入的场景下，单条 TN WAL 写入仍被拖至一分钟，属于可用性长尾缺陷。调整时要保留“超时的 Append 可能已提交”的未知结果语义，不能简单盲目并发重提而忽略重复物理日志。该机制已有 [#32133](https://github.com/matrixorigin/matrixone/issues/32133) 跟踪，本次不建重复 issue。

原始证据在 51 的 `driver-tail-repro-{a,b}.{py,timeline}`、`driver-tail-repro-{a,b}-20261104.load`、`driver-tail-repro-final.replay`。两轮均使用真实 `tn-driver-probe`，没有启动 TN/CN。

### 超时重试、合法截断与旧 leader 恢复交叠（2026-11-04 17:53–17:55）

继续在同一三副本集群启动真实 TN WAL driver；每确认 10 条便调用一次 `driver.Truncate(已确认 DSN - 20)`。用 `ss -tnp` 和实时 shard 状态确认 driver 连到数据 leader node2 后，将 node2 `SIGSTOP` 80 秒；另外两台继续运行，110 秒自动 `SIGCONT` 兜底。暂停前最后确认 DSN 930，下一条 DSN 931 的 `WaitDone` **60.833 秒**，`WAL-Commit-Append` 记录内部重试 1 次。node2 仍暂停时，driver 已确认到 DSN 987；`Wal-Truncate-Execute` 在该窗口继续成功，合法截断意图越过 DSN 931。node2 恢复后继续写到 DSN 1038，driver 正常退出，远端物理截断水位为 PSN **1170**。

新 driver 回放保留 DSN **1010–1038** 共 29 条，与本轮 139 条确认写入清单中的对应 payload 逐条一致；三地址从 PSN 1171 读回 SHA256 均为 `8a63def3313bb50d71a67cb398e26a69125858c6e8be25eaa2739fdd6735fc41`，数据 shard 三成员视图一致。早先三次 1 分钟超时重试在截断前的回放日志中明确记录三对物理副本：DSN 526 对应 PSN 613/616，DSN 681 对应 785/788，DSN 807 对应 916/918；`Wal-Replay-Duplicate-Entry` 跳过后一个副本。这是**真实 driver 路径下的物理重复日志**，不是重复的逻辑 DSN 或 SQL 执行；本轮截断没有导致保留区间丢失或重放重复。

随后将三个测试节点全部 `SIGKILL`，按原磁盘目录冷启动。启动脚本首次立即重启 node0 时碰到旧 PID 尚未释放，`node.sh` 自身拒绝启动；核实旧进程已退出后重试成功，node1/node2 已启动。这是注入脚本的 PID 文件时序，不是 LogService panic。三个节点随后都恢复原三成员；Dragonboat 数据 shard 分别从本地 snapshot index **1124/1130/1056**（node0/1/2）恢复。独立 driver 仍回放 DSN 1010–1038 并与确认清单一致；再起新 driver 写入 **30/30** 条到 DSN 1068，三地址从保留物理区间读回 SHA256 均为 `6327a107023a9dc243b1aa9e9697540bc9bb959040a89c3a1c7da5bb6ed13f51`。这轮没有历史 HAKeeper 多 ID，因此不能拿它否定前文 #32151 的特定冷启动条件。

### driver 在异步 Truncate 意图后崩溃（2026-11-04 17:58）

新 driver 写入 10 条至 DSN 1078；它刚记录 `truncate_intent dsn=1058`，后台 worker 已计算 `psn-intent=1242`、尚未记录远端 `Wal-Truncate-Execute` 时，对**测试 driver 进程**发 `SIGKILL`，三台 LogService 不停。远端水位仍为 PSN **1170**，说明这个尚未完成的异步截断意图没有持久化；新 driver 回放 DSN **1010–1078** 共 69 条，包括刚确认的 10 条。再起 driver 写入 50 条并继续按合法 DSN 截断，远端水位升至 PSN **1326**；最终独立回放 DSN **1109–1128** 共 20 条，与本轮所有保留的确认 payload 一致。

`driver.Truncate` 在当前代码里先更新内存意图并入队，再由后台 worker 转为物理 PSN 调用远端；因此返回成功不表示远端水位已经推进。本次进程崩溃导致**多保留 WAL**，没有观察到已确认记录丢失。测试未启动 TN，未覆盖上层 checkpoint 重放如何重新提交该意图。证据在 51 的 `driver-late-duplicate.*`、`driver-cold-restart.*`、`driver-post-cold.*`、`driver-truncate-crash.*`、`driver-after-truncate-crash.*` 及节点日志。测试结束时三个 LogService 节点运行。

## 受控 driver 缺陷：迟到的同 DSN 物理副本跨越截断水位后回放失败（2026-11-05）

跟踪：[matrixorigin/matrixone#32220](https://github.com/matrixorigin/matrixone/issues/32220)，已指派给 `jiangxinmeng1`。

**入口与受控时序。** 仍不启动 TN/CN，测试程序直接使用真实 `logservicedriver.NewLogServiceDriver`、`ReplayForWrite/Read`、`Append`、`WaitDone` 和 `Truncate`。为了确定旧请求是在目标节点停顿后才发出，探针新增仅测试用的 `-pause-before-seq` 文件握手；为了确定唯一连接，设 `-max-client 1`，但始终保留三个 LogService 发现地址。在第 31 条已确认、下一条尚未发出时暂停**当前已连接的普通副本**，放行下一条 Append，观察 5 秒无确认进度后杀掉旧 driver 进程。目标副本仍停顿时，新 driver 完成回放并写入 30 条；恢复旧副本后，旧请求迟到，形成与新 driver 第一条写入**相同 DSN、不同 payload** 的第二条物理日志。接着用新 driver 写入 10 条，只提出一次合法 `Truncate(当前已确认 DSN - 20)`，使物理水位恰好处于两个副本之间；再启动独立 driver 回放。

| 独立隔离集群 | 先到的已确认新记录 / 迟到的旧记录 | 合法截断与结果 |
|---|---|---|
| 原 driver 集群 `/home/mo/jxm/logservice-driver-chaos-20261104/`，RPC 57401/57411/57421 | DSN **1679**：新 payload `driver-prepaused-e-new-20261105/000000` 在 PSN **1978**；旧进程未收到确认的 payload `driver-prepaused-e-old-20261105/000031` 在较晚的 PSN **2008**（随后回放写 skip 记录，原始读回显示旧 payload 于 PSN 2011）。截断前新 driver 顺序回放至 DSN 1708；日志记录跳过后一份 `Wal-Replay-Duplicate-Entry`。 | 新增 10 条并调用 `driver.Truncate(1698)`；实际水位 PSN **1997**，落在两个副本之间。全新 driver 回放先把旧 payload 当作 DSN 1679，接着报 **`internal error: dsn 1680 not found`**，进程退出码 2。 |
| 全新独立集群 `/home/mo/jxm/logservice-driver-repro-20261105/`，RPC 57601/57611/57621，独立磁盘、UUID、deployment-id 32105 | DSN **155**：新 payload `driver-prepaused-fresh4-new-20261105/000000` 在 PSN **168**，旧未确认 payload `driver-prepaused-fresh4-old-20261105/000031` 迟到于 PSN **198**。截断前新 driver 顺序回放至 DSN 184，记录跳过后一份。 | 新增 10 条并调用 `driver.Truncate(174)`；实际水位 PSN **187**。全新 driver 回放先输出旧 payload 为 DSN 155，接着报 **`internal error: dsn 156 not found`**，进程退出码 2。 |
| 第三套全新隔离集群 `/home/mo/jxm/logservice-driver-repro2-20261105/`，RPC 57801/57811/57821，独立磁盘、UUID、deployment-id 32106；使用仓库自动脚本 | DSN **63**：先到的已确认新记录在 PSN **70**，迟到副本在 PSN **100**；截断前回放至 DSN 92 且跳过迟到副本。 | 新增 10 条并调用 `driver.Truncate(82)`；实际水位 PSN **89**。新 driver 回放先输出旧 payload `stable1-attempt2-old/000031` 为 DSN 63，接着报 **`internal error: dsn 64 not found`**，进程退出码 2。 |

这是**三套独立集群各一次、3/3 次命中目标时序后同样故障**，第三轮由 `test/logservice-chaos/tn_driver_probe/reproduce_orphan_truncate.py` 自动执行并检查旧 payload 与 `dsn not found` 错误。自动脚本首次候选连接落在 leader，未注入；第二次连到普通副本并复现。全新集群还有一个阴性对照：旧请求发往暂停的**leader**时，leader 切换后没有形成迟到物理副本，driver 回放正常；它不计入成功复现次数。其他选择连接地址未命中的预检查轮也在注入前安全退出。

**代码判断。** `logservicedriver/replayer.go` 只在同一次回放仍能读到先到副本时，才用 DSN 范围去掉迟到副本。`info.go` 的 `getNextValidPSN` 与 `isToTruncate` 根据已登记的有效 PSN→DSN 映射选物理截断点；本轮它在旧副本前停下，却已经截掉有效的先到副本。新进程从 LogService 的物理水位之后开始回放，没有可用的已确认副本或足够的持久化逻辑水位来识别迟到副本，因而先应用旧 payload，随后在已截掉的后续 DSN 处报错。这不需要未来水位或无效输入；两个 `Truncate` 目标都落后于已确认的最大 DSN 20 条。直接影响是同一份真实 TN WAL driver 不能完成重启回放，而且截断后读取范围中不再有已确认的第一份 payload。本测试没有启动 TN/CN，尚未测 SQL 可用性或 Raft snapshot 导入后的物理文件清理；不据此声称 SQL 已重复执行。

原始证据：原集群的 `driver-prepaused-e-*`、`driver-intermediate-cut.{load,replay,rawread}`；第二集群的 `driver-prepaused-fresh4-*`、`driver-fresh-intermediate-cut.{load,replay,rawread}`；第三集群的 `stable1.*`、`stable1-attempt2-*`，以及各集群三个 `node-*/stdout.log`。三个故障现场均保留，且没有继续向故障 shard 写入。

**冷重启与写模式验证。** 第三套故障集群保留原磁盘，三个 LogService 进程全部 `SIGKILL` 后从原目录重新启动，三进程均存活；新 driver 的 `ReplayForRead` 仍先回放旧 payload 为 DSN 63，再报 `dsn 64 not found` 并退出码 2。随后只启动 `-mode cycle -count 1` 的测试探针，它先走真实 `ReplayForWrite`；这一模式同样在上述错误处退出码 2，**没有发出新 Append**。这证明故障状态跨整组冷重启保留，且阻断 driver 的正常写入初始化。第三套证据增加 `bug-cold-restart.timeline`、`bug-after-cold.replay`、`bug-write-mode.replay`。

**真实 TN 可达性边界（代码复查）。** 本实验使用了 TN 的 `logservicedriver` 实现，但没有启动 TN，也没有执行 TAE checkpoint。TN 的 `factory.go` 将 LogService client factory 交给 TAE WAL；`wal/store.go` 创建这个 driver，`db/replay.go` 会在正常写入前切到 `ReplayForWrite`。同一 TN store 原地重启时，`tnservice/store_metadata.go` 从本地恢复 shard 的 `ReplicaID`，`store.go` 按该记录启动，`factory.go` 用该 ID 配置新 LogService 客户端。LogService `rsm.go` 按 leaseholder ID 校验用户写入；**换成新 ID 的迁移**可能拒绝旧请求，**沿用同一 ID 的重启**没有额外进程代际栅栏。这个代码链只说明受控同 ID 场景有意义，**仍不能证明真实 TN 会在旧请求滞留期间完成新实例回放、写入和 checkpoint 截断**。正常 TN 的截断由已提交 checkpoint 推导 DSN 后在 `wal/checkpoint.go` 调用 `driver.Truncate`；探针直接调用 `driver.Truncate`，只验证了目标在已确认 DSN 内，没有模拟 checkpoint 的生成时序。探针还将 client pool 上限设为 1 以确定唯一连接，而 TN 默认上限是 100；没有用默认池配置复现完整交错。因此 #32220 的已证实范围是**受控 driver 时序下的回放失败**，生产 TN 生命周期可达性及 SQL 影响仍待验证，不能把它归咎于 LogService 违反协议。

## 追加验证：TN WAL 入口和默认连接池（2026-11-05 14:01–14:17）

为核实 #32220 的真实 TN 可达性，另建独立三投票副本集群 `/home/mo/jxm/logservice-tnwal-20261105/`，RPC 为 `127.0.0.1:58001/58011/58021`，三个新空数据目录。`test/logservice-chaos/tn_driver_probe/wal_probe/main.go` 通过 TN TAE 使用的 `wal.NewLogserviceHandle`、`StoreImpl.Replay(ReplayForWrite)`、`StoreImpl.AppendEntry(GroupUserTxn)` 和 `StoreImpl.RangeCheckpoint` 操作，底层 `logservice.NewClient` 使用三个地址和同一个 `TNReplicaID=100`。没有覆盖默认配置，启动日志确认 `ClientMaxCount:100`。仍未启动 TN/CN 或 TAE checkpoint 调度器，`RangeCheckpoint` 由测试程序按已确认 user LSN 主动触发。

**两个旧、新 WAL 进程同时存活：可产生同 ID 错序。** 旧进程写到 user LSN 386 后，暂停普通副本 node0，使下一笔 `AppendEntry` 等待。旧进程保持存活；新进程 `ReplayForWrite` 后确认 30 条新记录，到 LSN 416，并调用 `RangeCheckpoint(0,396)`。日志确认 checkpoint intent 396 已提交、`Wal-Truncate-Execute` 截到物理 PSN 2339。恢复 node0 后，旧进程等待约 41.689 秒的写入以 user LSN 387 成功，继续写入。新进程退出后再起一个 WAL 进程回放，LSN 387–396 的 payload 为旧进程的 `old-c/000031`–`000040`，而新进程先前在这些位置确认的是 `new-c/000000`–`000009`。本轮回放**没有**报 `dsn not found`；它证明实际 WAL 入口和默认连接池在两个同 ID 写进程并存时可发生 payload 错序，不能称为完整 TN 生命周期复现。证据：`old-c.log`、`new-c.log`、`replay-c.log`。

**旧 WAL 进程强杀后再起新进程：未复现迟到写入。** 每轮在旧进程已确认 31 条后暂停一个副本，释放下一笔写入，等 2 秒确认该写入无 ACK，再 `SIGKILL` 旧进程；新进程回放并确认 20–30 条，随后恢复副本、再起独立进程回放。默认 100 client 下，先后共 **13 次写入确实卡住的窗口**：`old-a`、`kill-race-{5,6,8,11,13,14,15,21,23,27,28,30}`。其中第二组脚本的 8 次（`13,14,15,21,23,27,28,30`）在每轮查询 leader 后只选择普通副本；`old-a` 也在暂停前确认是普通副本。所有 13 次恢复后的回放都没有发现旧进程未确认的第 32 条记录，新实例回放成功。其他连接未命中暂停副本的预选轮次不计入。原始日志保留在同一目录。

**结论与缺口。** 代码中 TN 正常关闭会先停止接收新 RPC，等待已接受 handler，再关闭 replica/WAL；`StoreImpl.Close` 等待 append 队列，driver `Close` 等待已接受的 committer。原地重启会从本地 metadata 沿用 ReplicaID，但这并不证明两个可写 TN 实例会同时存活。当前测试只在强行并存时复现错序，在强杀再恢复的默认连接池实验中未复现旧请求迟到。因此仍不能证实 #32220 所需的真实 TN 故障切换请求顺序，按提问者要求关闭该 issue；保留日志与测试程序作为后续发现真实同 ID 并存路径时的证据。

## 追加验证：单 TN WAL 实例与 LogService 节点故障（2026-11-05 14:24–14:36）

继续使用独立集群 `/home/mo/jxm/logservice-tnwal-lifecycle-20261105/`，三个旧盘副本 RPC 为 `127.0.0.1:58201/58211/58221`。探针仍通过 TN 实际的 `wal.NewLogserviceHandle`、`ReplayForWrite`、`AppendEntry(GroupUserTxn)`、`RangeCheckpoint` 调用默认 100 client 的 driver；每个进程先回放，写入按 user LSN 和唯一 payload 记录 ACK。每轮恢复后另起进程回放，把 checkpointed user LSN 之后每条已 ACK 记录的 LSN/payload 与回放逐条比较。每个交接点先停止旧 WAL 进程，**没有让两个同 ID 写实例并存**。

| 时间线 | 结果 |
|---|---|
| 单 WAL 实例持续写入、每 20 条 ACK 后 checkpoint 最近 10 条之前的范围；先杀 leader 15 秒并从旧盘恢复，再杀普通副本 15 秒并恢复；随后强杀 WAL 进程 | 旧进程 ACK 341 条，checkpoint intent 17 次。新实例 `ReplayForWrite` 成功，checkpointed=330，水位之后 11 条已确认记录逐条一致；另有 1 条在强杀后可见、此前未获 ACK，符合未知提交结果。最大已确认 Append 延迟 6.337 秒，driver 内部有 2 次重试。证据：`lifecycle-a.log`、`lifecycle-recover-a.log`。 |
| 单 WAL 实例在第二次 `RangeCheckpoint` 已返回、尚未 `WaitDone` 时强杀 | kill 时前 40 条用户写入已 ACK，第二次 checkpoint intent 指向 user LSN 372；新进程回放确认该 intent 已提交，checkpointed=372，水位之后 10 条已确认记录逐条一致。证据：`lifecycle-b.log`、`lifecycle-recover-b.log`、`lifecycle-b.ckp-ready`。 |
| 单 WAL 实例持续写入并 checkpoint，三个 LogService 进程短停 8 秒后均从原盘冷启动 | WAL 进程存活并继续运行，最终 ACK 250 条、完成 12 次 checkpoint intent。一次底层 `WAL-Commit-Append` 重试 6 次、耗时 13.173 秒；完成后又通过同一 WAL 实例继续写入。证据：`lifecycle-c.log`。 |
| 单 WAL 实例持续写入和 checkpoint，轮流杀 leader 6 秒、普通副本 4 秒、重新选出的 leader 6 秒、普通副本 4 秒，每次只停一台 | ACK 250 条、完成 12 次 checkpoint intent。第三个窗口 leader 下线期间 ACK 暂停，恢复后继续；最终独立回放 checkpointed=862，水位之后 20 条已确认记录逐条一致。最大已确认 Append 延迟 6.575 秒。证据：`lifecycle-d.log`、`lifecycle-d.faults`、`lifecycle-recover-d.log`。 |
| 旧 WAL 进程在第 32 次写入前暂停；先杀数据 leader，再释放旧写入并立即强杀旧 WAL 进程；保持另两个副本运行，启动新 WAL 进程完成回放、80 条写入和 4 次 checkpoint，最后恢复旧 leader | 新实例从 user LSN 913 回放并推进到 993；最终独立回放 checkpointed=983，水位之后 10 条已确认记录逐条一致。旧进程未确认的第 32 条未被观察为迟到写入。证据：`lifecycle-e-old.log`、`lifecycle-e-new.log`、`lifecycle-e-final.replay`。 |
| 单 WAL 实例用 `RangeCheckpoint(1,end,files...)` 的双记录形式，每 15 条写入 checkpoint；期间暂停 leader 25 秒，之后杀普通副本 10 秒，均恢复 | ACK 150 条，10 次 checkpoint intent 成功，最终回放 checkpointed=1138，水位之后 5 条已确认记录逐条一致。最大已确认 Append 延迟 3.454 秒。证据：`lifecycle-f.log`、`lifecycle-f.replay`。 |

最后一轮的 `files...` 是测试 marker 路径，目的是覆盖 TAE checkpoint 使用的 `GroupCKP` 加 `GroupFiles` WAL 编码顺序；没有生成真实 TAE checkpoint 文件，也没有执行 TAE 的文件加载、事务回放或 SQL 查询。因此本节证明的是实际 TN WAL driver 在这些故障时间线中的写入、截断和回放行为，不能代替完整 TN 端到端恢复。所有已确认且未被 checkpoint 覆盖的记录在独立回放中逐条一致；本轮没有发现新缺陷。

## 缺陷记录：两票可用时 TN WAL 回放获取截断位点等待 3 分钟并失败（2026-11-05 14:52–14:59）

**环境。** 继续使用上述三副本独立集群及已经写入的真实 WAL driver 记录，数据 shard 1 始终保持三投票成员。新启动的 WAL 进程仍使用 TN 的 `wal.NewLogserviceHandle`、默认 `ClientMaxCount=100`、实际 `logservice.NewClient` 三地址客户端，并执行 `StoreImpl.Replay(ReplayForWrite)`。测试探针新增“创建完 100 个客户端连接、调用 Replay 前暂停”屏障，以只在故障注入时改变时序；`-replay-timeout 6m` 只放宽测试程序的外层 context，**driver 内部仍是默认 `MaxTimeout=3m`**。独立复现脚本 `test/logservice-chaos/tn_driver_probe/reproduce_replay_tail.py` 会查当前 leader，只暂停一个普通副本，另两票始终运行；脚本保证最终 `SIGCONT`，且设置自动恢复保险。

**结果。** 两轮均先建立 WAL 客户端池，然后暂停一个它已连接的普通副本，放行回放。第一轮暂停 node0，第二轮暂停 node2；两次均在约 3 秒内确认回放未完成。期间从另外两个地址分别直接调用 `GetTruncatedLsn`，均立即成功返回 `3612`；第一轮的 `GetLatestLsn` 也成功。这证明不是 Raft 多数派或截断位点本身不可用。两轮 `Wal-Get-Truncate` 最终分别在 **3m0.000644s**、**3m0.000224s** 返回 `context deadline exceeded`，`retry-times=1`，整个 `ReplayForWrite` 退出码 2，没有读取或应用任何 WAL 记录。恢复普通副本后，健康对照回放 **229.536 毫秒**完成，水位与预期一致。原始证据：`replay-tail-1.log`、`replay-tail-9.log`、`replay-tail-control.log` 及三节点 `stdout.log`；两个失败轮次均在节点暂停时用存活地址独立验证了截断位点可读。

**代码路径与影响。** `logservicedriver/replayer.go:initReadWatermarks` 在回放初始化首先调用 `getTruncatedPSNFromBackend`；后者从默认 100 client 池取一个已有连接，对 `GetTruncatedLsn` 单次设置 `MaxTimeout=3m`。`pkg/logservice/client.go` 的数据客户端设置 `WithBackendReadTimeout(0)`，只有 RPC 报错后 `managedClient` 才关闭旧连接；所以无响应但 TCP 仍建立的副本会耗尽整段 3 分钟。driver 在失败后检查 `time.Since(start) > MaxTimeout`，此时不再试另一连接，直接把错误交给回放。TAE 的 `ReplayWal` 使用这条 WAL 路径，`ReplayCtl.StopForWrite` 会把回放错误交给晋升流程；因此该实验已证明 WAL 初始化失败，**尚未启动真实 TN 进程来量化服务层重启或 SQL 影响**。预期是当另外两票已经能提供截断位点和日志时，旧连接失效应触发有限时间内换地址继续回放，不能把单个失联副本的 3 分钟超时直接当作整组不可用。该路径与 #32133/#32148 已记录的 Append/Read 60 秒长尾有相同的连接失效机制，但首次回放的 3 分钟后直接失败是独立的启动后果。

跟踪缺陷：[matrixorigin/matrixone#32235](https://github.com/matrixorigin/matrixone/issues/32235)。

## 追加验证：两票可用时 TN WAL 回放首次 Read 的 1 分钟长尾（2026-11-05 15:12–15:19）

**对照。** 在创建 WAL driver 的 100 client 池**之前**暂停一台普通副本，另外两票运行；新实例仍通过三地址 `ReplayForWrite` 成功，driver 创建 739.555 毫秒，`GetTruncatedLsn` 119.833 毫秒，完整回放 121.560 毫秒。这说明故障前已有旧连接是长尾的关键条件，而不是只要三地址列表中含一台坏节点就会卡住。证据：`replay-cold-pool-follower-down.log`。

**故障交错。** 测试探针仅在首次 `GetTruncatedLsn` 成功后、回放首次 `Read` 前设置屏障；其余仍通过原始 `logservice.NewClient` 与 TN 默认 WAL driver 运行。每轮先让客户端池连三地址，查询当前 leader 后暂停一台普通副本，再放行 Read。未命中当前 Read 连接的预选轮次约 4 秒内完成；命中的两轮 `read-tail-6`、`read-tail2-10` 均暂停 node1，另两票继续运行。独立 `GetLatestLsn` 从存活地址立即成功，driver 的后台截断位点轮询也每 5 秒从其他连接成功返回。两轮 `Wal-Replay-Info.read-duration` 分别为 **1m0.603s**、**1m0.338s**，完整 `ReplayForWrite` 分别为 **61.102s**、**60.810s**；超时重连后回放成功，LSN/payload 未出现不一致。原始证据保留在 51 同一目录下的 `read-tail-6.log`、`read-tail2-10.log`；复现脚本为 `test/logservice-chaos/tn_driver_probe/reproduce_read_tail.py`。

此处没有新的错误类型；它把已有 [Read 长尾缺陷 #32148](https://github.com/matrixorigin/matrixone/issues/32148) 的证据扩展到 **TN WAL 启动回放**，不另建重复 issue。代码中 `readFromBackend` 给每次 `Read` 默认 1 分钟 deadline，只有 RPC 退出后 `managedClient.Read` 才重置旧连接；后台轮询可借别的 pooled client 正常运行，不能让被卡住的这次 Read 提前换地址。

**另外一处代码风险已在 51 上复现。** `truncateFromRemote` 调用 `clientPool.GetOnFly()` 后只特别处理 `ErrClientPoolClosed`；其他建连错误会返回 nil client，后续解引用导致 panic。完整的两轮 TN WAL driver 复现和无故障对照见文末“新缺陷：后台物理 Truncate 建连失败导致 TN WAL 进程 panic”。

## 追加验证：两票可用时 checkpoint 物理 Truncate 延迟 3 分钟（2026-11-05 15:23–15:26）

同一独立三副本集群和 TN WAL driver 默认 100 client。测试进程顺序确认 20 条 user WAL 后调用 `RangeCheckpoint(1,1153)`，checkpoint intent 已提交并返回 ACK；driver 后台计算出的物理截断目标为 PSN **6923**。测试屏障只在后台新建的 LogService 客户端已连接、首次 `Truncate` RPC 发送前暂停。`ss -tnp` 显示该进程共持有 101 条到三地址的数据连接，新建连接（最高 fd 106）指向普通副本 node2；暂停 node2 后放行 `Truncate`，另两票运行。两存活地址直接查询截断位点立即返回旧值 **3612**，最新 LSN 可读；测试进程的 `summary cycles=20` 已输出，但后台截断和正常 `Close` 一直未完成。

在 node2 **仍暂停**时，首次尝试的默认 3 分钟 context 到期；该 context 在测试屏障前已创建，因此屏障等待也占其中一小段。`Wal-Truncate-Execute` 最终记录 `retry-times=1`、总耗时 **3m0.529s**，第二次尝试换到存活地址后成功将物理截断位点从 **3612** 推进到 **6923**。`Wal-Truncate-By-PSN` 删除 15 个旧 PSN 映射，进程随后正常退出。原始证据：`truncate-tail-a.log` 和节点日志；故障副本已恢复。这再次说明两票能提供服务时，单条旧 TCP 连接仍可消耗整个 `MaxTimeout=3m`。该物理截断是后台工作，不能把 3 分钟直接换算为单笔事务提交延迟；可观察影响是 WAL 清理滞后及本轮正常 `Close` 等待。`driver.Close` 的 4 分钟 deadline 在调用 `truncateQueue.Stop()` 之前建立，`Stop()` 本身没有剩余时间约束；持续失败时理论等待可更久，但本轮只观察到 3 分钟。与 #32235 同属过长的旧连接失败检测，暂不另建同根因 issue。

## 追加验证：单普通副本 RPC 网络黑洞时的 TN WAL 回放（2026-11-05 15:35–15:42）

继续使用上述独立集群、TN 的 `wal.NewLogserviceHandle` 和默认 100 client。每轮先创建包含三地址的客户端池，在 `ReplayForWrite` 前的测试屏障停住；确认数据 shard 当前 leader 后，只对一台**普通副本**的测试 RPC 端口添加本机 `iptables OUTPUT DROP`，再放行回放。三台 LogService 进程始终运行，另外两台的端口没有阻断。注入规则有独立 comment、进程退出时删除，并有延迟自动删除保险；两轮结束后核对 OUTPUT 链均无测试规则。可重复脚本为 `test/logservice-chaos/tn_driver_probe/reproduce_replay_network.py`，只针对该独立集群的 `58201/58211/58221` 端口。

两次命中故障窗口时目标均为普通副本 node0。故障期间，用另外两地址直接查询 `GetTruncatedLsn` 返回 PSN **6923**，`GetLatestLsn` 返回约 **7803**，确认两票仍能提供数据服务。`replay-net-4.log` 中首次 `Wal-Get-Truncate` 在 **2m32.150s** 后成功，`retry-times=1`；`replay-net2-2.log` 中为 **2m32.116s**，同样重试一次后成功。日志记录指向被阻断端口的 `dial tcp4 ... i/o timeout`；本机规则计数确有丢弃流量。完整回放分别耗时 **152.154s**、**152.119s**，两次均正常退出，回放出的 10 条保留 user LSN/payload 与网络恢复后的健康对照逐条一致；健康对照完整回放 **280.795ms**。未命中已连接目标的预选轮次没有计入故障复现。

这与普通副本 `SIGSTOP` 的两轮 **180 秒后首次取水位失败**不同：网络 DROP 两轮在约 152 秒收到 TCP 错误，driver 才能重建连接并成功；进程暂停时直到 driver 的 3 分钟期限都未收到足以切换的错误。两种故障均说明**两票可用时，回放仍因旧连接等待远超 1 分钟**。本轮没有发现记录丢失或最终回放错误，归入现有 [#32235](https://github.com/matrixorigin/matrixone/issues/32235) 的连接故障检测问题，不另建 issue。原始证据保留在 51 的 `replay-net-4.log`、`replay-net2-2.log`、`replay-net-control.log` 和三节点日志。

## 新缺陷：并发 WAL 写入后的合法 checkpoint 截断使回放遇到 DSN 空洞（2026-11-05 15:51–16:11）

**真实入口与可达性。** `test/logservice-chaos/tn_driver_probe/concurrent_probe/main.go` 在**单个** `wal.NewLogserviceHandle` / 默认 100 client 上由 32 或 64 个协程并发调用 `StoreImpl.AppendEntry(GroupUserTxn)`、等待每笔 `Entry.WaitDone`。没有两个同 ID TN WAL 实例同时写入。完成所有写入并关闭旧实例后，独立新实例通过 `ReplayForWrite` 验证全部已确认的 LSN/payload。随后另一个实例按 TN checkpoint executor 使用的 `RangeCheckpoint(1, end, files...)` 提交 **不超过已确认 user LSN** 的 checkpoint，等待 intent ACK 及后台物理 `Truncate` 成功，再起独立实例回放。仍未启动 TN/CN、TAE checkpoint 文件生成或 SQL 查询；这里的 `files` 是测试 marker，但 WAL 调用顺序与 TN 增量 checkpoint 一致。

| 独立集群与时序 | 截断前 | 截断后 |
|---|---|---|
| 旧 WAL 数据较多的 `logservice-tnwal-lifecycle-20261105`：32 协程共 3200 条确认；期间普通副本 node0 下线 8 秒、恢复，再让当时 leader node1 下线 8 秒并恢复 | 加上本轮 160 条基线，**3360/3360** 条确认的 LSN 连续，独立回放逐条 payload 一致；leader 故障期间出现约 60.8–61.0 秒的 Append 重试长尾，已属既有连接故障问题。 | `RangeCheckpoint` 到 user LSN **4424**，物理截断到 PSN **10530** 成功；新实例从仍残留的 DSN **4484** / user LSN **4415** 开始回放，随即 `dsn 4485 not found`，退出码 2。 |
| 新空盘集群 `logservice-concurrent-fault2-20261105`：先前一轮故障控制器在普通副本恢复后暂时读到 `leader=0` 而提前结束，未计为完整故障轮；随后独立一轮 32 协程 1600/1600 条确认，普通副本、leader 各下线 8 秒并恢复 | 独立回放到 user LSN **2230** 成功。 | checkpoint 到 **2131**、物理截断 PSN **1540** 后，独立回放**成功**。说明固定故障时序本身不足以稳定命中；需要选择并发 PSN/DSN 排序产生空洞的截断边界。 |
| 在同一第二集群追加 64 协程 3200/3200 条确认，先用只读 `sequence_probe` 收集 PSN→DSN/LSN 映射；按映射选择合法 checkpoint 水位 **5375** | 全量记录在截断前可读；映射预报截断后保留 DSN **5341**，其后至少 DSN **5342** 不在保留区间。 | 远端物理截断 PSN **5141** 成功；新实例先应用 DSN **5341** / user LSN **5339**，再报 `dsn 5342 not found`，退出码 2。用只读映射检查，**56/56** 条未被 checkpoint 覆盖的 user LSN/payload 仍在物理日志中。 |
| 完全新建、**全程无节点故障**的空盘集群 `logservice-concurrent-clean2-20261105`：64 协程 3200/3200 条确认，按只读映射选合法 checkpoint 水位 **3140** | 独立全量回放成功；映射预报截断后保留 DSN **3105**，并在 DSN **3111** 出现空洞。 | 远端物理截断 PSN **2915** 成功；新实例回放 DSN 3105–3110 后报 `dsn 3111 not found`，退出码 2。只读物理扫描确认 **60/60** 条未被 checkpoint 覆盖的 user LSN/payload 仍在日志中。 |

**复现选点。** 对新空盘 user-only WAL shard 运行 `concurrent_probe -workers 64 -per-worker 50 -delay 0s`；用 `sequence_probe` 在截断前输出每条用户记录的 PSN、DSN、LSN，并运行 `choose_checkpoint.py <mapping>`。脚本只接受 DSN 与 user LSN 一一对应且 LSN 1..N 完整的新 shard，选择一个保留至少 50 条用户记录、且按现有截断算法会留下较小 DSN 但删除其后至少 3 个 DSN 的合法水位。随后再用现有 `wal_probe -count 1 -checkpoint-every 1 -checkpoint-lag <script 输出的 checkpoint_lag> -checkpoint-files` 发出 checkpoint，等 `Wal-Truncate-Execute` 成功，再由新 WAL 实例 `ReplayForWrite`。第三套无节点故障复现中水位 3140、lag 61 是在当轮映射上预先选出的值；换一轮物理排序，应重新选点，而非硬编码 3140。

**代码机制和影响边界。** `logservicedriver` 允许多个写 client 并发，使逻辑 DSN 顺序与 LogService 分配的物理 PSN 顺序不同。`truncate.go:doTruncate` 按 PSN 递增扫描，推进到“本物理记录最大 DSN ≤ checkpoint DSN”的连续物理前缀；该前缀内可能有较大的、已 checkpoint 的 DSN，而前缀后仍留有较小的、也已 checkpoint 的 DSN。`replayer.go` 的缺口处理只在**尚未应用任何 LSN**时跳过缺失的前缀 DSN；一旦先应用一个残留的旧 DSN，后面的合法截断空洞就被当成错误 `dsn not found`。因此已证实的是**TN WAL driver 不能完成重启回放/写模式初始化**，即使未被 checkpoint 覆盖的用户记录仍物理存在；本轮没有证明物理数据丢失或 SQL 层后果。与已关闭的 #27410“乱序跨读取 batch 边界”以及 #32220“两个同 ID 进程并存后的迟到写入”不同，本场景在单实例并发正常写入和合法 checkpoint 中可达，不要求迟到旧实例或跨读取 batch。

原始证据在 51 上述三个独立目录：`concurrent-fault-a.{log,timeline,replay}`、`concurrent-post-checkpoint.{log,replay}`、`map-stress.{log,mapping}`、`map-stress-checkpoint.log`、`map-stress-after.{mapping,replay}`、`clean2-stress.log`、`clean2-before.{mapping,replay}`、`clean2-checkpoint.log`、`clean2-after.{mapping,replay}`，以及三个目录的节点日志。未继续向失败 shard 写入。

**原盘冷重启验证。** 保留第三套无故障复现的三个数据目录，停止全部 LogService 进程后按原盘重启。首次重启脚本只等监听端口释放，node1 旧进程尚在退出、PID 仍存活，脚本自身拒绝启动 node1；这次部分启动未计入验证，并终止了等待多数派的探针。确认旧进程退出后启动 node1/node2，三地址均报告数据 shard 有 leader 且三成员可见。新 WAL 实例 `ReplayForWrite` 仍在同一 `dsn 3111 not found` 处退出码 2；失败状态跨整组三副本原盘冷重启保留。证据：`clean2-after-cold-retry.replay` 与节点日志。该过程没有新 WAL 写入。

## 追加验证：单 WAL 实例持续写入期间并行 checkpoint 与节点故障（2026-11-05 16:33–16:42）

**入口与安全水位。** 扩展 `concurrent_probe`：仍只有一个 `wal.NewLogserviceHandle`、默认 100 client；32/64 个协程持续调用 `AppendEntry(GroupUserTxn)` 和 `WaitDone`。另一个协程每累计约 300/400 条 ACK 调用一次 `RangeCheckpoint(1,end,files...)`，并等待 checkpoint entry 完成。它维护**从起始水位连续已确认**的 user LSN 前缀，始终让 `end` 至少落后该前缀 100 条；不把已分配但未确认的 LSN 用作 checkpoint 水位。每轮结束时再补一个合法 checkpoint，并等后台物理 Truncate 完成。TN 的事务 WAL 写入口在 `txnimpl/cmdmgr.go`，checkpoint executor 的 WAL 入口在 `db/checkpoint/executor.go`；本探针仍不包含实际 TAE checkpoint 文件生成、事务应用或 SQL。

| 时间线 | 写入与 checkpoint | 新实例回放 / 物理核对 |
|---|---|---|
| 三副本健康对照；64 协程 × 60 条 | **3840/3840** ACK；10 次 checkpoint intent，远端物理截断成功；最终 user LSN 3840、checkpointed 3740。 | `ReplayForWrite` 成功，保留区间无错误。证据 `liveckp-control.{log,replay}`。 |
| 32 协程 × 80 条；普通副本停 8 秒并恢复，稍后当前 leader 停 8 秒并恢复，任一时刻至少两进程运行 | **2560/2560** ACK；7 次 checkpoint intent 与物理截断成功；最终 user LSN 6400、checkpointed 6300。leader 故障触发原有约 1 分钟 Append 重试长尾。 | 回放成功；checkpoint 之后 **100/100** 条确认记录的 LSN/payload 与新实例回放一致。证据 `liveckp-fault-a.{log,timeline,replay}`。 |
| 32 协程 × 80 条；将当时 leader 的**测试 RPC 端口**用 `iptables OUTPUT DROP` 黑洞 12 秒，其他两地址保持可读，规则随后删除 | 黑洞期间 ACK 226→230，存活地址直接 `GetTruncatedLsn=3212`；最终 **2560/2560** ACK、7 次 checkpoint intent 和物理截断成功。最慢一组 Append 约 **13.3 秒**后成功，本轮未耗尽 1 分钟期限。 | 回放成功；checkpoint 之后 **100/100** 条确认记录逐条一致。规则已清理。证据 `liveckp-leader-rpc-blackhole.{log,timeline,replay}`。这里仅阻断客户端到 leader 的 RPC，**没有**隔离 Raft 复制网络。 |
| 32 协程 × 60 条；在 ACK 276 时同时杀当时的 leader node2 与普通副本 node0，6 秒后原盘恢复，随后继续写入与 checkpoint | 两票短暂失去时 ACK 停在 276，恢复后最终 **1920/1920** ACK、7 次 checkpoint intent 和物理截断成功；有一次 Append 在多数派恢复后仍等 **60.519 秒**才重试成功。最终 user LSN 10880、checkpointed 10780。 | 新实例先读到残留的 checkpoint 范围内 user LSN **10773、10774**，随后 `dsn 10833 not found`，退出码 2。物理扫描核对 checkpoint 后 **100/100** 条已确认记录仍在且 payload 一致；同时残留 4 条已 checkpoint 的旧记录。这与 [#32240](https://github.com/matrixorigin/matrixone/issues/32240) 的“旧 DSN 空洞阻断后续回放”同一签名，暂不另建 issue。证据 `liveckp-two-down.{log,timeline,replay,mapping}`。 |

这轮说明持续写入与并行 checkpoint 在健康、单节点下线及单端口网络黑洞场景都可完成并恢复；短时两副本下线后再次遇到 #32240 的边界依赖回放失败。它没有证明两节点同时失去时仍可提交：该 6 秒窗口本来就没有多数派。所有 checkpoint 水位均来自连续 ACK 前缀，失败轮也没有发现未被 checkpoint 覆盖的确认记录在物理日志中丢失。远端目录 `/home/mo/jxm/logservice-liveckp-20261105/` 保留完整原始证据。

从各轮 `ack elapsed_ms` 独立统计，最大单条确认延迟依次为健康 **1.823 秒**、单节点下线 **61.091 秒**、leader RPC 黑洞 **13.308 秒**、两节点短停 **60.519 秒**；各轮最大 checkpoint intent `WaitDone` 为 **1.784、0.845、0.316、1.043 秒**。这些是本次注入与负载下的观测值，不是超时上界。

## 新缺陷：短时失去多数派时在途 Append 的服务端提议等待整整 1 分钟（2026-11-05 17:16–17:26）

**与既有旧连接长尾的区别。** #32133 的连接网络隔离让客户端已连接副本不响应；本轮在一个**仍存活的 LogService 节点**上观察到 `store.append` 的 `SyncPropose` 最终返回 Dragonboat `timeout`。测试仍是一个 TN WAL handle、默认 100 client、三地址；单次 Append 的 driver 默认期限由 `DefaultOneTryTimeout = 1m` 给出。另两节点只是同时停 **6 秒**，原盘恢复后大量其他 Append 已经确认，但在故障边缘发出的某些旧提议直到其原请求期限耗尽才结束。LogService `store.propose` 对 `ErrShardNotReady` / `ErrSystemBusy` 会重试，对最终 `ErrTimeout` 直接返回；`managedClient.Append` 仅在 RPC 出错后重置连接，`wrappedClient.Append` 也只有拿到错误后才重试。

**发起时机对照。** 新空盘三副本集群 `/home/mo/jxm/logservice-tail-quorum-20261105/`。测试探针加“已创建默认 100 client、回放完毕，尚未发起第一笔 Append”文件屏障。先杀当前 leader 与一普通副本、再释放 32 条并发 Append、6 秒后恢复，两轮分别保留普通副本或 leader 为唯一存活节点。两轮 **32/32** 条均确认，完成约 **9.07 / 8.88 秒**，没有 60 秒长尾。其中一轮首个 backend 调用先收到 connection refused，另一轮首个 backend 调用在约 5 秒收到 context deadline；都能在恢复后较快重试或完成。这说明“短时两节点下线”本身不必然产生 60 秒长尾，**故障边缘的在途请求**是关键窗口。证据：`quorum-tail-{a,b}.{log,timeline}`。

**默认期限的在途窗口。** `reproduce_quorum_tail.py` 在同一个 WAL handle 上用 32 协程 × 60 条持续写入，并行按连续 ACK 前缀每约 300 条做合法 checkpoint。确认约 250 条时同时杀 leader 与一普通副本，6 秒后原盘恢复；只在这个短窗口内失去多数派。两轮 `quorum-tail-{c,d}` 均 **1920/1920** 条 ACK、7 次 checkpoint intent 与物理截断成功，独立 `ReplayForWrite` 成功，最终保留的 **100/100** 条确认记录逐条匹配。

| 默认轮次 | 多数派恢复及其他写入 | 在途调用及最终 ACK |
|---|---|---|
| C | 17:17:12 下线两台、17:17:18 恢复；17:17:19 已有其他 Append 成功 | sole survivor node2 于 17:18:12 记录 `propose failed: timeout`；测试包装器记录该底层 Append **59.889 秒**后返回 `timeout`，driver 重试 1 次，单条最大确认 **60.325 秒**。 |
| D | 17:19:34 下线两台、17:19:40 恢复；其他请求继续成功 | 3 条底层 Append 分别约 **59.803 秒**后返回 `timeout`，driver 重试后单条最大确认 **61.071 秒**。 |

**快速重试反事实。** 在独立的临时构建 worktree 中，**仅把测试版** `DefaultOneTryTimeout` 从 1 分钟改为 2 秒，复制同一探针并构建另一二进制；没有改动或推送生产代码。对相同的“持续写入、两台短停 6 秒”控制流程，`quorum-tail-fast-{a,b}` 均 **1920/1920** 条 ACK，最大单条确认分别 **11.621、9.608 秒**，新进程回放成功，最后保留的 **100/100** 条记录逐条一致。测试版两轮分别出现 **65、44** 次底层约 2 秒的 `timeout`，由 driver 重试；无故障快速版 `quorum-tail-fast-healthy` **1920/1920** 条 ACK、0 次底层 timeout、最大确认 **1.509 秒**。2 秒阈值仅用于证明更短期限可以明显缩短该时间线，不表示它适合所有生产负载；过短期限会增加未知提交结果和重复物理 Append 的机会，必须评估正常尾延迟、远程链路、批量大小以及幂等回放语义。

**判断。** Raft 在只剩一票的 6 秒内不能提交是预期行为；多数派恢复后，个别旧提议仍等满客户端单次 1 分钟期限而阻塞 WAL 写入，不是 Raft 多数派协议必然要求。当前 `wrappedClient.Append` 的单次固定 1 分钟期限既不可在 TN `NewLogserviceHandle` 配置，也不会因为其他写入已经恢复而提前重试；总 `MaxTimeout=3m` 只在每次尝试结束后检查，并非严格总上限。需要一个能及时处理服务端失去 leader/提议悬挂的有限重试策略，同时把先前 Append 的结果视为**未知**，保留同 DSN 去重/回放正确性。这里的直接证据是 TN WAL driver 的长尾与服务端提议 timeout；没有启动真实 TN/CN，也未测 SQL 事务延迟。此路径不同于 #32133 的已连接失联节点，也不同于 #32148 的 Read 长尾。

原始证据保留在 51 上 `/home/mo/jxm/logservice-tail-quorum-20261105/`：`quorum-tail-{a,b,c,d,fast-a,fast-b,fast-healthy}.{log,timeline,replay}`（不存在的组合仅以实际文件为准）、三个 `node-*/stdout.log`、`quorum-tail-fast-current.mapping`。快速版的唯一生产代码差异保留在临时 worktree `/tmp/mo-logservice-fast-retry-20261105/`，完成后将移除该临时目录，测试二进制和日志保留在 51。

独立缺陷跟踪：[matrixorigin/matrixone#32246](https://github.com/matrixorigin/matrixone/issues/32246)，已指派 `jiangxinmeng1`。该 issue 只跟踪存活节点上在途 `SyncPropose` 的 1 分钟尾延迟；不把 2 秒测试值当作生产修复建议。

## 新缺陷：后台物理 Truncate 建连失败导致 TN WAL 进程 panic（2026-11-05 17:34–17:47）

**环境和入口。** 51 上另建空盘三投票副本集群 `/home/mo/jxm/logservice-newconn-failure-20261105/`，数据 shard 1，RPC 为 `127.0.0.1:58201/58211/58221`。测试程序 `test/logservice-chaos/tn_driver_probe/concurrent_probe/main.go` 用 TN 的 `wal.NewLogserviceHandle`、`ReplayForWrite`、`AppendEntry(GroupUserTxn)`、`RangeCheckpoint(1,end,files...)`，默认 `ClientMaxCount=100`、默认建连重试参数。checkpoint 水位只来自连续已确认的 user LSN，始终落后已确认前缀 40–50 条。没有启动真实 TN/CN 或 TAE checkpoint 文件生成；但经过的是实际 TN WAL driver 的后台 `doTruncate` 路径，而非直接调用内部函数。

**精确故障注入。** 每轮等 WAL 探针完成回放、预建客户端池并写出 `*.ready`；首轮用 `ss` 确认到三个 LogService RPC 地址已有 ESTABLISHED 连接。随后在 51 的 `OUTPUT` 链插入仅匹配目的 `127.0.0.1`、目的端口 `58201,58211,58221`、TCP SYN 首包的 DROP 规则，并释放探针开始写入。现有连接与三个 LogService 进程及其 Raft 通信仍工作；只有新建到测试 RPC 端口的 TCP 连接失败。每轮有 10 分钟自动撤销规则的保险脚本，结束时手动删除并检查规则为空。此注入模拟“现有连接可用，新连接无法建立”的网络窗口；并未模拟整台 LogService 节点下线。

| 轮次 | 现有连接上的已确认操作 | 后台建连及结果 |
|---|---|---|
| A | 新空盘；32 协程各写 10 条，**320/320** ACK；checkpoint intent 到 user LSN 62、181、270，均 `WaitDone` 成功 | 首次 `Wal-Truncate-Prepare` 计划截到物理 PSN 211；`GetOnFly` 的新客户端建连持续得到 `rpc timeout`。约 **5m0.633s** 后重试窗口结束，进程以 nil pointer panic；堆栈指向 `truncateFromRemote:178`，并在 deferred `(*wrappedClient).Putback(0x0)` 再次 panic。物理截断位点仍为 0。 |
| B | 撤规则后新 WAL 实例先成功回放 A 的 320 条记录；16 协程各写 10 条，**160/160** ACK；checkpoint intent 到 user LSN 363、440，均成功 | 相同 SYN 阻断；`Wal-Truncate-Prepare` 计划截到物理 PSN 581。约 **5m0.632s** 后同一 `rpc timeout`、同一 `truncateFromRemote:178` 与 nil `Putback` 堆栈再次崩溃；物理截断仍为 0。 |

**无故障对照及恢复。** 删除阻断规则后，同一三副本及旧磁盘上的新 WAL 实例 `ReplayForWrite` 成功，从 user LSN 480 继续；再确认 **160/160** 条写入及两次 checkpoint。后台 `Wal-Truncate-Execute` 分别约 1.091 秒、0.307 秒成功，LogService 物理截断位点推进到 PSN 954。再起新实例回放成功，报告 `ready lsn=640 checkpointed=600`；本次没有观察到 WAL 数据损坏。服务端三个副本在两轮注入期间均保持正常状态。原始证据为该目录的 `probe.log`、`probe-b.log`、`control.log`、`replay-check.log` 与 `node-*/stdout.log`。

**代码原因与影响。** `clientPool.GetOnFly()` 建连失败明确返回 `(nil, err)`；`truncateFromRemote` 仅在 `err == ErrClientPoolClosed` 时返回，对普通网络超时继续 `defer client.Putback()` 并访问 `client.wrapped.Truncate`。因此后台截断 goroutine 的 panic 会终止整个 TN 进程，尽管当时 WAL 写入和 checkpoint intent 已成功，三个 LogService 副本也可用。默认 5 分钟重试使错误潜伏。预期是在任何新客户端创建失败时将错误返回给 `doTruncate`，记录失败并允许以后重试物理截断，不能 panic。此测试证明了 TN WAL driver 进程的崩溃路径；没有声称已在真实 TN 服务进程中复现 SQL 层故障。

独立缺陷跟踪：[matrixorigin/matrixone#32248](https://github.com/matrixorigin/matrixone/issues/32248)，已指派 `jiangxinmeng1`。

## 最新 upstream/main 回归验证：已连接副本 Read 与 TN WAL 回放（2026-11-09 10:08–10:13）

51 上的测试版 MO 更新为 upstream/main `b19ef50f05ab0e398b65b4cf3f745470f803df09`，安装在 `/home/mo/jxm/mo-upstream-main-b19ef50f05-20261109/`，`/home/mo/jxm/mo-current` 指向该目录。`mo-service -version`、本地与远端 SHA256、动态库解析均已核对。该提交包含 LogService `Read` 与 `GetTruncatedLsn` 的七秒子期限及重连重试。测试客户端和 TN WAL 探针也在这个提交的模块环境中重新编译；没有沿用旧版本的客户端代码。

**已有连接的 Read 故障切换。** 新空盘三副本目录 `/home/mo/jxm/logservice-main-b19ef-read-failover-20261109/`，RPC `127.0.0.1:58201/58211/58221`。单个客户端先 Append 成功得到 LSN 6，然后在 Read 之前暂停；`ss` 确认其 TCP 连接指向普通副本 `58211`。仅对这个 RPC 端口加 `OUTPUT DROP` 并放行 Read，另两副本仍能服务。客户端内部在约 **8.002 秒**完成 Read 并核对刚确认的 payload；应用层 `read_attempts=1`、`read_retries=0`，没有 EOF 或最终错误。故障期间阻断规则命中 64 包；规则撤销后三地址均报告同一 leader 和三成员。证据：`read-a.log`、`read-a.ready`、`read-a.release`、节点日志。

**TN WAL 启动回放的截断水位读取。** 另建只由 TN WAL driver 写入的空盘三副本目录 `/home/mo/jxm/logservice-main-b19ef-wal-replay-20261109/`；使用真实 `wal.NewLogserviceHandle`、默认 100-client 池、`ReplayForWrite`，先确认 10/10 条 user WAL，水位 10。新实例完成连接池初始化但尚未回放时暂停，然后对三个测试 RPC 端口同时加 `OUTPUT DROP` **8 秒**并放行回放；Raft 复制端口未隔离。规则共命中 1190 包。`Wal-Get-Truncate` 记录 **1 次内部重试、8.911 秒**，整次回放 **8.914 秒**成功；LSN 1–10 的 10 条 payload 全部读回，结束后三地址仍报告完整三成员。证据：`seed.log`、`replay-b.log`、`replay-b.ready`、`replay-b.release`、节点日志。仅隔离一个 RPC 地址的预选轮未命中回放实际使用的连接，1.370 毫秒完成，不计为故障回放验证。

两次有效注入都没有出现旧版约 60 秒的读等待；这是本机这组时间线的观测值，不是所有网络故障的延迟上界。第一次尝试 WAL 回放时误把直接 LogService 客户端写入的原始 `UserRecord` 与 driver 专用 WAL 编码放在同一 shard，得到 `unsupported version`；该输入不符合 TN WAL 使用方式，已换全新空盘独立重测，不作为缺陷。所有测试规则均已删除。

**持续 WAL + 轮流节点故障 + snapshot + 整组冷重启。** 在上述仅含 WAL driver 编码的集群，单个真实 TN WAL handle 继续顺序确认 **200/200** 条 user WAL，每 20 条用 `RangeCheckpoint(1,end,files...)` 确认一次合法 checkpoint intent，始终落后最新 ACK 10 条。确认到本轮 seq 52 时杀普通副本 node1，8 秒后原盘恢复；三地址确认它重新加入完整成员表后，seq 135 时杀当时的 leader node0，8 秒后原盘恢复，任何时刻至少两台 LogService 进程存活。leader 故障期间 ACK 短暂停顿，恢复后继续。最终 10 次 checkpoint intent 均成功，后台物理 Truncate 推进到 PSN 1059；三个节点日志都有 `export snapshot success`。独立 WAL 实例 `ReplayForWrite` 报告 user LSN 210、checkpointed 200，LSN **201–210** 的 10 条保留记录与 ACK payload 逐条一致。再把三台 LogService 同时 `SIGKILL` 并从各自旧盘启动：三地址重新看到相同三成员与 leader；独立回放仍成功，LSN 201–210 保持一致。证据：`lifecycle.log`、`lifecycle.progress`、`lifecycle-replay.log`、`after-cold-replay.log`、三个节点的 `stdout.log`。这验证了本轮 snapshot 导出后原盘冷启动和 driver 回放，没有模拟真实 TAE checkpoint 文件读取或 SQL 事务恢复。

## 最新 upstream/main 回归验证：正式 snapshot 目录崩溃后重启（2026-11-09 10:22–10:25）

测试二进制仍为 `b19ef50f05`。另建空盘三投票副本集群 `/home/mo/jxm/logservice-main-b19ef-snapshot-crash-20261109/`，数据 shard 1，配置 `truncate-interval=1s`、`max-exported-snapshot=4`。直接 LogService 客户端先 **400/400** 条 Append 成功，末条 LSN 405。node0 是普通副本；在它的正式 snapshot 目录 `IN_MOVED_TO` 事件上用 `snapshot-crash-watch.py` 立即 `SIGKILL`，始终保留 leader node1 和普通副本 node2 两票。随后再用追加写入和更高截断位点重复一次同样的导出窗口。

| 轮次 | 杀进程时磁盘证据 | 原盘启动后的结果 |
|---|---|---|
| 1 | `Truncate(200)` 后，node0 正式目录 index **407** 刚从 `.generating` 重命名；目录同时有 `.gbsnap`、`snapshot.metadata`、`dragonboat.snapshot.message` 三文件 | 原盘重启后 index 407 残留目录被清理，node0 重新加入三成员，之后正常导出 index 408 等快照。停机期间另两票从 LSN 201 读到 405，共 205 条。`Truncate(350)` 成功，三地址各读出 LSN 351–405 的 55 条相同记录。 |
| 2 | 再确认 **120/120** 条 Append，末条 LSN 535；`Truncate(450)` 后，同样在正式目录 index **537** 的重命名事件杀 node0，重启前确认同样的三文件布局 | 原盘重启后 index 537 目录再度出现，但两文件时间戳晚于 kill，且 node0 日志明确记录重启后的 `export snapshot success index=537`；这是同 index 的**重新导出**，旧标记文件已清除。三地址恢复完整成员；从 LSN 451 各读到 85 条，内容 SHA256 均为 `94d0b673359c6cfe8953af4d8765bc425014fc399e174642521c4f5caaab0194`。 |

本轮没有观察到旧版 #32144 的“带标记文件的正式目录使同 index 导出持续拒绝”；node0 日志中 `request export snapshot failed` / `request rejected` 计数为 0。第二轮结束时 node0 正式导出目录数为 2，未超出配置配额。证据留在上述远端目录：`snapshot-watch{,-b}.log`、`seed.append`、`followup.append`、`survivor.read`、各节点 `after-truncate.read` / `after-second.read`、三个 `stdout.log` 和磁盘上的快照目录。本轮证明重启清理与重新导出、截断和读回，未模拟网络分区或 TN WAL 编码。

## 最新 upstream/main 回归验证：迟到导出、崩溃恢复与三地址连接（2026-11-09 11:26–11:39）

继续使用 `b19ef50f05` 版本，在全新三副本目录 `/home/mo/jxm/logservice-main-b19ef-late-export-20261109/` 测试。配置 `max-exported-snapshot=1`、`truncate-interval=1s`、HAKeeper 默认 5 分钟过期。数据 shard 的三个 RPC 地址为 `127.0.0.1:58201/58211/58221`；node0 为普通副本。直接客户端先确认 400 条写入，再通过 `SIGSTOP` 在 node0 的 `.generating` 目录出现时暂停进程 5 秒。两轮均观察到 `SyncRequestSnapshot` 返回 timeout，暂停结束后导出仍在磁盘形成完整的两文件正式目录；后续追加写入及截断分别使 index 407、657 的迟到快照被导入，旧目录被清理。第一轮截断后，三个地址各从 LSN 451 读到 78 条完全一致的记录；第二轮完成后正式导出目录数为 1，未超过配额。证据为 `pause-a.log`、`pause-b.log`、`seed.append`、`after.append`、`second.append`、`third.append` 和各节点 `after-late.read`。

再做一次组合故障：在 index 818 的 `.generating` 目录出现时暂停 node0 5 秒，恢复后监视正式目录的重命名事件，并立即 `SIGKILL` node0；另外两票全程存活。杀进程时磁盘保留正式 `snapshot-0000000000000332` 目录。node0 从原盘重启后，三个地址都报告同一 leader 和完整三成员；随后 100/100 次 Append 成功，末条 LSN 919。执行 `Truncate(860)` 后，node0 日志确认导入 index 818；三个地址各从 LSN 861 读出 59 条记录，内容 SHA256 均为 `14e6bffb2a527ebc5ea46968a0470b9ff2b4e0e58d11ae5cf480ac40e3f7230b`。证据为 `pause-crash.log`、`pause-crash.truncate`、`crash-late.append`、各节点 `crash-late.read` 和 `stdout.log`。

连接回退另测：仅对 node0 的 RPC 端口加入 `OUTPUT DROP`，保留两个可用副本，以同一份三地址配置独立发起 12 次 `GetLatestLsn`。12/12 次成功，进程总耗时约 0.1–1.0 秒；这组试验没有复现启动连接长尾。规则在试验结束时已删除，证据为 `connect-blackhole.log`。以上场景没有发现新的数据错误或可稳定复现的新缺陷；它们不能证明所有网络故障或导出时序都满足同样的耗时界限。

## 最新 upstream/main 回归验证：恢复未稳定时重复下线与整组冷重启（2026-11-09 12:05–12:09）

继续用上述独立集群和直接 LogService 三地址客户端，持续执行 Append→Read payload 核对；另一个控制器只在已确认 LSN 后方至少 100 条的位置推进 `Truncate`。先让普通副本 node0 四次下线、从原盘启动，其中前三次启动约 150 毫秒后再次杀进程，没有等待完整成员确认。任何时刻 node1、node2 两票均存活。持续客户端确认 172 条新记录，内部 Append 重试 1 次、Read 重试 0 次，最大单次写读周期 839.399 毫秒；四次截断成功，最终水位 970。三地址从 LSN 971 读取各 130 条记录，内容 SHA256 相同：`315acd2f3484573e85e2558f13f5f86db0c06aed3b937b3bb237df5d3ebebbbe`。

随后对当时的 leader node2 做四次同类下线，另两票 node0、node1 一直存活；每次在停机期间发起安全水位截断，前三次原盘启动后约 150 毫秒再次杀进程。持续客户端再次确认 172 条新记录，内部 Append 重试 2 次、Read 重试 0 次，最大单次写读周期 6.250366 秒；四次截断成功，最终水位 1162。三地址从 LSN 1163 读取各 133 条记录，SHA256 均为 `0bd5a983c6098cda19c818a6301bd51c2af444d9ce2efd992f71787180d1332e`。最后同时 `SIGKILL` 三节点并全部从原盘启动，三个地址恢复相同三成员；再次读取同一范围仍为 133 条且 SHA256 不变。原始证据位于远端目录的 `rapid-restart.log`、`rapid.load`、`rapid-leader.log`、`rapid-leader.load` 和各节点的 `rapid-verify.read`、`rapid-leader-verify.read`、`rapid-cold-verify.read`。这组短时故障没有发现新缺陷；没有证明被反复重启的副本每次都曾进入追赶状态，也没有覆盖长时间 HAKeeper 成员替换。

## 最新 upstream/main 回归验证：默认过期阈值下 HAKeeper leader 长时间下线（2026-11-09 14:19–14:26）

同一独立三副本集群的 `log-store-timeout` 仍为默认 **5 分钟**。先确认 HAKeeper shard 0 的 leader 是 node1、数据 shard 1 的 leader 是 node0，然后在持续三地址 Append→Read 客户端运行期间 `SIGKILL` node1，保持 node0、node2 两票存活 **375 秒**，越过默认过期阈值。控制器每约 10 秒把 `Truncate` 推进到已核对 LSN 后方至少 100 条，并监视两台存活进程。下线期间持续客户端最终完成 **2452/2452** 次写读核对，Append 内部重试 1 次、Read 重试 0 次，最长单轮 1.082134 秒；37/37 次截断成功，最终截断 LSN 3736。node0、node2 日志分别只有 1、2 次 LogService 心跳失败，没有 `Expired logStore info` 或 `L/shutdown`。

原盘恢复 node1 后，三个地址对 shard 0 和 shard 1 均报告相同的原始三成员；node1 日志记录数据 shard snapshot 导入成功。三个地址从 LSN 3737 各读到 102 条记录，SHA256 均为 `28b06e2ae4476cd7864b50718a5723bbd80f9b7d9f496bf0634b6e9caa6a7afb`。证据为远端 `hakeeper-long.log`、`hakeeper-long.load`、`hakeeper-long.progress`、各节点 `hakeeper-long-verify.read` 及 `stdout.log`。本轮未复现 30 秒加速配置的 #32134 误关停；进程下线场景也不等同于双向网络隔离或先后两次隔离。

## 最新 upstream/main 回归验证：64 MiB 读取上限附近的大 TN WAL 记录（2026-11-09 14:44–14:49）

使用只含 TN WAL driver 编码的独立三副本目录 `/home/mo/jxm/logservice-main-b19ef-wal-replay-20261109/`，仍为 `b19ef50f05` 版本；探针源码为 `test/logservice-chaos/tn_driver_probe/large_wal_probe/main.go`，实际运行二进制在该版本源码环境构建。探针通过 TN 使用的 `wal.NewLogserviceHandle`、`ReplayForWrite`、`AppendEntry(GroupUserTxn)`、`RangeCheckpoint` 入口，不打印大 payload，而在写入确认和独立回放时分别输出长度及 SHA256。driver 默认单 WAL 记录上限 99 MiB，回放读取批次上限 64 MiB；本轮 payload 为 70、95 MiB，均低于单记录上限。

70 MiB 记录在约 6.29 秒确认，独立回放得到 user LSN 211、73,400,320 字节，SHA256 `2cc2a358f5d47ee6dd0efff52464d052a048e6de246a9e08a5196578af126555`。95 MiB 记录在约 8.11 秒确认，独立回放得到 user LSN 212、99,614,720 字节，SHA256 `0e7c664bafdfb11db17667d709cd83432b1a7b172869eea0ff1989ec6b5bcd65`。第一次包含两条大记录的回放约 21.53 秒；同数据的三次重复回放约 3.20、1.35、0.79 秒，未形成稳定长尾。node1 为普通副本时将其 `SIGKILL`，其余两票存活；该状态下的独立回放约 0.78 秒完成，两条大记录哈希仍一致，随后恢复 node1。

用 `RangeCheckpoint(1,211,files...)` 确认 checkpoint intent，后台物理 `Truncate` 推进到 PSN 1980，三个 LogService 节点均记录 snapshot 导入成功。独立回放只保留未 checkpoint 的 95 MiB、user LSN 212，哈希不变。接着同时强杀三个节点并从原盘启动，独立回放仍得到同一记录、同一哈希。最后在当前 leader node1 和普通副本 node0 存活时暂停另一普通副本 node2，写入另一条 95 MiB 记录；在 node2 保持暂停的 15 秒内，这笔写入以 5.43 秒确认，未发生重试。恢复 node2 后独立回放确认新记录 user LSN 213、SHA256 `0b68b8e1dc0de8694242437607e8147f7815b153147abb8139d50fb3c0b001be`，与 ACK 相同。证据为远端 `large70.*.log`、`large95.*.log` 与节点日志。本轮未发现大记录截断/回放错误；未模拟 TAE checkpoint 文件读取，也未验证更接近 99 MiB 的边界值。

## 新缺陷：导出过程中崩溃留下不受配额管理的 `.generating` 目录（2026-11-09 15:06–15:16）

**环境与触发。** 已部署的 `b19ef50f05` 服务二进制；核对 upstream/main `e2459cc55f` 与该版本之间没有 `pkg/logservice/snapshot.go`、`truncation.go` 的变更。51 上全新空盘三投票副本目录 `/home/mo/jxm/logservice-main-b19ef-generating-crash-20261109/`，数据 shard 1，RPC `127.0.0.1:58201/58211/58221`，`truncate-interval=1s`、`max-exported-snapshot=1`、HAKeeper 过期阈值 5 分钟。node0 是 leader，node1 是目标普通副本；node0、node2 在每次故障期间保持存活。使用 `test/logservice-chaos/kill_generating_export.py` 监视 node1 的 exported-snapshot 目录；只在目标进程的 `snapshot-*.generating` 目录确实存在时 `SIGSTOP` 后 `SIGKILL`。本轮目录在 kill 时均为空，无手工创建或修改快照文件。

**稳定复现步骤。** 先确认 400 条普通 Append，末条 LSN 405；监视器就绪后执行 `Truncate(200)`，在 node1 创建 `snapshot-0000000000000197-262147.generating` 时杀进程。node0、node2 从 LSN 201 读到 205 条；node1 原盘重启并正常重新导出 index 408，但旧 index 407 的 `.generating` 目录未清理。之后每轮先追加约 120 条，让已有正式快照 index 小于下一次合法截断位点；监视器就绪后截断，命中新 `.generating` 目录并杀 node1。为稳定避开同 index 重试复用，在后续三轮里 node1 下线期间从另外两票再确认一条 Append，然后才从原盘重启 node1：新 applied index 使下次导出使用新目录名。首轮及这三轮故障后，node1 磁盘上同时保留 index **407、656、890、1017** 的四个 `.generating` 目录。另一次没有在重启前推进 index 的对照轮，新导出复用了同 index 临时目录，未增加残留数。每轮监视器日志、Append/Truncate 输出及三个节点日志都保存在上述远端目录。

**存活与影响边界。** `Truncate(700)` 及后续正式 snapshot 导入成功后，前两个临时目录仍存在；第四次故障恢复后，三个地址都报告原三成员，从 LSN 926 各读到 91 条完全相同的记录，SHA256 均为 `c2657a1e8aa1be47e70f4628578e50511c608fd5881ea1f2927124f66f5cf4de`。没有观察到 WAL 丢失、客户端最终错误或导出阻塞。当前四个残留目录均为空，各占约 4 KiB；已证明的是反复崩溃可使不受 `max-exported-snapshot` 管理的目录项持续增长，没有实测大文件占用。代码上，`ReconcileInterruptedExport` 只识别正式 `snapshot-%016X` 目录的 marker，不处理 `snapshot-<index>-<replica>.generating`；`loadCompletedSnapshots` 同样忽略它。Dragonboat 对内部 snapshot 目录的 zombie 清理不覆盖 LogService 指定的外部导出目录。若在数据文件写入后、重命名前崩溃，可能遗留更大的临时文件，这是代码路径推断，本轮未命中。期望原盘启动或后续导出/导入清理不再使用的临时导出目录，并保护当前正在生成的目录。本问题不同于 #32161 的超时后完成的正式快照未登记。

缺陷跟踪：[matrixorigin/matrixone#32412](https://github.com/matrixorigin/matrixone/issues/32412)，已指派 `jiangxinmeng1`。

## 最新 upstream/main 回归验证：快照窗口轮换下线、持续负载与冷重启（2026-11-09 15:35–15:41）

继续使用 `b19ef50f05` 二进制及上述三副本目录，HAKeeper 过期阈值 5 分钟。单个三地址客户端持续执行 Append→Read payload 核对，另一控制器只在已核对 LSN 后至少 100 条处推进 Truncate。按 node0、node1、node2 的顺序轮换三遍，共九轮；前六轮分别围绕导出快照、导入快照等待，后三轮在发起截断请求后立即杀节点。每轮只杀一台，另两台始终运行；恢复原盘、确认三地址均报告原三成员后才进入下一轮。node1 在第一组导出轮和第二组导入轮、node0/node1/node2 在最后三轮均作为当时 leader 被杀。两次导出等待及两次导入等待未在 18 秒内观察到目标日志，改在定时器上注入，因此这些轮次只证明持续负载下的节点下线，不能声称精确命中快照函数内部。其余轮次观察到目标节点的导出或导入成功日志，或与截断请求并发。

第一段负载确认 **982** 次写入并回读，Append 重试 5 次、Read 重试 1 次，最长单轮 **7.397 秒**；21 次截断成功、1 次请求在 leader 切换时返回连接错误，下一次截断成功。第六轮脚本在发出 `SIGKILL` 后立即检查 `/proc`，因进程尚未退出而误报存活数异常；检查时 node2 已停止，持续客户端正常结束。调整脚本为等待进程退出最多 3 秒后，从第六轮继续：第二段确认 **412** 次写入并回读，Append 重试 5 次、Read 重试 0 次，最长单轮 **6.051 秒**；9 次截断全部成功。两段合计 **1394** 次 payload 核对、30 次成功截断、11 次客户端内部重试，没有最终写读错误。最终 Truncated LSN 为 2366、Latest LSN 为 2506；三个 RPC 地址从 LSN 2367 各读出 127 条相同用户记录，SHA256 均为 `e3945a53fd7fbeb9b61d6b3e4d0fafaa6a1c58b3551f3fcdd813400a952ad742`。

三节点正常停止后从各自旧盘整组重启，三地址恢复相同三成员与 leader；再次从 LSN 2367 读取，三份记录数与 SHA256 均不变。同一测试目录的节点日志从建群起累计各有 13 次 `import snapshot success`、14 次 `export snapshot success`，无 panic、FATAL 或 `L/shutdown`。前述四个 `.generating` 残留仍未被正常导入和冷重启清理，支持 #32412 的持久性结论。证据保存在远端同一目录的 `generating-followup-20261109.{load,progress}`、`generating-followup-b-20261109.{load,progress}`、轮次截断输出及三个节点的 `stdout.log`。本轮未发现新的可稳定复现缺陷；测试进程已停止。

## 最新 upstream/main 回归验证：默认阈值下 HAKeeper leader 长时间网络隔离（2026-11-09 15:55–16:03）

使用 `b19ef50f05` 服务二进制和新空盘三投票副本集群 `/home/mo/jxm/logservice-main-b19ef-long-partition-20261109/`。各节点在独立 Linux network namespace，RPC 地址为 `198.18.51.10:58401`、`.11:58411`、`.12:58421`；`log-store-timeout=5m`。开始时 node2 同时是 HAKeeper shard 0 和数据 shard 1 的 leader。运行 `test/logservice-chaos/run_long_partition.py`，单个三地址客户端连续 Append→Read payload 核对，另一线程把 Truncate 保持在已核对 LSN 后至少 100 条。热身确认到 LSN 51 后，关闭 node2 的宿主侧 veth **340 秒**，使它与 node0、node1 双向断开；后两台始终互通并保持运行。

隔离期间，持续客户端确认 **2119** 次写入并回读、无重试和最终错误，最长单轮 **3.123 秒**；31 次 Truncate 全部成功，已核对 LSN 从 51 推进到 2191。恢复 node2 网络约 15 秒后，三个 RPC 地址对 shard 0 和 shard 1 均报告相同的原三成员和新 leader。截断水位为 2070，三地址从 LSN 2071 各读出 117 条用户记录，SHA256 均为 `7454bd10052802be5a2387e07e032d4fb973c4ba3c4e9acdcb11e111f83a2372`。

为验证 node2 不只是出现在成员表里，网络恢复后再停止 node0 **35 秒**，只留 node1、node2 两票。新的连续客户端在这段时间将已核对 LSN 从 2215 推进到 2475，`Truncate(2375)` 成功。原盘恢复 node0 后，此段客户端累计确认 **293** 次写入并回读、无重试，最长单轮 **0.502 秒**；三个地址各从 LSN 2376 读出 114 条相同用户记录，SHA256 均为 `524b7c9970d4fb8860f7a42626b719ce283c6aa61ff5d1d7247a87d6d2e4dbe1`。节点日志无 panic、`L/shutdown` 或 `Expired logStore info`。这些结果未复现 30 秒加速阈值下的 #32134；仅覆盖这次默认 5 分钟配置、单 leader 长网络隔离和恢复后的两票服务。远端证据为 `hakeeper-leader-340s-20261109.{load,progress}`、`after-heal-vote.{load,progress}` 及三个节点的 `stdout.log`。测试进程已停止，测试 veth、bridge 和 network namespace 已删除。

## 最新 upstream/main 回归验证：大 WAL 模糊成功重试、重复 PSN 与 checkpoint（2026-11-09 16:19–16:32）

继续使用 `b19ef50f05` 的独立 TN WAL 编码三副本集群 `/home/mo/jxm/logservice-main-b19ef-wal-replay-20261109/`。探针 `test/logservice-chaos/tn_driver_probe/large_wal_probe/main.go` 通过真实 `wal.NewLogserviceHandle`、默认 100-client 池、`ReplayForWrite` 和 `AppendEntry(GroupUserTxn)` 写入，新增仅在完成回放后、写入前暂停的文件屏障。先独立回放并核对已有两条 95 MiB 记录，watermark 为 user LSN 213、checkpointed 为 211。暂停探针，确认数据 leader 在 node2，随后 `SIGSTOP` node2 并释放一条 95 MiB WAL 写入；node0、node1 两票继续运行。25 秒后 `SIGCONT` node2。

这笔 WAL 写入最后以 user LSN **214** 成功，payload SHA256 为 `06670f0752e7d86bca233c8385492f6ac95590053b0a130c01ead1c707e5f6a8`，总耗时 **32.171 秒**，driver 底层 Append 重试 1 次。独立回放成功，日志确认同一 DSN **236** 的物理副本在 PSN **4746**、**4750**，后者被回放器识别为重复；逻辑回调只得到一次 LSN 214，哈希与 ACK 一致。这次未记录探针实际选中的 RPC 连接，不能把 32 秒全部归因于旧连接；物理重复与既有 Append 模糊成功后的重试机制一致，不能据此声称逻辑 WAL 重复提交。

随后再确认一条 2 MiB WAL，user LSN **215**、SHA256 `7e360f0cde34f8e021d3d5d2334191c24c5026e2d17a0072eb36f8ff1baef8e6`。调用正常 `RangeCheckpoint(1,214,...)` 后，后台物理 Truncate 到第一份副本的 PSN **4746**；下一次独立 `ReplayForWrite` 报 `checkpointed=214`、`watermark=215`，回调仍能看见 LSN 214 和 215，其中 LSN 214 来自保留的 PSN 4750。重复回放及三副本整组正常停止、原盘冷重启后的回放仍是同样结果。检查 TAE `db.WalReplayer.checkLSN`：真实 TAE 回调用 checkpoint LSN 过滤 `lsn <= ckpLSN`，因此这次探针回调看见 214 **不能推断真实 TN 会重新应用已 checkpoint 的事务**；本轮没有启动 TN，也没有生成真实 TAE checkpoint 文件。

再通过正常 `RangeCheckpoint(1,215,...)` 将物理截断推进到 PSN **5057**。最终独立回放得到 `checkpointed=215`、`watermark=215`，没有任何大记录回调，说明前次残留在下一次 checkpoint 后消失。本轮观察到一次 checkpoint 周期内保守保留重复副本，但没有最终 WAL 丢失、回放失败或可证明的 TN 重复应用；暂不单列新缺陷。51 上证据为 `large-leader-stop-a.log`、`large-after-duplicate.{write,checkpoint,replay,cold-replay,checkpoint215,final-replay}` 与各节点日志，三个测试节点已停止。

## 最新 upstream/main 回归验证：HAKeeper 内部快照生成中崩溃与长时间两票服务（2026-11-09 17:42–17:57）

在 51 新建空盘三投票副本集群 `/home/mo/jxm/logservice-main-b19ef-hakeeper-snap-crash-20261109/`，服务版仍为 `b19ef50f05`，RPC `127.0.0.1:58601/58611/58621`，store 过期阈值默认 **5 分钟**。为密集命中内部快照，将 `hakeeper-truncate-interval` 从 1 分钟缩为 **1 秒**；数据 shard 1 的 Truncate 本轮未调用。扩展 `test/logservice-chaos/kill_generating_export.py` 的 `--hakeeper` 选项，监视 Dragonboat 的 `snapshot-part-0/snapshot-0-<replica>` 目录，只有 `snapshot-*.generating` 确实出现且目标 PID、可执行文件、配置路径均匹配后，才暂停并杀目标进程。一个三地址客户端同时持续执行 Append→Read payload 核对，每次只下线一台。

轮流在 node0、当时 HAKeeper leader node1、当时数据 leader node2 的内部快照目录创建时杀进程；命中的 snapshot index 分别为十六进制 **BC、110、155**，三个目录在杀进程时均为空。node0、node1 各自约数秒后从原盘启动，三地址重新报告 shard 0 原三成员。node2 于 **17:43:43** 被杀，至 **17:56:43** 才原盘启动，停机约 **13 分钟**，超过默认 5 分钟阈值；node0、node1 两票期间继续服务。持续客户端累计完成 **6142** 次写入并回读，Append 重试 3 次、Read 重试 0 次，最大单轮 **6.528 秒**，无最终写读错误。原盘恢复后，shard 0、shard 1 的三个地址均报告原三成员。三个地址从 LSN 1 各读取 6144 条用户物理记录，SHA256 均为 `6272c51600800c6e8491224314de77c61941bb411ae988d2d31ed86d54804c35`；物理条数比逻辑周期多 2，与故障时模糊成功后重试可能产生的重复记录一致，本轮未逐条归因。

随后三节点正常停止、全部从各自旧盘冷重启；两 shard 均恢复相同三成员，三个地址再次读到 6144 条和相同 SHA256。被杀时的 BC、110、155 三个 `.generating` 目录均未残留；节点日志无 panic、`L/shutdown` 或 `Expired logStore info`。因此这次 Dragonboat **内部** HAKeeper 快照的清理与恢复正常，和 #32412 的 LogService **外部导出**目录残留不同。证据为远端 `hakeeper-snap-crash.node{0,1,2}.watch`、`hakeeper-snap-crash.{load,progress}`、三个节点的 `stdout.log` 和磁盘快照目录。测试进程已停止。本轮未发现新的可稳定复现缺陷；1 秒快照间隔是加速测试配置，不能等同于默认周期的资源消耗。

## 最新 upstream/main 回归验证：快照配额满时隔离普通副本并恢复（2026-11-10 09:49–09:52）

复用已停止的三投票副本隔离集群 `/home/mo/jxm/logservice-main-b19ef-long-partition-20261109/`，从各节点原盘启动，服务二进制仍为 `b19ef50f05`；RPC 地址 `198.18.51.10:58401`、`.11:58411`、`.12:58421`，数据 shard 1 的 leader 为 node1，node0 是普通副本。配置为 `truncate-interval=1s`、`max-exported-snapshot=4`、HAKeeper store 过期阈值 5 分钟。先启动三地址 Append→Read 持续客户端，确认 node0 的已导出快照目录达到 4/4（index 2491、2495、2496、2497），再只隔离 node0 网络，始终保留 node1、node2 两票互通。隔离期间客户端继续写读，另以三地址客户端依次将逻辑截断水位推进至 2621、2643、2679、2717、2748、2768、2805、2833，八次 `Truncate` 均成功。

node0 的 `getTruncatedLsn` 在隔离期间返回 `timeout`；09:50:03 日志确认配额释放路径执行一次：`droppedIndex=2497, before=4, after=3`。这只删除未导入的导出快照，未改变 Raft 已提交日志。恢复网络后，三个地址重新报告原三成员；node0 先于 09:50:54 导入仍保留的 index 2496，之后继续导出，并在逻辑截断推进到 3050 后于 09:52:03 导入 index 3048。连续客户端最终 **567/567** 轮成功、Append/Read 重试均为 0，最大整轮 **806.111 ms**。最终从 LSN 3051 读取，三地址输出 SHA256 均为 `ea196965151768185f24023f9614cb5004000ef069d6b5f95b19835281c1b4ba`；保留区间内 33 条本轮确认写入按 LSN/payload 核对通过，没有重复 payload。随后三节点全部从原盘冷重启，三个地址均恢复原三成员，仍从 LSN 3051 读到同一 SHA256。测试后停止全部节点并拆除测试网络；现场证据为同目录 `quota-escape-20261110.{load,progress}`、`quota-escape-20261110.{final,cold}-node{0,1,2}.read` 和各节点 `stdout.log`。这轮没有发现新的最终错误或长尾；隔离后第一次查成员时 node0 仍只看到自身、另两票只看到彼此，随后收敛为相同三成员视图。

## 最新 upstream/main 回归验证：数据 shard 内部快照导入时崩溃（2026-11-10 10:22–10:24）

继续复用上述已停止的三投票副本集群，从各自原盘启动；此时数据 shard 1 的 leader 为 node0，node1 是普通副本。扩展 `test/logservice-chaos/kill_generating_export.py` 的 `--internal-data` 选项，监听 node1 的 Dragonboat 内部目录 `snapshot-part-1/snapshot-1-262147`，并在目标 PID、可执行文件、配置路径均匹配后，只在 `snapshot-*.generating` 目录确实存在时暂停并杀目标进程。三地址客户端从 10:22:20 起持续 Append→Read payload 核对。监视器就绪后提交 `Truncate(3100)`，命中内部 `snapshot-0000000000000C14-262147.generating`（index 3092）；杀进程时目录为空。node0、node2 两票全程存活；node1 下线期间继续确认写入，并成功将截断水位推进到 3353。

node1 原盘重启后，三地址重新报告原三成员；它于 10:23:18 导入 index 3104，后续 `Truncate(3509)` 后于 10:23:38 导入 index 3498。持续客户端 **500/500** 轮成功，Append/Read 重试均为 0，最大整轮 **867.673 ms**。三个地址从 LSN 3510 读回的 SHA256 均为 `175a4e7ac5208958eac5dd24ba3df49c59135129e78e8b9e9f08918d1571202b`；保留区间内 102 条本轮确认写入按 LSN/payload 核对通过，没有重复 payload。整组三节点正常停机、从原盘冷重启后，三地址恢复同一三成员视图，读回哈希不变。node1 的内部 `.generating` 目录没有残留，节点日志未见 panic、`L/shutdown` 或 `Expired logStore info`。证据在远端同目录的 `import-crash-20261110.{watch,load,progress}`、`import-crash-20261110.{node,cold-node}{0,1,2}.read` 及节点日志。测试进程与网络命名空间已清理。本轮未发现新缺陷；此内部 Dragonboat 快照目录与 #32412 的外部导出目录属于不同清理路径。

## 最新 upstream/main 回归验证：leader 内部快照导入崩溃后迅速换第二台故障（2026-11-10 11:13–11:15）

继续复用同一三投票副本集群和 `--internal-data` 监视器。起始数据 shard 1 的 leader 为 node0。三地址 Append→Read 客户端持续运行时提交 `Truncate(3625)`；监视器在 node0 的内部 `snapshot-0000000000000E26-262145.generating`（index 3622）目录创建且目标进程身份匹配时暂停并杀掉 node0，目录当时为空。node1、node2 两票选出新 leader node1，继续确认写入，期间 `Truncate(3707)` 成功。然后 node0 从原盘启动，**约 0.25 秒后**杀掉 node2；两台进程 node0、node1 存活，继续确认写入，期间 `Truncate(3934)` 成功。最后从 node2 原盘启动，三地址恢复相同三成员，再执行 `Truncate(4108)`。

客户端累计 **575/575** 轮写入回读成功；Append 内部重试 1 次（第二次换故障时 `EOF`），Read 重试 0 次，无最终错误。最大整轮 **4.988 秒**，发生在第一次 leader 故障后选举窗口，未出现 1 分钟长尾。三个地址从 LSN 4109 读回的 SHA256 均为 `b6de14e51a04196b31ccb2ffe3f9b644680cdd29fb89e1739ce0be66b661404e`；保留区间内 103 条本轮已确认记录按 LSN/payload 核对通过，无重复 payload。三节点正常停机、全部原盘冷重启后恢复原三成员，读回哈希不变；node0 的内部 `.generating` 目录无残留，节点日志未见 panic、`L/shutdown` 或 `Expired logStore info`。证据为远端同目录 `leader-import-20261110.{watch,load,progress}`、`leader-import-20261110.{node,cold-node}{0,1,2}.read` 和各节点日志。测试进程与网络命名空间已清理。本轮没有发现新缺陷；第二次故障只保证两个进程存活，并未在杀 node2 前等待刚重启的 node0 完全追上，这是本轮刻意覆盖的恢复窗口。

## 最新 upstream/main 回归验证：只隔离原 leader 的 Raft 通道，保留 RPC 与心跳（2026-11-10 11:44–11:46）

复用同一三投票副本网络命名空间集群，起始数据 shard 1 的 leader 为 node0。三地址客户端持续 Append→Read payload 核对时，仅在 node0 的命名空间加入四条 TCP DROP 规则：阻断来自 node1/node2 到 node0 Raft 端口 58400 的入站，以及 node0 到 node1/node2 Raft 端口 58410/58420 的出站。**没有阻断 RPC 58401、gossip 58402 或其他两个节点之间的流量**；三进程均持续运行。规则从 11:44:37 保持到 11:45:51，约 74 秒。撤销前规则计数分别命中入站 59/33 包、出站 199/199 包，确认不是仅加了空规则。隔离期间经原 leader node0 的 RPC 地址查询 shard 0、shard 1 仍有响应，三个地址最终均报告两个 shard 的新 leader 在另外两票上；安全水位截断 4312、4331、4372、4415、4455、4478 均成功。

恢复 Raft 通道后，再截断到 4707，三个地址重新报告相同三成员。持续客户端 **570/570** 轮成功；Append 重试 0 次，Read 在 `seq=58` 有一次 `timeout`（单次约 6.969 秒）后成功，最大整轮 **7.754 秒**，没有最终错误或 1 分钟长尾。从 LSN 4708 经三个地址分别读回的 SHA256 均为 `78a38dc097ab5fb4fc0a4442ac7f961679e330817331522761282`；保留区间内 103 条本轮确认写入按 LSN/payload 核对通过，无重复 payload。三节点正常停机、原盘冷重启后仍显示原三成员，读回哈希不变。规则已删除，节点与测试网络命名空间已停止清理。证据为远端同目录 `raft-only-20261110.{load,progress}`、`raft-only-20261110.{node,cold-node}{0,1,2}.read` 和各节点日志。本轮未发现新的稳定缺陷；它验证的是 Raft 端口定向隔离，和整机网络断开、只阻断客户端 RPC 的既有用例不同。

## 最新 upstream/main 回归验证：原 leader 只丢 Raft 入站，仍可出站（2026-11-10 14:00–14:02）

复用同一三投票副本集群，本轮起始数据 shard 1 的 leader 是 node1，HAKeeper leader 是 node2。三地址客户端持续 Append→Read 核对期间，只在 node1 的网络命名空间 `INPUT` 链加入四条 TCP DROP 规则：来自 node0/node2、目的端口为 node1 Raft 端口 58410 的连接包，以及源端口为 node0/node2 Raft 端口 58400/58420 的回包。`OUTPUT` 链保持 ACCEPT，RPC、gossip 与另外两票的互通没有阻断。规则从 14:00:39 保持至 14:02:04，约 **85 秒**；撤销前四条规则分别命中 89、25、39、89 包，证实单向 Raft 故障实际发生。旧 leader 的 RPC 地址仍能查询状态；数据 shard 改由 node0 领导，HAKeeper shard 仍由 node2 领导。隔离期间，另外两票继续服务，安全水位截断 4897、4924、4971、5019、5045、5090、5137、5163 共 8 次均成功。

撤销规则后截断到 5378，三个地址均报告原三成员。持续客户端 **640/640** 轮成功，Append/Read 重试均为 0、无最终错误；最长整轮 **3.093 秒**，发生在选举窗口。三个地址从 LSN 5379 读回的 SHA256 均为 `0526726657a190a8612671d59b5177ca2d6464800e4048e9bbed9f4e43216a87`；保留区间内 103 条本轮已确认记录按 LSN/payload 核对通过，无重复 payload。三节点正常停机、原盘冷重启后恢复相同三成员，读回哈希不变。规则、节点与测试网络命名空间均已清理。证据为远端同目录 `raft-inbound-20261110.{load,progress}`、`raft-inbound-20261110.{node,cold-node}{0,1,2}.read` 及各节点日志。本轮未发现新缺陷；与上一轮双向阻断原 leader 的 Raft 端口不同，这轮只阻断了它接收 Raft 流量。

## 最新 upstream/main 回归验证：TN WAL driver 并行 checkpoint 遇到 leader 单向 Raft 故障（2026-11-10 14:07–14:12）

在 51 新建**空盘、只写 TN WAL** 的三投票副本集群 `/home/mo/jxm/logservice-main-b19ef-tnwal-raft-inbound-20261110/`，服务仍为 `b19ef50f05`，RPC 地址 `198.18.51.10:58201`、`.11:58211`、`.12:58221`，HAKeeper store 过期阈值 5 分钟。探针从同一 b19 源码编译，使用 TN 实际的 `wal.NewLogserviceHandle`、默认 **100 client**、`ReplayForWrite`、`AppendEntry(GroupUserTxn)`、`RangeCheckpoint(1,end,files...)`；每 20 条确认记录提交 checkpoint，水位落后最新 user LSN 10 条。`files` 为测试 marker，未生成真正的 TAE checkpoint 文件，也未启动 TN/CN。为支持命名空间地址，扩展 `wal_probe/main.go` 的测试地址白名单；`node.sh` 的 PID 校验改为比较可执行文件的真实路径，以支持本轮使用的服务二进制符号链接。

写入期间数据 shard 原 leader 为 node1。14:08:01–14:09:20，只在 node1 命名空间阻断来自 node0/node2 的 Raft **入站与回包**（目的端口 58210 或源端口 58200/58220），其出站、RPC 与另外两票互通均保持可用；规则计数非零，另两票选出新 leader node0。单笔 `seq=53` WAL Append 在旧连接的读取超时后内部重试 1 次，**16.621 秒**才确认；随后继续写入、提交 checkpoint。规则撤销、三成员视图恢复后，又单独杀普通副本 node2 **10 秒**并原盘恢复，任一时刻至少两票运行；这期间另有一次 Append 内部重试，耗时 **0.922 秒**。全程 **600/600** 条 user WAL 确认，p99 **1.149 秒**、最大 **16.621 秒**，无最终写入错误；**30/30** 次 checkpoint intent 与 **30/30** 次后台物理 Truncate 完成，没有观察到 3 分钟截断长尾。

旧实例结束后，新 WAL 实例 `ReplayForWrite` 返回 `user_lsn=600, checkpointed=590, truncated_dsn=648`；回放的 user LSN 591–600 共 10 条与旧进程 ACK 的 LSN/payload **10/10** 逐条一致，文本 SHA256 `a31835142ab2e1904d45963a10a11e7010dec51ff3bcfcece635ccd09ed68df5`。三节点全部原盘冷重启后，再起新实例回放，同样得到这 10 条、相同哈希及相同水位。证据为远端同目录 `tn-raft-inbound-20261110.{load,progress,replay,cold-replay,expected,observed,cold-observed}`、`baseline.replay`、各节点日志和磁盘数据。测试规则、节点和网络命名空间已清理。**16.621 秒长尾是真实 TN WAL driver 在两票可用时的观测**；日志显示到旧 leader RPC 地址的读取超时，随后同笔 Append 重试成功，与已有 Append 旧连接问题 #32133 一致。本轮没有新建同根因缺陷，也没有证明真实 TN/TAE 文件恢复或 SQL 层的结果。

## 最新 upstream/main 回归验证：checkpoint 提交前隔离 leader 的 Raft 入站（2026-11-10 14:46–14:50）

复用上一节的三副本 TN WAL 专用集群、b19 服务和默认 100-client WAL 探针。先测试 `-pause-at-checkpoint 0`：它在 `RangeCheckpoint` 返回后、`WaitDone` 前暂停，但在屏障就绪时 checkpoint intent 与后台物理 `Truncate` 已完成，**不能把这个屏障视为在途 checkpoint**；放行后该轮健康对照完成 120 条写入，未用于故障结论。正式故障轮改用 `-pause-before-seq 19`，在第 20 条用户 WAL 写入之前暂停：此时已有 19 条新 ACK（最新 user LSN 739），本轮还没有发出 checkpoint。确认数据 shard 原 leader 为 node0 后，14:48:06 在 node0 命名空间阻断来自 node1/node2 的 Raft 入站与回包，保留出站、RPC 和两台普通副本之间的通信，再释放写入屏障；故障持续到 14:49:18，约 **72 秒**。规则在撤销前四项计数分别命中 18、73、47、26 包，另外两票选出新 leader node2。

故障生效后，`seq=19` 的 Append 在旧连接上等待、重试 1 次，于 **16.643 秒**后确认；紧随其后的首个 `RangeCheckpoint(1,730,files...)` 在故障期间确认，后台物理 `Truncate` 也成功。持续到探针结束时 **120/120** 条新 WAL 确认，**6/6** 次 checkpoint intent 与 **6/6** 次物理截断完成，均在单向隔离期间；没有最终错误或 3 分钟截断长尾。恢复 Raft 通道后，新 WAL 实例 `ReplayForWrite` 返回 `user_lsn=840, checkpointed=830, truncated_dsn=912`。回放的 user LSN 831–840 共 10 条与故障轮 ACK 的 LSN/payload **10/10** 逐条一致，文本 SHA256 `72bc65e26b7f3aacb9ed19f988b0ff654e7e50f39a53c9b0f5a3400bec516a92`；三节点原盘冷重启后再次回放仍是相同水位和哈希。证据在同一远端目录的 `checkpoint-window-20261110.{load,ready,release}`（无效屏障对照）与 `checkpoint-submit-20261110.{load,ready,release,progress,replay,cold-replay,expected,observed,cold-observed}`、各节点日志。规则、测试进程与网络命名空间已清理。本轮未发现新的 checkpoint 正确性缺陷；16.643 秒 Append 长尾与上一节同类旧连接切换有关，暂不另建重复 issue。`files` 仍为测试 marker，不代表完成真实 TAE checkpoint 文件恢复。

## 最新 upstream/main 回归验证：落后副本用 Raft snapshot 追赶时立即换第二台故障（2026-11-10 16:05–16:10）

继续使用上述 b19 三副本 TN WAL 专用集群及默认 100-client WAL 探针，持续 `AppendEntry(GroupUserTxn)`，每 20 条 ACK 提交一次带 `GroupFiles` 测试 marker 的 checkpoint，保留最新 10 条。起始数据 shard leader 为 node2。探针在第 20 条新 WAL 前暂停；16:06:14 杀普通副本 node0 后放行，node1/node2 两票持续写入与后台物理截断。node0 下线时本轮 ACK 从 19 推进到约 318，node1/node2 数据 shard 的本地导入快照 index 已到 **3525–3528**，远超 node0 停机前最后一次本地导入的 index **2332**。16:07:28 从原盘启动 node0，约 **0.25 秒后**杀 node1；此后只保留 node0、leader node2 两台进程，继续写入与 checkpoint，最后恢复 node1。

本轮明确命中物理压缩后的 Raft snapshot 追赶：node2 在 16:07:28 记录 `sending snapshot (3591) to n62147, entry compacted`，node0 随即记录 `starts to restore snapshot index 3591 term 9`；同一恢复期间 HAKeeper shard 也收到 snapshot index 5301。刚恢复的 node0 与 node2 两票继续提交，探针 **600/600** 条新 user WAL 确认，**30/30** 次 checkpoint intent 和 **30/30** 次后台物理 Truncate 成功。第二次换故障时有一次 Append 内部重试，总耗时 **1.123 秒**；全轮单笔 ACK 最大 **1.280 秒**、p99 **735 ms**，没有最终错误或 1 分钟长尾。新 WAL 实例 `ReplayForWrite` 返回 `user_lsn=1440, checkpointed=1430, truncated_dsn=1572`；保留的 user LSN 1431–1440 与故障轮 ACK 的 LSN/payload **10/10** 一致，文本 SHA256 `f1972b1f853d20ee31e41f6a24b988e3ff2419b71ba710803f13c6b08d4c1455`。三节点全部原盘冷重启后，三地址恢复原三成员，独立回放水位和哈希不变。证据为远端同目录 `stagger-catchup-20261110.{load,ready,release,progress,replay,cold-replay,expected,observed,cold-observed}` 与三个节点日志。测试进程和网络命名空间已清理；本轮未发现新缺陷。未启动真实 TN/TAE checkpoint 文件加载。

## 最新 upstream/main 回归验证：后台物理 Truncate 连接黑洞期间关闭 WAL driver（2026-11-10 16:13–16:17）

复用同一三副本 TN WAL 专用集群和默认 100-client 探针。代码风险是 `LogServiceDriver.Close()` 创建 4 分钟 deadline 后，最后调用不带该 deadline 的 `truncateQueue.Stop()`；后台 `truncateFromRemote` 使用 `context.Background()` 和单次 3 分钟超时，若一直不返回，理论上可拖住关闭。本轮只检验**一个**已连接 RPC 地址失响应时的实际退出行为。探针先确认 20 条新 WAL，并用 `-truncate-ready-file` 在后台首次物理 `Truncate` 前暂停；主循环已经输出 `summary cycles=20`，随后进入 `Close` 等待截断队列。第一次尝试在屏障停留约 35 秒后才加规则，`Wal-Truncate-Execute` 的 34.8 秒计时包含人为暂停且请求放行后立即成功，**不作为网络长尾证据**。

第二次独立尝试加了写入前屏障：探针完成 `ReplayForWrite` 时 `ss -tnp` 记录到 **100** 个池连接；写入 20 条、进入后台截断屏障后记录到 **101** 个连接，唯一新增连接为 `198.18.51.1:34874 → 198.18.51.12:58221`（fd 106）。只在 node2 的测试网络命名空间丢弃来自 bridge `198.18.51.1` 到该 RPC 端口的入站 TCP，确认规则生效后释放截断屏障；Raft 与另外两个 RPC 地址均未阻断。规则命中 **469** 包。`Truncate` 对目标 PSN **5232** 首次连接失败后内部重试 1 次，最终物理截断成功；日志总时长 **34.842 秒**包括约 18 秒屏障等待，释放到完成约 **16 秒**。探针的 `Close` 随后正常返回，没有触及 4 分钟期限；在规则仍生效时，另两个地址可查询已推进的截断水位 5232。

第二轮 **20/20** 条新 WAL 确认，恢复 RPC 规则后的新实例 `ReplayForWrite` 返回 `user_lsn=1480, checkpointed=1470, truncated_dsn=1616`；回放的 LSN 1471–1480 与 ACK 的 payload **10/10** 一致，文本 SHA256 `381b4fe554b50ea8956383bb8b2c5e5262ac96ba78fcdb88e686b82f119eafbc`。证据在远端同目录 `close-truncate-20261110.{load,ready,release}`（未命中对照）、`close-truncate-hit-20261110.{load,write-ready,write-release,truncate-ready,truncate-release,pool-sockets,onfly-sockets,replay,expected,observed}` 及节点日志。规则、进程、网络命名空间已清理。本轮没有发现新的关闭失败；不能据此证明多次连续 3 分钟超时也能满足 4 分钟关闭期限。

## 最新 upstream/main 回归验证：后台物理 Truncate 时客户端三地址同时失联（2026-11-10 16:20–16:23）

继续使用同一三副本 TN WAL 专用集群和默认 100-client 探针。真实 TN WAL driver 先确认 20 条新记录（user LSN 1481–1500），提交 checkpoint intent 到 1490；在后台物理 `Truncate(5655)` 发出前的屏障处，只阻断探针所在 bridge `198.18.51.1` 到三个 LogService RPC 端口 58201/58211/58221 的 TCP 入站包。三条规则分别命中 **472、528、444** 包；三个服务进程与 Raft 通道始终运行。该故障是客户端与整个服务的 RPC 失联，测试期间所有 RPC 地址均不可达。

屏障释放后，后台 `Truncate` 重试 **11 次**，约 **49.003 秒**后报 `rpc timeout`；物理截断水位仍为 **5232**。`LogServiceDriver.Close()` 随后返回，探针进程退出码为 0，没有超过其 4 分钟关闭期限。checkpoint intent 的确认发生在网络阻断之前；`Close()` 未把这次异步物理截断失败上报给调用方，符合当前代码将错误记录在后台日志的行为，本轮不能据此判定数据丢失。

撤销规则后，新 WAL 实例 `ReplayForWrite` 成功，返回 `user_lsn=1500, checkpointed=1490, truncated_dsn=1616`，仍能读到保留记录。再用新实例确认 20 条 WAL、提交 checkpoint intent 到 1510，后台物理截断在 **432.845 ms** 内成功推进到 PSN **5978**。最终独立回放返回 `user_lsn=1520, checkpointed=1510, truncated_dsn=1660`；保留的 LSN 1511–1520 与 ACK **10/10** 逐条一致，文本 SHA256 `53548d7c34c02dab7e4a7e9c31678d893e94f282f4ae1a6a7cee862ebdfce2c0`。三节点整组原盘冷重启后回放水位与记录不变。证据在远端同目录 `close-truncate-allrpc-20261110.{load,ready,release,replay,recover,final-replay,cold-replay}` 和节点日志。规则、节点、网络命名空间均已清理。本轮未发现新的正确性缺陷；短期未完成的物理截断保守保留了旧日志，下一次 checkpoint 后恢复推进。

## 最新 upstream/main 回归验证：落后副本导入 Raft snapshot 与新 TN WAL 实例回放重叠（2026-11-10 16:46–16:50）

复用上述 b19 三副本 TN WAL 专用集群。初始数据 shard leader 为 node1，普通副本 node0 在 16:46 左右被杀；node1、node2 两票期间，真实 TN WAL 探针确认 **400/400** 条新记录、**20/20** 次 checkpoint intent 与 **20/20** 次后台物理 Truncate。单笔写入最大 **1.680 秒**、p99 **890 ms**。停机的 node0 与存活副本之间形成了被压缩的 Raft 日志缺口。

16:48:50.472 从旧盘启动 node0，约 **0.28 秒后**杀当时 leader node1，立即启动新的 `wal.NewLogserviceHandle` 实例执行 `ReplayForWrite`，仅 node0、node2 两进程存活。node2 于 16:48:53.533 向 node0 发送数据 shard Raft snapshot **index 7141**（日志明确写明 `entry compacted`），node0 于 16:48:53.809 开始导入；HAKeeper shard 同期也收到 snapshot index 12234。新实例从启动到退出约 **6.236 秒**，成功返回 `user_lsn=1920, checkpointed=1910, truncated_dsn=2100`；其中 `ReplayForWrite` 自身报告约 **4 ms**，其余时间包括创建默认 100-client 池、故障后的发现与关闭。没有把这 6.236 秒全算作回放耗时。

保持 node1 下线，在 node0、node2 两票上再确认 **40/40** 条 WAL、**2/2** 次 checkpoint intent 和 **2/2** 次物理 Truncate；单笔写入最大 **785 ms**。恢复 node1 后独立回放为 `user_lsn=1960, checkpointed=1950, truncated_dsn=2144`，保留的 LSN 1951–1960 与 ACK **10/10** 一致，文本 SHA256 `6889cff6698370d7ecb328282a4b88674f557704c0c4647b6d6ca08a336db6da`。整组三副本各自从旧盘冷重启后，三个 RPC 地址均报告原三成员，独立回放水位和记录不变。证据在远端同目录 `replay-catchup-switch-20261110.{load,progress,exit,switch-start,switch-end,replay,replay-start,replay-end,replay-exit,two-votes,final-replay,cold-three-replay}` 和节点日志。节点与测试网络已清理。本轮未发现新的回放错误或一分钟长尾；未启动真实 TN/TAE checkpoint 文件加载。

## 最新 upstream/main 回归验证：并发 TN WAL 写入和 checkpoint 期间连续换副本（2026-11-10 17:14–17:17）

在同一 b19 三副本专用集群中，复用真实 `wal.NewLogserviceHandle`、默认 100-client 池，以 **32 个并发写入者**共写入 6400 条 user WAL；另一 goroutine 按确认数推进 checkpoint，保留连续确认水位之后至少 300 条，使用 `GroupFiles` 测试 marker。为让该探针接入隔离网络，只扩展 `concurrent_probe/main.go` 的测试地址白名单，未修改生产代码。初始数据 shard leader 为 node2。约 800 条确认后于 17:14:13 杀普通副本 node0，node1/node2 两票持续写入、checkpoint 和物理截断；17:14:58.173 从旧盘恢复 node0，约 **0.28 秒后**杀原 leader node2，继续只保留 node0/node1 两票。node1 于 17:15:01.430 发送数据 shard Raft snapshot **index 9362** 给 node0（日志标明 `entry compacted`），node0 于 17:15:01.924 开始导入；HAKeeper shard 同期也追赶 snapshot index 13934。

探针最终 **6400/6400** 条确认，**29/29** 次 checkpoint intent 与 **29/29** 次后台物理 Truncate 成功，driver 未向上层返回写入或 checkpoint 错误。故障窗口有 **33** 次底层 Append 返回 EOF（11 次）、连接失败（17 次）或 timeout（5 次），均经 driver 重试得到最终确认；单笔确认最大 **6.954 秒**、p99 **1.267 秒**，没有超过 1 分钟的长尾。node2 仍下线时，新 WAL 实例 `ReplayForWrite` 成功，返回 `user_lsn=8360, checkpointed=8060, truncated_dsn=8301`。checkpoint 水位之后的 LSN 8061–8360 共 **300** 条与 ACK 的 LSN/payload **300/300** 一致，文本 SHA256 `009b42bfa3f5fb014db822e41a4cf48aeceb8f98d1eac2a2fe237fbb6ae88ffe`，没有 DSN 空洞回放错误。探针回调还看见 7 条 `lsn <= checkpointed` 的旧记录；真实 TAE 回放会按 checkpoint LSN 过滤，不能把它们算作重复应用。

恢复 node2、整组三副本各自原盘冷重启后，再次独立回放得到相同水位，保留的 300 条与 ACK 仍全部一致。证据在远端同目录 `concurrent-rolling-20261110.{load,exit,node0-down,switch-start,switch-end,two-vote-replay,cold-replay}` 与节点日志。三个测试节点和网络命名空间均已清理。本轮未发现新的正确性缺陷；未生成或加载真实 TAE checkpoint 文件。

## 最新 upstream/main 回归验证：已接收 WAL 的提交与 driver Close 遭遇 leader 故障（2026-11-10 17:25–17:28）

新增隔离探针 `test/logservice-chaos/tn_driver_probe/close_probe/main.go`，调用真实 `wal.NewLogserviceHandle`、`ReplayForWrite`、`AppendEntry(GroupUserTxn)`，在底层 LogService `Append` 发出前设置文件屏障；`AppendEntry` 返回 LSN 后，另起 goroutine 执行 `WaitDone`，主线程同时发起 `Close`。屏障仅控制测试时机，底层请求放行后仍走真实客户端和服务。这样可检验已经被 WAL 接收、尚未得到持久化结果的记录在关闭时由谁交付终态；未启动 TN/CN。

第一轮起始数据 shard leader 为 node2。探针确认已接收 user LSN **8361**、`Close` 已发起后，杀 node2 并释放底层 Append，node0/node1 两票继续运行。底层 Append 依次经历连接重置、约 **5.302 秒**的 `context deadline exceeded`，第三次成功；`Close` 约 **20.175 秒**返回 nil，`WaitDone` 返回 nil。node2 仍下线时新 WAL 实例回放得到 LSN 8361 和原 payload。

第二轮恢复 node2 后，数据 shard leader 为 node1；探针已接收 LSN **8362** 并发起 `Close` 后，只在 node1 命名空间丢弃来自客户端 bridge `198.18.51.1` 到 RPC 端口 58211 的入站 TCP，服务进程、Raft 与另外两个 RPC 地址均未阻断，规则累计命中至少 **784** 包。旧连接上的底层 Append 等约 **49.837 秒**才以 `context deadline exceeded` 返回；随后重试约 **284 ms**成功，`Close` 总耗时 **60.477 秒**并返回 nil，`WaitDone` 返回 nil。规则仍生效时记录已确认，说明后续请求使用了可达路径；本轮没有记录重试具体选中的 RPC 地址。撤销规则后独立回放得到 LSN 8362 与原 payload，三个地址仍报告 node1 为数据 shard leader。整组原盘冷重启后，两条新 WAL 仍以 LSN 8361、8362 回放，水位 `user_lsn=8362, checkpointed=8060, truncated_dsn=8301`。

证据在远端同目录 `close-accepted-leader-20261110.{load,exit,append-ready,append-release,close-ready,fault,replay}`、`close-accepted-rpc-blackhole-20261110.{load,exit,append-ready,append-release,close-ready,replay,cold-replay}` 和各节点日志。所有节点与网络规则均已清理。第一轮未见提交结果丢失；第二轮在三票仍运行时出现约 1 分钟的关闭长尾，根因仍是旧 RPC 连接等待超时，与现有旧连接长尾记录同类，不另建重复 issue。该场景未证明真实 TN 一定会在每次关闭时保留未完成的提交，结论限于真实 WAL driver API 的可达调用顺序。

## 最新 upstream/main 回归验证：1000 条已接收 WAL 的批量提交与 Close 遇到 leader 下线（2026-11-10 17:41–17:43）

将上一节的真实 TN WAL `close_probe` 扩展为可一次接收多条 user WAL；仍在底层 LogService `Append` 前设置屏障，等所有 `AppendEntry` 返回 LSN、所有 `WaitDone` 已启动、`Close` goroutine 已发起后再制造故障。本轮在同一 b19 三副本集群接收 **1000** 条记录（LSN **8363–9362**），起始数据 shard leader 是 node2。关闭窗口内杀 node2、释放底层屏障，node0/node1 两票持续运行。请求分成多个底层批次，日志记录到底层 Append 成功 **104** 次，以及连接重置 **56** 次、EOF **9** 次、`context deadline exceeded` **35** 次、`timeout` **56** 次；这些是重试尝试数，不是 156 条上层写入失败。

`Close` 在 **61.137 秒**后返回 nil，**1000/1000** 个 `WaitDone` 均返回 nil；没有 goroutine 持续卡住或超过 driver 的 4 分钟关闭期限。node2 仍下线时，独立新实例 `ReplayForWrite` 返回 `user_lsn=9362, checkpointed=8060, truncated_dsn=8301`；LSN 8363–9362 的 **1000** 条 payload 与原探针接收记录逐条一致，文本 SHA256 `e5e686579dcb51b8acc78da8fa7224df6fabb21057ddda316a84f36c038e2d50`。恢复 node2 后整组三副本各自原盘冷重启，独立回放水位及这 1000 条记录不变。证据在远端同目录 `close-batch-leader-20261110.{load,exit,append-ready,append-release,close-ready,fault,replay,cold-replay}` 和节点日志。所有节点与测试网络已清理。本轮未发现新的提交丢失或关闭死锁；约 1 分钟关闭时长与故障下旧连接和批量重试有关，不能当作无故障关闭耗时。

## 最新 upstream/main 回归验证：无用户写入时导出配额释放后的重新导出与消费（2026-11-10 17:47–17:50）

复用已停止的 b19 三副本数据 shard 集群 `/home/mo/jxm/logservice-main-b19ef-long-partition-20261109/`，`truncate-interval=1s`、`max-exported-snapshot=4`、默认 5 分钟 HAKeeper store 过期阈值。从各自旧盘启动后，数据 leader 为 node0，普通副本 node1 已持有 4 个有效外部导出快照（index 5486、5487、5489、5490），数据 shard latest LSN 为 5492。本轮在故障和恢复观察阶段**没有启动用户写入负载**。17:47:21 只断开 node1 的测试 veth，node0/node2 两票继续运行；node1 于 17:47:24 因本地 `getTruncatedLsn` 超时触发配额释放，删除最新导出 index **5490**，计数 **4→3**，其余三个快照仍保留。17:47:51 恢复 node1 网络后，三个地址最终恢复原三成员视图；node1 于 17:48:16 重新导出 index **5494**，计数回到 4。

随后显式提交安全的 `Truncate(5494)`，node1 于 17:49:10 **导入并删除刚重新导出的 index 5494**；三地址客户端查询的截断水位推进到 5494。再进行 20 轮 Append→Read payload 核对，**20/20** 成功、Append/Read 重试均为 0，最大整轮 **901.150 ms**。三副本各自原盘冷重启后，三个地址均从 LSN 5495 读到本轮 LSN 5501–5520 的 20 条记录，按 LSN/payload 与 ACK **20/20** 一致，文本 SHA256 均为 `77ca240966fcaafbc84944d4ceed86d1b62f1584e5d1a87249af0bcbe6675eb0`。证据在远端同目录 `quiescent-reexport-20261110.{before-latest,down,up,truncate-start,truncate,cycle,cold-node0.read,cold-node1.read,cold-node2.read}`、node1 导出/导入日志及快照目录；节点与测试网络均已清理。

这轮验证了**无用户写入**时配额释放后能重新导出并消费快照，没有发现新的数据错误。但数据 shard 的物理 latest LSN 在隔离与恢复期间从 5492 前进到 5494，即使没有用户 Append 也有内部 Raft 记录变化；因此不能据此单独证明 `snapshotIndex` 下调是重新导出的必要条件，也不能把本轮称为“应用 index 完全不变”的测试。

## 过期 TN 回放水位对数据 shard 快照导入的影响（2026-11-10 17:57–18:02）

在 51 上以 b19 版新建三副本空盘集群 `/home/mo/jxm/logservice-main-b19ef-tn-expiry-20261110/`，数据 shard 1，`truncate-interval=1s`、导出配额 4、Log store 过期阈值 5 分钟。使用 `test/logservice-chaos/tn_heartbeat_probe/` 向 HAKeeper **人工注入一次 TN heartbeat**，设置 `ReplayedLsn=1`；该探针不启动真实 TN/TAE，也不生成 checkpoint 文件。先完成 120/120 轮 Append→Read，`Truncate(10)` 后再完成 120/120 轮，三个副本均产生导出快照。随后 `Truncate(136)` 成功，三节点均记录回放水位低于目标 index，拒绝导入。

TN heartbeat 超过默认 10 秒有效期后，控制面 `GetClusterDetails` 报 `TimeoutState`；在 tick 803 和 1064 查询时，这个 TN 仍在集群状态里，回放水位保持 1。期间新增 80/80 轮 Append→Read 成功、没有重试、最大周期 190.408 ms，但三个副本在一分钟内分别记录约 48/49/49 次快照导入否决，导出目录持续各保留 4 个。将**同一个人工 TN** 的回放水位更新为 200 后，三副本立即导入 index 133/136 的可用快照，截断水位推进至 136。第二轮使该 heartbeat 再次过期，提交 `Truncate(337)` 后三节点仍因水位 200 拒绝导入；人工将水位提高到 400 后三节点导入 index 337。随后 30/30 轮 Append→Read 成功，最大周期 741.129 ms，集群已停止并清理网络。

该行为在两次过期、恢复循环中稳定出现，说明**过期状态本身不解除快照导入的 TN 回放水位保护**。这也可能是保护仍需从旧 checkpoint 恢复的 TN：不能仅凭过期就安全丢弃其 WAL。第二轮人工设置的 400 还高于当时 data shard latest LSN 341，只用于验证门槛，不代表真实 TN 可达状态。因此本轮只记录潜在长期保留快照/日志的活性风险，尚不认定为生产缺陷或建立 issue。证据保存在远端该目录的 `baseline.cycle`、`expired.cycle`、`final.cycle`、`tn-heartbeat.*` 和节点日志。

## 最新 upstream/main `45917e803f`：三副本故障、TN WAL 截断与恢复（2026-11-11 09:49–09:56）

将 upstream/main 更新至 `45917e803f2915f6f0838b2b2fe9f591fc5b6018`，包含 `39e470bad8` 的 LogService required LSN、WAL 截断及故障切换恢复修复。按这一提交重建 Go 服务、直接客户端、真实 `logservicedriver` 探针和更新过头文件接口的 `libmo.so`；四个二进制/库的本地与 51 远端 SHA256 一致。服务 SHA256 为 `c5965df87d5236efbf35778c605febf7dfcfb4501eb9fcfe1473e83aa5f64445`。51 上 `/home/mo/jxm/mo-current` 已指向 `/home/mo/jxm/mo-upstream-main-45917e803f-20261111/`。构建未注入 Git 版本变量，因此 `mo-service -version` 的 commit 字段为空，版本以源码检出和哈希绑定。两个隔离三副本集群使用独立空目录、网络命名空间、数据 shard 1 与配置为 5 分钟的 Log store 过期阈值；未启动 TN/CN。

**直接客户端集群** `/home/mo/jxm/logservice-main-45917e803f-20261111/`：三地址 `198.18.51.10:59001`、`.11:59011`、`.12:59021` 一致发现 leader node2。无故障 Append→Read **40/40**，零重试。持续 180 轮写读时杀 node2 5 秒、恢复后杀普通副本 node0 约 7 秒，两票始终在线；在 node0 下线时 `Truncate(99)` 成功。负载 **180/180** 完成，Append 共 3 次重试、Read 无重试，最大完整写读周期 **4.328 秒**。整组三副本原盘冷重启后，从三个单独 RPC 地址各读取截断位点后的 135 条用户记录；其中本轮已确认的 134 条按 LSN/payload 在三个地址均 **134/134** 匹配。

**仅写 TN WAL 的集群** `/home/mo/jxm/logservice-main-45917e803f-driver-20261111/`：三地址端口 59201/59211/59221，使用同提交编译的真实 `logservicedriver`，从 `ReplayForWrite` 开始写入并每 20 条调用一次 `Truncate(DSN-10)`；该调用提交后台截断意图。基线 80/80 条确认，重新打开 driver 回放剩余 30 条，DSN 连续至 80。接着持续写入 180 条，杀 leader node0 5 秒、恢复后杀普通副本 node2 5 秒；**180/180** 条确认，最大单条确认 **7.603 秒**，没有超过 10 秒的写入。新 driver 回放剩余 30 条、DSN 连续至 260；三节点整组原盘冷重启后再次得到相同结果，三个进程都保持运行。最后在另外两票互通时隔离 leader node0 的网络 8 秒，持续写入 100 条并继续提交截断意图；**100/100** 确认，最大单条 **7.854 秒**，恢复后回放剩余 30 条、DSN 连续至 360。

故障窗口可见预期的连接失败、超时及 driver 慢写日志；进程关闭时最后一次异步截断意图出现 `context canceled`，重开 driver 的回放与水位检查均通过。冷重启节点日志中也有 `shard already exist` 启动错误，但此次三个服务进程继续运行并形成多数派，与前文记录的旧 HAKeeper ID 启动问题同类，未单列新缺陷。**本轮没有发现新的数据错误或一分钟长尾。** 证据在两个远端目录的 `baseline.cycle`、`fault.cycle`、`fault.truncate`、`cold-read-*.log`、`baseline.log`、`fault.log`、`net.log`、各次 `*replay.log` 与节点日志。两个测试集群和网络命名空间均已停止/清理；上述 `mo-current` 仅保留新版本文件指向。

## 最新 upstream/main `45917e803f`：TN WAL 原子截断遇到 leader 切换及模糊成功（2026-11-11 10:03–10:11）

在 51 上新建空盘三副本集群 `/home/mo/jxm/logservice-main-45917e803f-wal-truncate-20261111/`，数据 shard 1，RPC 端口 59401/59411/59421，`mo-service` 和客户端均基于 `45917e803f`。扩展 `test/logservice-chaos/tn_driver_probe/wal_probe/main.go` 的客户端包装层，使它能在新接口 `TruncateWAL(PSN, checkpointedDSN)` **调用前**和**服务端成功返回后**设屏障；后者还能仅在第一次成功回包处注入 `context.DeadlineExceeded`，模拟已提交但回包丢失。业务路径仍使用真实 `wal.NewLogserviceHandle`、`ReplayForWrite`、`AppendEntry(GroupUserTxn)`、`RangeCheckpoint` 和后台 `logservicedriver` 截断；没有启动 TN/CN 或加载真实 TAE checkpoint 文件。测试期间另外两票始终互通。

**请求调用前切换 leader。** 探针在第一次 `TruncateWAL(psn=211, checkpointedDSN=10)` 调用前暂停；已确认 user LSN 21 后，杀数据 leader node2，立即放行请求，约 5 秒后从原盘恢复 node2。120/120 条 user WAL 和 6 次 checkpoint 均成功；第一次后台物理截断重试 1 次，约 **17.206 秒**后完成，前台写入最大 **3.696 秒**，没有超过 10 秒。独立新 WAL 实例报告 `user_lsn=120, checkpointed=110, truncated_dsn=98`；保留回放的 LSN 91–120 共 30 条与 ACK 逐条匹配。整组三副本冷重启后水位和这 30 条记录不变。第一次优雅停止全组三节点时，最后存活的 log 节点等待配置的 1 分钟 shutdown 阶段超时才退出；重启脚本最初只等 20 秒，此后等节点实际退出再启动，不将该脚本失败算作数据缺陷。

**提交成功后丢回包并切换 leader，短运行。** 同一集群继续写入。探针确认服务端已提交 `TruncateWAL(psn=908, checkpointedDSN=142)`，暂停返回；此时杀 leader node2，释放包装层并把本次成功转换为一次测试注入的 deadline 错误，5 秒后恢复 node2。80/80 条 user WAL 均确认；旧 driver 在结束关闭时取消了尚未完成的后台重试。新 driver 从服务端恢复到 `user_lsn=200, checkpointed=190, truncated_dsn=142`，回放的 LSN 131–200 共 70 条与此前 ACK 全部匹配。三节点强制冷重启后水位和记录完全相同。此轮证明新实例可以从持久状态恢复，不能据此称旧实例已完成重试。

**相同模糊成功窗口，保持旧 driver 长运行。** 再写入 200 条，启用 100 ms 轮间间隔，定期 checkpoint。服务端成功提交 `TruncateWAL(psn=1666, checkpointedDSN=230)` 后丢首次回包并杀 leader node2；约 5 秒后恢复。旧 driver 在约 **4.091 秒**、3 次重试后通过 `GetWALTruncate` 读回持久 `(PSN 1666, checkpointed DSN 230)`，把该次模糊结果判为成功，后续后台物理截断继续推进。200/200 条 user WAL 确认，前台单条最大 **3.427 秒**、无超过 10 秒的写入。新实例回放到 `user_lsn=400, checkpointed=390`，保留的 LSN 371–400 共 30 条与 ACK 一致；整组强制冷重启后水位和记录不变。

三轮均未出现数据缺失、DSN 回放缺口或一分钟前台长尾。回放探针会打印 checkpoint 水位之前尚未物理截断的旧记录；真实 TAE 会按 checkpoint LSN 过滤，不能把打印出的全部记录算作需要重新应用。提交成功后丢回包是**测试包装层注入**，明确验证了 driver 的模糊成功处理，但不等同于证明实际网络必然产生同一错误形态。本轮证据在远端该目录的 `wal.log`、`replay.log`、`cold-replay.log`、`ackloss*.log`、`ackloss*replay.log`、节点日志及运行脚本；节点和测试网络已清理。

**探针路径更正（2026-11-11 10:21）。** 上述三轮启用故障钩子时，测试包装层只暴露 `logservice.Client` 接口，而 `AppendWAL` 是该接口之外的可选方法；`logservicedriver` 的类型断言因此失败并回退到普通 `Append`。WAL driver、checkpoint、`TruncateWAL` 与回放逻辑仍是生产代码，记录内容和截断结果的核对有效；但上述写入耗时**不能代表**新 `AppendWAL` 的 7 秒提议期限及其重试表现。已在探针包装层显式转发可选 `AppendWAL`；下轮用修正后的路径重新测相关故障。

## 最新 upstream/main `45917e803f`：落后副本 Raft snapshot 追赶时切走 leader（2026-11-11 10:23–10:26）

在 51 上以空目录新建三副本集群 `/home/mo/jxm/logservice-main-45917e803f-snapshot-catchup-20261111/`，数据 shard 1，RPC 端口 59601/59611/59621，服务与探针均基于 `45917e803f`。修正 `concurrent_probe` 包装层以显式转发可选 `AppendWAL`，确保 driver 使用新版本的 WAL 专用提议期限；测试层还记录失败或超过 3 秒的底层尝试。使用真实 `wal.NewLogserviceHandle` 和 `ReplayForWrite`，32 个并发写入者共计划 6400 条 user WAL，每约 200 条确认发起一次带 `GroupFiles` marker 的 checkpoint，并保留至少 300 条连续确认的 user LSN。未启动 TN/CN 或加载真实 TAE checkpoint 文件。

起始数据 leader 为 node2。约 500 条确认时杀普通副本 node0，保持 node1/node2 两票写入与 checkpoint；到约 5000 条确认时从旧盘恢复 node0。node2 于 **10:25:07.141** 开始向 node0 发送数据 shard Raft snapshot index **2563**，日志明确写明 `entry compacted`；约 **46 ms** 后杀 node2。node1 在新 term 中于 10:25:11.039 向 node0 发送数据 shard snapshot index **2579**，node0 于 10:25:11.492 记录开始恢复该 snapshot。node0/node1 两票继续运行至负载结束，node0 随后还导入外部导出快照 index 3124、3209。

最终 **6400/6400** 条 user WAL、**29/29** 次 checkpoint 成功；底层 `AppendWAL` 记录到 **31** 次失败尝试，均未向上层形成最终写入错误。单条确认 p99 **1.200 秒**、最大 **5.934 秒**，没有超过 10 秒的写入。保持 node2 下线时，独立新 WAL 实例回放到 `user_lsn=6400, checkpointed=6100, truncated_dsn=6056`；checkpoint 之后必须保留的 LSN **6101–6400** 与 ACK 的 LSN/payload **300/300** 一致，没有缺口。恢复 node2 后整组三副本强制冷重启，再次得到相同水位，回放输出的 **396** 条记录与两票时逐条一致。证据在远端该目录的 `load.log`、`two-votes-replay.log`、`cold-replay.log`、`run-catchup.sh` 及三节点日志。节点和测试网络已清理。本轮未发现新的数据错误或一分钟写入长尾；Raft snapshot 与后续外部快照消费均在目标副本的真实日志中得到确认。

## 最新 upstream/main `45917e803f`：旧 leader 的 RPC 可达而 Raft 无法提交（2026-11-11 10:39–10:41）

51 上另建空盘三投票副本集群 `/home/mo/jxm/logservice-main-45917e803f-raft-only-20261111/`，数据 shard 1，RPC 端口 59801/59811/59821，Log store 过期阈值设为 5 分钟。使用已修正可选 `AppendWAL` 转发的真实 TN WAL driver 探针，32 个并发写入者共计划 5120 条 user WAL，每约 200 条确认发起一次带 `GroupFiles` 测试 marker 的 checkpoint，并保留至少 200 条连续确认的 user LSN。初始数据 leader 为 node1。约 311 条确认后，仅在 node1 的测试网络命名空间加入四条 TCP DROP 规则：丢弃 node0/node2 到 node1 Raft 端口 59810 的入站包，以及 node1 到两个普通副本 Raft 端口 59800/59820 的出站包；**RPC 59811 和 gossip 保持可达，另外两票互通**。

规则持续约 **50 秒**。隔离 20 秒时 ACK 已到 1004，隔离 50 秒时到 2932；旧 leader 的 RPC 地址仍响应 shard 查询并报告新 leader 为 node2。撤销前四条规则分别命中入站 **19/42** 包、出站 **150/195** 包，确认 Raft 故障确实生效。随后撤销规则，持续负载 **5120/5120** 条确认、**24/24** 次 checkpoint 成功；记录 **20** 次底层 `AppendWAL` 超时尝试，但无上层写入或 checkpoint 最终错误。超时尝试约 **6.96–6.99 秒**，随后由 driver 重试；单条上层确认 p99 **2.939 秒**、最大 **8.339 秒**，没有超过 10 秒或一分钟的写入长尾。

独立新 WAL 实例回放得到 `user_lsn=5120, checkpointed=4920, truncated_dsn=4774`；checkpoint 后必须保留的 LSN **4921–5120** 共 200 条与 ACK 的 LSN/payload **200/200** 一致。整组三副本强制冷重启后水位不变，回放输出的 **390** 条记录与重启前逐条一致。证据在远端该目录的 `load.log`、`old-leader-rpc-status.log`、`replay.log`、`cold-replay.log`、`run-raft-only.sh` 和节点日志；规则、进程及网络命名空间均已清理。本轮是旧 leader 仍可接 RPC、另外两票仍能提交时的真实 `AppendWAL` 重试验证；未发现新的长尾或数据缺陷。

## 最新 upstream/main `45917e803f`：TN WAL 双节点短暂停机、批量 Close 与故障中回放（2026-11-11 10:46–10:54）

在 51 上以空目录新建三副本集群 `/home/mo/jxm/logservice-main-45917e803f-two-down-20261111/`，数据 shard 1，RPC 端口 60201/60211/60221，Log store 过期阈值 5 分钟。探针调用真实 `wal.NewLogserviceHandle`、`ReplayForWrite`、`AppendEntry(GroupUserTxn)`、`RangeCheckpoint`、`Close` 与新版本可选 `AppendWAL`；没有运行 datasync、真实 TN/CN，也没有加载真实 TAE checkpoint 文件。

**持续负载期间短暂失去多数派。** 16 个并发写入者计划写 1920 条，约每 100 条确认后提交带 `GroupFiles` 测试 marker 的 checkpoint，保留至少 100 条连续确认的 user WAL。起始数据 leader 为 node1；确认数到 212 时同时杀 node1 和 node0，约 8 秒内只剩 node2 一票，确认数停在 212，没有错误地报告新成功。恢复 node0、保持 node1 下线后，node0/node2 两票完成 **1920/1920** 条确认和 **18** 次 checkpoint。底层 `AppendWAL` 有 **27** 次失败尝试，driver 最终重试成功；上层无写入、checkpoint 错误。单笔确认 p99 **3.082 秒**、最大 **9.937 秒**，无超过 10 秒或 1 分钟的写入。两票独立回放报告 `user_lsn=1920, checkpointed=1820, truncated_dsn=1755`；回放的 195 条记录与已确认 LSN/payload **195/195** 一致。

**已接收 WAL 的批量 Close 同时短暂失去多数派。** 在同一集群恢复三票后，新实例先完成 `ReplayForWrite`，接收 100 条 user WAL（LSN 1921–2020），在底层 `AppendWAL` 前设屏障，启动每条记录的 `WaitDone` 和 driver `Close`。然后杀 node1/node2，仅保留 node0 约 9 秒；释放写入屏障后恢复 node1。底层 `AppendWAL` 共记录 **52** 次连接失败、连接重置或 timeout 尝试；`Close` 约 **13.385 秒**返回 nil，**100/100** 个 `WaitDone` 均返回 nil。整组三节点原盘强制冷重启后，新 driver 回放报告 `user_lsn=2020, checkpointed=1820, truncated_dsn=1755`；这 100 条记录按 LSN/payload **100/100** 匹配，无重复。屏障包装层显式转发可选 `AppendWAL`，确保测到生产 driver 的 WAL 专用请求路径。

**故障中回放。** 冷重启后，三个 RPC 地址一致报告数据 leader 为 node1。新 driver 在成功读取 WAL 截断水位、即将读取日志时被测试屏障暂停；杀 node1，保持 node0/node2 两票，放行回放。`ReplayForWrite` 约 **3.616 秒**完成，水位仍为 `user_lsn=2020, checkpointed=1820, truncated_dsn=1755`。首次尝试因测试包装层仍拦截旧 `GetTruncatedLsn`，没有触发该屏障，故不计入故障验证；随后改为拦截当前回放使用的 `GetWALTruncate` 并重新编译执行，实际命中屏障。

证据在远端上述目录的 `load.log`、`two-votes-replay.log`、`close-load.log`、`cold-replay.log`、`replay-leader-fail.log`、三份 `run-*.sh` 和节点日志。三个节点与测试网络命名空间均已停止、清理。本轮未发现新的 WAL 丢失、重复、挂起或一分钟长尾。测试仅覆盖 TN WAL driver API 与 LogService，`GroupFiles` 使用测试 marker，不验证真实文件服务内容。

## 最新 upstream/main `45917e803f`：TN WAL 持续回放切换为写入并接管旧 writer（2026-11-11 11:28–11:33）

继续使用 51 上独立三副本集群 `/home/mo/jxm/logservice-main-45917e803f-two-down-20261111/`，从上一轮的 `user_lsn=2020` 开始。新增 `test/logservice-chaos/tn_driver_probe/promotion_probe/main.go`，使用真实 `wal.NewLogserviceHandle`、`Replay` 的动态 mode getter、`AppendEntry`、`RangeCheckpoint` 与 `Close`，先以 `ReplayMode_ReplayForever` 持续回放，外部文件触发切换至 `ReplayMode_ReplayForWrite`。这是 TAE 从持续回放切换到可写的同一 driver 状态转换；探针只控制切换时机。没有运行 datasync、真实 TN/CN 或加载真实 TAE checkpoint 文件。

**切换时杀 leader。** 三票启动后，数据 leader 为 node0。回放者应用到首条保留 WAL 后杀 node0，并请求切换可写；node1/node2 两票互通。约 **7.436 秒**后 `Replay` 返回，driver 报告原水位 `user_lsn=2020, checkpointed=1820`；随后确认 **100/100** 条新 WAL（LSN 2021–2120），提交一次 checkpoint 到 LSN 2100。node0 仍下线时，独立新 driver 回放确认这 100 条的 LSN/payload **100/100** 匹配。

**切换时短暂失去多数派。** 先保持 node0 下线，在 node1/node2 两票下启动另一持续回放者；回放开始后再杀 node1，只留 node2 一票并请求切换。单票约 **8 秒**期间新写入 ACK 为 **0**。恢复 node1 后，切换约 **9.492 秒**完成，随后确认 **50/50** 条新 WAL（LSN 2121–2170），checkpoint 到 LSN 2150。

**活跃 writer 与持续回放者交接。** 三票恢复后，旧 writer 使用真实 TN WAL driver 并发写入 **640/640** 条（LSN 2171–2810）；第二个 driver 同时以持续回放模式追尾，在旧 writer 约确认 145 条时，回放者已应用到 LSN **2300**。此时杀数据 leader node2，node0/node1 两票继续服务；旧 writer 完成余下写入并关闭。只在旧 writer 退出后才请求回放者切换可写；它报告已回放到旧 writer 最终 LSN **2810**，随后确认 **50/50** 条新 WAL（LSN 2811–2860），checkpoint 到 LSN 2840。这样避免了两个可写 driver 同时分配 DSN，符合单个 TN writer 交接的使用顺序。

保持 node2 下线时，独立 `ReplayForWrite` 返回 `user_lsn=2860, checkpointed=2840, truncated_dsn=1755`；第二轮的 50 条、旧 writer 的 640 条和接管后的 50 条分别按 LSN/payload **50/50、640/640、50/50** 匹配。随后三节点各自原盘强制冷重启，再次回放到相同水位；本轮四段写入合计 **840/840** 条与 ACK 逐条一致，其中 checkpoint 水位 2840 之后必须保留的 LSN 2841–2860 **20/20** 一致。四段负载的单条确认最大耗时分别为 **0.672、0.416、7.685、0.228 秒**；本轮未见 DSN 缺口、重复回放、写入错误或一分钟长尾。

证据保存在远端该目录的 `promotion.log`、`promotion-one.log`、`live-writer.log`、`live-promotion.log`、`promotion-two-votes-replay.log`、`live-two-vote-replay.log`、`live-cold-replay.log`、三份 `run-*promotion*.sh` 和节点日志。三个节点与测试网络命名空间已停止、清理。checkpoint 使用测试 `GroupFiles` marker；记录匹配证明 WAL 路径正确，不代替真实 TAE checkpoint 文件加载验证。

## 最新 upstream/main `45917e803f`：TN WAL 客户端池饱和时 Close 遭遇双节点停机（2026-11-11 11:49–11:51）

复用 51 上独立三副本集群 `/home/mo/jxm/logservice-main-45917e803f-two-down-20261111/`，从前轮 `user_lsn=2860` 开始。将测试探针 `close_probe` 的允许条数上限从 5000 提到 10000，本轮使用 **8000** 条；仍调用真实 `wal.NewLogserviceHandle`、`ReplayForWrite`、`AppendEntry`、`WaitDone`、`Close` 和底层 `AppendWAL`，只在请求出发前用测试屏障控制时机。没有运行 datasync 或真实 TN/CN。

探针已接收 LSN **2861–10860** 共 8000 条、启动每条记录的 `WaitDone` 和 driver `Close` 后，杀 node0/node1，释放底层写入屏障，仅保留 node2 一票约 8 秒。单票期间完成的 `WaitDone` 为 **0**；恢复 node0 后 `Close` 约 **14.270 秒**返回 nil，**8000/8000** 个 `WaitDone` 返回 nil。底层共记录 **249** 次成功 `AppendWAL` 与 **279** 次失败尝试；这些是物理批次/重试，不是用户记录数。日志有一次获取写入客户端等待 **11.878 秒**，说明该轮确实触及客户端池压力，并非仅把 8000 条全部合成少量提交。

保持 node1 下线的两票回放得到 `user_lsn=10860, checkpointed=2840, truncated_dsn=1755`，8000 条按 LSN/payload **8000/8000** 匹配。整组三节点各自原盘强制冷重启后得到相同水位，再次 **8000/8000** 匹配。证据在远端该目录的 `close-sat.log`、`close-sat-two-vote-replay.log`、`close-sat-cold-replay.log`、`run-close-saturation.sh` 和节点日志。本轮没有发现已接收 WAL 的最终结果丢失、关闭挂起或一分钟写入长尾。

## 最新 upstream/main `45917e803f`：后台物理 TruncateWAL 窗口整组冷停机（2026-11-11 11:51–11:54）

在同一集群使用真实 TN WAL `wal-probe` 连续确认 **200/200** 条新记录（LSN 10861–11060），每 20 条提交一次带 `GroupFiles` 测试 marker 的 checkpoint，并保留最近 10 条。第一次后台 `TruncateWAL(psn=5794, checkpointedDSN=10912)` 发出前命中测试屏障，已确认 20 条；此时杀全组三个 LogService 进程，释放屏障，5 秒后从各自原盘重启。全停期间 ACK 仍为 20，没有错误地确认写入。第一次截断执行因连接拒绝失败（共 11 次尝试，约 303 ms）；服务恢复后后台在约 3.495 秒内成功推进同一 PSN，后续截断继续推进。旧 driver 最终确认 **200/200** 条、**10** 次 checkpoint intent；进程关闭时最后一次尚未完成的异步截断收到 `context canceled`，没有当作已完成的物理截断。

新 driver 回放得到 `user_lsn=11060, checkpointed=11050, truncated_dsn=11088`；物理截断后仍可回放的本轮 30 条与 ACK **30/30** 匹配，其中 checkpoint 水位之后必须保留的 LSN 11051–11060 **10/10** 匹配。整组再次原盘冷重启后水位与这 10 条保持一致。随后新 driver 再确认 **60/60** 条 WAL、**3** 次 checkpoint intent；最终回放到 `user_lsn=11120, checkpointed=11110, truncated_dsn=11154`，新水位后必须保留的 LSN 11111–11120 **10/10** 匹配，证明恢复实例仍可继续推进写入和截断。

证据在远端该目录的 `truncate-cold.log`、`truncate-cold-replay.log`、`truncate-cold-cold-replay.log`、`truncate-cold-recover.log`、`truncate-cold-recover-replay.log`、`run-truncate-cold-window.sh` 和节点日志。三个节点与测试网络命名空间均已停止、清理。本轮未发现新的 WAL 丢失或回放错误；测试 marker 不代替真实 TAE checkpoint 文件加载验证。

## 最新 upstream/main `45917e803f`：单 TN driver 乱序物理批次、leader 切换与重复记录回放（2026-11-11 11:58–12:03）

复用 51 上隔离三副本集群 `/home/mo/jxm/logservice-main-45917e803f-two-down-20261111/`。给 `concurrent_probe` 增加只暂停**首个**底层 `AppendWAL` 的测试屏障；其余客户端仍调用真实 LogService `AppendWAL`。探针使用真实 `wal.NewLogserviceHandle`、`ReplayForWrite` 和 100 个并发写入者，各写 20 条。暂停仅改变请求到达顺序，不修改 WAL 内容或 driver 的提交代码；未运行 datasync、真实 TN/CN 或加载真实 TAE checkpoint 文件。

第一次尝试用“首批暂停时上层已出现 ACK”判断后续物理批次是否提交，但 30 秒内 ACK 为 0；脚本释放屏障，2000/2000 条正常完成，没有注入节点故障，**不计为故障验证**。这说明仅凭 ACK 观察不能确定后续物理请求的状态。第二次改用直接 LogService `GetLatestLsn` 查询物理 PSN：首个 `AppendWAL` 仍在屏障上时，物理最新 PSN 从 **8889** 升到 **9164**，且已有 **59** 条上层 ACK，确认后续批次已进入日志。此时杀数据 leader node0，再放行首批；node1/node2 两票持续服务。第二轮 **2000/2000** 条确认，底层有 **47** 次失败尝试，driver 最终重试成功。

保持 node0 下线时，新 TN driver 回放到 `user_lsn=15120, checkpointed=11110, truncated_dsn=11154`；两轮已确认的 LSN 11121–15120 共 **4000/4000** 条按 LSN/payload 匹配。回放日志明确识别到 **10 条重复的物理 WAL 记录**（相同 DSN 范围、不同 PSN），没有 DSN 重叠错误，也没有将其重复交付为用户记录。三节点各自原盘强制冷重启后，水位、**4000/4000** 条匹配和这 10 条物理副本的识别结果不变。

新 driver 随后再确认 **200/200** 条 user WAL，按每 20 条提交一次带 `GroupFiles` 测试 marker 的 checkpoint，将 checkpoint 水位推进到 **15270**。物理截断后回放到 `user_lsn=15320, checkpointed=15270, truncated_dsn=15330`；本轮可回放的 70 条与 ACK **70/70** 匹配，水位后必须保留的 LSN 15271–15320 **50/50** 匹配，旧的重复物理记录已不在可回放范围。整组再次原盘冷重启后水位和这 50 条不变，回放未再记录那些旧副本。

证据在远端该目录的 `ooo-load.log`、`ooo2-load.log`、`ooo2-two-vote-replay.log`、`ooo2-cold-replay.log`、`ooo-postcp.log`、`ooo-postcp-replay.log`、`ooo-postcp-cold-replay.log`、`run-ooo-append.sh`、`run-ooo-physical.sh` 与节点日志。三个节点与测试网络命名空间已停止、清理。本轮未发现新数据错误；重复物理记录与故障时已提交、回包丢失后重试的形态一致，用户 WAL 回放保持一次交付。

## 近轮阴性结果的覆盖复查与高并发旧 leader 黑洞（2026-11-11 14:02–14:04）

拉取 upstream/main 后，远端最新为 `553e16f52b`；从 51 上测试二进制的 `45917e803f` 到该提交只有 parser 相关提交，`pkg/logservice`、`pkg/vm/engine/tae/logstore`、`pkg/tnservice` 没有代码差异。因此本轮继续使用 `45917e803f` 的已验证二进制。此前版本已经合入 `d2dab994dd` 的 RPC 停顿超时与连接切换、`ee5e93bcef` 的读写/快照/截断恢复、`39e470bad8` 的 WAL 恢复修复；近几轮主要验证这些修复后的故障恢复路径，不能把阴性结果解释为这些模块从无缺陷。

**新的压力组合。** 在 51 的三副本集群 `/home/mo/jxm/logservice-main-45917e803f-two-down-20261111/` 使用默认 100-client 的真实 TN WAL driver，100 个并发写入者共计划 12000 条 user WAL，每约 300 条确认发起带 `GroupFiles` 测试 marker 的 checkpoint，保留最近 300 条连续确认记录。热身至 500 条后，确认数据 leader 是 node0；当时 `ss` 记录约 **44** 条到 node0 RPC 端口的连接。随后只在 node0 的网络命名空间丢弃来自客户端 bridge 的 RPC 入站包，并双向阻断 node0 与 node1/node2 的 Raft 通道；node1/node2 互通、各进程均运行。规则持续约 **60 秒**，RPC DROP 命中 **779** 包，四条 Raft DROP 分别命中 **33、39、274、268** 包，证明故障生效。故障 20 秒时 ACK 为 1299，60 秒时为 7788；撤销规则后继续至 **12000/12000** 条确认、**39** 次 checkpoint intent，底层记录 **49** 次失败 `AppendWAL` 尝试，无最终用户写入或 checkpoint 错误。单条确认 p99 **5.238 秒**、最大 **11.305 秒**，**98** 条超过 10 秒、**0** 条超过 60 秒。

保持 node0 下线、只留 node1/node2 两票时，独立新 driver 回放为 `user_lsn=27320, checkpointed=27020, truncated_dsn=15962`；本轮可回放的 384 条与 ACK **384/384** 匹配，其中 checkpoint 水位后的 LSN 27021–27320 **300/300** 匹配。三节点各自原盘强制冷重启后水位不变，这 300 条再次 **300/300** 匹配。故障规则、节点和测试网络命名空间已清理。证据在远端同目录 `blackhole100-load.log`、`blackhole100-preconn-count`、`blackhole100-two-vote-replay.log`、`blackhole100-cold-replay.log`、`run-leader-blackhole-100.sh` 及节点日志。

**为何最近没有新缺陷及剩余盲区。** 本轮显示旧连接黑洞会带来秒级尾部，但当前 8 秒底层 RPC 读超时、重新发现与 driver 重试在这一组合下没有退化成旧版约 60 秒单次等待。此前多数进程停机实验仅持续 5–9 秒，验证的是恢复与多数派语义，不能覆盖持续超过 driver 默认 3 分钟重试检查窗口的长期故障。现有探针以 ACK、可回放 LSN/payload 和测试 checkpoint marker 为主要 oracle；它能识别 WAL 丢失、重复或水位错误，但不能验证真实 TAE checkpoint 文件的生成/加载、事务可见性、SQL 结果，也不能仅凭成功 ACK 排除所有未确认但已提交的请求。历史报告已记录旧版实际缺陷和修复后的回归结果；本轮没有发现新的数据正确性缺陷或一分钟长尾。

## 最新 upstream/main `45917e803f`：TN driver 并发乱序 Append、进程崩溃与补空洞（2026-11-11 14:21–14:27）

继续使用 51 上隔离的三副本数据 shard 1 集群 `/home/mo/jxm/logservice-main-45917e803f-two-down-20261111/`，地址为 `198.18.51.10:60201`、`.11:60211`、`.12:60221`。`upstream/main` 当前是 `553e16f52b`，两版之间 LogService、TAE logstore 和 TN service 源码无差异。仍使用真实 `wal.NewLogserviceHandle`、`ReplayForWrite`、并发 `AppendEntry`、底层 `AppendWAL`、`RangeCheckpoint` 和 WAL 回放；只在**第一个**底层 `AppendWAL` 发出前设测试屏障。没有启动真实 TN/CN，也没有运行 datasync。

**构造与复现。** 每轮启动 100 个并发写入者，每人计划写 8 条；首个 `AppendWAL` 停在屏障，其他请求继续。直接查询 LogService `GetLatestLsn`，待物理 PSN 比屏障前至少增加 20，再用 `SIGKILL` 结束旧 driver 进程，始终不释放该屏障。随后创建新 driver 执行 `ReplayForWrite`，核对旧进程已打印 ACK 的 `(user LSN, payload)`；除第二轮外，新 driver 再写 20 条、原盘冷重启服务并复查。前六轮三票运行；后两轮先杀普通副本 node2，只留 node0/node1 两票，在每轮中只冷重启这两票。每轮的进程终止都发生在首个请求尚未发送、后续物理 PSN 已增长的窗口，形成实际的物理乱序和未发送/未完成批次。

八轮的物理 PSN 在屏障期间分别增加 **223、353、224、273、270、283、177、152**；`Wal-Replay-Append-Skip-Entries` 分别记录 **15、3、59、9、3、12、2、2** 个 DSN→PSN 补空洞映射，合计 **105** 个。按进程结束后落盘的输出统计，旧进程已确认的 user WAL 在第 1/3/6 轮分别为 **50/91/14** 条，其他轮为 0；这 **155/155** 条在新 driver 回放中按 LSN 和内容匹配，回放结果按 user LSN 严格递增且无重复。某些未收到 ACK 的写入也可能在故障前完成物理提交；本轮只对有 ACK 的记录作持久性承诺。第 1、3–8 轮的回放记录集在对应的原盘冷重启后完全一致；第 2 轮没有单独做紧随其后的冷重启，但后续多次冷重启和最终三票回放结果与该轮回放一致。

恢复 node2 后，三票最终回放到 `user_lsn=27615, checkpointed=27020`。水位后 LSN **27021–27615** 共 **595** 条连续、无缺失或重复，其中上述八轮新写入的 **155** 条已确认记录与每轮新实例写入的 **140** 条记录均可核对。再用新 driver 写入 **100/100** 条，并每 20 条提交一次带测试 `GroupFiles` marker 的 checkpoint，保留最近 20 条；新实例和三节点整组原盘冷重启后均报告 `user_lsn=27715, checkpointed=27695, truncated_dsn=27867`，保留的 LSN **27696–27715** 与最后 20 条 ACK **20/20** 精确匹配。

证据位于远端 `ooo-gap-20261111/` 的各轮 `*.load.log`、`*.replay.log`、`*.cold-replay.log`、`*.write.log`，以及 `final-three-vote-replay.log`、`post-checkpoint*.log`；复现脚本是集群根目录的 `tn_ooo_rounds.py`。这是 driver WAL 层的真实调用顺序与故障恢复验证；测试 `GroupFiles` marker 仍不等同于真实 TAE checkpoint 文件。八轮 WAL 故障测试未发现已确认写入丢失、回放乱序、重复交付或空洞处理错误，也没有据此建立新缺陷。所有测试节点和网络命名空间已停止、清理。

## 最新 upstream/main `45917e803f`：真实 TAE checkpoint 文件加载遇到 LogService leader 停机（2026-11-11 14:33–14:34）

在 51 上以**新空盘**建立三副本集群 `/home/mo/jxm/logservice-main-45917e803f-tae-checkpoint-20261111/`，数据 shard 1，RPC 端口 60401/60411/60421、Log store 过期阈值 5 分钟。没有启动 TN 服务，而是在同机测试进程内打开真实 TAE DB，并通过 `options.WithWalClientFactory` 将其 WAL 接到真实 LogService 三地址客户端。测试调用仓库的 `testutil.CreateRelAndAppend2`，它同时写入 `mo_catalog` 系统目录行，创建一张 50 行表，确认事务可扫描，然后强制生成增量 checkpoint 文件。`tae_checkpoint lsn=1 meta_files=1` 后在测试屏障停住；此时杀数据 leader node0，保留 node1/node2 两票，再放行 TAE 关闭与重开。

重开日志明确读取 `meta_0-0_1794378830125939584-0.ckp` 并执行 `Replay-Catalog`；查询仍扫描到 **50/50** 行，checkpoint LSN 为 1。之后通过同一 TAE DB 创建 `after_restart` 数据库，第二次关闭重开，原表仍是 **50/50** 行且新数据库可见；测试进程整体 **21.54 秒** PASS。这个结果覆盖真实 checkpoint 文件生成、加载、目录恢复、两票期间 WAL 重新连接与事务读写，不代表已覆盖真实 TN 服务的生命周期或 SQL 层。

最初的本地最小探针使用 `CreateRelAndAppend`，只建了内存目录项、没有写 `mo_catalog` 行；它在 checkpoint 后回放找不到表。改用生产更接近的 `CreateRelAndAppend2` 后本地 mock 测试通过，并才执行上述 51 远端实验。因此初版失败是测试夹具缺失系统目录写入，**不列为产品缺陷**。远端证据是 `tae-first.log`、三个节点日志和测试二进制；对应探针源码在测试工作树的 `test/logservice-chaos/tn_driver_probe/tae_checkpoint_probe/`。本轮没有发现新的数据错误；测试节点和网络命名空间已停止、清理。

## 最新 upstream/main `45917e803f`：旧 leader 网络隔离超过 3 分钟，两票持续写入（2026-11-11 14:36–14:41）

复用 51 上隔离集群 `/home/mo/jxm/logservice-main-45917e803f-two-down-20261111/`。先确认 node2 为数据 leader，再启动真实 TN WAL driver 的 100 个并发写入者（每人计划 1000 条，每条后间隔 20 ms），每约 300 条 ACK 提交一次带测试 `GroupFiles` marker 的 checkpoint，保留最近 300 条。热身到 510 条 ACK 后，使用 node2 网络命名空间中的五条 `iptables DROP` 规则阻断 node2 与 node0/node1 的双向 Raft 通道，以及客户端到 node2 RPC 的入站包；node0/node1 保持互通并选举出新 leader。持续隔离 **220 秒**，超过 driver 默认的 3 分钟重试检查窗口；结束时规则计数分别命中 **862** 个客户端入站包和 **194/25/453/318** 个 Raft 方向包，证明故障仍在生效。

隔离开始 20/180/220 秒时，已确认写入分别为 **1607/24800/30774** 条；清理网络规则并结束旧 driver 时共有 **30874** 条 ACK、**95** 次 checkpoint ACK、**36** 次底层 `AppendWAL` 失败尝试，**0** 个最终 Append 错误。单条确认 p99 **2.292 秒**、最大 **10.409 秒**，仅 1 条超过 10 秒、**0** 条超过 60 秒。独立新 driver 回放到 `user_lsn=58689, checkpointed=58289, truncated_dsn=58041`；checkpoint 水位之后 **58290–58689** 共 **400** 个 LSN 连续、无重复，其中旧进程已确认且应保留的 **300/300** 条按 LSN/payload 匹配。结束时主动 `SIGKILL` 旧进程，因此水位之后另有尚未打印 ACK 但已提交的记录，不把它们算作客户端已确认写入。两票始终在线，所以这轮验证的是持续故障下**各次请求能不断切到可用多数派**；不证明单个请求在无多数派时可无限重试。

证据在远端 `long3m-20261111/load.log`、`replay.log`、根目录 `long3m_logservice.sh` 和节点日志。本轮没有出现一分钟长尾或可回放数据缺失。

## 最新 upstream/main `45917e803f`：Append 已提交但旧 driver 未获回包后崩溃（2026-11-11 14:41）

在同一集群的全部三个副本恢复后，扩展测试客户端包装层：让**第一次真实 `AppendWAL` 成功返回**后、把结果交回真实 TN driver 前停在屏障，记录服务端返回的物理 **PSN 33692**。旧 driver 仅计划写 `ambig-postcommit-r1/00/0000` 一条；屏障就绪后不放行，直接 `SIGKILL` 旧进程，因此旧进程 **0** 条 ACK。这是一次确定的“服务端已提交、调用方未得到结果”，不需要推测 TCP 回包是否丢失。

新 driver 的 `ReplayForWrite` 找到该 payload，逻辑 **LSN 58690**。整组三节点原盘冷重启后的回放仍恰好找到一次同一 `(LSN, payload)`；新实例随后 **20/20** 条写入从 LSN **58691** 连续推进到 **58710**，没有将模糊成功的旧记录重复交付。证据在远端 `ambig-postcommit-20261111/load.log`、`replay.log`、`cold-replay.log`、`after.log`，以及测试包装层源码 `test/logservice-chaos/tn_driver_probe/concurrent_probe/main.go`。这是包装层控制的故障时序，不表示网络本身必然以同一方式丢回包；恢复结果符合预期，未建立新缺陷。所有测试节点、隔离规则和测试网络命名空间已停止、清理。

## 最新 upstream/main `45917e803f`：TN driver 并发乱序 Append 追加 16 轮矩阵（2026-11-11 14:52–14:55）

按用户要求在 51 上复用同一隔离集群，再运行**16 轮新的**同一 TN WAL driver 内并发 Append。每轮使用 64–100 个写入者、每人计划 10–30 条；暂停一个真实底层 `AppendWAL` **发送之前**，确认其他请求使 LogService 物理 PSN 至少增加 25/75/125/200 后，以 `SIGKILL` 结束旧 driver。前 12 轮暂停**首个**底层调用，分别覆盖三票运行 4 轮、普通副本先下线 4 轮、物理乱序已形成后再杀 leader 4 轮。追加 4 轮将暂停点改到第 **3/5/10/20** 次底层调用，分别覆盖三票、普通副本下线、leader 下线、三票；该变化使前面的物理批次有机会先确认。整个故障窗口始终至少有两票存活。每轮均由新 driver 执行 `ReplayForWrite`，核对旧进程 ACK 的 `(user LSN, payload)`，再写 10 条并对存活副本做原盘冷重启，比较前后回放记录集；下线副本随后恢复。

16 轮均观察到屏障仍未放行时物理 PSN 前进，并在重启回放中写出非空的 `Wal-Replay-Append-Skip-Entries`，合计 **351** 个 DSN→PSN 补空洞映射。旧进程累计打印 **424** 条已确认 user WAL，逐轮回放及最后的全局回放 **424/424** 按 LSN/payload 匹配；各轮冷重启前后记录集相等、按 user LSN 递增且无重复。第 3/5/10/20 次暂停的四轮分别有 **25/50/88/129** 条 ACK 和 **25/47/72/62** 个补空洞映射，其中暂停第 10 次时叠加了 leader 停机。每轮新实例的后续 **10/10** 条写入成功，合计 **160/160** 条；最终全局回放到 `user_lsn=59294, checkpointed=58289`，水位后 **58290–59294** 共 **1005** 个逻辑 LSN 连续、无缺失或重复。

最后再写 **100/100** 条，并每 20 条提交一次带测试 `GroupFiles` marker 的 checkpoint，保留最近 50 条。新实例及三节点整组原盘冷重启后均得到 `user_lsn=59394, checkpointed=59344, truncated_dsn=59694`；水位后 LSN **59345–59394** 与最后 50 条 ACK **50/50** 精确匹配。物理尚未截断的旧 checkpointed 记录也可能被探针打印，核对时按 checkpoint 水位过滤。

证据在远端 `ooo-repeat-20261111/` 与 `ooo-nth-20261111/` 的 `summary.json`、各轮 `*.load.log`、`*.replay.log`、`*.cold-replay.log`、`*.after.log`，以及 `final-all16.log`、`post-checkpoint*.log`；运行脚本为集群根目录的 `tn_ooo_repeat_matrix.py`、`tn_ooo_repeat_nth.py`。测试屏障只改变底层请求出发时机，真实 TN WAL driver 负责写入、重试、补空洞、回放和 checkpoint；没有运行 datasync 或真实 TN 服务。本轮未发现乱序、空洞、节点切换或重启导致的已确认 WAL 丢失/重复。测试节点和网络命名空间已停止、清理。

## 最新 upstream/main `45917e803f`：并发乱序空洞与 checkpoint/物理截断重叠（2026-11-11 15:03–15:11）

复用 51 上隔离三副本集群 `/home/mo/jxm/logservice-main-45917e803f-two-down-20261111/`，继续用真实 TN WAL driver；100 个写入者并发 Append，同时每约 10 或 20 条 ACK 提交一次带测试 `GroupFiles` marker 的 checkpoint。测试包装层可在第 20/100 次真实 `AppendWAL` 发出前、第一次 `TruncateWAL` 发出前或服务端成功返回后设屏障，不修改 WAL 内容与 driver 实现。每次旧进程被杀后，独立新 driver 回放并按 `(user LSN, payload)` 核对 ACK，再进行两票原盘冷重启。没有启动 TN 服务或运行 datasync。

**尚未确认的 checkpoint 与乱序写入。** 暂停第 20 次底层 `AppendWAL` 时，其他物理日志继续提交，旧 driver 已有 **117** 条 ACK，但 checkpoint ACK 为 0、`TruncateWAL` 还未发出。在此窗口杀旧 driver；新 driver 补 **40** 个空洞并回放 **117/117** 条，按 LSN 递增。随后杀数据 leader，仅保留两票回放，再冷重启这两票；三次回放记录集完全一致，checkpoint 水位保持 59344。这个结果不证明被暂停的调用一定是 checkpoint marker，只说明 checkpoint 尚未确认时旧实例崩溃没有丢失已确认 WAL。

**checkpoint 已确认，物理截断尚未发送。** 暂停第 100 次 `AppendWAL`，同时让第一次 `TruncateWAL(PSN 49949, checkpointed DSN 59910)` 停在发出前；旧 driver 已确认 checkpoint 到 user LSN **59532**、写入 **241** 条。此时杀 leader 和旧 driver，两票新实例读到仍为旧值的物理截断水位，补 **89** 个空洞，按序回放 **241/241** 条；两票原盘冷重启后结果一致。恢复第三票后，新实例继续写 **100/100** 条、推进 checkpoint 到 **59802**、物理截断到 **PSN 60164**；整组冷重启后水位后 **59803–59852** 共 50 条与 ACK **50/50** 匹配。

**物理截断已提交，成功回包未交给旧 driver。** 暂停第 100 次 `AppendWAL`；第一次真实 `TruncateWAL(PSN 50893, checkpointed DSN 60292)` 已由服务端成功返回，包装层在交给旧 driver 前停住。当时旧 driver 已确认 checkpoint 到 user LSN **59902**、写入 **213** 条。杀 leader 和旧 driver 后，两票新实例从服务端读回同一个 `(PSN 50893, checkpointed DSN 60292)`，补 **79** 个空洞，按序回放 **213/213** 条；两票原盘冷重启后截断水位及记录集不变。恢复第三票后继续写 **100/100** 条，推进 checkpoint 到 **60115**、物理截断到 **PSN 60489**；整组冷重启后水位后 **60116–60165** 共 50 条与 ACK **50/50** 匹配。

证据位于远端 `ooo-checkpoint-overlap-20261111/` 的 `load*.log`、`replay*.log`、`two-vote-*.log`、`after*.log`，及屏障文件所记录的物理截断参数。成功回包被拦住是测试注入的模糊成功时序；结果说明新实例可用服务端持久水位恢复，但不声称实际网络必然产生同一回包形态。本轮未发现已确认 WAL 丢失、重复、乱序，或 checkpoint/物理截断水位回退。测试节点和网络命名空间已停止、清理。

## 最新 upstream/main `9381db5dde`：中断中的真实 snapshot 导出与副本原盘重启（2026-11-11 15:26–15:32）

拉取 upstream/main 至 `9381db5dde`，相对前一测试版 `45917e803f`，LogService 的新改动为 `pkg/logservice/snapshot.go` 在 store 启动时清理本副本遗留的 `snapshot-<index>-<replica>.generating` 目录。按该提交重新编译 `mo-service`，本地与 51 上 SHA256 均为 `295d29e6f72c8c5ee1d7671722b27da5837956003ea31bc24ffb3d39963d74b1`；定向单测 `TestReconcileInterruptedExport` 与 `TestReconcileInterruptedExportPreservesFailedRemoval` 通过。51 上 `/home/mo/jxm/mo-current` 已指向 `/home/mo/jxm/mo-upstream-main-9381db5dde-20261111/`。继续使用隔离集群 `/home/mo/jxm/logservice-main-45917e803f-two-down-20261111/` 的原盘，数据 shard 1、三地址 60201/60211/60221；测试过程没有启动 TN/CN，也没有运行 datasync。

**旧版对照与新版启动范围。** 在 node0 真实导出快照目录旁创建同副本 `snapshot-000000000000FFFF-262146.generating`、其他副本 ID `snapshot-000000000000FFFE-262147.generating`、格式不匹配的 `.generating.old`，以及带 `dragonboat.snapshot.message` 的未完成最终目录。旧版服务原盘启动后，同副本 `.generating` 仍在、未完成最终目录被清理，TN driver 回放到 `user_lsn=60165, checkpointed=60115`。停机后换成 `9381db5dde`，再放入一个带完成标记的最终目录并启动：同副本 `.generating` 与该最终目录均被清理，外来 ID 和格式不匹配目录保留，原有 4 个正常快照保留；三地址状态一致，TN driver 回放水位不变。这里人工目录只用于验证旧/新版启动处理的差异。

**真实导出中断。** 在 node0 的导出目录上用 Linux inotify 监听创建事件，同时用真实 TN WAL driver 持续写入并每 10 条提交一次带测试 `GroupFiles` marker 的 checkpoint。观察到真实 `snapshot-000000000000D1E9-262146.generating` 目录创建事件后，测试脚本立刻杀 node0；该临时目录留在磁盘上，对应最终目录尚不存在。node1/node2 两票继续完成 **300/300** 条写入和 **30** 次 checkpoint，独立回放得到 `user_lsn=60465, checkpointed=60455`，水位后 10 条与 ACK **10/10** 匹配。node0 从原盘重启时清除了真实中断目录，三地址重新发现完整三副本，回放水位和这 10 条记录不变。

**恢复节点参与多数派。** 随后杀原 leader node1，只留刚恢复的 node0 与 node2；两票选出新 leader，继续完成 **20/20** 条 TN WAL 写入和 2 次 checkpoint。两票回放以及三节点整组原盘冷重启后的回放均到 `user_lsn=60485, checkpointed=60475`，水位后 10 条与 ACK **10/10** 匹配。node0 此后又生成 4 个正常导出快照，达到配置配额，说明残留清理没有阻断后续导出。

远端证据在 `snapshot-generating-9381-20261111/` 的 `watch-events.log`、`old-replay.log`、`new-status.log`、`new-replay.log`、`live-load.log`、`two-vote-replay.log`、`three-vote-replay.log`、`recovered-node-load.log`、`recovered-node-replay.log`、`cold-replay.log`，以及节点日志。测试造的外来目录已清掉，节点和网络命名空间均已停止、清理。本轮没有发现新缺陷；真实中断目录为空，带文件的残留目录由旧/新版对照验证清理，尚未在真实导出中途捕获到带部分文件的目录。

## 最新 LogService 版 `9381db5dde`：真实 snapshot 文件写入中断（2026-11-11 15:37–15:40）

继续使用 51 上已升级至 `9381db5dde` 的隔离集群和同一份副本原盘。测试开始时 upstream/main 已前进到 `d9190ff6d0`，但相对 `9381db5dde`，`pkg/logservice`、TAE logstore 与 TN service 均无代码差异，本轮继续使用已验证哈希的 `9381db5dde` 服务二进制。没有运行 datasync 或真实 TN 服务；负载使用真实 TN WAL driver。

这次在 node0 的导出路径上同时监听新建 `.generating` 目录及目录内部文件创建事件。TN WAL driver 连续写入并每 10 条提交一次带测试 `GroupFiles` marker 的 checkpoint。真实 `snapshot-000000000000D888-262146.generating` 创建后约 132 ms，监听器捕获到其中的 `snapshot-000000000000D888.gbsnap` 文件创建，立即停掉 node0；磁盘上留下 **1116 字节**的 `.gbsnap` 文件，最终 `snapshot-000000000000D888` 目录不存在。这次确实命中了含部分文件的导出中断，并非手工放入的残留。

node1/node2 两票继续完成 **400/400** 条 user WAL、**40** 次 checkpoint。独立新 driver 回放到 `user_lsn=60885, checkpointed=60875`，水位后最后 10 条与 ACK **10/10** 匹配。node0 原盘重启时清除了上述非空 `.generating` 目录，三地址重新发现完整副本组，回放水位与 10 条内容不变。随后杀数据 leader node2，只留刚恢复的 node0 与 node1，两票继续完成 **20/20** 条写入与 2 次 checkpoint；两票回放及整组三节点原盘冷重启后均为 `user_lsn=60905, checkpointed=60895`，水位后最后 10 条与 ACK **10/10** 匹配。node0 此后又保留了 4 个正常导出快照，未积累新的 `.generating` 目录。

证据位于远端 `snapshot-generating-files-20261111/` 的 `watch-events.log`、`live-load.log`、`two-vote-replay.log`、`three-vote-replay.log`、`recovered-load.log`、`recovered-two-vote-replay.log`、`cold-replay.log`，以及节点日志；监听脚本为集群根目录的 `kill_generating_after_file.py`。本轮未发现部分文件残留导致的启动失败、快照导出阻塞或已确认 WAL 丢失。测试节点和网络命名空间已停止、清理。

## 单 TN WAL driver 覆盖复查与全量 checkpoint 后空 WAL 恢复（2026-11-11 15:43–15:50）

本轮限定**任一时刻只有一个 TN WAL driver 实例存活**；每次新实例启动前都结束旧实例。没有运行 datasync 或真实 TN 服务。LogService 服务端仍为 `9381db5dde`；检查当时最新 upstream/main `d9190ff6d0`，其 `pkg/logservice`、TAE logstore、TN service 相对该服务端版本没有代码差异。

### 已覆盖路径及仍需补测的路径

| 路径 | 既有证据 | 当前边界 |
| --- | --- | --- |
| 三副本 leader/普通副本短时或长时下线、网络隔离、两票写读、恢复与原盘冷重启 | 多轮故障矩阵、超过 3 分钟的旧 leader 隔离、双节点短暂停机 | 已覆盖服务端和 TN WAL driver；不推断真实 TN 服务整体可用性 |
| 单 driver 并发 Append 的 DSN/PSN 乱序与空洞 | 16 轮追加矩阵产生 351 个补空洞映射，424 条旧实例 ACK 均在回放中匹配 | 尚未覆盖大小差异显著的 WAL 混写批次边界 |
| checkpoint、后台 TruncateWAL、模糊成功、Close 和 leader 切换 | checkpoint/物理截断发送前后、回包丢失等窗口已有逐条回放核对 | 本轮补测所有 user WAL 均已被物理截断后的下一次写入 |
| Raft snapshot 导出、导入、追赶和中途崩溃 | 落后副本追赶、真实 `.generating` 文件写入中断及原盘恢复 | 与 TN checkpoint 文件不同；后者另有同进程 TAE 测试 |
| 持续回放转写入、真实 TAE checkpoint 文件加载 | 已测切换期间 leader 停机及同进程 TAE DB 重开后扫描 50 行 | 本轮不扩展同时存活的多个 TN driver 实例 |

剩余优先路径是**一个 driver 内大小差异显著的 WAL 并发混写**：跨 LogService 读取批次边界做 checkpoint 和 leader 切换，再结束旧实例，以单独的新实例回放，核对每条已确认记录的长度、摘要和 LSN。这可检查 `group_committer` 的装包和 `replayer` 的跨批次解析。另一个边界是真实 TN 服务的启动、关闭和文件服务配合；当前测试夹具只复用生产 WAL driver。datasync 路径不纳入本轮范围。

### 新增实测：物理 user WAL 为空时从截断水位续写

复用 51 上隔离三副本集群 `/home/mo/jxm/logservice-main-45917e803f-two-down-20261111/`。旧 driver 用真实 `wal.NewLogserviceHandle`、`ReplayForWrite`、`AppendEntry`、带 `GroupFiles` 测试 marker 的 checkpoint 写入 100 条，最后一条 ACK 为 `empty-retained-r1/000099`、**user LSN 61025**；checkpoint ACK 覆盖到 **61025**。探针在计划的第 101 条发送前暂停，等待后台物理截断完成；日志确认 `psn-intent=57777, psn-truncated=57777`。随后结束旧 driver，未发送第 101 条。新 driver 独立回放时**没有交付任何 user WAL 条目**，但从服务端持久水位恢复 `user_lsn=61025, checkpointed=61025, truncated_dsn=61577`，没有把下一条逻辑 LSN 重置为 1。

随后杀数据 leader node2，仅保留 node0/node1 两票。新 driver 再次回放到同一水位，接着独立单 driver 写入 **20/20** 条，LSN 从 **61026** 连续到 **61045**，checkpoint ACK 到 **61035**。旧实例退出后，另一新 driver 在两票状态回放 `user_lsn=61045, checkpointed=61035`；水位后的 **61036–61045** 十条与写入 ACK 的 LSN/payload **10/10** 一致。node0/node1 原盘冷重启、node2 仍下线后的回放结果不变；node2 原盘恢复后三票回放仍为同一水位，没有缺失、重复或水位回退。

证据保存在远端 `empty-retained-wal-20261111/` 的 `writer.log`、`two-vote-replay.log`、`after.log`、`two-vote-after-replay.log`、`cold-two-vote-replay.log`、`three-vote-replay.log` 和节点日志。结论只覆盖单个可写 driver 的顺序交接及 LogService 持久恢复；测试 checkpoint 文件名为 marker，不验证真实文件服务内容。本轮没有发现新缺陷。

## 单 TN WAL driver 大小混写、两次 leader 切换与回放（2026-11-11 15:55–16:01）

### 未覆盖路径再梳理

从 `logservicedriver/commit.go` 的单条物理 WAL 大小上限、`group_committer.go` 的装包，以及 `replayer.go` 的 `MaxReadBatchSize=64 MiB` 分批读取出发，前轮独立覆盖了 64 MiB 单条 WAL 和小记录并发乱序，但**没有覆盖同一个 driver 内大/小记录混写、跨多个读取批次、同时发生 leader 切换和 checkpoint**。本轮以此为优先。此前已覆盖的短时/长时副本下线、物理截断、Raft snapshot、客户端池饱和及小记录的 Close 故障交错不再原样重复。datasync 和同时存活的两个 TN driver 均排除。

本轮后仍有这些单 driver 路径待测：

| 优先级 | 路径 | 与已有测试的差别 |
| --- | --- | --- |
| 高 | 大 WAL 已接收但仍在提交时，driver `Close` 与 leader 断线重叠 | 已测批量小 WAL 的 Close 故障；尚未覆盖接近装包上限的内存与等待者生命周期 |
| 高 | 真实 TAE checkpoint 文件写入中仍有并发事务 WAL，随后节点故障和原盘重开 | 已测同进程 TAE 的单次 checkpoint 文件加载及 50 行恢复；尚未把并发事务与真实文件生成放在同一窗口 |
| 中 | 接近 99 MiB 默认物理 WAL 上限的有效记录与小记录混写 | 已测 64 MiB 单条和本轮 8 MiB 混写；靠近大小边界的装包分支未做集成验证 |
| 中 | 历史版本持久 WAL 的 skip 命令兼容与升级后回放 | 当前代码有 legacy v2 单测；尚未用旧版二进制产生真实数据再用新版回放 |

### 新增实测

在 51 上使用三副本隔离集群 `/home/mo/jxm/logservice-main-45917e803f-two-down-20261111/`，服务端为 `9381db5dde`。探针只使用生产 `wal.NewLogserviceHandle` 的回放、并发 `AppendEntry`、`RangeCheckpoint` 和 `Close` 路径。其大记录是 **8 MiB**，小记录约 19 字节；ACK 和回放仅输出 LSN、长度、SHA256。每一阶段旧 driver 完全退出后才启动新的 driver。服务端 LogService shard 1 有三个地址，故障时始终保持两票。

第一阶段从 `user_lsn=61045` 开始，8 个并发写入者各写 12 条，每人每 3 条有一条 8 MiB，共 **96/96 ACK、32 条大 WAL、约 256 MiB 大记录数据**。约第 20 条 ACK 后杀当时的数据 leader node2，node0/node1 两票完成剩余写入；最终 LSN **61141**。底层 `AppendWAL` 出现 **1 次 EOF、5 次 timeout**，由生产 driver 重试，最终上层写入错误为 0。ACK 最大耗时 **5.716 秒**，无超过 60 秒的写入。旧 driver 退出后，新 driver 在两票状态回放，**96/96** 条按 LSN、字节数和 SHA256 匹配，checkpoint 水位仍为 61035。

第二阶段保持 node2 下线，另一个单 driver 并发写 **64/64** 条，其中 **32 条为 8 MiB**，每约 16 条 ACK 提交一次带测试 `GroupFiles` marker 的 checkpoint；最终 `user_lsn=61205, checkpointed=61173`。新 driver 在两票状态回放，水位后 **32/32** 条与 ACK 的 LSN、长度、SHA256 一致，其中 13 条为 8 MiB；单条 ACK 最大耗时 **2.745 秒**。node0/node1 原盘冷重启后，回放水位及全部 35 条物理可见记录的集合完全相同；其中 3 条虽物理可见但已在 checkpoint 水位之前，不计入必须保留的用户 WAL。

node2 原盘恢复后，确认三个地址均报告同一副本组，再杀当前 leader node0，只留 node1/node2 两票。独立新 driver 继续写 **8/8** 条，LSN **61206–61213**；再启动一个新 driver 回放，原先水位后的 32 条仍全部匹配，新增 8 条按 LSN、长度、SHA256 **8/8** 匹配。这说明先前落后的 node2 追上本轮大 WAL 后可与 node1 组成多数派继续服务。

证据位于远端 `mixed-wal-20261111/` 的 `write.log`、`two-vote-replay.log`、`checkpoint-write.log`、`checkpoint-replay.log`、`cold-two-vote-replay.log`、`three-status.log`、`recovered-two-vote-write.log.gz` 和 `recovered-two-vote-replay.log`。其中 `three-status.log` 记录了 node2 刚启动时短暂连接失败；随后再次查询确认三个地址均可访问。探针源码是测试工作树 `test/logservice-chaos/tn_driver_probe/concurrent_probe/main.go` 的混写参数扩展；已完成 `go list`、`go build`、`go vet` 和定向包编译测试，并在 51 上运行该二进制。测试结束已停止三个节点并清理网络命名空间。本轮没有发现已确认 WAL 丢失、重复、回放摘要不符或一分钟写入长尾。

## 单 TN WAL driver 大 WAL 排空与 Close、leader 下线重叠（2026-11-11 16:07–16:15）

从上节未覆盖清单选择两条有效负载：**4 条各 24 MiB** 的已接收 WAL，以及接近 `DefaultMaxWALRecordSize=99 MiB` 的**单条 95 MiB** WAL。扩展现有 `close_probe` 的大小参数：探针完成 `ReplayForWrite` 后由真实 TN WAL handle 接收记录，让第一次底层 `AppendWAL` 停在**发出 RPC 前**，随后启动 `w.Close()` 并发等待；脚本确认屏障和 Close 信号后杀数据 leader，保持另外两票，再放行底层提交。新实例只在旧 driver 完全退出后启动。记录以 LSN、字节数和 SHA256 作为独立回放核对依据；没有运行真实 TN 服务或 datasync。

**4 × 24 MiB。** 初始水位 `user_lsn=61213, checkpointed=61173`，四条记录被 driver 接收为 LSN **61214–61217**，当时 leader 为 node2。杀 node2 后放行，底层观察到一次连接拒绝、一次 `context deadline exceeded` 和一次 timeout，后续重试成功。Close 约 **22.352 秒**返回 nil，**4/4** 个 `WaitDone` 返回 nil。旧 driver 退出后的两票回放 **4/4** 按 LSN、长度、SHA256 匹配；node0/node1 原盘冷重启后的水位仍为 `user_lsn=61217, checkpointed=61173`，全部物理可见回放记录集与冷重启前一致。

**单条 95 MiB。** 恢复第三票后，leader 为 node1。新 driver 接收 LSN **61218**，字节数 **99,614,720**；Close 发起后杀 node1，仅留 node0/node2，放行底层提交。一次底层 timeout 后重试成功，Close 约 **13.296 秒**返回 nil，`WaitDone` 返回 nil。两票回放和两票原盘冷重启后，LSN 61218 的长度及 SHA256 均与接收时一致，水位为 `user_lsn=61218, checkpointed=61173`。再由独立新 driver 写 **8/8** 条小 WAL，LSN **61219–61226** 连续，后续独立回放 **8/8** 按 LSN、长度、SHA256 匹配。

**追加尝试：放行后、底层调用未返回时下线 leader。** 第一次尝试等待 socket 发送队列的观测条件，底层调用却先 timeout，之后才杀 leader；该次不能算目标窗口。它最终重试成功，Close 约 **25.695 秒**返回 nil，独立两票回放验证 95 MiB 的 LSN **61227** 与接收时长度、SHA256 一致。第二次重新建立三票，探针接收另一条 95 MiB WAL，LSN **61228**；启动 Close、放行首次底层 `AppendWAL` 后约 **105 ms**，确认当时底层调用**尚无返回日志**，立即杀 leader node2。该调用随后 timeout，driver 重试成功，Close 约 **24.026 秒**返回 nil，`WaitDone` 返回 nil。两票回放及两票原盘冷重启后，LSN 61228 的 **99,614,720 字节**和 SHA256 `d84f0fc7b6817971de9b180d81dbb33beb637f008b09dacdc5e001807fc51f0e` 均不变，水位为 `user_lsn=61228, checkpointed=61173`。

远端证据在 `large-close-20261111/`、`near-limit/`、`inflight/` 与 `inflight-fast/` 子目录：各自的 `run.log`、`two-vote-replay.log`；冷重启验证对应 `cold-two-vote-replay.log`，最后一轮的放行时序记录为 `inflight-fast/timing.txt`。后一轮另有 `after-write.log`、`after-replay.log`。可复现探针源码为 `test/logservice-chaos/tn_driver_probe/close_probe/main.go`。本轮没有发现 Close 挂起、已接收 WAL 丢失、重复或水位回退。**时机边界：**首两轮屏障在底层 RPC 发送前；追加快速轮证明放行后约 105 ms 底层调用尚未返回，**没有抓包证明请求字节已到达服务端**，因此不能把该轮解释为已提交后回包丢失。真实 TAE checkpoint 文件写入中并发事务、以及旧版持久 skip 命令升级回放，也仍待测。测试节点和网络命名空间已停止、清理。

## 单 TN WAL driver：真实 TAE checkpoint 与并发事务、两票冷重启（2026-11-11 16:30–16:38）

在 51 上新建**空盘**三副本集群 `/home/mo/jxm/logservice-tae-concurrent-20261111/`，数据 shard 1、RPC 地址 60601/60611/60621、Log store 过期阈值 5 分钟，服务端为 `9381db5dde`。同一个测试进程内只打开一个 TAE DB，经 `options.WithWalClientFactory` 使用真实 LogService 三地址和生产 TN WAL driver；没有启动 TN/CN 服务或 datasync。探针源码新增于 `test/logservice-chaos/tn_driver_probe/tae_checkpoint_probe/tae_checkpoint_concurrent_test.go`。

创建含 `mo_catalog` 系统目录写入的测试表，先确认 **50 行**。随后一个写入 goroutine 按不同主键偏移连续提交每批 10 行，前台调用真实 `DB.ForceCheckpoint`；写入者在 checkpoint 完成后停止。脚本在开始 ForceCheckpoint 前杀数据 leader node2，node0/node1 两票继续服务。checkpoint 耗时 **5.418 秒**，期间按提交时间戳确认 **13 笔事务**落在调用区间内；总共提交 **84 笔**，生成 **1 个真实 checkpoint meta 文件**，checkpoint LSN 为 **72**。停写后另提交 10 行，预期总数 **900 行**、WAL LSN **90**；同进程关闭重开 TAE 后扫描到 **900/900 行**，并明确从 `meta_0-0_1794385933434353408-0.ckp` 读取 checkpoint。

随后 node2 保持下线，node0/node1 **原盘冷重启**，原测试进程已退出。独立新进程用同一 TAE 本地目录和同一 LogService shard 打开，扫描仍为 **900/900 行**，checkpoint LSN **72**、WAL LSN **90**；再写 10 行并关闭重开，扫描 **910/910 行**、WAL LSN **91**。再次独立跨进程打开，读 **910/910**、续写并重开后读 **920/920**，WAL LSN **92**。整个过程中任一时刻只有一个 TAE 实例及其 TN WAL driver 存活。

首次跨进程检查曾误报 `ExpectedEOB`：`catalog.MockSchemaAll` 在新进程生成了新的随机表名，探针按这个新名字查旧表；第二次尝试使用尚未实现的 `database.Relations()`，得到空列表。改为通过 `MakeRelationIt()` 从持久目录枚举原表 schema 后，两次跨进程校验均通过。前两次失败是**测试夹具错误，不列为产品缺陷**；服务端当时已回放到 checkpoint LSN 72、WAL LSN 90。

远端证据在该集群根目录 `test.log`、`cold-reopen-fixed2.log`、`cold-reopen-repeat.log` 和节点日志；本地 mock 定向测试、vet、测试二进制编译均通过，race 模式定向测试按 30 秒预算重复 **32 次**及包级单次检查通过。测试结束已停止三个节点并清理网络命名空间。本轮没有发现行数丢失、checkpoint/WAL 水位回退或两票下无法重开。**剩余边界：**本轮证明事务提交与 `ForceCheckpoint` 调用区间重叠，未在 checkpoint 文件写入事件上精确触发 leader 停机；旧版持久 skip 命令升级回放仍待测。

## 预分配 TN WAL 序列、并发物理追加、截断与尾部裁剪（2026-11-11 17:01–17:13）

使用 51 上新空盘隔离三副本 `/home/mo/jxm/logservice-preassigned-20261111/`，服务端 `9381db5dde`，shard 1，三个地址为 `198.18.51.10:60201`、`198.18.51.11:60211`、`198.18.51.12:60221`。探针 `test/logservice-chaos/tn_driver_probe/preassigned_probe/main.go` 调用生产 `wal.NewLogserviceHandle`、`ReplayForWrite`、`AppendEntry`、`RangeCheckpoint`；只在底层 `AppendWAL` 包装器设置屏障。先在一个 driver 中依次取得全部 user LSN，确认底层成功回包数为 0，才并发释放物理 Append。每次独立回放之前均退出旧 driver，不运行真实 TN 服务或 datasync。以预分配时的 `(LSN, 长度, SHA256)` 为独立预期值，按回调**顺序**核对。

### 大并发乱序与物理截断

第一轮先分配 **LSN 1–64**，每条 3 MiB；首个底层 Append 另行阻塞。放行其他请求后，已成功提交 22 条时杀 leader node0，只保留 node1/node2；再放行首个请求，最终 **64/64** 条上层 ACK 成功。64 个成功物理 `(PSN, LSN)` 存在 **1017 对**顺序逆转。新 driver 在两票状态回放 **64/64** 条，严格按 LSN 1–64 递增，长度和摘要全匹配。checkpoint 到 48 时，最早的物理记录仍是必须保留的 LSN 49，所以物理截断不能前进；推进 checkpoint 到 63 后，物理 PSN 截断到 **232**。两票原盘冷重启后水位为 `user_lsn=64, checkpointed=63`，水位后必须保留的 LSN 64 内容正确。回调仍可能交付已 checkpoint 的物理可见记录，判断保留集须使用 checkpoint 水位。

恢复三票后，第二轮先分配 **LSN 65–160 共 96 条**，每条 3 MiB；放行前仍无底层成功回包。首个请求继续阻塞，其他请求中已有 40 条物理成功时杀 leader node1，留 node0/node2 两票，再放行首个请求。最终 **96/96** 条上层 ACK 成功，96 个成功物理 `(PSN, LSN)` 有 **2053 对**逆序；负载期间底层累计 **574 次失败尝试并重试**（417 次 `context deadline exceeded`、130 次 timeout，其余为连接写错误），但没有上层最终错误。两票独立回放严格按 LSN 65–160 递增，**96/96** 长度与摘要匹配。checkpoint 到 **144** 后物理 PSN 截断到 **1178**；两票原盘冷重启后 `user_lsn=160, checkpointed=144`，必须保留的 LSN **145–160 共 16/16** 条按序且内容全匹配。大并发的底层重试次数说明该负载下有明显重试压力；本轮没有记录逐条 ACK 耗时，不据此判断一分钟长尾。

### 真实 DSN 缺口、复用上层 LSN 与截断后回放失败

第三轮用同一集群恢复三票，旧 driver 先顺序分配 **LSN 161–184 共 24 条**，每条 1 MiB；首个底层 Append 阻塞，其余 **23 条物理 Append 返回成功**，但按 driver 的有序确认路径，**上层 ACK 为 0**。直接 `SIGKILL` 旧 driver，确认它已退出后启动新 driver。新回放写入覆盖 **DSN 168–190** 的 skip 命令，裁掉未确认尾部，报告 `user_lsn=160, checkpointed=144`，没有交付旧轮的任何记录。新的唯一 driver 重新分配并确认 **LSN 161–176 共 16/16 条**；这些上层 LSN 与旧轮相同，payload 摘要全部不同。独立 driver 在截断前能回放这 16 条，水位为 176。

随后 checkpoint 到 **LSN 172**，后台 `Wal-Truncate-Execute` 把物理水位从 PSN 1178 推到 **1841**，checkpointed DSN 为 178。下一次独立回放在 `replayer.go:1024` 报错并由探针 panic：

```text
Wal-Replay-DSN-Overlap: wal record dsn range [183, 184] at psn 2762 overlaps pending dsn range [184, 184] at psn 2209
```

同一持久状态上连续再回放两次，均得到相同错误；整组三副本各自原盘强制冷重启后再次回放，仍得到相同错误，故这不是临时 leader/连接故障。失败前的日志显示 skip 命令为版本 1，读取后多条 `Wal-Replay-Stale-Skip-Entry`，例如 DSN 184 的 skip 目标 PSN 2210、当前 pending PSN 2209；随后对一些旧物理记录报告 duplicate，最后在新旧 DSN 范围部分重叠时终止。**缺陷影响：**16 条已 ACK 的新 WAL 在截断后不能完成 `ReplayForWrite`，TN 重启可能无法恢复可写。证据足以确认回放失败和时间线；PSN 对不上的根因还需进一步定位，不能仅凭日志断言是 skip 命令生成、物理截断或读取索引中的哪一处实现错误。

远端 `preassigned/r1/`、`r2/`、`r3/` 保存每轮 `write.log`、独立回放、checkpoint 和冷重启日志；第三轮关键文件是 `r3/write.log`、`post-kill-replay.log`、`post-skip-write.log`、`checkpoint172.log`、`checkpoint175.log`、`repro-1.log`、`repro-2.log`、`cold-repro.log`。本轮结束后已停止三个节点并清理隔离网络命名空间，保留原盘和日志以便进一步定位。此处的 checkpoint 使用测试 marker；它验证 WAL 截断和回放，不代替真实 TAE checkpoint 文件加载。

## 独立空盘复测裁尾、复用 LSN 与 PSN 跳号（2026-11-11 17:20–17:31）

对上节故障做两套**独立空盘**对照，沿用三副本配置、真实 TN WAL driver 和生产 LogService `9381db5dde`；任一时刻只有一个 driver。地址仍为 `198.18.51.10:60201`、`:60211`、`:60221`，但两套分别使用独立根目录、UUID、数据盘和启动周期，不与原故障盘混用。探针的 `scan` 模式只读调用 `GetTruncatedLsn`/`Read`，比较每条返回记录自带的真实 `record.Lsn` 与当前回放器采用的 `batchFirst + index` 推算值；指定 `-scan-min/-scan-max` 时输出实际 PSN、推算 PSN、记录类型和 WAL DSN。

**空盘 A：缺口在首条 LSN。** 旧 driver 顺序分配 LSN 1–24，挡住第一条底层请求，其余 23 条物理 Append 返回成功，0 条上层 ACK，随后直接杀旧进程。新 driver **跳过未发送的 LSN 1，交付 LSN 2–24 共 23 条**，水位到 24；后续独立 driver 从 LSN 25 写入 16/16 条。checkpoint 到 36，物理截断 PSN 437，三副本原盘冷重启后 `user_lsn=40, checkpointed=36`，必须保留的 LSN 37–40 四条顺序和摘要匹配。此观察说明当前实现在**没有已应用前缀**的缺口处可跳过首个 DSN 并应用后续物理成功记录；这些记录旧轮没有上层 ACK，不能仅由 ACK=0 推断它们必然没有提交。是否满足完整 TAE 事务语义需单独验证。

**空盘 A：已有 checkpoint 前缀。** 同一独立集群接着从水位 40 分配旧轮 LSN 41–64，首条不发送、余下 23 条物理成功、0 条上层 ACK。新 driver 写入 skip 命令后裁回 `user_lsn=40`，新轮复用 LSN 41–56 并 16/16 ACK；截断前独立回放 16/16。checkpoint 到 52，物理截断 PSN 1502，之后独立回放成功，水位为 `user_lsn=56, checkpointed=52`。这次旧物理记录的 PSN 1117–1139 已落在截断水位之前；不能用它证明保留旧物理记录时的 skip 匹配行为。

**空盘 B：完全未发送的尾部。** 旧 driver 顺序分配 LSN 1–24，在释放任何底层请求前杀进程，确认底层成功数为 0。新 driver 回放水位仍为 0，重新使用 LSN 1–16 并 16/16 ACK。checkpoint 到 12、物理截断 PSN 774、三票原盘冷重启后，必须保留的 LSN 13–16 四条顺序及摘要匹配。

**空盘 B：两票状态保留旧物理记录。** 以已 checkpoint 到 12 的水位 16 为前缀，旧 driver 分配 LSN 17–40，首条阻塞、其余 23 条物理成功、0 条上层 ACK；杀 node1 后杀旧 driver，仅留 node0/node2。新 driver 写 skip 命令、裁回 `user_lsn=16`，复用 LSN 17–32 并 16/16 ACK。checkpoint 到 28、物理截断 PSN 962 后，旧物理记录仍在约 PSN 1265–1287，独立回放**没有** stale skip 或 DSN overlap；两票原盘冷重启后 `user_lsn=32, checkpointed=28`，必须保留的 LSN 29–32 四条顺序和摘要匹配。可见**旧物理记录保留与重复上层 LSN 本身不会必然失败**。

**故障盘的直接 PSN 核对。** 原故障盘在物理截断到 PSN 1841 后，用只读客户端扫描 LogService `Read`：某批起点为 1842，返回列表的第 0 条真实 PSN 1842，第 1 条真实 PSN **1844**，中间 PSN 1843 未出现在结果中；该批后续记录的 `record.Lsn` 继续比 `batchFirst + index` 大 1。例如旧记录 `DSN 184` 的真实 PSN 是 **2210**，而故障回放把它按 **2209** 放入 pending；已持久化的 skip 命令正确指向 PSN **2210**，因此被判为 stale，旧记录未被移除。随后新物理记录 PSN 2762 占 DSN 183–184，与旧待处理记录发生部分重叠，回放在交付用户 WAL **之前**失败。`replayer.go:readNextBatch` 用 `psnToRead + i` 推算 PSN，尽管每条 `LogRecord` 已带真实 `Lsn`；`store.go:markEntries` 对部分 Raft 条目不返回记录，使批内 PSN 可以不连续。这是 skip 目标与 pending PSN 不一致的直接证据。尚未单独确定缺席的 PSN 1843 对应哪种 Raft 命令，也尚未测量这种跳号触发最终回放失败的频率。空盘 B 的只读扫描也观察到批内推算 PSN 与真实 PSN 不同，但其 skip 生成和消费阶段的推算方式相同，因此该次未失败；这支持**跨阶段 PSN 推算不稳定**是关键条件。

远端证据：空盘 A 为 `/home/mo/jxm/logservice-preassigned-repeat-a-20261111/` 的 `initial-replay.log`、`old/`、`tail/`、`checkpoint36.log`、`post-truncate-replay.log`、`cold-replay.log`、`scan.log`；空盘 B 为 `/home/mo/jxm/logservice-preassigned-repeat-b-20261111/` 的 `unsent/`、`two-vote/`，特别是 `two-vote/scan-window.log`。故障盘真实 PSN 扫描保存在原根目录 `preassigned/r3/scan-start.log`、`scan-raw-window.log`。两套对照的节点与测试网络均已停止，磁盘和日志保留。对应缺陷见 [#32534](https://github.com/matrixorigin/matrixone/issues/32534)。

## 固定逻辑 LSN、物理跳号、截断 PSN 的确定性回放矩阵（2026-11-11）

在生产 `logservicedriver.replayer` 的包内，用注入的只读后端固定持久记录，不使用计时或随机调度。每组物理顺序均为：已 checkpoint 的前缀、旧进程留下的 LSN/DSN 3、指向旧记录**真实 PSN**的 skip 命令、新进程替代记录；新旧 payload 不同。两种替代形状分别为新 LSN/DSN **2–3**（与旧 DSN 3 部分重叠）和新 LSN/DSN **3**（与旧 DSN 3 完全相同）。每条返回记录携带真实 `LogRecord.Lsn`，与线上 LogService `Read` 协议一致。矩阵再固定交叉四个物理 PSN 跳号位置（无跳号、旧记录前、skip 前、替代记录前）、两个物理截断 PSN（0、1）和六个读取批大小（1、2、3、4、5、64），共 **2 × 4 × 2 × 6 = 96 种**。独立预期是只交付新记录，LSN 顺序与新 payload 均正确。

当前服务端对应的生产回放代码运行结果为 **72/96 通过、24/96 失败**。所有失败均为物理跳号位于旧记录之前：部分重叠形状 **12 组**返回 `Wal-Replay-DSN-Overlap`，且在错误前没有用户 WAL 回调；完全相同形状 **12 组**返回成功，却把旧 payload 交付为 LSN 3，新 payload 被当成重复物理记录忽略。后者是确定性包内复现，**尚未在真实三副本上单独复现静默交付旧内容**。其余 72 组全部按正确 LSN 和新 payload 交付，覆盖没有跳号、跳号晚于旧记录、不同截断 PSN 和不同批边界的对照。

仅在本地临时把 `replayer.go:readNextBatch` 的 `psnToRead + i` 换成返回记录自带的 `record.Lsn`，同一矩阵变为 **96/96 通过**；测试后撤回该临时代码改动，没有推送生产修复。这一对照支持 PSN 推算是直接原因，不代替对完整 `Read` 合约和已有回放测试的修复审查。可重复运行的诊断夹具和自动清理脚本保存在 `test/logservice-chaos/tn_driver_probe/replay_matrix/`。上述初始 96 组结论已追加到 [#32534](https://github.com/matrixorigin/matrixone/issues/32534#issuecomment-6792540727)。

### 扩展跳号宽度与重跑（2026-11-12）

固定矩阵增加物理 PSN 连续缺失 **2 个**的情况，在旧记录前、skip 前、替代记录前各设置一组；形状、截断水位和读取批大小不变。总计 **2 × 7 × 2 × 6 = 168 组**。当前代码实跑 `MATRIX_SUMMARY total=168 failed=48`：**120/168** 组通过；旧记录之前有 1 或 2 个 PSN 缺失时，部分重叠形状共 **24** 组报 `Wal-Replay-DSN-Overlap`，完全重叠形状共 **24** 组错误交付旧 payload。其余位置与无缺口对照通过。临时改用返回记录的真实 `record.Lsn` 后 **168/168** 通过，随后撤回临时代码改动。此处仍是包内注入 `Read` 结果的确定性诊断，不能据此宣称静默旧 payload 已在真实三副本集群重现。

## 单 TN driver 重复追加、checkpoint 与两票/三票原盘冷重启（2026-11-11 17:48–17:49）

在 51 的隔离三副本 `/home/mo/jxm/logservice-preassigned-repeat-b-20261111/` 上，用生产 `wal.NewLogserviceHandle` 连续执行 **5 轮**：每轮由唯一 driver 写入并确认新 WAL、提交 checkpoint intent、停止当轮所有存活 LogService 进程、从各自原盘冷重启，再由独立新 driver 回放。轮次写入量为 **8、16、24、8、16**，累计 **72/72** 条 ACK；每次重启后独立核对分配 LSN、ACK 的长度和 SHA256，以及 checkpoint 以上必须保留的四条 WAL 顺序与摘要。五轮结束水位依次为 **40、56、80、88、104**，checkpoint 到 **36、52、76、84、100**，五轮保留集均 **4/4** 匹配。第 1、3、5 轮只有两票存活，分别为 node0+node2、node0+node1、node1+node2；第 2、4 轮为三票。检查脚本 `restart-append/run-cycles.sh` 及每轮 `write.log`、`checkpoint.log`、`cold-replay.log` 保存在该远端根目录。测试后节点和隔离网络均已停止。checkpoint 使用测试 `GroupFiles` marker，未加载真实 TAE checkpoint 文件。

## TAE checkpoint 水位与 LogService truncate 的持久化边界（代码检查，2026-11-12）

真实 TAE 启动先从 `ckp/` 元数据及其引用的数据文件恢复，取得 `ckpLSN`，再把它交给事务 WAL 回放；`db/replay.go:checkLSN` 跳过 `lsn <= ckpLSN`，对其后的 WAL 要求逐一递增。因此，**旧 WAL 恰好与 checkpoint 前缀重叠时会被跳过**；对于高于 `ckpLSN` 的旧记录，单靠这个水位无法判别它与同 LSN 的新记录属于不同代，driver 在回调前报 DSN overlap 时 TAE 也无从补救。

增量 checkpoint 的顺序是：写入数据及 checkpoint metadata，`saveCheckpoint` 的 `WriteEnd` 成功后将 meta 文件登记；然后 `RangeCheckpoint` 分别追加 `GroupCKP` 和 `GroupFiles`，等待 checkpoint intent 完成；后台再根据 LSN→DSN 映射请求 driver `Truncate`。这些动作与对象存储上的 checkpoint 文件**没有跨存储原子事务**。崩溃在文件发布后、WAL checkpoint intent 之前时，重启会列出该 meta 文件，而未推进的物理截断仍保留旧 WAL；从代码顺序看，这是可以回放的方向，但尚未在该精确切点做真实 TAE 进程崩溃实验。`RangeCheckpoint` 将 `GroupCKP` 与 `GroupFiles` 分成两次 Append，也留下需要单独故障注入的中间状态。

LogService 侧的物理截断 PSN 与 `checkpointedDSN` 则由同一条 Raft `WALTruncate` 命令提交并应用，重启查询返回这对水位；这只保证 **LogService 内部**两者一致。定向执行 `TestWALTruncatePreservesCheckpointedDSN` 与 `TestReplayCheckpointedInteriorHoleAfterPhysicalTruncate` 通过，分别覆盖服务端 snapshot/恢复和 driver 对已截断内部缺口的处理。下节验证两处崩溃切点；对象存储跨节点可见性和 GC 的所有交错仍未覆盖，不能把跨存储窗口判定为完全安全。

### 精确崩溃切点的真实 TAE 恢复（2026-11-12）

复用 51 上三副本原盘 `/home/mo/jxm/logservice-tae-concurrent-20261111/` 和先前真实 TAE 测试的本地目录；每次只有一个 TAE 进程存活。仅为测试二进制临时设置屏障，实验后已从本地生产源码移除，未提交到分支。两次均在测试进程确认追加 10 行后，由屏障确认指定阶段成功，再 `SIGKILL` 进程；LogService 节点保留原盘，之后由独立进程加载同一 TAE 本地目录，扫描每列并核对行数，继续提交 10 行再重开核对。该测试没有逐行比对所有列值。LogService 三副本保持存活，不把 TN driver 已知尾部复用故障混入此实验。

1. **checkpoint metadata 已 `WriteEnd`、`GroupCKP` 尚未发送。** 原有 920 行，追加到 **930 行**、WAL LSN **93**；临时屏障在 `saveCheckpoint` 返回后命中，进程日志无新 `TRACE-WAL-TRUNCATE-Send-Intent`。杀进程后，`ckp/` 中出现新 meta 文件。独立重开读取 **2 份** checkpoint 元数据，得到 `ckpLSN=93`，LogService 仍返回旧的 `(truncatedPSN=275, checkpointedDSN=72)`；扫描 **930/930** 行，继续写入并再次重开为 **940/940** 行、WAL LSN **95**。
2. **`GroupCKP` 已提交、`GroupFiles` 尚未发送。** 从 940 行追加到 **950 行**、WAL LSN **96**。临时屏障等待 `GroupCKP` 条目的 `WaitDone` 成功才写出 `checkpoint-intent-committed` 标记，随后阻止 `GroupFiles` 发送；确认标记后杀进程。独立重开读取 **3 份** checkpoint 元数据，得到 `ckpLSN=96`，LogService 仍返回 `(275,72)`；扫描 **950/950** 行，继续写入并再次重开为 **960/960** 行、WAL LSN **98**。

两处切点都显示“对象存储 checkpoint 水位领先于 LogService 物理截断水位”时仍能读取并继续写入。它们没有覆盖文件尚未全局可见、GC 同时运行或物理 truncate 已提交但回包丢失等其他交错。第二轮后仍观察到旧的 LogService 截断水位；是否会在没有后续 checkpoint 的情况下长期保留可删除日志，需要另测。远端证据为 `meta-crash.log`、`meta-crash.marker`、`meta-crash-reopen.log`、`intent-crash.log`、`intent-crash.marker`、`intent-crash-reopen.log` 和对应的 `ckp/` 文件。

**后续 checkpoint 推进对照。** 同一份 TAE 数据重开后有 960 行，LogService 起始 `(truncatedPSN, checkpointedDSN)=(275,72)`；新增 10 行并正常提交下一份真实 TAE checkpoint，水位到 `ckpLSN=99`、WAL LSN 100。后台物理截断在 30 秒观察窗口内推进到 **`(2526,102)`**；同进程重开后按列扫描仍为 **970/970** 行。说明此前两处崩溃窗口留下的旧物理水位可由下一次 checkpoint 推进；没有下一次 checkpoint 时是否长期滞留，仍未验证。远端证据为 `truncate-after-crash.log`；环境门控的复现夹具为 `test/logservice-chaos/tn_driver_probe/tae_checkpoint_probe/tae_checkpoint_truncate_recovery_test.go`。

## LogService 自身 snapshot：落后副本、截断、切主和原盘冷重启（2026-11-12）

在 51 上创建**全新空盘**三副本 `/home/mo/jxm/logservice-snapshot-recovery-20261112/`，使用 `9381db5dde` 服务端、shard 1、地址 `198.18.51.10:61201`、`:61211`、`:61221`、1 秒截断循环和 5 分钟 HAKeeper store 过期阈值。全程使用直接 LogService 客户端的 `Append`、`Read`、`Truncate`，没有 TN driver 或 TAE。先确认 100 条种子日志，然后停 node2；node0/node1 两票持续做 Append 后立即 Read，先后 `Truncate(169)`、`Truncate(323)`。第一次截断后两个存活副本日志均记录成功导出 snapshot；写读负载期间从原盘启动 node2。连续负载完成 **352/352** 轮，Append 和 Read 重试均为 **0**，最大整轮 **852.894 ms**；node2 本地日志记录 `import snapshot success`，index **242**。截断水位 323 后的 **138** 条按 LSN/payload 与负载记录核对，分别经三个服务地址建连读取，均完全一致。

随后再次停 node2 并从原盘启动，确认三成员；再杀原 leader node0，仅 node1+node2 两票继续完成 **100/100** 轮写读，两个操作的重试均为 **0**，最大整轮 **591.869 ms**。两票期间 `Truncate(500)` 成功；node0 从原盘恢复后，三地址均发现新 leader node1 与原三成员，node0 本地日志记录成功导入 index **381** 的 snapshot，node2 记录成功导入 index **471**。从 LSN 501 读取的 **70** 条与负载预期逐条一致。再将**三个 LogService 进程全部停止并从各自原盘冷重启**，`GetTruncatedLsn=500`，三地址成员和 leader 一致；同一保留集 **70/70** 条再次逐条匹配。

这证明本轮外部 snapshot 的导出/导入、落后副本原盘恢复、两票切主和全组冷重启后可继续服务。按 MO 客户端方式从三个入口建连的 `Read` 可能转发到 leader，因此三份一致读取不单独证明每个副本本地日志逐条相同；各节点本地 snapshot 导入日志是独立的恢复证据。远端脚本和证据位于 `run-snapshot-catchup.sh`、`verify-post-failover.py` 与 `snapshot-follower-catchup/`，包括负载、三次 truncate、节点状态和冷重启读取日志。

**导入成功后清理失败的定向测试。** 在 `pkg/logservice` 使用真实单节点 Dragonboat shard 导出 index 14 的 snapshot，再推进截断使其可导入；仅对导出目录的第一次 `RemoveAll` 注入写错误。第一次截断循环日志确认 `import snapshot success` 后 `remove exported snapshots failed`；恢复文件系统后再次执行截断循环，重新导入并完成目录清理，快照管理器计数回到 0。新增 `TestImportSnapshotRetryAfterCleanupFailure` 连续运行 **10 次通过**。它验证单进程重试路径；不代替杀进程恰在导入成功与清理之间的实机测试。

## LogService snapshot 其他恢复窗口：导入成功后崩溃与多快照边界（2026-11-12）

在 51 上创建另一套**全新空盘**三副本 `/home/mo/jxm/logservice-snapshot-import-window-20261112/`，shard 1，地址 `198.18.51.10:61401`、`:61411`、`:61421`，1 秒截断循环、5 分钟 HAKeeper store 过期阈值。服务端基于 `9ab445e0d8` 对应的 LogService 源码，只在测试二进制中于 `importSnapshot` 的 `SyncRequestImportSnapshot` **成功返回之后、`snapshotMgr.Remove` 之前**加屏障，并将其限定到该隔离目录的 node2。二进制及配套 native 库在 51 上单独保存；本地生产源码已恢复，屏障没有提交。全程使用直接 LogService 客户端，不涉及 TN driver 或 TAE。

**精确导入后崩溃。** 先写 100 条并 `Truncate(50)`，node2 导出 index 107、109、112、114 四份快照；打开屏障后再写 100 条、`Truncate(130)`。node2 日志确认 **index 114 导入成功**，屏障写出 `import-completed-before-cleanup` 标记，随后立即杀 node2，因而它尚未执行本地 `snapshotMgr.Remove` 和 `forwardTruncate`。node0/node1 两票继续完成 **100/100** 轮 Append 后立即 Read，Append/Read 重试均为 **0**，最大整轮 **847.171 ms**，再 `Truncate(180)`。撤销屏障并从 node2 原盘启动后，node2 日志第二次确认**同一 index 114 导入成功**，原 `snapshot-0000000000000072` 导出目录最终消失。三地址均恢复原三成员；从 LSN 181 读取，保留的 **128** 条与每次成功写入的 LSN/payload 完全匹配。三节点整组从各自原盘冷重启后 `GetTruncatedLsn=180`，同一 **128/128** 条再次经三个地址读取匹配。证据在远端 `exact-import-crash/`、`import-ready`、`mo-exact-snapshot-import-crash-20261112.sh`、`verify-exact-import-crash.py` 及 node2 `stdout.log`。这证明该精确**导入成功后**窗口可重复导入并完成清理；尚未命中 Dragonboat 导入请求**执行中**的进程崩溃。

**多份导出快照与截断水位边界。** 上述全组冷重启后，node2 同时持有 index **314、318、320、322** 四份有效导出快照，物理 latest LSN 为 323。再追加 `boundary-tail` **100/100** 条，成功返回的 LSN 为 **325–424**；依次执行 `Truncate(319)`、`Truncate(320)`、`Truncate(322)`。node2 本地日志依次确认导入 **318、320、322**：水位 319 选不超过它的最大候选 318，水位恰等于 320 和 322 时分别选对应快照。旧的 314/318/320/322 导出目录全部清理。三个地址从 LSN 325 读取的 **100/100** 条按 LSN/payload 匹配；再次整组三副本原盘冷重启后 `GetTruncatedLsn=322`，三地址同一 **100/100** 条仍匹配。证据在远端 `snapshot-boundary-sequence/`、`verify-snapshot-boundaries.py` 和 node2 日志。按 MO 客户端方式从各入口建连可能转向 leader，三地址读取结果不单独证明每个副本本地数据逐字节一致；node2 的三次本地导入日志是独立证据。

**多快照部分清理失败后重建管理器。** 新增 `TestSnapshotPartialCleanupReloadsRemainingCandidates`：固定三份导出快照 index 100、200、300，删除 100 成功、删除 200 注入文件系统写错误；重建 snapshot manager 模拟进程重载磁盘目录，确认只恢复 200/300，再继续清理 200、保留 300。定向测试重复 **20 次通过**。该测试覆盖本地管理器的部分进度与重载，不代替三副本文件系统故障。

## TN WAL 所用 LogService 接口：原子截断水位与 snapshot 故障恢复（2026-11-12）

代码调用复查：单个 TN `logservicedriver` 对数据 shard 的服务端操作是 `AppendWAL`（客户端不支持时回退 `Append`）、`Read`、`GetWALTruncate`、`TruncateWAL`，兼容路径也会调用 `GetTruncatedLsn`；`UpdateLeaseholderID`、`SetRequiredLsn`、`ReadLsn` 不在该 driver 的实际调用链里。本轮补测此前三副本 snapshot 实验只用普通 `Truncate` 而没有覆盖的 **`TruncateWAL(PSN, checkpointedDSN)` 二元持久水位**。复用 51 上隔离三副本的原盘 `/home/mo/jxm/logservice-snapshot-import-window-20261112/`，服务端仍为上一节的测试二进制，未启用其导入屏障。扩展直接客户端探针的 `wal-truncate`、`wal-state`、`append-wal` 操作；本轮 `checkpointedDSN` 是测试传入的固定值，**没有运行真实 TN driver 或验证 DSN 与业务 checkpoint 的关系**。

1. **普通副本 node1 下线。** node0/node2 两票完成普通 `Append` **200/200**，提交 `TruncateWAL(PSN 564, checkpointedDSN 77)`。node1 原盘恢复，本地日志确认导入 index **434**；三个入口的 `GetWALTruncate` 均返回 `(564,77)`，从 PSN 565 起的 **79/79** 条用户日志与 ACK 的物理 LSN 和 payload 匹配。
2. **当时 leader node2 下线。** node0/node1 两票完成普通 `Append` **100/100**，推进 `TruncateWAL(716,88)`。node2 原盘恢复，本地日志确认导入 index **653**；三个入口的 `GetWALTruncate` 均为 `(716,88)`，保留 **39/39** 条匹配。整组三节点原盘冷重启后，同一水位和保留集再次经三个入口匹配。
3. **冷重启后新 leader node0 下线，使用真实 `AppendWAL` 提议接口。** node1/node2 两票完成 **100/100**，推进 `TruncateWAL(834,99)`。node0 原盘恢复，本地日志确认导入 index **766**；三个入口水位均为 `(834,99)`，保留 **39/39** 条匹配。再次整组原盘冷重启后水位及 39 条记录不变。

每个阶段的成功写入集合均从独立 ACK 日志生成，按返回的物理 LSN 和 payload 与 `Read` 比较；三个入口可能重定向到 leader，故它们的结果不单独证明每个副本本地字节一致，本地导入日志提供额外恢复证据。远端证据在 `wal-snapshot-state-20261112/` 的 `append-{a,b,c}.log`、`truncate-{a,b,c}.log`、各阶段 `state`/`read`/`cold` 日志和节点 `stdout.log`。本轮未见水位回退或已确认用户日志丢失。测试进程和网络命名空间已停止，磁盘与证据保留。

仍需针对**真实单 TN driver**补测的交错：后台 `TruncateWAL` 已提交且随后发生落后副本的 snapshot 导入时，旧 driver 继续并发追加、checkpoint、关闭，再由新 driver 读取持久二元水位并回放；此前已分别测过 driver 截断和 LogService snapshot，但尚未把两者放进同一故障窗口。另一个服务端窗口是 Dragonboat `SyncRequestImportSnapshot` **执行中**崩溃，上一节仅精确覆盖成功返回后的崩溃。旧版本持久 WAL 的升级兼容和 datasync 按本轮限定不纳入此清单。

## 单 TN WAL driver：checkpoint 物理截断与落后副本 snapshot 导入交错（2026-11-12）

为补上节真实 driver 的空白，在 51 上新建**全空盘**三副本 `/home/mo/jxm/logservice-wal-snapshot-driver-20261112/`，数据 shard 1，RPC `198.18.51.10:61601`、`:61611`、`:61621`，5 分钟 HAKeeper store 过期阈值，1 秒截断循环。服务端由当前 `48d13d7a01` 分支重新构建，未加故障钩子，SHA256 `4ec39f6ab84072741b1d4505b712eb46724362424d1a92cd6e745b0dd6928701`。探针复用 TN 生产 `wal.NewLogserviceHandle` 的 `ReplayForWrite`、`AppendEntry(GroupUserTxn)`、`RangeCheckpoint`、后台 `TruncateWAL` 与 `Close`；checkpoint 的 `GroupFiles` 内容是测试 marker，不加载真实 TAE 文件。每次新 driver 启动前旧 driver 均已结束，全程没有两个同时存活的 TN driver，也没有运行 datasync。

**第一轮：物理截断已提交，落后副本在旧 driver 仍写入时导入。** 先确认 100 条种子 WAL 和 checkpoint，再停 node2。首次探针在第 20 条新 WAL 后错误地请求了**低于已确认水位**的 checkpoint，收到 `small checkpoint intent` 并退出；该 20 条已确认 WAL 在下一实例回放中得到保留，属于夹具参数错误。修正为每 100 条 checkpoint、保留最近 80 条，新的单 driver 完成 **500/500** 条、**5** 次 checkpoint。node2 在第 **412** 条 ACK 后从原盘启动，仍有 88 条待写；它于 11:15:28 本地导入 index **346** 的 snapshot，旧 driver 直到 11:15:39 才完成写入。此前两票已成功提交 `TruncateWAL(PSN 897, checkpointedDSN 454)`。独立新 driver 恢复 `user_lsn=620, checkpointed=540, truncated_dsn=454`，回放 LSN **441–620** 共 180 条：全部与旧实例的独立 ACK 集匹配，且包含 checkpoint 水位后的 **80/80** 条。水位前的 100 条仍可回放，是物理截断只推进到 DSN 454 的结果，不代表 checkpoint 回退。随后杀 leader node1，仅让刚恢复的 node2 和 node0 两票继续写 **50/50** 条；新 driver 回放 LSN **441–670** 共 230 条，全部匹配。两票原盘冷重启后，`user_lsn=670, checkpointed=540, truncated_dsn=454` 与同一 230 条记录不变。

**第二轮：snapshot 导入时 checkpoint 与物理截断持续推进。** 先恢复 node1，再停 node2；另一个单 driver 每 20 条提交一次 checkpoint，完成 **300/300** 条和 **15** 次 checkpoint。node2 于第 **155** 条 ACK 后原盘启动，在 11:18:41 与 11:18:53 分别导入 index **1483、2151** 的 snapshot；旧 driver 同时持续写入，11:18:43、11:18:51、11:18:56 等时间点的 `TruncateWAL` 均成功，故这次确实命中了导入、写入和物理截断交错。新 driver 回放为 `user_lsn=970, checkpointed=950, truncated_dsn=972`；交付 LSN **931–970** 共 40 条全部匹配 ACK，其中 checkpoint 后必须保留的 **20/20** 条齐全。服务端返回 `(truncatedPSN=2246, checkpointedDSN=972)`。整组三节点原盘冷重启后，上述回放集合及二元水位不变。

两轮累计确认 **970** 个连续上层用户 LSN；核对以每次 ACK 的 LSN/payload 为独立基准，拒绝重复回放，并要求所有 checkpoint 后的 ACK 都出现。节点本地 snapshot 导入日志和 driver 的 `Wal-Truncate-Execute` 日志提供交错时序证据；各轮日志与进度文件保存在远端 `driver-snapshot-overlap/`。未发现已确认 WAL 丢失、重复、payload 错配或截断水位回退。测试节点和隔离网络均已停止，证据磁盘保留。仍未覆盖 `SyncRequestImportSnapshot` **执行中**崩溃、真实 TAE checkpoint 文件与对象存储的跨系统交错；本节只证明 LogService 与单个 TN WAL driver 的恢复路径。

## 单 TN WAL driver：截断已提交但回包丢失，期间副本导入 snapshot（2026-11-12）

复用上一节的全空盘起步三副本与相同服务端二进制，先从原盘冷启动，确认起始服务端 `GetWALTruncate=(PSN 2246, checkpointedDSN 972)`。停 node2，让 node0/node1 两票运行真实单 TN WAL driver；探针只包装 `TruncateWAL` 的第一次**成功返回**，写出 `psn=2293 checkpointed_dsn=1016 committed=true` 标记并扣住回包，随后把这次成功转换为一次测试注入的 `context.DeadlineExceeded`。其他 `AppendWAL`、checkpoint、driver 内部重试和新实例回放均执行生产代码；没有启动真实 TN 或 datasync。

旧 driver 在回包屏障期间继续写入和确认 checkpoint，node2 于主负载第 **182** 条 ACK 后从原盘恢复。node2 本地于 **11:23:12** 导入 index **2291** 的 snapshot，而屏障直到 **11:23:34** 才放行；因此确实覆盖了“截断已提交、旧 driver 尚未获回包、落后副本导入 snapshot”的时间窗。放行模拟错误后，driver 的 `GetWALTruncate` 于 11:23:35 读回已提交的 `(2293,1016)`，该次后台截断记录 **1 次重试后成功**；后续物理截断继续到 `(PSN 3352, checkpointedDSN 1412)`，node2 又导入 index **3224、3352**。主负载 **400/400** 条 ACK、**20/20** 次 checkpoint ACK，无上层写入失败；单条 ACK 最大 **1001.532 ms**。首次截断日志记录的约 54 秒包含人为屏障等待，不是未注入条件下的服务端延迟。

旧实例完全退出后，独立新 driver 恢复 `user_lsn=1370, checkpointed=1350, truncated_dsn=1412`，回放 LSN **1331–1370** 共 40 条，全部与 ACK 的 LSN/payload 匹配，其中 checkpoint 后必须保留的 **20/20** 条齐全。再杀当时 leader node0，让 node1 与刚恢复的 node2 两票继续写 **30/30** 条；新 driver 回放 LSN **1331–1400** 共 70 条，全部匹配。两票从各自原盘冷重启后仍为 `user_lsn=1400, checkpointed=1350, truncated_dsn=1412`、服务端 `(3352,1412)` 和同一 **70/70** 条。与上一节累计 **1400** 个上层 LSN 的独立 ACK 集核对，未见缺失、重复或内容错配。本轮没有发现新缺陷。

证据保存在 51 的 `driver-ackloss-snapshot-20261112/`：`truncate.committed`、`restart.time`、`release.time`、`write.log`、`replay-three.log`、`two-vote-write.log`、`two-vote-replay.log`、`cold-two-vote-replay.log` 与各节点本地日志。测试结束已停所有节点并清理隔离网络；仅保留磁盘与日志。该轮只模拟**服务端已返回成功、客户端处理前丢回包**，不能证明真实 TCP 丢包恰在相同位置的概率；它验证该可达错误结果下的 driver 与 LogService 状态恢复。

## Dragonboat 外部 snapshot 导入复制中断与重试（2026-11-12）

代码检查发现 `SyncRequestImportSnapshot` 会先在目标副本的临时 `.generating` 目录复制导出 snapshot，再请求 Dragonboat 导入；此前实机测试精确杀在**导入成功返回后**，没有覆盖复制中断留下的目标临时文件。本轮新增 `TestImportSnapshotRetryAfterPartialCopy`，使用真实单副本 LogService/Dragonboat shard、内存文件系统的定向故障包装，以及 TN WAL 会使用的 `TruncateWAL(PSN, checkpointedDSN)` 状态更新。先写入、截断并导出有效 snapshot，再继续写入、推进二元水位，使其成为可导入候选；故障只命中 Dragonboat 目标临时目录，不命中源导出目录。

四个子场景为：目标 snapshot 文件**写入 1 字节后报错**、文件**写完但 `Sync` 报错**，各自分别在**同进程重试**和**关闭服务、保留原文件系统重新启动后重试**。首次导入均返回注入的 I/O 错误，snapshot manager 仍保留源候选，目标临时文件确实存在；关闭服务后文件仍保留。故障解除后下一轮导入成功，候选被清理、目标临时文件消失，`GetWALTruncate` 始终保持 `(PSN 15, checkpointedDSN 2)`。定向测试连续 **20 次通过**，race 模式 **1 次通过**；`go list`、`go build`、`go vet` 也通过。未发现新缺陷。

这是**复制阶段 I/O 错误加原文件系统重载**的包级实验，不等同于在 51 上把进程杀在 Dragonboat 导入请求执行中；后者仍是实机故障注入空白。本轮没有运行 datasync，也没有启动真实 TN driver；水位由服务端生产 `truncateWAL` 路径提交。

## Dragonboat 外部 snapshot 复制首字节后进程崩溃：单 TN driver 与三副本恢复（2026-11-12）

先拉取 upstream main 到 `4760bcaa88`；对比已测的 `9381db5dde`，`pkg/logservice` 只有 gossip fixture 测试变化，生产 LogService 代码没有新增差异。本轮复用 51 上**全空盘起步、当前已保存 WAL 证据**的三副本 `/home/mo/jxm/logservice-wal-snapshot-driver-20261112/`，正常节点使用上一节的 `48d13d7a01` 分支服务端。为精确命中剩余窗口，仅给 node2 单独构建临时 Dragonboat 故障二进制：在 `tools.copyFile` 已打开目标 `.generating` snapshot 文件、从源文件复制并同步**首字节**后记录目标路径并 `os.Exit(137)`。该构建使用临时 module replacement，SHA256 `f509d5ebbd2469a32fb9884e7f68c0f1f5604c7012654832554914f20c3ede1e`；仓库的 `go.mod` 和生产源码均未改，正常重启又换回原二进制。没有运行 datasync。

起始服务端水位为 `(truncatedPSN=3352, checkpointedDSN=1412)`。停 node2，node0/node1 两票运行真实单 TN WAL driver 的 `ReplayForWrite`、`AppendEntry(GroupUserTxn)`、每 20 条一次 `RangeCheckpoint` 与后台 `TruncateWAL`；checkpoint 文件仍是测试 marker。主负载第 **458** 条 ACK 后以故障二进制从 node2 原盘启动。钩子记录 Dragonboat 目标 `snapshot-0000000000000D48-262145.generating/snapshot-0000000000000D48.gbsnap`，文件实测只留下 **1 字节**，node2 进程已退出，故障进程日志**没有** `import snapshot success`；这证明崩溃发生在 `SyncRequestImportSnapshot` 的文件复制阶段、成功返回之前。node0/node1 的 driver 继续写入；在第 **592** 条 ACK 后用正常二进制从 node2 原盘重启。node2 本地随后成功导入同一 index **3400** 的 snapshot，又导入更新的 index **4953**，残留的 1 字节目标文件消失；三个客户端入口恢复同一三成员视图。

旧 driver 最终确认 **1000/1000** 条 WAL 和 **50/50** 次 checkpoint，最高用户 LSN **2400**；单条 ACK 最大 **2323.434 ms**，没有超过 60 秒的写入。它完全退出后，独立新 driver 读回 `user_lsn=2400, checkpointed=2380, truncated_dsn=2542`，服务端 `GetWALTruncate=(5351,2542)`；回放 LSN **2361–2400** 共 40 条全部与独立 ACK 的 LSN/payload 匹配，checkpoint 后必须保留的 **20/20** 条齐全。再杀当时 leader node1，仅让 node0 与经历复制中崩溃的 node2 两票继续写 **30/30** 条；新 driver 回放 LSN **2361–2430** 共 70 条匹配。两票原盘冷重启后同一 70 条及水位不变；恢复 node1 后三个入口均报告原三成员，独立回放也与两票冷重启的 70 条逐条相同。与此前同一隔离盘上的 ACK 累计核对 **2430** 个连续上层 LSN，未发现缺失、重复或内容错配。

远端证据在 `driver-midcopy-crash-20261112/` 的 `crash.marker`、`partial-file.stat`、`write.log`、`replay-three.log`、`two-vote-write.log`、`two-vote-replay.log`、`cold-two-vote-replay.log`、`three-restored-replay.log`、状态日志和 node2 的 `stdout-crash.log`/`stdout.log`。本轮没有发现新缺陷。该实验精确覆盖**复制首字节后的进程崩溃**；不代表复制完成后但 Dragonboat 完成导入前的所有内部阶段，也不验证真实 TAE checkpoint 文件。三个测试节点和隔离网络均已停止，磁盘证据保留。

## Dragonboat 外部 snapshot 复制完成、导入请求前崩溃：单 TN driver 与三副本恢复（2026-11-12）

继续复用 51 上 `/home/mo/jxm/logservice-wal-snapshot-driver-20261112/` 的隔离三副本原盘和真实单 TN WAL driver，不运行 datasync。为了命中此前未测的切点，仅给 node2 构建临时 Dragonboat 故障二进制：`tools.CopySnapshot` **成功返回后、调用 `n.requestImportSnapshot(ss)` 之前**，记录目标 `.generating` 目录并 `os.Exit(137)`。node0/node1 始终运行正常服务端；node2 故障后从同一原盘换回正常二进制。临时模块替换和钩子未写入仓库生产源码或 `go.mod`，故障二进制 SHA256 为 `fee75a1aec0207e57bd86541d332da276f0b27b83df5b57affcfbbe878d793f3`。

起始服务端 `GetWALTruncate=(PSN 5351, checkpointedDSN 2542)`。停 node2，让 node0/node1 两票承担写入；顺序启动三个**不重叠**的单 driver 实例。前两次分别确认 **800/800**、**500/500** 条 WAL，并完成 **40、25** 次测试 checkpoint；第三个持续写入实例运行期间，node2 用故障二进制从原盘启动。钩子命中 snapshot index **8306**（文件名 `2072` 为十六进制）、副本 ID **385026**；目标 `.generating` 中的 `.gbsnap` 为 **1094 字节**，与源导出文件的 SHA256 完全相同。故障进程退出，日志没有 `import snapshot success`；正常二进制从该原盘重启后，node2 日志确认成功导入同一 index **8306**，残留目标临时目录消失。三客户端入口随后报告同一三成员视图。

本轮 node2 停止时间超过测试环境的 **5 分钟** store 过期阈值，HAKeeper 已把原 ID **262145** 替换为 **385026**；因此上述导入发生在**新成员 ID** 上，不能解读成旧 ID 原样回归。后续原盘整组冷重启、切主和恢复后，三入口的成员表均为 `{262146, 262147, 385026}`，旧 ID 未重新加入。

第三个 driver 共确认 **501/501** 条 WAL，最高用户 LSN **4231**；终止它时另有一条未收到 ACK 的 `driver-postcopy-live/000501` 已提交为 LSN **4232**。独立新 driver 回放到 4232，确认前述 **4231** 个连续 ACK LSN 的内容全部匹配，并把 4232 单独列为“已提交、回包未确认”的记录。接续 driver 再确认 **30/30** 条，LSN **4233–4262**。三个节点随后从各自原盘整组冷重启：`GetWALTruncate=(PSN 8773, checkpointedDSN 4574)`，独立回放 `user_lsn=4262, checkpointed=4210`；checkpoint 后 LSN **4211–4262** 共 **52** 条逐一匹配，其中 **51** 条有 ACK，4232 为上述未确认提交。

冷重启后 leader 为 node2 的 **385026**。杀 node2，仅保留 node0/node1 两票，继续确认 **30/30** 条 WAL，LSN **4263–4292**；独立回放到 4292。node2 原盘恢复后三入口成员一致，独立回放 checkpoint 后 **82/82** 条（4211–4292）内容和顺序均正确。五段本轮写入合计 **1861** 条 ACK；与该隔离盘此前 **2430** 条 ACK 合并，已确认用户 LSN **1–4231、4233–4292** 连续且无内容错配，唯一间隔 4232 是确实提交但无 ACK 的在途请求。本轮未发现新缺陷。五段 ACK 最大耗时依次约 **1.49、1.80、1.17、0.21、0.95 秒**；没有发生一分钟级写入长尾。

证据保存在 51 的 `driver-postcopy-crash-20261112/`：`postcopy.marker`、`postcopy-files.txt`、`postcopy-sha256.txt`、`write.log`、`round2.log`、`live.log`、`continued-write.log`、`cold-replay.log`、`cold-status.log`、`two-vote-write.log`、`two-vote-replay.log`、`three-restored-replay.log`、`three-restored-status.log` 和 node2 的 `stdout-postcopy-crash.log`/`stdout.log`。实验结束已停三个节点并拆除隔离网络，原盘及日志保留。该切点位于**复制完成与调用导入函数之间**；导入函数内部的后续持久化阶段见下节。真实 TAE checkpoint 文件的故障交错仍需另测。

## Dragonboat 外部 snapshot 导入内部两处崩溃：Commit 返回与内存引用更新（2026-11-12）

沿用 51 上同一隔离三副本原盘、同一组客户端地址和真实**单 TN WAL driver**；不运行 datasync。临时 Dragonboat 二进制在 `n.requestImportSnapshot` 内设置两个精确崩溃切点：① `snapshotter.Commit` 成功返回后、`logReader.CreateSnapshot` 前；② `logReader.CreateSnapshot` 成功返回后、`compactLog` 前。各切点写入标记再 `os.Exit(137)`，仅 node2 使用钩子二进制；恢复和整组冷重启均使用正常二进制。第一切点临时构建的 SHA256 为 `26e5827ff8d819cbdd6223da7288df9a667365cababfb1aef1ba14d6328c129d`；第二切点最终去掉了对固定副本 ID 的依赖，其临时构建 SHA256 为 `f1184080a3e84393f1bba061bca16f1379be1bd01c18c4e8e5211bd97a9880d7`。仓库生产源码和 `go.mod` 未改。

**Commit 返回后崩溃。** node2 下线期间，node0/node1 两票由单 driver 连续写入并每 20 条提交一次测试 checkpoint。node2 从原盘以钩子二进制启动，标记记录 `replica=385026 index=9062 after-commit`；此时 `snapshotter.Commit` 已完成正式 snapshot 目录、LogDB snapshot 记录和恢复标记删除，故障进程退出。换正常二进制从同一原盘重启后，node2 日志记录成功导入**同一 index 9062**。本段 driver 确认 **600/600** 条 WAL，LSN **4293–4892**，完成 **30** 次 checkpoint；独立新 driver 回放到 `user_lsn=4892, checkpointed=4872`。

**内存引用更新后、日志压缩前崩溃。** 代码复核确认 `logReader.CreateSnapshot` 只设置进程内的 snapshot 引用，LogDB 持久化已在前面的 `snapshotter.Commit` 中完成；先前把这个切点称为“LogDB 更新后”不准确。首次尝试只让 node2 落后约 125 条，它直接追上，未导入 snapshot；该尝试仍确认 **400/400** 条 WAL，LSN **4893–5292**，但不能算切点命中。第二次尝试扩大落后量，确认 **1000/1000** 条 WAL，LSN **5293–6292**；期间 HAKeeper 将 node2 的副本 ID 从 **385026** 替换为 **405505**，旧钩子限定旧 ID，故未触发。紧接着一次快速重启因数据目录锁尚未释放而失败，也未进入目标切点。恢复正常三成员后，改用不限定副本 ID 的临时钩子，再让 node2 落后约 500 条；标记记录 `replica=405505 index=14109 after-logdb`，正式 snapshot 目录已存在，故障进程退出。标记名 `after-logdb` 是旧测试命名，实际位置为内存引用更新后。该故障进程在目标 index **14109** 上没有 `import snapshot success`；正常二进制原盘重启后继续处理更新的 snapshot，记录成功导入 index **14113**。本段最终写入确认 **800/800** 条 WAL，LSN **6293–7092**，完成 **40** 次 checkpoint。后两次没有证明旧 ID 在原成员关系中恢复；最终成员为 `{262146,262147,405505}`。

最后独立 driver 回放到 `user_lsn=7092, checkpointed=7072, truncated_dsn=7694`，服务端 `GetWALTruncate=(PSN 14323, checkpointedDSN 7694)`。checkpoint 后尾部 LSN **7053–7092** 的 **40/40** 条内容与 ACK 相同。三节点从各自原盘整组冷重启后，截断水位、三入口成员表和同一 **40/40** 条回放均不变；再确认 **30/30** 条续写，LSN **7093–7122**，新 driver 回放 LSN **7053–7122** 共 **70/70** 条全部匹配。连同两次未命中切点的有效负载，本轮 **2830** 条 ACK 的 LSN **4293–7122** 连续、无重复或内容错配；四段较大负载单条 ACK 最大分别约 **1.40、1.38、1.85、1.03 秒**。本轮未发现新数据一致性缺陷或一分钟级写入长尾。

证据在 51 的 `driver-import-stages-20261112/`：两个命中切点的 `commit.marker`、`logdb-final.marker`，对应 node2 日志、正式 snapshot 路径清单，各轮 `*-write.log`、`commit-replay.log`、`pre-cold-replay.log`、`cold-replay.log`、`cold-continue-replay.log`、水位和三入口状态日志。实验结束已停三个节点并拆除隔离网络。该实验分别验证两个**成功返回后的调用边界**；`snapshotter.Commit` 内部“正式目录完成但 LogDB 尚未保存”等窗口见下节，也不验证真实 TAE checkpoint 文件。

## Dragonboat snapshotter.Commit 内部两处崩溃：正式目录与 LogDB 保存之间（2026-11-12）

代码复核明确了持久化顺序：`snapshotter.Commit` 先在临时目录保存 metadata，`FinalizeSnapshot` 创建 `dragonboat.snapshot.message` 标记并重命名到正式目录，随后 `saveSnapshot` 将 snapshot 记录写入 LogDB，最后 `RemoveFlagFile` 删除标记；`logReader.CreateSnapshot` 随后只更新进程内引用。本轮在 51 的同一隔离三副本原盘上精确测试**正式目录重命名成功、LogDB 尚未保存**与**LogDB 保存成功、标记尚未删除**两处。仅 node2 运行临时 Dragonboat 故障二进制，钩子限定 `req.OverrideCompaction` 的导入路径并在指定调用返回后记录标记、`os.Exit(137)`；其余节点、所有恢复启动均使用正常二进制。临时构建 SHA256 为 `a89746290dacc12e92e50bb04ea6eb61a424e71483296e57bb7c47e15f59f664`，未改仓库生产源码或 `go.mod`，未运行 datasync。

从已核对的用户 LSN **7122**、服务端 `GetWALTruncate=(PSN 14323, checkpointedDSN 7694)` 起步。每轮只运行一个真实 TN WAL driver，node2 下线时 node0/node1 两票继续写入，并每 20 条提交一次测试 checkpoint。**重命名后崩溃**命中 `replica=405505 index=14678 after-finalize`；崩溃前正式 snapshot 目录存在，包含 `dragonboat.snapshot.message`、metadata 和 `.gbsnap`。正常二进制从 node2 同一原盘重启后，服务端记录成功导入**同一 index 14678**，正式目录中的标记消失。本段确认 **800/800** 条 WAL，LSN **7123–7922**、**40** 次 checkpoint；独立 driver 回放 `user_lsn=7922, checkpointed=7902, truncated_dsn=8604`，尾部 **40/40** 条与 ACK 匹配。按 `processOrphans` 代码，LogDB 尚无该 index 时会删除带标记的孤儿目录，再从仍在的导出 snapshot 重试；实机证据直接确认的是标记变化、同 index 导入和回放成功，没有单独记录删除与重建的两个文件系统事件。

**LogDB 保存后崩溃**命中 `replica=405505 index=16501 after-save`；崩溃前正式目录仍有 `dragonboat.snapshot.message`。正常二进制从 node2 同一原盘重启后，标记消失，服务端记录 index **16501** 的导入成功。本段确认 **800/800** 条 WAL，LSN **7923–8722**、**40** 次 checkpoint。独立 driver 回放 `user_lsn=8722, checkpointed=8702, truncated_dsn=9484`，服务端 `GetWALTruncate=(PSN 17628, checkpointedDSN 9484)`；尾部 **40/40** 条与 ACK 匹配。按代码，`processOrphans` 在 LogDB 已有同 index 时应保留正式目录并清理标记；实机观察到标记清理和恢复服务，但没有单独记录该分支的内部决策。

三个节点从各自原盘**整组冷重启**后，三客户端入口均报告成员 `{262146,262147,405505}`，水位仍为 `(PSN 17628, checkpointedDSN 9484)`，独立回放尾部 **40/40** 条不变。再确认 **30/30** 条续写，LSN **8723–8752**；新 driver 回放 LSN **8683–8752** 共 **70/70** 条顺序和内容匹配。本轮累计 **1630** 条 ACK 的 LSN **7123–8752** 连续、无重复或内容错配；两段主负载最大单条 ACK 分别约 **1.46、1.13 秒**，未见一分钟级写入长尾或新数据一致性缺陷。

远端证据在 `/home/mo/jxm/logservice-wal-snapshot-driver-20261112/driver-commit-stages-20261112/`：`finalize.marker`、`save.marker`、`finalize-files-before-restart.txt`、`save-files-before-restart.txt`、两轮 `*-write.log`、`finalize-replay.log`、`pre-cold-replay.log`、`cold-replay.log`、`cold-continue-replay.log`、水位及三入口状态日志。实验结束已停三个节点并拆除隔离网络，磁盘证据保留。尚未覆盖单个文件写入或 LogDB `SaveSnapshots` **调用中途**的 I/O 失败，以及真实 TAE checkpoint 文件。

## 导出 snapshot 正式目录完成但标记未清理：单 TN driver 与原盘恢复（2026-11-12）

复用 51 上同一隔离三副本原盘，从用户 LSN **8752**、服务端 `GetWALTruncate=(PSN 17628, checkpointedDSN 9484)` 起步。正常服务端维持 node0/node1；node2 使用临时 Dragonboat 二进制，仅当 `snapshotter.Commit` 处理 **Exported** 请求且 `FinalizeSnapshot` 成功返回后，在 `RemoveFlagFile` 前记录标记并 `os.Exit(137)`。临时构建 SHA256 为 `f81ecdb198ebed524c19d0e20c4ff74098593386b3c29b8beb3b194354033cde`。仓库生产源码和 `go.mod` 未改，不运行 datasync。

单个真实 TN WAL driver 持续写入、每 20 条提交一次测试 checkpoint。标记命中 `replica=405505 index=18724 after-export-finalize`，node2 退出；其导出正式目录当时包含 `.gbsnap`、`snapshot.metadata` 和 `dragonboat.snapshot.message`。导出还没有成功返回到 LogService 的 `exportSnapshot`，因此也没有执行 `snapshotManager.Add`。node0/node1 两票继续写入。换正常二进制从 node2 原盘重启后，带标记的 index **18724** 目录消失，随后 node2 记录更高 index 的新导出与导入；继续观察的导出目录数为 **3–4**，没有超过配置上限 **4**。这与启动时 `ReconcileInterruptedExport` 清理带标记残留的代码路径一致；实机直接核对了目录消失和后续进度，没有对清理函数内部单独插桩。

主负载确认 **400/400** 条 WAL，LSN **8753–9152**、**20** 次 checkpoint；独立 driver 回放 `user_lsn=9152, checkpointed=9132, truncated_dsn=9954`，服务端水位 `(PSN 19104, DSN 9954)`，尾部 **40/40** 条与 ACK 匹配。三节点整组原盘冷重启后，水位和三入口成员表 `{262146,262147,405505}` 不变，同一 **40/40** 条再次匹配。再确认 **60/60** 条续写和 **3** 次 checkpoint，LSN **9153–9212**；水位推进到 `(PSN 19787, DSN 10020)`，回放 `user_lsn=9212, checkpointed=9192`，尾部 **40/40** 条匹配。本轮 **460** 条 ACK 连续，主负载最大单条 ACK 约 **1.59 秒**，未发现新数据一致性缺陷或一分钟级写入长尾。

证据保存在 51 的 `driver-export-finalize-20261112/`：`export-finalize.marker`、`export-files-before-restart.txt`、`export-dir-count-before-restart.txt`、`export-node2.log`、`write.log`、`pre-cold-replay.log`、`cold-replay.log`、`cold-continue-write.log`、`cold-continue-replay.log`、水位及三入口状态日志。实验后节点和隔离网络已停止。

## 导出成功返回至 snapshotManager.Add 之间崩溃：未登记快照的重启恢复（2026-11-12）

继续复用同一隔离三副本原盘，从用户 LSN **9212**、服务端 `GetWALTruncate=(PSN 19787, checkpointedDSN 10020)` 起步。只在 node2 的临时 LogService 构建中，于 `SyncRequestSnapshot(Exported=true)` **成功返回后、`snapshotManager.Add` 前**记录返回的 index 并 `os.Exit(137)`；构建使用 Go overlay，未改仓库源码或 `go.mod`，SHA256 为 `785af46f4801a87c9f25da1d87a55a18f3e8823b60cae8800d273a4584a6a863`。node0/node1 两票仍运行正常服务端，负载仍是单个真实 TN WAL driver，不运行 datasync。

标记命中 `replica=405505 index=20370 after-export-success`，node2 退出；当时导出正式目录只有 `.gbsnap` 和 `snapshot.metadata` 两个文件，没有完成标记，LogService 尚未调用 `snapshotManager.Add`。正常二进制从 node2 同一原盘重启后，该目录先保持存在，随后服务端记录**同一 index 20370** 的导入成功，目录消失。由代码与目录、日志顺序可判定启动时的 `snapshotManager.Init` 找回了完整导出候选；未对 `Init` 本身插桩。后续导出目录数为 **4**，没有超过配置上限。

主负载确认 **400/400** 条 WAL，LSN **9213–9612**、**20** 次 checkpoint；独立 driver 回放 `user_lsn=9612, checkpointed=9592, truncated_dsn=10460`，服务端水位 `(PSN 20745, DSN 10460)`，尾部 **40/40** 条与 ACK 匹配。三节点整组原盘冷重启后，水位、三入口成员和同一 **40/40** 条均不变。再确认 **60/60** 条续写和 **3** 次 checkpoint，LSN **9613–9672**；水位推进到 `(PSN 21525, DSN 10526)`，回放 `user_lsn=9672, checkpointed=9652`，尾部 **40/40** 条匹配。本轮 **460** 条 ACK 连续，主负载最大单条 ACK 约 **1.50 秒**；未发现新数据一致性缺陷或一分钟级写入长尾。

证据保存在 51 的 `driver-export-preadd-20261112/`：`export-preadd.marker`、`export-files-before-restart.txt`、`export-dir-count-before-restart.txt`、`export-node2.log`、`write.log`、`pre-cold-replay.log`、`cold-replay.log`、`cold-continue-write.log`、`cold-continue-replay.log`、水位及三入口状态日志。实验后节点和隔离网络已停止。这两轮均在 node2 执行，没有锁定它在导出瞬间的 leader 角色；刻意让当前 leader 命中同一窗口、或底层文件写入中途失败的组合仍需单测。

## 导出快照配额已满时隔离 node2：真实 SyncRead 超时与 DropNewest（2026-11-12）

复用 51 上同一隔离三副本原盘、原服务端和真实**单 TN WAL driver**；不运行 datasync，也未用故障二进制。起始用户 LSN **9672**、服务端 `GetWALTruncate=(PSN 21525, checkpointedDSN 10526)`，node2（副本 **405505**）的导出 snapshot 目录有 **4** 份，恰好达到 `max-exported-snapshot=4`。三节点正常启动并确认三成员后，用隔离网络的 `netns.sh down 2` 切断 node2 对另两节点的通信，仍保持 node0/node1 两票连通。

隔离开始于 **16:37:00**。node2 日志在 **16:37:03** 记录一次 `get truncated lsn timed out, tick skipped`，紧接着记录 `dropped newest exported snapshot to unblock truncation`，字段为 `droppedIndex=21561, before=4, after=3`；磁盘目录数也从 **4** 降到 **3**。随后 node2 丢失 leader 视图，截断循环反复记录 `no leader yet, skip truncate`，所以本轮只观察到**一次**真实超时触发的 DropNewest；不能把后续跳过 tick 计作重复超时。此时较老的 index **21559** 导出候选没有被丢弃。

隔离期间 node0/node1 两票完成 **200/200** 条 WAL，LSN **9673–9872**，以及 **10** 次测试 checkpoint；最大单条 ACK **904.867 ms**。约 **105 秒**后恢复 node2 网络，此时旧 driver 已结束，没有新写入推动恢复。node2 于 **16:38:48** 导入保留下来的 snapshot index **21559**；三个客户端入口仍报告成员 `{262146,262147,405505}`。独立新 driver 回放 `user_lsn=9872, checkpointed=9852, truncated_dsn=10746`，服务端水位 `(PSN 22137, DSN 10746)`，checkpoint 后 LSN **9833–9872** 共 **40/40** 条与 ACK 匹配；node2 的导出目录数观察为 **2**。

三节点随后从各自原盘整组冷重启，水位、三入口成员表和同一 **40/40** 条回放不变。再确认 **60/60** 条续写、**3** 次 checkpoint，LSN **9873–9932**；服务端水位推进到 `(PSN 23022, DSN 10834)`，独立回放 `user_lsn=9932, checkpointed=9912`。物理截断后实际保留的尾部为 LSN **9913–9932** 共 **20/20** 条，与 ACK 逐条匹配；导出目录数回到配置上限 **4**，没有观察到越界。本轮 **260** 条 ACK 连续，无重复或内容错配，也没有一分钟级写入长尾。

证据在 51 的 `driver-quota-timeout-20261112/`：`partition.time`、`reconnect.time`、`partition-write.log`、`recovered-replay.log`、`recovered-wal-state.log`、`recovered-status.log`、`cold-replay.log`、`cold-wal-state.log`、`cold-continue-write.log`、`cold-continue-replay.log`、`cold-continue-wal-state.log`，以及 node2 的 `stdout.log` 中带时间戳的 timeout、DropNewest 和导入日志。实验后节点与隔离网络均已停止。本轮验证了真实网络隔离造成的一次超时及候选保留和恢复；没有覆盖持续可获得 leader 视图、连续多次 `SyncRead` 超时时的循环行为，也没有覆盖 `max-exported-snapshot=1` 的保留单候选分支。

## 连续截断水位读取超时、配额降至 1：保留单候选与同进程恢复（2026-11-12）

承接上一节，在 51 的同一隔离三副本原盘上测试持续可见 leader 时的超时分支。起始用户 LSN **9932**、node2（副本 **405505**）有 **4** 份导出 snapshot，index 为 **23041、23043、23046、23049**。仅给 node2 使用 Go overlay 临时二进制，在 `store.getTruncatedLsn` 读取 shard 1 的水位前检查测试标记文件；标记存在时返回 `dragonboat.ErrTimeout`，删除标记后同一进程恢复生产读取。临时二进制 SHA256 为 `04c9f4c2ce4f2e50b79f1765c49b9fa6a2dad15c23ea2571a471931649fcbc13`。同时将**该测试节点**的 `max-exported-snapshot` 从 **4** 暂改为 **1**，原配置另存并在测试后原样恢复；node0/node1 配置与正常服务端未变。未改仓库源码或 `go.mod`，未运行 datasync。

node2 重启后，三票持续互通，leader 视图可用。开启受控超时的前几轮截断 tick 依次记录 `droppedIndex=23049, before=4, after=3`、`23046, 3→2`、`23043, 2→1`。磁盘最终只保留最老的 index **23041**；在标记文件仍存在期间，node2 累计记录 **105** 次 `get truncated lsn timed out`，DropNewest 始终只有 **3** 次，没有把最后一份候选删掉。测试时由唯一真实 TN WAL driver 完成 **200/200** 条写入，LSN **9933–10132**、**10** 次测试 checkpoint，最大单条 ACK **1956.609 ms**，没有一分钟级长尾。这里的 timeout 是**受控返回值注入**，并非真实网络超时；上一节才是真实网络故障对照。

删除超时标记后**不重启 node2**，其 PID 始终为 **1565366**。最后一次 timeout 日志早于删除时刻；约 2 秒后，node2 记录成功导入保留的 snapshot index **23041**，随后又出现一份新导出候选，目录数仍为 **1**。独立 driver 回放 `user_lsn=10132, checkpointed=10112, truncated_dsn=11032`，服务端 `GetWALTruncate=(PSN 23697, checkpointedDSN 11032)`，尾部 LSN **10093–10132** 共 **40/40** 条与 ACK 匹配。继续在**同一个 node2 进程**确认 **60/60** 条写入、**3** 次 checkpoint，LSN **10133–10192**；水位推进到 `(PSN 24227, DSN 11098)`，独立回放尾部 **40/40** 条匹配。

然后停止三个节点，还原 node2 的原配置 `max-exported-snapshot=4`，全部换正常二进制从各自原盘冷重启。三入口仍报告成员 `{262146,262147,405505}`，水位保持 `(PSN 24227, DSN 11098)`，同一尾部 **40/40** 条回放不变。再确认 **60/60** 条写入和 **3** 次 checkpoint，LSN **10193–10252**；水位推进到 `(PSN 25085, DSN 11164)`，独立回放 LSN **10213–10252** 共 **40/40** 条匹配。全轮 **320** 条 ACK 的 LSN **9933–10252** 连续，无重复或内容错配；最终导出目录数为 **3**，低于还原后的配额 **4**。未发现新缺陷。

证据在 51 的 `driver-quota-single-20261112/`：`node2-hook.log`、`log-2.toml.original`、`release.time`、`node2.pid.before-release`、`timeout-write.log`、`released-replay.log`、`released-write.log`、`released-continue-replay.log`、`cold-replay.log`、`cold-write.log`、`cold-continue-replay.log`、水位及三入口状态日志。实验后节点和隔离网络均已停止，node2 配置已恢复。这验证了**已存在 4 份候选时把配额降到 1**的清理与单候选保留路径；不等同于从空盘起步一直使用配额 1 的部署，也不证明真实网络会连续 105 次保持 leader 视图并恰好只让 `SyncRead` 超时。

## 导出请求超时但 Dragonboat 迟到完成：三次连续回归验证（2026-11-12）

针对前文记录的 #32161 时间线，在 51 同一隔离三副本原盘上用正常服务端和真实**单 TN WAL driver**重测。服务端二进制 `go version -m` 标识为 `48d13d7a01bf+dirty`；本轮未用故障二进制、未改配置、未运行 datasync。起始用户 LSN **10252**，node2 当前副本 **405505**，导出快照配额 `max-exported-snapshot=4`。用从旧复现脚本改成当前测试目录及副本 ID 的 `inotify` 监听 node2 的导出 `.generating` 目录创建，命中后只对该正常 `mo-service` 进程 `SIGSTOP` **5 秒**再 `SIGCONT`；node0/node1 保持连通和运行。每轮监听器退出后立即启动下一轮，node2 全程保持**同一 PID 1605423**。

三次分别命中导出 index **25560、25678、25754**。每次暂停后，node2 都记录真实 `request export snapshot failed: timeout`；恢复后 Dragonboat 在磁盘留下只有 `.gbsnap` 与 `snapshot.metadata` 的有效正式目录。第一、二份在 node2 未重启时被后续流程清理；第三份也在主负载结束前消失。每次检查时正式导出目录数均为 **4**，没有重现旧实验中 **7>4** 的配额越界。代码的 `MarkExportUncertain` 与 `ReconcileUncertainExports` 可解释该结果；实机证据直接确认的是三次 timeout、迟到形成的两文件目录、同 PID 期间目录消失和配额抽样，未对 manager 内部登记动作单独插桩。该抽样不能证明任意瞬间都不可能短暂超过配额。

单 driver 主负载确认 **400/400** 条 WAL，LSN **10253–10652**、**20** 次测试 checkpoint；最大单条 ACK **5848.035 ms**，未达到一分钟。独立新 driver 回放 `user_lsn=10652, checkpointed=10632, truncated_dsn=11604`，服务端 `GetWALTruncate=(PSN 25940, checkpointedDSN 11604)`；checkpoint 后 LSN **10613–10652** 共 **40/40** 条与 ACK 匹配，三客户端入口仍报告成员 `{262146,262147,405505}`。三节点整组原盘冷重启后水位、成员和同一 **40/40** 条回放不变。再确认 **60/60** 条续写和 **3** 次 checkpoint，LSN **10653–10712**；水位推进到 `(PSN 26404, DSN 11670)`，独立回放 LSN **10673–10712** 共 **40/40** 条匹配。三份迟到目录均仍不存在，导出目录数为 **4**。本轮 **460** 条 ACK 连续，无重复或内容错配；未发现新的数据一致性缺陷。

远端证据在 `/home/mo/jxm/logservice-wal-snapshot-driver-20261112/driver-late-export-20261112/`：`pause_snapshot_export.py`、`watcher{1,2,3}.log`、`round1-write.log`、`pre-cold-replay.log`、`pre-cold-wal-state.log`、`cold-replay.log`、`cold-continue-write.log`、`cold-continue-replay.log`、水位和三入口状态日志，以及 node2 `stdout.log` 的三次 timeout。实验后节点和隔离网络均已停止。本轮验证连续三次**真实请求超时、迟到完成**在当前服务端上的运行中清理；没有覆盖更大的 snapshot 文件、长时间停机或超过三次事件持续发生时的磁盘峰值。

## 导出正式目录尚带完成标记时超时：运行中扫描、放行与冷重启（2026-11-12）

继续使用 51 上同一隔离三副本原盘和真实**单 TN WAL driver**，不运行 datasync。起始已确认用户 LSN **10712**，服务端水位 `(PSN 26404, checkpointedDSN 11670)`。node2（副本 **405505**）已有 4 份导出快照，等于原配额；为给本次导出留一个位置，仅把 node2 测试配置的 `max-exported-snapshot` 暂改为 **5**，原配置另存。node0/node1 保持正常服务端；node2 使用临时 Dragonboat 构建，在 `FinalizeSnapshot` 已把 `.generating` 重命名为正式目录之后、`RemoveFlagFile` 删除 `dragonboat.snapshot.message` 之前，按标记文件阻塞导出线程。临时二进制 SHA256 为 `7f6ead3fe49aa0516aa5265306daf6c0c3d19b636d21a639d769e31d65f47785`；仓库生产源码和 `go.mod` 未改。

钩子于 **17:09:39** 命中导出 index **26722**，node2 PID **1688410** 保持运行。正式目录 `snapshot-0000000000006862` 当时确有 **3 个文件**：3251 字节 `.gbsnap`、442 字节 `snapshot.metadata`、442 字节 `dragonboat.snapshot.message`；连同旧目录共 5 份。阻塞约 **20 秒**期间，node2 截断循环多次记录 `request export snapshot failed: timeout`，仍在运行并进入后续 tick；目标目录没有被删除。**17:09:59** 放行后，目标目录只剩 `.gbsnap` 和 metadata 两文件；node2 后续日志确认于 **17:10:27** 成功导入同一 index **26722**。这些观察支持扫描没有把带完成标记的目录当作可导入快照；本轮没有对 `snapshotManager` 内存列表单独插桩。

随后单 driver 确认 **240/240** 条用户 WAL、**12** 次测试 checkpoint，LSN **10713–10952** 连续，最大单条 ACK **1927.864 ms**。独立新 driver 回放尾部 LSN **10913–10952** 共 **40/40** 条，与 ACK 的 LSN/payload 逐条相同，服务端水位 `(PSN 27261, DSN 11934)`。停三节点，恢复 node2 原配额 **4** 和正常二进制，从三份原盘整组冷重启；三个客户端地址报告同一成员 `{262146,262147,405505}`，水位和同一 **40/40** 条回放不变。冷重启刚完成时仍有 5 份旧导出目录；继续确认 **60/60** 条、**3** 次 checkpoint，LSN **10953–11012**，独立回放尾部 **10973–11012** 共 **40/40** 条匹配，水位推进到 `(PSN 27932, DSN 12000)`，目录数回到配额 **4**。这段暂时的 5>4 出现在测试主动调低配置之后，不能作为默认固定配额下的越界缺陷。

证据保存在 51 的 `driver-export-barrier-20261112/`：`ready`、`release`、`at-barrier.txt`、`after-release-files.txt`、`node2-hook.log`、`cycle.log`、`pre-cold-replay.log`、`cold-replay.log`、`cold-write.log`、`cold-continue-replay.log`、水位和三入口状态日志，以及原配置备份。实验后已恢复配置，停三个节点并拆除隔离网络。本轮未发现新缺陷；仅覆盖了 node2 一次约 20 秒的导出线程阻塞，以及小型 snapshot 文件。

## 导出 `.generating` 目录写成后进程崩溃：新 upstream 清理与单 TN driver 恢复（2026-11-13）

先合入当日最新 `upstream/main` **99605b3914**；相对 51 此前的服务端，新代码在 `snapshotManager.ReconcileInterruptedExport` 启动路径中清理同一副本遗留的 `.generating` 导出目录。三节点正常服务端重新从该版本构建，SHA256 `89de740d290acc5ddf31b1657d41637e41b8637f6788a2bfbe14805f6ab5e7e4`。另用临时 Dragonboat 模块副本构建 node2 专用故障二进制，SHA256 `d3d592392cc7b2c6cdc392a6fc7aa9d77311cbf58105d7962b49e0e1dddc9497`；钩子只在 `snapshotter.Commit` 已完成 `SaveSSMetadata`、尚未 `FinalizeSnapshot` 时对 exported snapshot 记录路径并 `os.Exit(137)`。仓库生产源码和 `go.mod` 未改；不运行 datasync。

复用 51 同一隔离三副本原盘，起始 `GetWALTruncate=(PSN 27932, checkpointedDSN 12000)`。node2 已有 4 份导出目录，因此只对它把测试配额从 **4** 临时改为 **5**，原配置备份。先让三节点正常启动并运行一个真实 TN WAL driver，负载中途停 node2 正常进程，改用钩子版从原盘启动。钩子实际命中副本 **405505**、导出 index **28467**，进程退出；原盘留下 `snapshot-0000000000006F33-405505.generating`，其中 `.gbsnap` **1099 字节**、`snapshot.metadata` **442 字节**。该目录和大小在恢复前实测记录。node0/node1 两票仍可服务；故障进程退出后单 driver 至少继续确认到第 **262** 条。随后以正常新版从 node2 **同一原盘**启动，目标 `.generating` 目录在启动后检查已消失，node2 继续参与三成员集群。这里直接观察到的是目录消失与进程恢复；清理归因于新增启动代码，未对 `ReconcileInterruptedExport` 函数调用单独插桩。

主负载最终确认 **400/400** 条用户 WAL、**20** 次测试 checkpoint，LSN **11013–11412** 连续；单条 ACK 最大 **1640.589 ms**。独立新 driver 回放保留尾部 LSN **11373–11412** 共 **40/40** 条，与 ACK 的 LSN/payload 逐条匹配，服务端水位 `(PSN 28768, DSN 12440)`。停三节点、恢复 node2 原配额 **4**，三份原盘用正常新版整组冷重启；三个地址报告相同成员 `{262146,262147,405505}`，水位和同一 **40/40** 条回放不变。再确认 **60/60** 条、**3** 次 checkpoint，LSN **11413–11472**；独立回放 LSN **11433–11472** 共 **40/40** 条匹配，水位推进到 `(PSN 29410, DSN 12506)`，node2 导出目录数回到 **4**。合计 **460** 条 ACK 连续，无内容错配或一分钟级写入长尾。

远端证据在 `/home/mo/jxm/logservice-wal-snapshot-driver-20261112/driver-generating-crash-20261113/`：`binaries.sha256`、`crash.marker`、`crash-dir-before-restart.txt`、`node2-hook.log`、`two-vote-status.log`、`cycle.log`、三次 replay 日志、`cold-write.log`、水位及三入口状态日志、原配置备份。实验后配置已还原，三个节点与隔离网络已停止，磁盘证据保留。本轮未发现新缺陷；只覆盖该副本一次小型导出快照在 metadata 写完后的崩溃，未覆盖文件写入中途的所有字节边界。

## 从空盘起步的单份导出快照配额：普通副本、leader 下线与冷重启（2026-11-13）

针对前面“已有 4 份候选再把配额降至 1”的测试边界，在 51 上新建独立空盘三副本 `/home/mo/jxm/logservice-quota-one-fresh-20261113/`，三个节点**从首次启动起**均配置 `max-exported-snapshot=1`、HAKeeper store 过期阈值 5 分钟，数据 shard 1，三个 RPC 地址 `198.18.51.10:61601`、`:61611`、`:61621`。服务端沿用上一节按 `upstream/main` **99605b3914** 构建的正常二进制，SHA256 `89de740d290acc5ddf31b1657d41637e41b8637f6788a2bfbe14805f6ab5e7e4`；检查当时新到的 upstream/main **437c648450**，`pkg/logservice` 与 `logservicedriver` 相对该构建没有代码变化。最初尝试 618xx 地址时，测试客户端的隔离地址白名单在写入前拒绝了它；随后清掉这套刚创建的空盘目录，改用白名单内且空闲的 616xx 地址重新初始化，正式实验从 `GetWALTruncate=(0,0)` 起步。全程仅使用一个真实 TN WAL driver 实例，不运行 datasync。

**普通副本下线。** node2 起初是 leader；单 driver 顺序写入唯一 WAL、每 20 条尝试一次带测试 `GroupFiles` marker 的 checkpoint。第 **127** 条 ACK 左右杀普通副本 node1，保持 node0/node2 两票约 **52 秒**，然后 node1 从原盘启动。两票期间 node0/node2 的导出目录数抽样均为 **1**。node1 恢复日志记录 snapshot 导入；本段最终 **400/400** 条 ACK、**19** 次 checkpoint ACK，用户 LSN **1–400** 连续，最大单条 ACK **894.144 ms**。独立新 driver 回放尾部 **361–400** 共 **40/40** 条与 ACK 匹配，服务端水位 `(PSN 620, checkpointedDSN 392)`。

**leader 下线。** 三票恢复后换一个单 driver 写入；第 **80** 条 ACK 左右杀当时的 leader node2，node0/node1 两票继续约 **45 秒**，再从 node2 原盘启动。两票期间 node0/node1 的导出目录数抽样均为 **1**；node2 恢复日志记录 snapshot 导入，三个地址重新报告同一三成员。此段 **400/400** 条 ACK、**20** 次 checkpoint ACK，用户 LSN **401–800** 连续，最大单条 ACK **3200.764 ms**，没有一分钟级尾延迟。独立新 driver 回放 **761–800** 共 **40/40** 条与 ACK 匹配，水位 `(PSN 1547, DSN 832)`；三节点各自最终保留 **1** 份导出快照。

**整组冷重启与续写。** 脚本对三节点发 `SIGKILL` 后几乎立即启动，首次三个进程都因 `failed to lock data directory` 退出；当时只等待 `/proc/<pid>/exe` 消失，未记录旧进程状态或锁持有者。稍等后再次启动同一原盘，三个地址恢复一致成员，独立回放 **761–800** 的 **40/40** 条以及 `(PSN 1547, DSN 832)` 水位均不变。随后确认 **60/60** 条、**3** 次 checkpoint ACK，用户 LSN **801–860**；独立回放 **821–860** 共 **40/40** 条匹配，水位推进到 `(PSN 2328, DSN 898)`，三个节点仍各保留 **1** 份导出目录。另在两票存活时对 node0 连续做 **3** 次快速 `SIGKILL`→原盘启动，再整组三节点连续做 **2** 次；这 5 次在观察到旧进程为 zombie 后均首次启动成功，最终独立回放仍到 LSN 860、水位 `(2328,898)`。因此锁错误目前只观察到一次，尚未稳定复现，不能据此认定新的服务端缺陷；本轮也不称整组第一次启动即成功。

本轮合计 **860** 条 ACK 的用户 LSN **1–860** 连续；五次独立回放各自保留的尾部均与 ACK 内容一致，未发现配额 1 导致的截断停滞、候选堆积或数据错配。51 上保留 `follower-write.log`、`leader-write.log`、`cold-write.log`、五次 replay 日志、`rapid-restart.log`、`rapid-all-restart.log`、各阶段 WAL 水位和三入口状态、节点日志以及下线/恢复时间文件。实验结束已停止三个测试节点并拆除隔离网络；原盘和证据保留。配额目录数是阶段性抽样，不代表对任意瞬间峰值的证明；checkpoint 文件仍为测试 marker。

## 导出配额 1 时连续三次超时后迟到完成：同进程消费与冷重启（2026-11-13）

继续使用上一节从空盘起步、三个节点均配置 `max-exported-snapshot=1` 的 51 隔离三副本；起始已确认用户 LSN **860**、`GetWALTruncate=(PSN 2328, checkpointedDSN 898)`。服务端仍为 `upstream/main` **99605b3914** 对应的正常二进制；当时新到的 **437c648450** 在 `pkg/logservice` 和 `logservicedriver` 没有代码差异。只运行一个真实 TN WAL driver，不运行 datasync。node2 为普通数据副本，使用 Linux inotify 监听其 exported snapshot 目录；每次出现新的 `.generating` 目录，立即对该**正常服务端进程** `SIGSTOP` 5 秒再 `SIGCONT`。node0/node1 两票保持运行，node2 不重启。

三次监听依次命中导出 index **3091、3300、3421**（目录名十六进制 `0C13`、`0CE4`、`0D5D`）。同一 node2 PID **3544551** 在三次暂停后各记录一次真实 `request export snapshot failed: timeout`；恢复后 Dragonboat 仍把对应导出完成，LogService 随后分别记录**同一 index 3091、3300、3421 的 `import snapshot success`**。前后还发生其他正常导出与导入。阶段性目录检查：第一次暂停后和主负载结束时，node2 均只有 **1** 份正式导出目录；主负载结束时 node0/node1 也各只有 **1** 份。代码中的 `MarkExportUncertain`、`ReconcileUncertainExports` 可以解释迟到目录如何重新进入管理器；本轮没有对其内部列表单独插桩，也未测任意瞬间的目录峰值。

负载确认 **500/500** 条用户 WAL、**25** 次测试 checkpoint，LSN **861–1360** 连续，最大单条 ACK **1034.844 ms**，无上层最终错误。独立新 driver 回放保留尾部 **1321–1360** 共 **40/40** 条与 ACK 的 LSN/payload 匹配，水位 `(PSN 3587, DSN 1448)`。等待三个旧进程进入退出状态后，用三份原盘整组冷重启：三个地址报告同一成员 `{262145,262146,262147}`，同一 **40/40** 条和水位不变。再确认 **60/60** 条续写、**3** 次 checkpoint，LSN **1361–1420**，最大单条 ACK **841.353 ms**；独立回放 **1381–1420** 共 **40/40** 条匹配，水位推进到 `(PSN 4293, DSN 1514)`，三个节点最终仍各保留 **1** 份目录。本轮 **560** 条 ACK 连续，未发现新缺陷或一分钟级写入长尾。

远端证据位于 `/home/mo/jxm/logservice-quota-one-fresh-20261113/`：`pause_export_quota1.py`、`pause-export{,-2,-3}.log`、`late-write.log`、`late-replay.log`、`late-cold-replay.log`、`late-cold-write.log`、`late-cold-continue-replay.log`、各阶段 WAL 水位与三入口状态、node2 `stdout.log` 及 `late-cold-start.log`。实验后已停三个节点并拆除隔离网络；原盘与证据保留。该窗口是通过暂停整个 node2 进程造成的超时，不等同于只阻塞 Dragonboat 导出线程；上一轮已单独覆盖后一种窗口，但配额为 5。

## 配额 1 下数据 leader 导出超时、选主及第二次切主（2026-11-13）

继续在 51 的空盘起步三副本 `/home/mo/jxm/logservice-quota-one-fresh-20261113/` 原盘上运行；三个节点仍配置 `max-exported-snapshot=1`，起始用户 LSN **1420**、`GetWALTruncate=(PSN 4293, checkpointedDSN 1514)`。服务端仍为 `upstream/main` **99605b3914** 对应的正常二进制；检查当时最新 **eac377eaa2**，`pkg/logservice` 和 `logservicedriver` 对该构建没有代码差异。只运行一个真实 TN WAL driver，不运行 datasync。先由三个地址确认数据 leader 为 node0 的副本 **262145**。对 node0 的导出目录设置 inotify 监听，命中 `.generating` 创建后对这个正常进程 `SIGSTOP` **5 秒**再 `SIGCONT`，始终保留 node1/node2 两票运行。

监听实际命中导出 index **4784**（目录 `snapshot-00000000000012B0-262145.generating`）；node0 记录真实 `request export snapshot failed: timeout`。暂停恢复后观察到数据 leader 已转为 node2 的 **262146**，旧 leader node0 于 **10:36:50** 记录**同一 index 4784** 的导入成功；node0 PID **3612815** 没有重启。负载继续到约第 **182** 条 ACK 后，确认新 leader 仍为 node2，再将 node2 杀进程约 **48 秒**。旧 leader node0 与 node1 两票选出 node0 为 leader，单 driver 持续写入；两票期间两台存活节点各只有 **1** 份导出目录。node2 从原盘重启后记录多次 snapshot 导入，三个入口恢复相同三成员。此时间线同时覆盖导出超时后的旧 leader 再次参与多数派，以及随后的第二次选主。

主负载确认 **700/700** 条用户 WAL、**35** 次测试 checkpoint，LSN **1421–2120** 连续；最大单条 ACK **7539.611 ms**，发生在第一次 leader 暂停附近，无上层最终错误或一分钟级长尾。独立新 driver 回放尾部 **2081–2120** 共 **40/40** 条与 ACK 的 LSN/payload 匹配，水位 `(PSN 5525, DSN 2284)`，三个节点各保留 **1** 份导出目录。等待旧进程退出并额外等待 2 秒后，三份原盘整组冷重启一次成功；三地址成员一致，同一 **40/40** 条和水位不变。再确认 **60/60** 条续写、**3** 次 checkpoint，LSN **2121–2180**，最大单条 ACK **683.229 ms**；独立回放 **2141–2180** 共 **40/40** 条匹配，水位推进到 `(PSN 6195, DSN 2350)`，目录数仍各为 **1**。本轮 **760** 条 ACK 连续，未发现新缺陷。

证据保存在 51 的 `leader-export-failover-20261113/`：`watcher.log`、`status-before.log`、`status-after-pause.log`、`status-before-second-kill.log`、`status-two-vote.log`、`new-leader-{down,up}.time`、`load.log`、`replay.log`、`cold-replay.log`、`cold-write.log`、`cold-continue-replay.log`、各阶段 WAL 水位与状态及节点日志。实验后已停三个节点并拆除隔离网络，原盘与日志保留。该实验用进程暂停制造导出超时与选主，不能据此推断仅暂停 Dragonboat 导出线程也会触发同样的 leader 变更；目录数仍是阶段性抽样。

## 配额 1 下导出时隔离 leader 网络、连续两次切主（2026-11-13）

继续使用 51 的三副本隔离原盘 `/home/mo/jxm/logservice-quota-one-fresh-20261113/`，三个节点均从空盘起步配置 `max-exported-snapshot=1`；起始用户 LSN **2180**、`GetWALTruncate=(PSN 6195, checkpointedDSN 2350)`。服务端仍是 `upstream/main` **99605b3914** 的正常构建；当时最新 **a626dd045a** 在 `pkg/logservice` 和 `logservicedriver` 对此构建没有代码差异。只运行一个真实 TN WAL driver，不运行 datasync。与上节暂停整个进程不同，这次用 inotify 监听当前 leader 的 `.generating` 目录创建，事件到来时只把该节点的宿主侧 veth 链路设为 DOWN，节点进程继续运行，另外两票保持互通。

第一次，原 leader node2（副本 **262146**）创建导出 index **6752**（十六进制 `1A60`）的临时目录时，切断 `mocxh2` **35 秒**。node0/node1 选出 node1 的 **262147** 为新 leader；node2 本地日志记录 index 6752 导出成功及一次 `get truncated lsn timed out`，网络恢复后又记录**同一 index 6752 导入成功**。第二次在同一 driver 负载中，当前 leader node1 创建 index **7035**（`1B7B`）时切断 `mocxh1` **25 秒**；隔离期间实测该链路为 DOWN，node0/node2 两票报告 node2 为 leader，node1 地址发生 RPC timeout。node1 进程未退出，本地导出 index 7035 成功，恢复网络后记录同一 index 的导入成功。两次故障均没有让测试集群少于两台互通节点。

主负载确认 **650/650** 条用户 WAL、**32** 次测试 checkpoint，LSN **2181–2830** 连续，无上层最终错误。两次网络切断各对应一笔成功 ACK 长尾：第 **35**、**275** 条分别耗时 **18312.818**、**18321.027 ms**；最大值低于 1 分钟，但明显高于正常数百毫秒。独立新 driver 回放实际保留尾部 **2801–2830** 共 **30/30** 条，与 ACK 的 LSN/payload 匹配，服务端水位 `(PSN 7444, DSN 3076)`，三个节点各只有 **1** 份导出目录。等待旧进程退出后整组原盘冷重启一次成功；三地址成员一致，同一 **30/30** 条和水位不变。再确认 **60/60** 条、**3** 次 checkpoint，LSN **2831–2890**，独立回放 **2851–2890** 共 **40/40** 条匹配，水位推进到 `(PSN 8157, DSN 3130)`，三个节点仍各保留 **1** 份导出目录。本轮 **710** 条 ACK 连续，无数据一致性新缺陷。

远端证据在 `leader-export-netcut-20261113/`：`netcut_export.py`、`netcut_export_second.py`、`watcher.log`、`watcher2.log`、`during-second.link`、`status-during-second.log`、`load.log`、`replay.log`、`cold-replay.log`、`cold-write.log`、`cold-continue-replay.log`、水位及三入口状态文件和各节点日志。实验后已停三个节点并拆除隔离网络，原盘与证据保留。第一次两票状态查询发生在网络恢复后，故第一次的隔离证据以链路操作记录、角色变化和节点本地超时日志为主；第二次则在链路仍 DOWN 时记录了状态。两笔约 18 秒的长尾与断网窗口相邻，可能包含旧连接等待及重新发现时间，本轮未对每次底层 RPC 逐步插桩，不能精确拆分耗时。

## HAKeeper 启动切主、删除 CN 与快照配额（2026-11-13）

本轮在本机对工作树 `f62fbcbc72` 测试；已拉取 `upstream/main` 至 `6c7457903d`，相关生产文件在两版之间无差异。未在 51 上启动新集群。只考虑单个 TN，不运行 datasync；原静态审查中的多 TN 副本移除 Operator 路径不纳入本轮结论。

1. **启动命令已复制、尚未执行时切主：全新接管者未复现“立即 BootstrapFailed”，旧进程重新当选有不同结果。** `TestBootstrapLeaderTakeoverAfterCommandsCommitted` 在三副本 HAKeeper 中提交启动命令后停掉 leader 的 HAKeeper 副本，剩余两票选主；新 leader 首轮检查仍保持 `HAKeeperBootstrapCommandsReceived`，本地 `bootstrapCheckDeadline` 已初始化。新增白盒 `TestCheckBootstrapLeaderTakeoverWithoutLocalState` 精确覆盖新 leader 的期限与 `bootstrapMgr` 均为空：副本未就绪时首轮不失败，期限到达时才失败；副本已就绪时可转 Running；WAL 恢复待完成时继续等待。与现有期限测试、三副本测试合跑 5 次通过。原静态审查所说的首次切主立即失败路径在当前代码中不存在。本测试尚未执行全部启动命令。

   另外检查 **A→B→A**：`getCheckerStateFromLeader` 失去领导权时不清理本地期限，因此“新 leader 会建立期限”只适用于该进程此前期限为空的情形。先前直接把 A 期限设为过去的三副本诊断连跑 3 次后，又在 [可复现测试](../../test/logservice-chaos/bootstrap_retake/README.md) 中去掉期限注入，使用实际经过的时间。短预算使用 `HAKeeperCheckInterval=30ms`，即 3 秒窗口，连跑 3 次；默认预算使用 3 秒间隔，即 5 分钟窗口，运行 1 次。三副本始终存活、仅转移 leader，Log shard 的启动命令已提交但不执行，以保持 `BootstrapCommandsReceived`。默认预算实测 A 期限 **12:05:48.846**，B 首轮接管期限 **12:06:50.028**；A 在 **12:05:53.923** 重新当选后将复制状态写为 `HAKeeperBootstrapFailed`，三副本均应用，而 B 的期限仍有约 56 秒。该结果确认同一复制状态的超时判定取决于当前 leader 的本地历史。若产品意图使用集群统一期限，B 的延长期限不应发生；若意图每次接管重建期限，A 的立即失败不应发生。尚未验证正常启动命令在 B 的额外时间内完成能否避免此故障。已记录为 [matrixorigin/matrixone#32667](https://github.com/matrixorigin/matrixone/issues/32667)，指派给 `jiangxinmeng1`。
2. **删除 CN：确认状态机不确定性。** 临时诊断测试构造两个 HAKeeper 状态机，放入相同 CN 记录，依次应用字节相同的 `GetDeleteCNStoreCmd`，两次应用间隔 10 ms。连跑 3 次，两个副本的 `DeletedStores[0].DownTime` 分别相差 10.397、10.625、10.660 ms；UUID 和其余输入相同。`handleDeleteCNCmd` 在状态机应用时读取 `time.Now()`，故复制命令无法保证相同状态；七天清理边界也由各副本本地时间决定。此处是状态机级稳定复现，尚未做真实多副本切主前后 API 查询，也未观察到用户 WAL 丢失。临时的“验证现有错误”测试没有作为永久回归测试提交。已记录为 [matrixorigin/matrixone#32661](https://github.com/matrixorigin/matrixone/issues/32661)，指派给 `jiangxinmeng1`。
3. **HAKeeper 不可用且快照配额满：确认可导入快照停滞、普通 Append 可继续、控制面恢复后可导入。** 新增 `TestTruncationWaitsForHAKeeperWithFullSnapshotQuota`：单普通 shard、单个 TN ID、无 HAKeeper、`MaxExportedSnapshot=1`，先导出一份，再写入 10 条并将截断水位推进到快照索引之后；明确断言 `EvalImportSnapshot` 返回该快照目录，确保进入读取 HAKeeper 的分支。随后跑三轮处理，三轮均保持同一份快照，索引不变，已导入截断水位保持 0；期间 Append 成功。启动单副本 HAKeeper 后，同一快照导入成功、导入水位前进、快照配额清空。连跑 3 次通过。当前 `shouldDoImport` 读取 HAKeeper 失败会跳过，配额满又阻止导出；只有 `getTruncatedLsn` 超时的分支尝试释放配额，单份快照时也会保留该份。这验证了阻塞条件与恢复路径，尚未测长期磁盘增长。

## 被拒绝的 WAL 回放、残缺快照重载与 KillZombie 清理（2026-11-13）

本轮对工作树 `4d43e6eb72` 测试，涉及生产代码与已获取的 `upstream/main` `6c7457903d` 一致。只使用一个逻辑 TN；租约测试按顺序将 Log shard 的 TN 副本 ID 从 100 切到 101，没有启动两个 TN。未运行 datasync。

1. **确认 LogService 拒绝的 WAL 仍被 Read 标为有效用户记录。** 临时诊断测试用 `CONNECT(100)`、`CONNECT(101)` 后发送携带旧 ID 100、DSN 1 的真实 TAE WAL 编码 `APPEND_WAL`。三轮均收到 `ErrNotLeaseHolder`（错误码 20419、响应 LSN 0），随后 `Read` 在 Raft LSN **5** 找到完全相同的命令字节，类型为 `UserRecord`。状态机拒绝了应用结果，但提案仍占用了日志位置；`markEntries` 只按命令类型分类，没有恢复该位置的拒绝结果。
2. **确认同一份 WAL 字节进入真实 TN driver 回放。** 第二个临时诊断测试把相同的固定 WAL payload 作为 `UserRecord` 交给 `logservicedriver` 的真实 `ReplayForWrite` 流程；三轮均调用 DSN **1** 的应用回调。两项测试分属两个 Go package，是字节相同的两段链路验证；尚未在单次完整 TN 进程生命周期中自然形成旧 ID 提案。复现代码和命令保存在 [rejected_append_replay](../../test/logservice-chaos/rejected_append_replay/README.md)。缺陷记录为 [matrixorigin/matrixone#32677](https://github.com/matrixorigin/matrixone/issues/32677)，指派给 `jiangxinmeng1`。
3. **“重载时丢弃 `snapshots.add()` 错误”与当前代码不符。** 当前 `loadCompletedSnapshots` 不再调用 `snapshots.add()`；它只把规范目录下同时有 `.gbsnap` 和 `snapshot.metadata` 的快照纳入新视图。新增 `TestSnapshotReloadSkipsIncompleteAndKeepsComplete` 用真实临时目录模拟重启：index 100 完整、index 200 仅有 metadata 时，重载后仅保留 100，200 目录仍留盘；随后 index 300 的新快照可以登记，补齐 200 的文件后再次重载可识别 100、200、300。测试连跑 3 次通过。残缺目录可能占用磁盘空间，但本测试没有发现它导致现有完整候选丢失或阻止新快照导出；未据此提出新缺陷。
4. **KillZombie 导出快照孤儿：确认清理遗漏。** 临时诊断测试先让 shard 1 / replica 1 导出 index **14** 的快照，再执行 `KillZombie`。三轮均确认副本停止、本地 metadata 删除，但目录 `exported-snapshot/shard-1/replica-1/snapshot-000000000000000E` 留在磁盘；再次运行 `processTruncateLog` 也没有删除。`handleKillZombie` 先移除 metadata，而常规 `cleanupStaleReplica` 正是从 metadata 扫描进入，因此后续失去回收入口。此处是单 store 实测，未测长期累计磁盘增长；复现代码在 [kill_zombie_snapshot](../../test/logservice-chaos/kill_zombie_snapshot/README.md)。缺陷记录为 [matrixorigin/matrixone#32678](https://github.com/matrixorigin/matrixone/issues/32678)，指派给 `jiangxinmeng1`。
