# MatrixOne 基于S3的跨集群数据同步 - 概要设计

**版本**: 2.0  
**创建日期**: 2025-11-19  
**文档类型**: 概要设计文档

---

## 目录

1. [系统概述](#1-系统概述)
2. [架构设计](#2-架构设计)
3. [SQL接口](#3-sql接口)
4. [系统表设计](#4-系统表设计)
5. [S3目录结构](#5-s3目录结构)
6. [上游处理流程](#6-上游处理流程)
7. [下游处理流程](#7-下游处理流程)
8. [关键设计要点](#8-关键设计要点)

---

## 1. 系统概述

### 1.1 目标

实现MatrixOne跨集群（跨区域、跨云）的表级数据实时同步，利用S3对象存储作为数据传输通道。

### 1.2 核心特性

- **基于S3中转**：利用对象存储的高可用性和跨区域复制能力
- **TAE不可变块**：直接复制TAE的object文件，无需重新序列化数据
- **增量同步**：首次全量快照，后续只同步增量对象（Appendable Object和CN Object）
- **Watermark追踪**：使用时间戳跟踪同步进度
- **分层存储**：保留最近时间段的增量数据 + 历史快照
- **上游GC管理**：由上游控制数据保留策略，自动清理过期数据
- **下游灵活消费**：下游可控制开始时间，每次同步到最新数据

---

## 2. 架构设计

### 2.1 整体架构

```
上游MO集群                                     下游MO集群
┌──────────────┐                           ┌──────────────┐
│   CN Node    │                           │   CN Node    │
│ ┌──────────┐ │                           │ ┌──────────┐ │
│ │Upstream  │ │                           │ │Downstream│ │
│ │Executor  │ │                           │ │Executor  │ │
│ └────┬─────┘ │                           │ └────┬─────┘ │
│      │       │                           │      │       │
│      │ 提交Job                           │      │ 提交Job
│      ↓       │                           │      ↓       │
│ ┌──────────┐ │                           │ ┌──────────┐ │
│ │Upstream  │ │                           │ │Downstream│ │
│ │SyncJob   │ │                           │ │ConsumeJob│ │
│ └────┬─────┘ │                           │ └────┬─────┘ │
└──────┼───────┘                           └──────┼───────┘
       │                                          │
       │ 读取Partition State                     │ 应用到Catalog
       ↓                                          ↓
┌──────────────┐                           ┌──────────────┐
│   TN Node    │                           │   TN Node    │
│ Table Catalog│                           │ Table Catalog│
└──────────────┘                           └──────────────┘
       │                                          ↑
       │ 复制Object + GC                          │ 下载Object
       ↓                                          │
┌─────────────────────────────────────────────────────────┐
│              S3 存储（分层结构）                         │
│  {table_id}/                                            │
│    ├── 0-{snapshot_ts}/          # 历史快照（只保留1个）│
│    │   ├── object_list.meta                             │
│    │   ├── {objects}...                                 │
│    │   └── manifest.json                                │
│    ├── {start_ts}-{end_ts}/      # 增量数据（保留N天） │
│    │   ├── object_list.meta                             │
│    │   ├── {objects}...                                 │
│    │   └── manifest.json                                │
│    └── {start_ts}-{end_ts}/                             │
│        └── ...                                          │
└─────────────────────────────────────────────────────────┘
```

### 2.2 核心组件

**上游组件**：
- **UpstreamSyncExecutor**：CN启动时创建，负责任务管理和调度
- **UpstreamSyncJob**：TaskService执行的任务，负责读取ObjectList、复制到S3、GC清理

**下游组件**：
- **DownstreamSyncExecutor**：CN启动时创建，负责任务管理和调度
- **DownstreamConsumeJob**：TaskService执行的任务，负责从S3消费数据、应用到Catalog

---

## 3. SQL接口

### 3.1 上游 - 创建同步任务

**语法**：

```sql
CREATE UPSTREAM S3 SYNC <task_name>
  SOURCE DATABASE <db_name> TABLE <table_name>
  S3 CONFIG (
    ENDPOINT '<s3_endpoint>',
    REGION '<s3_region>',
    BUCKET '<s3_bucket>',
    DIR '<s3_dir>',
    ACCESS_KEY '<access_key>',
    SECRET_KEY '<secret_key>'
  )
  [SYNC_INTERVAL <seconds>]
  [RETENTION_DAYS <days>];
```

**参数说明**：
- `task_name`：任务名称，集群内唯一
- `SOURCE DATABASE/TABLE`：源数据库和表名
- `S3 CONFIG`：S3配置信息
- `SYNC_INTERVAL`：同步间隔（秒），默认60秒
- `RETENTION_DAYS`：增量数据保留天数，默认7天，超过此时间的增量目录会被GC清理

**示例**：

```sql
CREATE UPSTREAM S3 SYNC sync_orders
  SOURCE DATABASE tpcc TABLE orders
  S3 CONFIG (
    ENDPOINT 'https://s3.us-west-2.amazonaws.com',
    REGION 'us-west-2',
    BUCKET 'mo-cross-sync',
    DIR 'cluster-a/sync_orders',
    ACCESS_KEY 'AKIAIOSFODNN7EXAMPLE',
    SECRET_KEY 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
  )
  SYNC_INTERVAL 60
  RETENTION_DAYS 7;
```

---

### 3.2 下游 - 创建消费任务

**语法**：

```sql
CREATE DOWNSTREAM S3 SYNC <task_name>
  TARGET DATABASE <db_name> TABLE <table_name>
  S3 CONFIG (
    ENDPOINT '<s3_endpoint>',
    REGION '<s3_region>',
    BUCKET '<s3_bucket>',
    DIR '<s3_dir>',
    ACCESS_KEY '<access_key>',
    SECRET_KEY '<secret_key>'
  )
  [SYNC_INTERVAL <seconds>];
```

**参数说明**：
- `SYNC_INTERVAL`：检查间隔（秒），默认60秒
- **注意**：下游任务始终从头开始同步（从快照开始），每次消费到最新的数据

**示例**：

```sql
-- 先创建目标表（与上游表结构相同）
CREATE TABLE tpcc_replica.orders LIKE tpcc.orders;

-- 创建消费任务（从头开始）
CREATE DOWNSTREAM S3 SYNC sync_orders
  TARGET DATABASE tpcc_replica TABLE orders
  S3 CONFIG (
    ENDPOINT 'https://s3.us-west-2.amazonaws.com',
    REGION 'us-west-2',
    BUCKET 'mo-cross-sync',
    DIR 'cluster-a/sync_orders',
    ACCESS_KEY 'AKIAIOSFODNN7EXAMPLE',
    SECRET_KEY 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
  )
  SYNC_INTERVAL 60;
```

---

### 3.3 查看任务状态

```sql
-- 查看所有任务
SHOW S3 SYNC TASKS;

-- 查看特定任务
SHOW S3 SYNC TASK <task_name>;
```

---

### 3.4 删除任务

```sql
DROP S3 SYNC TASK <task_name>;
```

**说明**：删除任务只会删除系统表记录，不会删除S3数据。S3数据由上游的GC机制自动清理。

---

## 4. 系统表设计

### 4.1 mo_s3_sync_tasks

**存储位置**：`mo_catalog` 数据库

**作用**：记录所有S3同步任务的配置和状态

**Schema**：

```sql
CREATE TABLE mo_catalog.mo_s3_sync_tasks (
    -- 任务标识
    task_id              VARCHAR(36) PRIMARY KEY,
    task_name            VARCHAR(128) NOT NULL,
    cluster_role         VARCHAR(16) NOT NULL,           -- 'upstream' 或 'downstream'
    
    -- 表信息
    account_id           BIGINT NOT NULL,
    db_name              VARCHAR(128) NOT NULL,
    table_name           VARCHAR(128) NOT NULL,
    table_id             BIGINT NOT NULL,
    
    -- S3配置
    s3_endpoint          VARCHAR(256) NOT NULL,
    s3_region            VARCHAR(64),
    s3_bucket            VARCHAR(128) NOT NULL,
    s3_dir               VARCHAR(256) NOT NULL,
    s3_access_key        VARCHAR(256),                   -- 加密存储
    s3_secret_key        VARCHAR(512),                   -- 加密存储
    
    -- 同步状态
    watermark            TIMESTAMP(6) NOT NULL DEFAULT '1970-01-01 00:00:00',
    snapshot_watermark   TIMESTAMP(6),                   -- 快照的watermark（上游）
    sync_interval        INT DEFAULT 60,                 -- 同步间隔（秒）
    
    -- 上游特有
    retention_days       INT DEFAULT 7,                  -- 增量数据保留天数（上游）
    
    -- 任务控制
    state                VARCHAR(16) NOT NULL DEFAULT 'running',  -- 'running', 'stopped'
    cn_uuid              VARCHAR(64),                    -- 执行任务的CN标识
    job_lsn              BIGINT DEFAULT 0,               -- Job序列号
    
    -- 错误追踪
    error_message        TEXT,
    retry_count          INT DEFAULT 0,
    first_error_ts       TIMESTAMP,
    
    -- 时间戳
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- 约束
    UNIQUE KEY uk_task_name (task_name, cluster_role),
    INDEX idx_state (state),
    INDEX idx_cn_uuid (cn_uuid)
);
```

---

## 5. S3目录结构

### 5.1 分层存储策略

```
s3://{bucket}/{dir}/{account_id}/{db_id}/{table_id}/
│
├── 0-{snapshot_ts}/                            # 历史快照（只保留1个）
│   ├── object_list.meta                        # 排序后的ObjectEntry列表
│   ├── {object_uuid_1}                         # TAE Object文件
│   ├── {object_uuid_2}
│   ├── ...
│   └── manifest.json                           # 完成标记
│
├── {start_ts}-{end_ts}/                        # 增量数据（最近N天）
│   ├── object_list.meta                        # 排序后的增量ObjectEntry
│   ├── {object_uuid}...
│   └── manifest.json
│
├── {start_ts}-{end_ts}/                        # 更早的增量
│   └── ...
│
└── {start_ts}-{end_ts}/                        # 最新的增量
    └── ...
```

### 5.2 文件说明

**快照目录（0-{snapshot_ts}）**：
- 包含从表创建到snapshot_ts时间点的所有数据
- 所有object按CreateTS排序
- 只保留一个快照，新快照生成后删除旧快照
- 用于下游从头开始消费

**增量目录（{start_ts}-{end_ts}）**：
- 包含该时间段内的aobj和cnobj
- 按CreateTS排序
- 保留最近`retention_days`天的数据
- 超过保留期的目录会被上游GC删除

**object_list.meta**：
- 序列化的ObjectEntry列表
- 已按CreateTS排序，下游可直接使用
- 包含：ObjectStats、CreateTS、DeleteTS、object_type、is_appendable

**manifest.json**：
- 标记批次写入完成
- 内容：
  - start_ts / end_ts：时间范围
  - object_count：对象数量
  - is_snapshot：是否为快照
  - state：'complete'
  - created_at：生成时间

### 5.3 GC清理规则（上游负责）

**增量数据清理**：
- 扫描所有增量目录，检查end_ts
- 如果`now() - end_ts > retention_days`，删除整个目录
- 保证至少保留最近一个增量目录

**快照更新**：
- 当最旧的增量目录即将被GC时，生成新快照
- 新快照包含：旧快照 + 所有待删除的增量数据
- 范围：`0-{最旧增量的end_ts}`
- 新快照生成后，删除旧快照和待删除的增量目录

---

## 6. 上游处理流程

### 6.1 Executor启动和调度

1. CN启动时创建UpstreamSyncExecutor（单例）
2. 从`mo_s3_sync_tasks`加载所有`cluster_role='upstream'`且`state='running'`的任务
3. 为每个任务启动定时器（间隔由`sync_interval`配置）
4. 定时向TaskService提交UpstreamSyncJob
5. 每次重启后把pending，running的job置成complete。job是幂等的，新的job覆盖上次做到一半的。

### 6.2 UpstreamSyncJob执行流程

**阶段1：状态检查并加锁**
- 检查系统表状态（cn_uuid、state、job_lsn）
- 清理S3中上次未完成的同步目录

**阶段2：读取增量数据**
- 从TN的Partition State读取ObjectEntry列表
- 只读取CreateTS > watermark的aobj和cnobj
- 按CreateTS对ObjectEntry排序
- 计算new_watermark = max(objects.CreateTS)

**阶段3：复制到S3**
- 创建增量目录：`{current_watermark}-{new_watermark}/`
- 复制object文件（保持排序）
- 写入排序后的object_list.meta
- 生成manifest.json（标记完成）

**阶段4：GC清理**
- 扫描S3增量目录，检查是否超过retention_days
- 如有待删除的增量目录：
  - 生成新快照（0-{待删除增量的最大end_ts}）
  - 合并：旧快照 + 待删除的增量目录
  - 删除旧快照和待删除的增量目录
- 更新系统表的snapshot_watermark

**阶段5：更新状态**
- 再次检查系统表状态（防并发冲突）
- 更新watermark、updated_at

### 6.3 快照生成逻辑

**触发条件**：
- 最旧的增量目录超过retention_days

**生成过程**：
1. 读取当前快照（如果存在）的object_list.meta
2. 读取所有待删除增量目录的object_list.meta
3. 合并并按CreateTS排序
4. 过滤已删除的object（DeleteTS不为空且已过期）
5. 创建新快照目录`0-{new_snapshot_ts}`
6. 复制所有object文件
7. 写入合并排序后的object_list.meta
8. 生成manifest.json

**原子性保证**：
- 先生成新快照
- 新快照完成后才删除旧数据
- 确保下游始终能读取到完整数据

---

## 7. 下游处理流程

### 7.1 Executor启动和调度

1. CN启动时创建DownstreamSyncExecutor（单例）
2. 从`mo_s3_sync_tasks`加载所有`cluster_role='downstream'`且`state='running'`的任务
3. 为每个任务启动定时器
4. 定时向TaskService提交DownstreamConsumeJob

### 7.2 DownstreamConsumeJob执行流程

**阶段1：状态检查并加锁**
- 检查系统表状态（cn_uuid、state、job_lsn）
- 读取当前watermark和start_timestamp

**阶段2：确定消费范围**
- 扫描S3目录，列出所有批次目录（包括快照和增量）
- 如果是首次消费（watermark=0）：
  - 从快照开始：`0-{snapshot_ts}`
  - 然后消费所有增量目录
- 否则，找到所有start_ts > watermark的增量目录
- **关键**：每次消费到最新的数据（包括所有可用的增量目录）

**阶段3：顺序消费批次**
- 按时间顺序遍历需要消费的批次目录
- 对每个批次：
  - 检查manifest.json是否存在（等待上游写完）
  - 读取object_list.meta（已排序）
  - 开启新事务
  - 遍历ObjectEntry：
    - 检查object是否已存在（幂等性）
    - 如不存在，下载object文件，创建CN类型ObjectEntry
  - 提交事务
  - 更新watermark为该批次的end_ts

**阶段4：更新状态**
- 再次检查系统表状态
- 更新watermark为最新消费的end_ts

### 7.3 消费策略

**从头开始**：
- 下游任务始终从快照开始消费
- 首次消费会应用快照 + 所有增量数据

**始终同步到最新**：
- 每次Job执行时，消费所有可用的批次
- 不会停在中间某个时间点
- 确保下游数据尽可能接近上游

**断点续传**：
- watermark记录已消费的最新时间点
- 重启后从watermark继续消费

---

## 8. 关键设计要点

### 8.1 分层存储设计

**问题**：如何平衡存储成本和数据可用性？

**解决方案**：
- **快照**：保留完整历史，供新下游从头开始消费
- **增量**：保留最近N天的增量数据，供增量同步
- **自动合并**：定期将过期增量合并到快照，避免增量目录过多

**优势**：
- 下游可从任意时间点开始消费
- 存储成本可控（只保留N天增量）
- 快照更新是增量的，不需要每次全量重建

### 8.2 上游负责GC

**设计理念**：数据生命周期由数据源控制

**实现**：
- 上游配置retention_days
- 上游Job中执行GC清理
- 上游决定何时生成快照

**优势**：
- 下游无需关心数据清理
- 统一的数据保留策略
- 避免下游误删导致其他下游消费失败

### 8.3 Object内部排序

**问题**：下游如何高效应用数据？

**解决方案**：
- 上游在生成object_list.meta时按CreateTS排序
- 下游直接按顺序读取，无需再次排序
- 快照合并时也保持排序

**优势**：
- 下游处理逻辑简化
- 保证数据应用的顺序性
- 提高下游消费效率

### 8.4 每次同步到最新

**设计**：下游不维护中间状态，每次跳到最新

**实现**：
- 每次Job扫描所有watermark之后的批次
- 顺序消费直到最新
- 更新watermark为最新的end_ts

**优势**：
- 简化状态管理
- 下游数据延迟最小
- 避免积压多个批次

### 8.5 时间范围命名

**格式**：`{start_ts}-{end_ts}`

**优势**：
- 自描述：目录名即表示时间范围
- 可追溯：便于排查问题
- 易排序：字典序即时间序
- 便于GC：根据end_ts判断是否过期

### 8.6 manifest.json作为完成标记

**问题**：上游写入多个文件，下游如何知道何时可以消费？

**解决方案**：
- 上游最后写入manifest.json
- 下游检测到manifest.json才开始消费
- 避免读取不完整的数据

### 8.7 幂等性保证

**场景**：Job可能因故障重试

**解决方案**：
- 下游应用前检查object是否已存在
- 存在则跳过，不存在则创建
- 支持Job安全重试

### 8.8 事务性应用

**要求**：批次内的对象要么全部应用，要么全部不应用

**实现**：
- 下游在一个事务中应用一个批次的所有ObjectEntry
- 事务成功才更新watermark
- 事务失败回滚，下次重试

### 8.9 防止重复执行

**问题**：多个CN可能同时运行Executor

**解决方案**：
- 系统表记录cn_uuid
- Executor和Job都检查cn_uuid
- 使用job_lsn序列号
- 更新系统表时验证状态（乐观锁）

### 8.10 简化的任务管理

**设计**：只有running和stopped两种状态

**理由**：
- 无需暂停恢复（下游可随时停止和重启）
- 无需复杂的错误状态（失败会自动重试）
- 简化状态机和运维操作

---

**End of Document**
