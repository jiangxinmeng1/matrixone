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
- **灵活消费模式**：下游支持定时同步或自动跟随最新数据

---

## 2. 架构设计

### 2.1 整体架构

```
上游MO集群                                     下游MO集群
┌──────────────┐                           ┌──────────────┐
│   CN Node    │                           │   CN Node    │
│ ┌──────────┐ │                           │ ┌──────────┐ │
│ │  S3Sync  │ │                           │ │  S3Sync  │ │
│ │ Executor │ │                           │ │ Executor │ │
│ └────┬─────┘ │                           │ └────┬─────┘ │
│      │       │                           │      │       │
│      ├──→ UpstreamSyncJob                │      ├──→ DownstreamConsumeJob
│      └──→ SnapshotGCJob                  │      │       │
│                                           │      │       │
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

**统一的Executor**：
- **S3SyncExecutor**：CN启动时创建，用同一个Executor处理本集群作为上游或下游的job
- 启动时从系统表加载所有任务（上游和下游）
- 为每个任务提交相应的Job到TaskService

**上游Job**：
- **UpstreamSyncJob**：读取增量数据并复制到S3
- **SnapshotGCJob**：生成快照并清理过期增量数据

**下游Job**：
- **DownstreamConsumeJob**：从S3消费数据并应用到Catalog

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
- `RETENTION_DAYS`：增量数据保留天数，默认7天

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
  [SYNC_MODE <mode>]
  [SYNC_INTERVAL <seconds>];
```

**参数说明**：
- `SYNC_MODE`：同步模式
  - `'auto'`（默认）：自动跟随最新数据，持续同步
  - `'manual'`：定时同步到当前时间戳，不自动跟随
- `SYNC_INTERVAL`：检查间隔（秒），默认60秒

**示例**：

```sql
-- 先创建目标表（与上游表结构相同）
CREATE TABLE tpcc_replica.orders LIKE tpcc.orders;

-- 自动模式：持续跟随最新数据
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
  SYNC_MODE 'auto';
  SYNC_INTERVAL 3600;  -- 每小时同步一次

-- 手动模式：定时同步到当前时间戳
CREATE DOWNSTREAM S3 SYNC sync_orders_manual
  TARGET DATABASE tpcc_replica TABLE orders
  S3 CONFIG (...)
  SYNC_MODE 'manual'
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
    
    -- S3配置（JSON格式）
    s3_config            JSON NOT NULL,                  -- {endpoint, region, bucket, dir, access_key, secret_key}
    
    -- 同步状态
    watermark            TIMESTAMP(6) NOT NULL DEFAULT '1970-01-01 00:00:00',
    snapshot_watermark   TIMESTAMP(6),                   -- 快照的watermark（上游）
    sync_interval        INT DEFAULT 60,                 -- 同步间隔（秒）
    
    sync_config            JSON NOT NULL,                 -- RETENTION_DAYS, sync_mode, sync_interval
    
    -- 任务控制
    state                VARCHAR(16) NOT NULL DEFAULT 'running',  -- 'running', 'stopped'
    cn_uuid              VARCHAR(64),                    -- 执行任务的CN标识
    job_lsn              BIGINT DEFAULT 0,               -- Job序列号
    
    -- 错误信息
    error_message        VARCHAR(1024),                  -- 最后错误信息
    
    -- 时间戳
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- 约束
    UNIQUE KEY uk_task_name (task_name, cluster_role),
    INDEX idx_state (state),
    INDEX idx_cn_uuid (cn_uuid),
    INDEX idx_role (cluster_role)
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
- 包含从表创建到snapshot_ts时间点的所有可见数据
- 所有object按CreateTS排序
- 只保留一个快照

**增量目录（{start_ts}-{end_ts}）**：
- 包含该时间段内的aobj和cnobj
- 按CreateTS排序
- 保留最近`retention_days`天的数据

**object_list.meta**：
- 序列化的ObjectEntry列表
- 包含：ObjectStats

**manifest.json**：
- 标记批次写入完成
- 内容：start_ts、end_ts、object_count、is_snapshot、state、created_at

---

## 6. 上游处理流程

### 6.1 Executor启动

1. CN启动时创建S3SyncExecutor（单例，上下游共用）
2. 从`mo_s3_sync_tasks`加载所有任务
3. 重启时将所有`state='pending'`或`'running'`的任务置为`'complete'`（Job是幂等的）
4. 提交UpstreamSyncJob和SnapshotGCJob

### 6.2 UpstreamSyncJob

**Object选择策略**：
- 从TN的Partition State读取ObjectEntry列表
- 只选择CreateTS > watermark的aobj和cnobj
- 每次以某个aobj的CreateTS作为结束时间戳（new_watermark）
- aobj是首尾相接的（前一个aobj的CreateTS等于后一个aobj的起始位置）
- 只选择aobj和cnobj就能保证所有数据完整：

**复制到S3**：
- 创建增量目录：`{current_watermark}-{new_watermark}/`
- 复制object文件到目录
- 写入排序后的object_list.meta
- 最后生成manifest.json标记完成

**状态管理和并发控制**：
- 执行前检查cn_uuid、job_lsn、state是否一致
- 完成后更新watermark、updated_at，将state更新为'complete'
- 清理S3中上次未完成的同步目录

### 6.3 SnapshotGCJob

**快照生成**：
- 触发条件：最旧的增量目录超过retention_days
- Object选择策略：读取快照时间戳（snapshot_ts）时所有可见的object
  - 从TN的Partition State读取，基于snapshot_ts判断可见性
  - 包含所有CreateTS <= snapshot_ts且未被删除的object
- 生成过程类似增量同步：
  - 创建快照目录：`0-{new_snapshot_ts}`
  - 按CreateTS排序object
  - 复制object文件
  - 写入object_list.meta
  - 生成manifest.json

**GC清理**：
- 删除旧快照（如果存在）
- 删除超过retention_days的增量目录
- 更新系统表的snapshot_watermark

**原子性保证**：
- 先生成新快照
- 新快照完成后才删除旧数据

---

## 7. 下游处理流程

### 7.2 DownstreamConsumeJob

**watermark落后检测**：
- 读取系统表的watermark和S3快照的snapshot_ts
- 如果watermark < snapshot_ts：
  - 下游数据已过期，无法继续增量同步
  - 清理本地表的所有object
  - 重置watermark为0
  - 从快照重新开始应用

**确定消费范围**：
- 扫描S3目录，列出所有批次目录
- 如果watermark=0，从快照开始消费
- 否则，找到所有start_ts >= watermark的增量目录
- 根据sync_mode决定消费策略：
  - `auto`模式：消费所有可用批次（到最新）
  - `manual`模式：消费到当前时间戳为止

**应用数据**：
- 按时间顺序遍历批次目录
- 对每个批次：
  - 检查manifest.json是否存在
  - 读取object_list.meta
  - 开启新事务
  - 遍历ObjectEntry，检查幂等性，下载并创建CN类型ObjectEntry
  - 提交事务

**状态管理和并发控制**：
- 执行前检查cn_uuid、job_lsn、state是否一致
- 每个批次成功后更新watermark
- 完成后将state更新为'complete'

---

## 8. 关键设计要点

### 8.1 防止重复执行

**问题**：多个CN可能同时运行Executor，或Job重复提交

**解决方案**：
- 系统表记录cn_uuid，标识任务归属的CN
- Executor提交Job前检查cn_uuid是否匹配
- Job执行时检查cn_uuid、job_lsn、state：
  - cn_uuid必须匹配当前CN
  - job_lsn必须与传入的一致
  - state必须为'pending'
- 更新系统表时再次验证上述字段（乐观锁）
- 重启时将pending/running的任务置为complete，新Job会覆盖

### 8.2 分层存储设计

**快照**：保留完整历史，供下游从头开始或过期后重建

**增量**：保留最近N天，供增量同步

**自动合并**：定期生成新快照并清理过期增量

### 8.3 上游负责GC

**设计理念**：数据生命周期由数据源控制

**实现**：独立的SnapshotGCJob执行GC和快照生成

### 8.4 Object内部排序

上游在生成object_list.meta时按CreateTS排序，下游直接使用

### 8.5 快照生成策略

快照生成过程类似增量同步，只是object选择策略不同：
- 增量：选择CreateTS > watermark的aobj/cnobj
- 快照：选择snapshot_ts时可见的所有object

### 8.6 下游消费模式

**auto模式**：每次消费到最新，确保数据延迟最小

**manual模式**：定时同步到当前时间戳，适合按需同步场景

### 8.7 下游过期重建

当watermark落后于快照时，自动清理并从快照重建，保证数据一致性

### 8.8 统一的Executor

上下游使用同一个S3SyncExecutor，简化架构，根据cluster_role区分任务类型

### 8.9 幂等性保证

Job可安全重试，下游应用前检查object是否已存在

### 8.10 事务性应用

下游在一个事务中应用一个批次的所有ObjectEntry

---

**End of Document**
