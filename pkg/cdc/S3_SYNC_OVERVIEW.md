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

**Executor**：
- **S3SyncExecutor**：CN启动时创建，用同一个Executor处理本集群作为上游或下游的job
- 启动时从系统表加载所有任务（上游和下游）
- 为每个表任务提交相应的Job到TaskService

**TaskScanner**：
- **S3SyncTaskScanner**：定期扫描组件
- 根据`mo_s3_sync_configs`中的配置扫描数据库和表
- 发现新表或表ID变化时，在`mo_s3_sync_tasks`中创建或更新记录
- 处理表truncate后的table_id变化

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
  [ACCOUNT <account_name>]
  [DATABASE <db_name>]
  [TABLE <table_name>]
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
- `task_name`：配置名称，集群内唯一
- 同步级别（三选一，不指定则为account级别）：
  - `ACCOUNT`：同步整个账号下的所有表
  - `DATABASE`：同步指定数据库下的所有表
  - `TABLE`：同步指定表
- `S3 CONFIG`：S3配置信息
- `SYNC_INTERVAL`：同步间隔（秒），默认60秒
- `RETENTION_DAYS`：增量数据保留天数，默认7天

**示例**：

```sql
-- Account级别：同步整个账号
CREATE UPSTREAM S3 SYNC sync_account
  S3 CONFIG (
    ENDPOINT 'https://s3.us-west-2.amazonaws.com',
    REGION 'us-west-2',
    BUCKET 'mo-cross-sync',
    DIR 'cluster-a/account',
    ACCESS_KEY 'AKIAIOSFODNN7EXAMPLE',
    SECRET_KEY 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
  );

-- Database级别：同步整个数据库
CREATE UPSTREAM S3 SYNC sync_tpcc
  DATABASE tpcc
  S3 CONFIG (...);

-- Table级别：同步单张表
CREATE UPSTREAM S3 SYNC sync_orders
  DATABASE tpcc TABLE orders
  S3 CONFIG (...)
  SYNC_INTERVAL 60
  RETENTION_DAYS 7;
```

---

### 3.2 下游 - 创建消费任务

**语法**：

```sql
CREATE DOWNSTREAM S3 SYNC <task_name>
  [ACCOUNT <account_name>]
  [DATABASE <db_name>]
  [TABLE <table_name>]
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
- 同步级别：与上游对应，可以是account/database/table级别
- `SYNC_MODE`：同步模式
  - `'auto'`（默认）：自动跟随最新数据，持续同步
  - `'manual'`：定时同步到当前时间戳，不自动跟随
- `SYNC_INTERVAL`：检查间隔（秒），默认60秒

**示例**：

```sql
-- Account级别：同步整个账号
CREATE DOWNSTREAM S3 SYNC sync_account
  S3 CONFIG (...);

-- Database级别：同步整个数据库
CREATE DOWNSTREAM S3 SYNC sync_tpcc
  DATABASE tpcc_replica
  S3 CONFIG (...);

-- Table级别：同步单张表（需先创建表结构）
CREATE TABLE tpcc_replica.orders LIKE tpcc.orders;

CREATE DOWNSTREAM S3 SYNC sync_orders
  DATABASE tpcc_replica TABLE orders
  S3 CONFIG (...)
  SYNC_MODE 'auto'
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

---

## 4. 系统表设计

### 4.1 mo_s3_sync_configs（同步配置表）

**存储位置**：`mo_catalog` 数据库

**作用**：记录S3同步任务的配置信息，支持account/database/table三个级别

**Schema**：

```sql
CREATE TABLE mo_catalog.mo_s3_sync_configs (
    -- 任务标识
    task_id              VARCHAR(36) PRIMARY KEY,
    task_name            VARCHAR(128) NOT NULL,
    cluster_role         VARCHAR(16) NOT NULL,           -- 'upstream' 或 'downstream'
    
    -- 同步级别和范围
    sync_level           VARCHAR(16) NOT NULL,           -- 'account', 'database', 'table'
    account_id           BIGINT NOT NULL,
    db_name              VARCHAR(128),                   -- database/table级别必填
    table_name           VARCHAR(128),                   -- table级别必填
    
    -- S3配置（JSON格式）
    s3_config            JSON NOT NULL,                  -- {endpoint, region, bucket, dir, access_key, secret_key}
    
    -- 同步配置（JSON格式）
    sync_config          JSON NOT NULL,                  -- {retention_days, sync_mode, sync_interval}
    
    -- 任务控制
    state                VARCHAR(16) NOT NULL DEFAULT 'running',  -- 'running', 'stopped'
    
    -- 时间戳
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- 约束
    UNIQUE KEY uk_config (config_name, cluster_role),
    INDEX idx_level (sync_level),
    INDEX idx_account (account_id),
    INDEX idx_state (state)
);
```

---

### 4.2 mo_s3_sync_tasks（表级同步任务表）

**存储位置**：`mo_catalog` 数据库

**作用**：记录每张具体表的同步状态，每行对应一张表

**Schema**：

```sql
CREATE TABLE mo_catalog.mo_s3_sync_tasks (
    -- 表标识
    task_id              VARCHAR(36) PRIMARY KEY,        -- 自动生成的UUID
    
    -- 表信息
    account_id           BIGINT NOT NULL,
    db_id                BIGINT NOT NULL,
    db_name              VARCHAR(128) NOT NULL,
    table_id             BIGINT NOT NULL,
    table_name           VARCHAR(128) NOT NULL,
    
    -- 同步状态 - data对象
    data_watermark       TIMESTAMP(6) NOT NULL DEFAULT '1970-01-01 00:00:00',
    data_snapshot_watermark TIMESTAMP(6),                -- data快照的watermark（上游）
    
    -- 同步状态 - tombstone对象
    tombstone_watermark  TIMESTAMP(6) NOT NULL DEFAULT '1970-01-01 00:00:00',
    tombstone_snapshot_watermark TIMESTAMP(6),           -- tombstone快照的watermark（上游）
    
    -- Job状态
    job_state            VARCHAR(16) NOT NULL DEFAULT 'pending',  -- 'pending', 'running', 'complete'
    cn_uuid              VARCHAR(64),                    -- 执行任务的CN标识
    job_lsn              BIGINT DEFAULT 0,               -- Job序列号
    
    -- 错误信息
    error_message        VARCHAR(1024),                  -- 最后错误信息
    
    -- 时间戳
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- 约束
    UNIQUE KEY uk_table (task_id, table_id),
    INDEX idx_config (config_id),
    INDEX idx_job_state (job_state),
    INDEX idx_cn_uuid (cn_uuid),
);
```

---

## 5. S3目录结构

### 5.1 分层存储策略

```
s3://{bucket}/{dir}/{account_id}/{db_id}/{table_id}/
│
├── data/                                       # 数据对象
│   ├── 0-{snapshot_ts}/                        # 历史快照（只保留1个）
│   │   ├── object_list.meta                    # 排序后的ObjectEntry列表
│   │   ├── {object_uuid_1}                     # TAE Object文件
│   │   ├── {object_uuid_2}
│   │   ├── ...
│   │   └── manifest.json                       # 完成标记
│   │
│   ├── {start_ts}-{end_ts}/                    # 增量数据（最近N天）
│   │   ├── object_list.meta                    # 排序后的增量ObjectEntry
│   │   ├── {object_uuid}...
│   │   └── manifest.json
│   │
│   ├── {start_ts}-{end_ts}/                    # 更早的增量
│   │   └── ...
│   │
│   └── {start_ts}-{end_ts}/                    # 最新的增量
│       └── ...
│
└── tombstone/                                  # 墓碑对象
    ├── 0-{snapshot_ts}/                        # 历史快照
    │   ├── object_list.meta
    │   ├── {object_uuid}...
    │   └── manifest.json
    │
    ├── {start_ts}-{end_ts}/                    # 增量数据
    │   └── ...
    │
    └── {start_ts}-{end_ts}/
        └── ...
```

### 5.2 目录结构说明

**data/ 和 tombstone/**：
- 按object类型分离存储
- data：存储普通数据对象
- tombstone：存储墓碑对象（用于标记删除）
- 两个目录下的结构完全相同

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
- 包含：ObjectStats、is_deleted（标记该object是否已被删除）

**manifest.json**：
- 标记批次写入完成
- 内容：start_ts、end_ts、object_count、is_snapshot、state、created_at

---

## 6. 上游处理流程

### 6.1 Executor和Scanner启动

**S3SyncExecutor**：
1. CN启动时创建S3SyncExecutor（单例，上下游共用）
2. 从`mo_s3_sync_tasks`加载所有表级任务
3. 重启时将所有`job_state='pending'`或`'running'`的任务置为`'complete'`（Job是幂等的）
4. 为每张表提交UpstreamSyncJob和SnapshotGCJob

**S3SyncTaskScanner**：
1. CN启动时创建TaskScanner（单例）
2. 从`mo_s3_sync_configs`加载所有配置
3. 定期（如每分钟）扫描：
   - account级别：扫描账号下所有数据库和表
   - database级别：扫描数据库下所有表
   - table级别：检查表是否存在和table_id是否变化
4. 发现新表：在`mo_s3_sync_tasks`中创建新记录
5. 表ID变化（如truncate后）：更新`mo_s3_sync_tasks`中的table_id，重置watermark

### 6.2 UpstreamSyncJob

**data和tombstone分开处理**：
- Job对data对象和tombstone对象分别执行同步流程
- 使用各自的watermark（data_watermark / tombstone_watermark）
- 分别写入到data/和tombstone/目录

**Object选择策略**：
- 从TN的Partition State读取ObjectEntry列表
- 选择某个已经刷盘的aobj的create at作为watermark
  - 选取所有create ts落在这个区间里的obj
- 每次以某个aobj的CreateTS作为结束时间戳（new_watermark）
- aobj是首尾相接的（前一个aobj的CreateTS等于后一个aobj的起始位置）
- 每次tombstone的watermark大于等于data的watermark

**复制到S3**：
- data对象：创建`data/{current_watermark}-{new_watermark}/`目录并复制
- tombstone对象：创建`tombstone/{current_watermark}-{new_watermark}/`目录并复制
- 分别写入object_list.meta和manifest.json

**状态管理和并发控制**：
- 执行前检查cn_uuid、job_lsn、state是否一致
- 完成后更新data_watermark、tombstone_watermark、updated_at
- 将state更新为'complete'
- 清理S3中上次未完成的同步目录

### 6.3 SnapshotGCJob

**data和tombstone分开处理**：
- Job对data对象和tombstone对象分别执行快照生成和GC
- 使用各自的snapshot_watermark
- 分别在data/和tombstone/目录下操作

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
- 分别删除data和tombstone的旧快照（如果存在）
- 分别删除超过retention_days的增量目录
- 更新系统表的data_snapshot_watermark和tombstone_snapshot_watermark

**原子性保证**：
- 先生成新快照
- 新快照完成后才删除旧数据

---

## 7. 下游处理流程

### 7.2 DownstreamConsumeJob

**data和tombstone分开处理**：
- Job对data对象和tombstone对象分别执行消费流程
- 使用各自的watermark（data_watermark / tombstone_watermark）
- 分别从data/和tombstone/目录消费

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
- 分别按时间顺序遍历data和tombstone的批次目录
- 对每个批次：
  - 检查manifest.json是否存在
  - 读取object_list.meta
  - 开启新事务
  - 遍历ObjectEntry，检查幂等性，下载并创建CN类型ObjectEntry
  - 提交事务

**状态管理和并发控制**：
- 执行前检查cn_uuid、job_lsn、state是否一致
- 每个批次成功后分别更新data_watermark或tombstone_watermark
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

### 8.2 快照生成策略

快照生成过程类似增量同步，只是object选择策略不同：
- 增量：选择CreateTS > watermark的aobj/cnobj
- 快照：选择snapshot_ts时可见的所有object

### 8.3 幂等性保证

Job可安全重试，下游应用前检查object是否已存在

### 8.4 事务性应用

下游在一个事务中应用一个批次的所有ObjectEntry

### 8.5 下游Object不参与Merge

**设计理念**：下游只负责复制数据，不参与数据整理

**实现**：
- 下游创建的ObjectEntry标记为CN类型
- 下游的object不参与merge操作
- 上游的删除和新增已经体现了上游的merge结果
- 下游直接应用上游merge后的object即可

**优势**：
- 简化下游逻辑，只做数据复制
- 避免下游重复执行merge，节省资源
- 保证下游数据与上游一致

### 8.6 下游消费延迟的影响

**关键特性**：因为没有内存数据，checkpoint不需要等待object的flush

**优势**：
- 下游消费慢不会影响checkpoint
- 不会阻塞系统的正常运行
- 下游可以按自己的节奏消费数据
- 适合批量或定时同步场景

### 8.7 多级别同步支持

**三个级别**：
- **account级别**：同步整个账号下的所有表，适合全局备份
- **database级别**：同步指定数据库下的所有表，适合按库同步
- **table级别**：同步指定表，适合精细控制

**实现机制**：
- 配置存储在`mo_s3_sync_configs`表，一个配置对应一个同步级别
- 实际任务存储在`mo_s3_sync_tasks`表，每行对应一张具体的表
- TaskScanner根据配置扫描并生成/更新表级任务

### 8.8 表Truncate处理

**问题**：表truncate后table_id会变化，但表名不变

**解决方案**：
- TaskScanner定期检查表的table_id
- 发现table_id变化时：
  - 更新`mo_s3_sync_tasks`中的table_id
  - 重置watermark为0
  - 从头开始同步该表
- 保证同名表truncate后能正确同步

---

**End of Document**
