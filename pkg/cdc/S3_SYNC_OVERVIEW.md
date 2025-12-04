# MatrixOne 基于S3的跨集群数据同步 - 概要设计

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

实现MatrixOne跨集群（跨区域、跨云）的数据实时同步，利用S3对象存储作为数据传输通道。

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
│                                          │      │       │
└──────┼───────┘                           └──────┼───────┘
       │                                          │
       │ 读取Partition State                      │ 应用到Catalog
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

### 3.1 创建S3 Stage（已支持）

**语法**：
```sql
CREATE/REPLACE STAGE <stage_name>
  URL = 's3://<bucket>/<dir>/'
  ENDPOINT = '<s3_endpoint>'
  REGION = '<s3_region>'
  CREDENTIALS = (
    AWS_KEY_ID = '<access_key>',
    AWS_SECRET_KEY = '<secret_key>'
  );
```

**示例**：

```sql
CREATE STAGE s3_sync_stage
  URL = 's3://mo-cross-sync/cluster-a/account/'
  ENDPOINT = 'https://s3.us-west-2.amazonaws.com'
  REGION = 'us-west-2'
  CREDENTIALS = (
    AWS_KEY_ID = 'AKIAIOSFODNN7EXAMPLE',
    AWS_SECRET_KEY = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
  );
```

---

### 3.2 上游 - 创建同步任务

**语法**：

```sql
CREATE PUBLICATION <publication_name>
  DATABASE <db_name>
  [TABLE <table_name>]
  TO STAGE <stage_name>
  [SYNC_INTERVAL = <seconds>]
  [RETENTION = <duration>];
```

**参数说明**：
- `publication_name`：配置名称，集群内唯一
- 同步级别，支持：
  - `database`：同步指定数据库下的所有表
  - `table`：同步指定表
- `DATABASE`：指定数据库名称
- `TABLE`：table级别必填，指定表名称
- `STAGE`：引用之前创建的Stage名称，要和上游的s3一致
- `SYNC_INTERVAL`：同步间隔（秒），默认60秒
- `RETENTION`：增量数据保留时间，默认1天

**示例**：

```sql
-- Database级别：同步整个数据库
CREATE PUBLICATION sync_tpcc
  DATABASE tpcc
  TO STAGE s3_sync_stage;

-- Table级别：同步单张表
CREATE PUBLICATION sync_orders
  DATABASE = tpcc
  TABLE = orders
  TO STAGE s3_sync_stage
  SYNC_INTERVAL = 60
  RETENTION_DAYS = 7;
```

---

### 3.3 下游 - 创建消费任务

**语法**：

```sql

CREATE DATABASE <db_name>
  [TABLE table_name]
  FROM STAGE <stage_name>.<publication_name>
  [SYNC_INTERVAL = <seconds>];
```

**参数说明**：
- `DATABASE`：指定下游数据库名称
- `TABLE`：table级别必填，指定下游表名称
- `STAGE`：引用之前创建的Stage名称，包含S3配置信息
- `publication_name`:上游的publication name]
- `SYNC_INTERVAL`：检查间隔（秒），默认60秒

**示例**：

```sql

-- Database级别：同步整个数据库
CREATE DATABASE tpcc_replica
  FROM STAGE s3_sync_stage;

-- Table级别：同步单张表
CREATE DATABASE tpcc_replica
  TABLE orders
  from STAGE s3_sync_stage
  SYNC_INTERVAL = 60;
```

---

### 3.4 查看任务状态

在原有基础上加了stage列：
```sql
SHOW PUBLICATIONS;

showPublicationsOutputColumns = [8]Column{
    "publication",           // VARCHAR - 发布名称
    "database",              // VARCHAR - 数据库名称
    "tables",                // TEXT - 表列表（* 表示所有表）
    "sub_account",           // TEXT - 集群内发布
    "subscribed_accounts",    // TEXT - 集群内发布
    "stage",                 // TEXT - 发布的stage
    "create_time",           // TIMESTAMP - 创建时间
    "update_time",           // TIMESTAMP - 更新时间（可为NULL）
    "comments",              // TEXT - 注释
}

SHOW SUBSCRIPTIONS;

showSubscriptionsOutputColumns = [9]Column{
    "pub_name",              // VARCHAR - 发布名称
    "pub_account",           // VARCHAR - 集群内部发布
    "stage",                 // VARCHAR - 发布的stage
    "pub_database",          // VARCHAR - 发布的数据库名称
    "pub_tables",            // TEXT - 发布的表列表（* 表示所有表）
    "pub_comment",           // TEXT - 发布注释
    "pub_time",              // TIMESTAMP - 发布时间
    "sub_name",              // VARCHAR - 订阅数据库名称（关键字段）
    "sub_time",              // TIMESTAMP - 订阅时间
    "status",                // TINYINT - 状态（0=Normal, 其他=Deleted/NotAuthorized）
}
```
---

### 3.5 删除任务

```sql
DROP PUBLICATION <publication_name>;
DROP DATABASE <db_name>;
DROP TABLE <table_name>;
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
    task_id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    publication_name     VARCHAR(5000) NOT NULL,         
    cluster_role         VARCHAR(16) NOT NULL,           -- 'upstream' 或 'downstream'
    
    -- 同步级别和范围
    sync_level           VARCHAR(16) NOT NULL,           -- 'account', 'database', 'table'
    account_id           BIGINT NOT NULL,
    db_name              VARCHAR(5000),                   -- database/table级别必填
    table_name           VARCHAR(5000),                   -- table级别必填
    
    -- S3配置
    stage_name            VARCHAR(5000),                  
    
    -- 同步配置（JSON格式）
    sync_config          JSON NOT NULL,                  -- {retention_days, sync_mode, sync_interval}
    
    -- 任务控制
    state                TINYINT,  -- 'running', 'stopped'
    
    -- 时间戳
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
);
```

---

### 4.2 mo_s3_sync_tasks（表级同步任务表）

**存储位置**：`mo_catalog` 数据库

**作用**：记录每张具体表的同步状态，每行对应一张表

**Schema**：

```sql
CREATE TABLE mo_catalog.mo_s3_sync_tasks (
    job_id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,

    task_id              INT UNSIGNED NOT NULL，
    
    -- 表信息
    account_id           BIGINT NOT NULL,
    db_id                BIGINT NOT NULL,
    db_name              VARCHAR(5000) NOT NULL,
    table_id             BIGINT NOT NULL,
    table_name           VARCHAR(5000) NOT NULL, 
    
    -- 同步状态 - data对象
    watermark            TIMESTAMP(6) NOT NULL DEFAULT '1970-01-01 00:00:00',
    snapshot_watermark   TIMESTAMP(6),                -- data快照的watermark（上游）
    
    -- Job状态
    job_state            TINYINT NOT NULL DEFAULT 'pending',  -- 'pending', 'running', 'complete', 'error', 'cancel'
    cn_uuid              VARCHAR(64),                    -- 执行任务的CN标识
    job_lsn              BIGINT DEFAULT 0,               -- Job序列号
    
    -- 错误信息
    error_message        VARCHAR(5000),                  -- 最后错误信息
    
    -- 时间戳
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
);
```

---
## 5. S3目录结构

### 5.1 分层存储策略

```
s3://{bucket}/{dir}/s3_publication/{publication_name}
├── {account_id}/{db_id}/{table_id}/            # 数据目录
│       |
│       ├── 0-{snapshot_ts}/                    # 历史快照（只保留1个）
│       │   ├── data/                           # 数据对象
│       │   │   ├── object_list.meta            # 排序后的ObjectEntry列表
│       │   │   ├── {object_uuid_1}             # TAE Object文件
│       │   │   ├── {object_uuid_2}
│       │   │   ├── ...
│       │   │   └── manifest.json               # 完成标记
│       │   │
│       │   └── tombstone/                      # 墓碑对象
│       │       ├── object_list.meta
│       │       ├── {object_uuid}...
│       │       └── manifest.json
│       │
│       ├── {start_ts}-{end_ts}/                # 增量数据（最近N天）
│       │   ├── data/                           # 数据对象
│       │   │   ├── object_list.meta            # 排序后的增量ObjectEntry
│       │   │   ├── {object_uuid}...
│       │   │   └── manifest.json
│       │   │
│       │   └── tombstone/                      # 墓碑对象
│       │       ├── object_list.meta
│       │       ├── {object_uuid}...
│       │       └── manifest.json
│       │
│       ├── {start_ts}-{end_ts}/                # 更早的增量
│       │   ├── data/
│       │   │   └── ...
│       │   └── tombstone/
│       │       └── ...
│       │
│       └── {start_ts}-{end_ts}/                # 最新的增量
│           ├── data/
│           │   └── ...
│           └── tombstone/
│               └── ...
│
└── ddl/                                         # DDL变化目录
```

### 5.2 目录结构说明

**时间戳目录（{start_ts}-{end_ts} 或 0-{snapshot_ts}）**：
- 每个时间戳目录下包含 data/ 和 tombstone/ 两个子目录
- data/：存储普通数据对象
- tombstone/：存储墓碑对象（用于标记删除）
- 同一时间段的 data 和 tombstone 放在同一个时间戳目录下，保证同步进度一致

**快照目录（0-{snapshot_ts}）**：
- 包含snapshot_ts时间点的所有可见数据（data和tombstone）
- 只保留一个快照
- 快照目录下包含 data/ 和 tombstone/ 子目录

**增量目录（{start_ts}-{end_ts}）**：
- 包含该时间段内的数据对象（data和tombstone）
- 保留最近`retention_days`天的数据
- 每个增量目录下包含 data/ 和 tombstone/ 子目录

**object_list.meta**：
- ObjectEntry列表
- 包含：ObjectStats、is_deleted（标记该object是否已被删除）
- data/ 和 tombstone/ 目录下各自有独立的 object_list.meta

**manifest.json**：
- 标记批次写入完成
- 内容：start_ts、end_ts、object_count、is_snapshot、state、created_at
- data/ 和 tombstone/ 目录下各自有独立的 manifest.json

**DDL目录（ddl/）**：
- 存储账号级别的DDL变更信息，与数据目录并列分tail和snapshot
- snapshot 是某个ts时的mo_tables, mo_databases, mo_columns
- tail内容是mo_tables, mo_databases, mo_columns + timestamp + is_delete

---

## 6. 上游处理流程

### 6.1 Executor和Scanner启动

**S3SyncExecutor**：
1. 集群启动时创建S3SyncExecutor（单例，上下游共用）
2. 从`mo_s3_sync_tasks`加载所有表级任务
3. 重启时将所有`job_state='pending'`或`'running'`的任务置为`'complete'`（Iteration是幂等的）
4. 为每张表提交UpstreamIteration,DownstreamIteration和GC

**S3SyncTaskScanner**：
1. 集群启动时创建TaskScanner（单例）
2. 从`mo_s3_sync_configs`加载所有配置
3. 定期（如每分钟）扫描：
   - database级别：扫描数据库下所有表
   - table级别：检查表是否存在和table_id是否变化
   - 上游从mo_catalog里读，下游从s3的ddl目录里读
4. 发现新表：在`mo_s3_sync_tasks`中创建新记录
5. 表ID变化（如truncate后）：停止旧任务，创建新任务

### 6.2 UpstreamIteration

**Watermark选择策略**：
- 从CN的Partition State读取ObjectEntry列表
- 选择某个已刷盘data aobj的最大commit ts之前的某个时间戳作为new_watermark
  - 保证该时间戳之前的所有数据都已刷盘
  - 不需要考虑内存中的数据
  - tombstone的watermark必须等于data的watermark（保证同步进度一致）
  - 如果tombstone的aobj跨越watermark边界，需要剪裁该aobj

**Object选择策略**：
- 选取所有CreateTS或DeleteTS落在`(current_watermark, new_watermark]`区间内的object
  - 复制新建的object
  - 删除的object只记录在objectlist里

**状态管理和并发控制**：
- 执行后前检查cn_uuid、job_lsn、state是否一致，更新state，watermark
- 清理S3中上次未完成的同步目录

### 6.3 GC

**快照生成**：
- 触发条件：最旧的增量目录超过retention_days
- 旧的文件合并成新的snapshot

**GC清理**：
- 删除旧的快照目录（如果存在）：`0-{old_snapshot_ts}/`
- 删除超过retention_days的增量目录：`{start_ts}-{end_ts}/`
- 更新系统表的data_snapshot_watermark和tombstone_snapshot_watermark

**原子性保证**：
- 先生成新快照
- 新快照完成后才删除旧数据

---

## 7. 下游处理流程

### 7.1 DownstreamIteration

**确定消费范围**：
- 扫描S3目录，列出所有批次目录
- 如果watermark=0，从快照开始消费
- 否则，找到所有start_ts = watermark + 1 的增量目录

**应用数据**：
- 按时间顺序遍历时间戳目录
- 对每个时间戳目录同时消费data和tombstone：
  - 遍历ObjectEntry，下载并创建CN类型ObjectEntry
  - 可能上个iteration应用了被切分的aobj，这次覆盖这个aobj
  - 为了保证原子性，在一个事务里执行

**watermark落后检测**：
- 读取系统表的watermark（data和tombstone的watermark相等）和S3快照的snapshot_ts
- 如果watermark < snapshot_ts：
  - 下游数据已过期，无法继续增量同步
  - 清理本地表的所有object（data和tombstone）
  - 重置watermark为0
  - 从0重新开始应用

**状态管理和并发控制**：
- 执行前检查cn_uuid、job_lsn、job_state是否一致
- 检查上游是否做了GC，如果GC了iteration用到的数据，数据可能有确实，需要重试
- 完成后将job_state更新为'complete'

---

## 8. 关键设计要点

### 8.1 下游Object不参与Merge

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

---

**End of Document**
