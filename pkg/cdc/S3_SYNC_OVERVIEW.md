# MatrixOne Cross-Cluster Replication - 概要设计

**创建日期**: 2025-11-19  
**文档类型**: 概要设计文档

---

## 目录

1. [系统概述](#1-系统概述)
2. [架构设计](#2-架构设计)
3. [SQL接口](#3-sql接口)
4. [系统表设计](#4-系统表设计)
5. [下游处理流程](#5-下游处理流程)
6. [关键设计要点](#6-关键设计要点)

---

## 1. 系统概述

### 1.1 目标

实现MatrixOne跨集群（跨区域、跨云）的Cross-Cluster Replication，通过SQL直接连接上游集群获取数据。

---

## 2. 架构设计

### 2.1 整体架构

```
上游MO集群                                     下游MO集群
┌──────────────┐                           ┌──────────────┐
│   CN Node    │                           │   CN Node    │
│              │                           │ ┌──────────┐ │
│              │◄────── SQL连接 ───────────┤ │  Sync    │ │
│              │                           │ │ Executor │ │
│              │                           │ └──────────┘ │
│              │                           │              │
│              │◄─── 1. SQL请求做snapshot ─┤              │
│              │◄───    SQL查询三表 ────────┤              │
│              │                           │              │
│              │◄─── 2. SQL查询objectlist ──┤              │
│              │      diff                 │              │
│              │                           │              │
│              │◄─── 3. SQL依次获取object ──┤              │
│              │                           │              │
│              │◄─── 4. SQL删除上上次 ──────┤              │
│              │      snapshot             │              │
│              │                           │              │
└──────────────┘                           └──────────────┘
```

### 2.2 核心组件

**Executor**：
- **Cross-Cluster Replication Executor**：CN启动时创建，只在下游运行
- 处理整个Cross-Cluster Replication任务，一起执行所有表的复制

**Iteration**：
- 连接到上游集群，获取数据并应用到Catalog

---

## 3. SQL接口

### 3.1 上游 - 创建Publication任务(现有的publication)
```sql
CREATE PUBLICATION <pub_name> ACCOUNT <account_name> DATABASE <db_name> TABLE <table_name>;
```

### 3.1 下游 - 创建Subscription任务

**语法**：

```sql
CREATE DATABASE [IF NOT EXISTS] db_name FROM connection_string PUBLICATION pub_name [SYNC INTERVAL interval]
```

```sql
CREATE TABLE [IF NOT EXISTS] dbname.table_name FROM connection_string PUBLICATION pub_name [SYNC INTERVAL interval]
```

```sql
CREATE ACCOUNT FROM connection_string PUBLICATION pub_name [SYNC INTERVAL interval]
```

**参数说明**：
- `subscription_name`：订阅名称，集群内唯一
- 复制级别，支持：
  - `database`：复制指定数据库下的所有表
  - `table`：复制指定表
  - `account`: 复制当前account
- `DATABASE`：指定数据库名称
- `TABLE`：table级别必填，指定表名称
- `FROM`：上游MatrixOne集群的连接字符串
- `SYNC_INTERVAL`：复制间隔（秒），默认60秒

**示例**：

```sql
-- Database级别：复制整个数据库
CREATE DATABASE tpcc
  FROM 'mysql://myaccount#root:password@127.0.0.1:6001' 
  PUBLICATION my_publication;

-- Table级别：复制单张表
CREATE TABLE tpcc.orders
  FROM 'mysql://myaccount#root:password@127.0.0.1:6001'
  PUBLICATION my_publication
  SYNC_INTERVAL = 60;
```

---

### 3.2 查看任务状态

- SHOW PUBLICATION和现有的一致;

- SHOW CCPR SUBSCRIPTIONS [pub_name]

| 列名 | 类型 | 说明 |
|------|------|------|
| pub_name | VARCHAR | Publication 名称 |
| pub_account | VARCHAR | 上游账号名 |
| pub_database | VARCHAR | 上游数据库名（可为NULL） |
| pub_tables | VARCHAR | 上游表名（可为NULL） |
| create_time | TIMESTAMP | 创建时间 |
| drop_time | TIMESTAMP | 删除时间，如果还没drop就是null |
| state | TINYINT | 订阅状态（0=pending, 1=running, 2=complete, 3 error, 4 cancel） |
| sync_level | VARCHAR | 同步级别（'account', 'database', 'table'）|
| upstream_conn | VARCHAR | 上游连接(密码被模糊处理)|
| iteration_lsn | BIGINT | 当前迭代 LSN |
| error_message | varchar | 错误信息（如果没有错误，为空） |
| watermark | TIMESTAMP | 水位 |

---

### 3.3 删除任务

下游和drop account/database/table一样，会同时删除同步记录
```
DROP DATABASE [IF EXISTS] db_name
DROP TABLE [IF EXISTS] table_name
DROP ACCOUNT [IF EXISTS] account_name
```

也可以单独删除同步记录，删除同步记录会停止同步，不会删除现有的database/table
```
DROP CCPR SUBSTRIPTIONS <publication_name>;
```

drop publication和原先一样
```sql
DROP PUBLICATION <publication_name>;
```

---

## 4. 系统表设计

### 4.1 mo_sync_configs（Cross-Cluster Replication配置表）

**存储位置**：`mo_catalog` 数据库

**作用**：记录Cross-Cluster Replication任务的配置信息，支持database/table级别

**Schema**：

```sql
CREATE TABLE mo_catalog.mo_sync_configs (
    -- 任务标识
    task_id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    subscription_name    VARCHAR(5000) NOT NULL,
    
    -- 复制级别和范围
    sync_level           VARCHAR(16) NOT NULL,           -- 'database', 'table'
    db_name              VARCHAR(5000),                   -- database/table级别必填
    table_name           VARCHAR(5000),                   -- table级别必填
    
    -- 上游连接配置
    upstream_conn         VARCHAR(5000) NOT NULL,          -- MySQL连接字符串
    
    -- 复制配置（JSON格式）
    sync_config          JSON NOT NULL,                  -- {sync_interval}
    
    -- 任务控制
    state                TINYINT,  -- 'running', 'stopped'
    
    -- 执行状态
    iteration_state      TINYINT NOT NULL DEFAULT 'pending',  -- 'pending', 'running', 'complete', 'error', 'cancel'
    iteration_lsn        BIGINT DEFAULT 0,               -- Job序列号
    context              JSON,                           -- iteration上下文，如snapshot名称等
    cn_uuid              VARCHAR(64),                    -- 执行任务的CN标识
    
    -- 错误信息
    error_message        VARCHAR(5000),                  -- 错误信息
    
    -- 时间戳
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
);
```

---

## 5. 下游处理流程

### 5.1 Executor启动

**Cross-Cluster Replication Executor**：
1. 集群启动时创建Cross-Cluster Replication Executor（单例，只在下游运行）
2. 从`mo_sync_configs`加载所有Cross-Cluster Replication任务配置
3. 重启时将所有`iteration_state='pending'`或`'running'`的任务置为`'complete'`（执行是幂等的）
4. 定期执行整个Cross-Cluster Replication任务，一起处理所有表

### 5.2 Downstream Iteration

**处理步骤**：

1. **请求上游做snapshot并查询DDL diff**：
   - 连接到上游集群
   - 请求上游创建新的snapshot
   - 查询上游mo_catalog三表（mo_databases、mo_tables、mo_columns）获取DDL diff
   - 应用DDL变更到下游

2. **查询objectlist diff**：
   - 查询上游两个snapshot（当前watermark对应的snapshot和最新snapshot）的objectlist和新的ts
   - 计算objectlist差异（新增、删除的object）

3. **依次获取并应用Object**：
   - 下游按objectlist逐个向上游请求获取object
   - 将object应用到下游Catalog
   - 更新watermark和snapshot_ts
   - 某些object要按ts做truncate

4. **清理旧snapshot**：
   - 删除上上次的snapshot（保留当前和上一次snapshot）

**状态管理和并发控制**：
- 执行前检查cn_uuid、iteration_lsn、iteration_state是否一致
- 完成后将iteration_state更新为'complete'

---

## 6. 关键设计要点

### 6.1 下游Object不参与Merge

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
