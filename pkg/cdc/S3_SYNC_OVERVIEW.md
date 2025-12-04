# MatrixOne 跨集群数据物理同步 - 概要设计

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

实现MatrixOne跨集群（跨区域、跨云）的数据实时同步，通过SQL直接连接上游集群获取数据。

---

## 2. 架构设计

### 2.1 整体架构

```
上游MO集群                                     下游MO集群
┌──────────────┐                           ┌──────────────┐
│   CN Node    │                           │   CN Node    │
│              │                           │ ┌──────────┐ │
│              │◄────── SQL连接 ────────────┤ │  Sync    │ │
│              │                           │ │ Executor │ │
│              │                           │ └──────────┘ │
│              │                           │              │
│              │◄─── 1. SQL请求做snapshot ─┤              │
│              │◄───    SQL查询三表 ───────┤              │
│              │                           │              │
│              │◄─── 2. SQL查询objectlist ─┤              │
│              │      diff                 │              │
│              │                           │              │
│              │◄─── 3. SQL依次获取object ─┤              │
│              │                           │              │
│              │◄─── 4. SQL删除上上次 ──────┤              │
│              │      snapshot             │              │
│              │                           │              │
└──────────────┘                           └──────────────┘
```

### 2.2 核心组件

**Executor**：
- **Sync Executor**：CN启动时创建，只在下游运行
- 处理整个同步任务，一起执行所有表的同步

**下游**：
- **Downstream Iteration**：连接到上游集群，获取数据并应用到Catalog

---

## 3. SQL接口

### 3.1 下游 - 创建同步任务

**语法**：

```sql
CREATE SUBSCRIPTION <subscription_name>
  DATABASE <db_name>
  [TABLE <table_name>]
  FROM 'mysql://<account>#<user>:<password>@<host>:<port>'
  [SYNC_INTERVAL = <seconds>];
```

**参数说明**：
- `subscription_name`：订阅名称，集群内唯一
- 同步级别，支持：
  - `database`：同步指定数据库下的所有表
  - `table`：同步指定表
- `DATABASE`：指定数据库名称
- `TABLE`：table级别必填，指定表名称
- `FROM`：上游MatrixOne集群的连接字符串
- `SYNC_INTERVAL`：同步间隔（秒），默认60秒

**示例**：

```sql
-- Database级别：同步整个数据库
CREATE SUBSCRIPTION sync_tpcc
  DATABASE tpcc
  FROM 'mysql://myaccount#root:password@127.0.0.1:6001';

-- Table级别：同步单张表
CREATE SUBSCRIPTION sync_orders
  DATABASE tpcc
  TABLE orders
  FROM 'mysql://myaccount#root:password@127.0.0.1:6001'
  SYNC_INTERVAL = 60;
```

---

### 3.2 查看任务状态

```sql
SHOW SUBSCRIPTIONS;

showSubscriptionsOutputColumns = [8]Column{
    "subscription_name",      // VARCHAR - 订阅名称
    "upstream_conn",          // VARCHAR - 上游连接字符串
    "database",               // VARCHAR - 数据库名称
    "tables",                 // TEXT - 表列表（* 表示所有表）
    "sync_interval",          // INT - 同步间隔（秒）
    "status",                 // TINYINT - 状态（0=Normal, 其他=Deleted/Error）
    "create_time",            // TIMESTAMP - 创建时间
    "update_time",            // TIMESTAMP - 更新时间
}
```

---

### 3.3 删除任务

```sql
DROP SUBSCRIPTION <subscription_name>;
```

---

## 4. 系统表设计

### 4.1 mo_sync_configs（同步配置表）

**存储位置**：`mo_catalog` 数据库

**作用**：记录同步任务的配置信息，支持database/table级别

**Schema**：

```sql
CREATE TABLE mo_catalog.mo_sync_configs (
    -- 任务标识
    task_id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    subscription_name    VARCHAR(5000) NOT NULL,
    
    -- 同步级别和范围
    sync_level           VARCHAR(16) NOT NULL,           -- 'database', 'table'
    account_id           BIGINT NOT NULL,
    db_name              VARCHAR(5000),                   -- database/table级别必填
    table_name           VARCHAR(5000),                   -- table级别必填
    
    -- 上游连接配置
    upstream_conn         VARCHAR(5000) NOT NULL,          -- MySQL连接字符串
    
    -- 同步配置（JSON格式）
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

**Sync Executor**：
1. 集群启动时创建Sync Executor（单例，只在下游运行）
2. 从`mo_sync_configs`加载所有同步任务配置
3. 重启时将所有`iteration_state='pending'`或`'running'`的任务置为`'complete'`（执行是幂等的）
4. 定期执行整个同步任务，一起处理所有表

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
