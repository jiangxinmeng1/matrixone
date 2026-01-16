# MatrixOne Cross-Cluster Replication (CCPR) 用户指南

**创建日期**: 2025-01-16  
**文档类型**: 用户使用指南

---

## 目录

1. [简介](#1-简介)
2. [快速开始](#2-快速开始)
3. [配置说明](#3-配置说明)
4. [查看和管理订阅](#4-查看和管理订阅)
5. [状态说明](#5-状态说明)
6. [错误处理](#6-错误处理)
7. [常见问题](#7-常见问题)

---

## 1. 简介

MatrixOne Cross-Cluster Replication (CCPR) 是一种跨集群（跨区域、跨云）的数据复制功能，允许通过SQL连接直接从上游集群获取数据并复制到下游集群。

### 1.1 功能特点

- **多种复制级别**：支持 Account、Database、Table 三种级别的复制
- **自动同步**：定期从上游集群同步数据变更
- **SQL接口**：通过标准的SQL语句创建和管理订阅
- **错误自动重试**：对可重试的错误自动进行重试，减少人工干预

---

## 2. 快速开始

### 2.1 上游集群创建Publication

首先，在上游集群创建Publication任务：

```sql
CREATE PUBLICATION <pub_name> DATABASE <db_name> ACCOUNT <account_name>;
```

**完整示例**：

以下示例展示了从上游账户account0授权account1的完整设置流程，下游将用account1的账号连接上游：

**步骤1：在上游账户（account0）创建数据库和表**

```sql
-- 使用上游账户（account0）登录
-- 创建数据库
CREATE DATABASE tpcc;

-- 使用数据库
USE tpcc;

-- 创建表
CREATE TABLE orders (
    id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE
);

-- 插入测试数据
INSERT INTO orders VALUES (1, 1001, '2025-01-01');
```

**步骤2：在account0创建Publication并授权给account1**

```sql
-- 在account0中执行
-- 创建Publication，指定数据库并授权给目标账户account1
CREATE PUBLICATION my_publication DATABASE tpcc ACCOUNT account1;
```

**说明**：
- `my_publication`：Publication名称
- `DATABASE tpcc`：指定要发布的数据库
- `ACCOUNT account1`：授权给account1，允许下游用其订阅此Publication

**发布限制**

#### 权限限制
- 只有 admin 角色可以创建、修改、删除 publication
- 只有 sys account 和授权的 normal accounts 可以发布到所有账户（account all）

#### 账户限制
- 不能发布到自己（can't publish to self）
- 不能订阅自己（can not subscribe to self）
- 指定的账户必须存在
- 发布账户如果被暂停（suspended），订阅时会失败

#### 数据库限制
- 不能发布系统数据库（如 mo_catalog）
- 只能发布用户数据库（dat_type 为空）
- 数据库必须存在

#### 表限制
- 指定的表必须存在于数据库中

#### Alter Publication 限制
- 如果 publication 使用 account all 选项，不能添加或删除账户

#### 订阅权限限制
- 账户必须被包含在 publication 的订阅账户列表中才能订阅

### 2.2 下游集群创建Subscription

在下游集群创建订阅任务，有三种方式：

#### 2.2.1 Account级别复制

复制整个Account下的所有数据库和表：

```sql
CREATE ACCOUNT FROM 'connection_string' PUBLICATION pub_name [SYNC INTERVAL interval];
```

#### 2.2.2 Database级别复制

复制指定数据库下的所有表：

```sql
CREATE DATABASE [IF NOT EXISTS] db_name 
  FROM 'connection_string' 
  PUBLICATION pub_name 
  [SYNC INTERVAL interval];
```

**示例**：

```sql
-- 复制整个数据库，同步间隔60秒
CREATE DATABASE tpcc
  FROM 'mysql://myaccount#root:password@127.0.0.1:6001' 
  PUBLICATION my_publication
  SYNC INTERVAL 60;
```

#### 2.2.3 Table级别复制

复制单张表：

```sql
CREATE TABLE [IF NOT EXISTS] dbname.table_name 
  FROM 'connection_string' 
  PUBLICATION pub_name 
  [SYNC INTERVAL interval];
```

**示例**：

```sql
-- 复制单张表，同步间隔120秒
CREATE TABLE tpcc.orders
  FROM 'mysql://myaccount#root:password@127.0.0.1:6001'
  PUBLICATION my_publication
  SYNC INTERVAL 120;
```

创建订阅的时候会检查上游的publication

---

## 3. 配置说明

### 3.1 连接字符串格式

连接字符串使用MySQL协议格式：

```
mysql://[account#]user:password@host:port
```

**参数说明**：
- `account`（可选）：账户名称，格式为 `account#user`，这个是被上游授权过的account(i.e. 2.1例子中的account1)
- `user`：用户名
- `password`：密码
- `host`：上游集群的IP地址或域名
- `port`：上游集群的端口号（通常是6001）

**示例**：

```sql
-- 带账户名的连接字符串
'mysql://myaccount#root:password@127.0.0.1:6001'

-- 不带账户名的连接字符串
'mysql://root:password@127.0.0.1:6001'
```

### 3.2 SYNC INTERVAL 参数

`SYNC INTERVAL` 指定同步间隔时间（单位：秒），系统会按照该间隔定期从上游集群拉取数据变更。

- **默认值**：60秒
- **最小值**：建议不小于30秒，避免对上游集群造成过大压力
- **最大值**：无限制，但建议根据业务需求设置合理的值

**示例**：

```sql
-- 设置同步间隔为120秒
CREATE DATABASE tpcc
  FROM 'mysql://myaccount#root:password@127.0.0.1:6001' 
  PUBLICATION my_publication
  SYNC INTERVAL 120;
```

---

## 4. 查看和管理订阅

### 4.1 查看所有订阅

查看当前账户下的所有订阅：

```sql
SHOW CCPR SUBSCRIPTIONS;
```

### 4.2 查看特定订阅

查看特定名称的订阅详情：

```sql
SHOW CCPR SUBSCRIPTION subscription_name;
```

**返回字段说明**：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| pub_name | VARCHAR | Publication 名称 |
| pub_account | VARCHAR | 上游账号名 |
| pub_database | VARCHAR | 上游数据库名（可为NULL，Account级别时为NULL） |
| pub_tables | VARCHAR | 上游表名（可为NULL，Account/Database级别时为NULL） |
| create_time | TIMESTAMP | 创建时间 |
| drop_time | TIMESTAMP | 删除时间，如果还没drop就是NULL |
| state | TINYINT | 订阅状态（0=running, 1=error, 2=pause, 3=dropped） |
| sync_level | VARCHAR | 同步级别（'account', 'database', 'table'） |
| upstream_conn | VARCHAR | 上游连接字符串（密码已被模糊处理） |
| iteration_lsn | BIGINT | 当前迭代 LSN |
| error_message | VARCHAR | 错误信息（如果没有错误，为NULL） |
| watermark | TIMESTAMP | 水位时间戳（最后一次成功同步的时间点） |

**示例输出**：

```
+------------------+-------------+-------------+-----------+---------------------+-----------+------------+------------------+----------------+------------------+-------------+---------------------+
| pub_name         | pub_account | pub_database| pub_tables| create_time         | drop_time | state      | sync_level       | upstream_conn  | iteration_lsn    | error_message| watermark           |
+------------------+-------------+-------------+-----------+---------------------+-----------+------------+------------------+----------------+------------------+-------------+---------------------+
| my_publication   | account1    | tpcc        | orders    | 2025-01-01 10:00:00 | NULL      | 1          | table            | mysql://***@...| 12345            | NULL         | 2025-01-01 12:00:00 |
+------------------+-------------+-------------+-----------+---------------------+-----------+------------+------------------+----------------+------------------+-------------+---------------------+
```

### 4.3 查看mo_ccpr_log

订阅任务的元数据存储在 `mo_catalog.mo_ccpr_log` 系统表中，可以通过SQL直接查询（需要系统账户权限）。

### 4.4 观测试图

- 复制的object数量和大小总合
- 有ddl更新的table数量
- 错误重试次数

### 4.4 暂停订阅

暂停订阅会停止同步任务，但保留订阅配置和数据：

```sql
PAUSE CCPR SUBSCRIPTION subscription_name;
```

**使用场景**：
- 临时停止同步，但希望保留订阅配置
- 等待上游集群问题解决

### 4.5 恢复订阅

恢复订阅会清除错误状态，重新开始同步：

```sql
RESUME CCPR SUBSCRIPTION subscription_name;
```

**使用场景**：
- 订阅处于错误状态（state=1），且无法自动重试恢复时
- 暂停后的订阅（state=2）需要恢复运行

**注意**：`RESUME` 会将 `iteration_state` 重置为 `complete`，并清除 `error_message`。

### 4.6 删除订阅

删除订阅会停止同步并移除订阅配置，但**不会删除**已同步的数据和数据库/表：

```sql
DROP CCPR SUBSCRIPTION subscription_name;
```

**注意**：
- 删除订阅不会删除下游集群中的数据
- 如果需要删除数据，需要手动执行 `DROP DATABASE` 或 `DROP TABLE`

### 4.7 删除Publication

在上游集群删除Publication：

```sql
DROP PUBLICATION publication_name;
```

---

## 5. 状态说明

订阅任务有以下几个状态：

### 5.1 状态值

| 状态值 | 状态名称 | 说明 |
|--------|----------|------|
| 0 | running | 运行中，订阅正在正常执行同步 |
| 1 | error | 错误，订阅遇到错误且无法自动恢复 |
| 2 | pause | 暂停，订阅已暂停同步 |
| 3 | dropped | 已删除，订阅已被删除 |

### 5.2 状态流转

```
running → error → (手动RESUME) → running → ...
    ↓
pause → (手动RESUME) → running → ...

running/error/pause → dropped (删除订阅)
```

### 5.3 状态检查

用sys租户查阅mo_ccpr_log表检查订阅状态：

```sql
-- 查看有错误的订阅
SELECT subscription_name, state, error_message 
FROM mo_ccpr_log
WHERE state = 1;
```

---

## 6. 错误处理

### 6.1 自动重试机制

系统会自动识别可重试的错误并进行重试，无需人工干预。

#### 6.1.1 自动重试的错误类型

系统会对以下类型的错误进行自动重试：

**1. 网络错误**（DefaultClassifier）
- 连接超时（connection timeout）
- 连接重置（connection reset）
- 网络不可用（network unavailable）
- IO超时（I/O timeout）
- 管道中断（broken pipe）
- TLS握手超时（TLS handshake timeout）
- EOF错误
- 临时网络故障

**2. MySQL协议错误**（MySQLErrorClassifier）
- **1205**: Lock wait timeout exceeded（锁等待超时）
- **1213**: Deadlock found（死锁）
- **2006**: MySQL server has gone away（服务器连接断开）
- **2013**: Lost connection to MySQL server（连接丢失）
- **2003**: Can't connect to MySQL server（无法连接）
- **1043**: Bad handshake（握手失败）

**3. 事务重试错误**（CommitErrorClassifier）
- RC模式事务需要重试的错误（ErrTxnNeedRetry）
- RC模式事务定义变更需要重试的错误（ErrTxnNeedRetryWithDefChanged）

**4. 测试注入错误**（UTInjectionClassifier）
- UT测试场景下的可重试错误

#### 6.1.2 重试策略

同步被分成一次次iteration,自动重试分两种：1.iteration内部的重试 2.多个iteration重试，i.e. iteration失败后用新的iteration重试

**iteration 内部**
- **重试间隔**：使用指数退避策略（Exponential Backoff）
  - 基础延迟：100ms
  - 增长因子：2
  - 每次重试的延迟时间 = 基础延迟 × 2^(重试次数-1)
- **最大重试时间**：30min
- 超过重试时间后iteration失败

**多个iteration之间**
- **最大重试次数**：10次
- iteration内部重试超时的错误不再用新iteration重试

#### 6.1.3 错误元数据格式

iteration内部的重试不会更新error_message，只有iteration失败后error_message会被更新。
重试过程中state为running。如果不再用新的iteration重试，state会被置成error。
自动重试的错误在 `error_message` 字段中会包含元数据信息，格式如下：

**可重试错误格式**：
```
R:count:firstSeen:lastSeen:message
```

**参数说明**：
- `R`：表示可重试（Retryable）
- `count`：当前重试次数
- `firstSeen`：首次出现时间（Unix时间戳）
- `lastSeen`：最后出现时间（Unix时间戳）
- `message`：错误消息内容

**示例**：
```
R:3:1704067200:1704067260:connection timeout
```
表示：这是一个可重试错误，已重试3次，首次出现于Unix时间1704067200，最后出现于1704067260，错误消息为"connection timeout"。

**不可重试错误格式**：
```
N:firstSeen:message
```

**参数说明**：
- `N`：表示不可重试（Non-retryable）
- `firstSeen`：首次出现时间（Unix时间戳）
- `message`：错误消息内容

**示例**：
```
N:1704067200:table does not exist
```
表示：这是一个不可重试错误，首次出现于Unix时间1704067200，错误消息为"table does not exist"。

### 6.2 手动处理错误

对于无法自动重试恢复的错误（如超过最大重试次数、非重试错误等），需要手动处理。

#### 6.2.1 错误处理流程

1. **查看错误信息**
   ```sql
   SHOW CCPR SUBSCRIPTION subscription_name;
   ```
   查看 `error_message` 字段了解具体错误。

2. **判断错误类型**
   - 如果`state`字段为error，表示不再自动重试，需要手动恢复

3. **解决根本问题**
   - **网络问题**：检查网络连接、防火墙、上游集群状态
   - **权限问题**：检查上游集群的用户权限
   - **表不存在**：检查上游集群的表是否存在
   - **配置错误**：检查连接字符串、Publication配置等

4. **恢复订阅**
   问题解决后，使用 `RESUME` 命令恢复订阅：
   ```sql
   RESUME CCPR SUBSCRIPTION subscription_name;
   ```
---

## 7. 常见问题

### 7.1 同步延迟问题

在没有设置SYNC INTERVAL参数时，为了等上游刷盘，下游依然会有几秒到一分钟的延迟。
设置SYNC INTERVAL后，每次同步间隔为SYNC INTERVAL + 等待上游刷盘时长。

**数据同步延迟较大可能原因**：
- 上游集群负载高，响应慢
- 网络延迟高
- 下游集群处理能力不足
- 同步间隔设置过长

**解决方法**：
1. 检查上游集群的负载情况
2. 检查网络连接质量
3. 调整 `SYNC INTERVAL` 参数（如果业务允许）
4. 检查下游集群的资源使用情况

### 7.1 上游DDL变化

CCPR能自动捕捉到上游的DDL变化，更新到下游

---

**文档版本**: 1.0
