# MatrixOne 基于S3的跨集群数据同步设计文档

**版本**: 1.0  
**创建日期**: 2025-11-19

---

## 1. 系统概述

实现基于S3对象存储的MatrixOne跨集群实时数据同步。利用TAE存储引擎的不可变block特性，通过S3中转实现高效的增量数据复制。

**核心特性**：
- 基于S3中转的数据传输
- 首次全量同步，后续增量同步（仅aobj/cnobj）
- Watermark机制跟踪同步进度（使用aobj的CreateTS）
- 下游事务性应用对象
- 故障自动恢复
- 下游消费状态反馈，上游根据反馈暂停同步

---

## 2. 系统表设计

### mo_s3_sync_tasks（同步任务表）

**位置**: `mo_catalog` 数据库

**存储内容**：
- 任务基本信息：task_id（UUID）、task_name、cluster_role（upstream/downstream）、state（running/paused/error）
- 表信息：account_id、db_name、table_name、table_id
- S3配置：endpoint、region、bucket、key_prefix、access_key、secret_key（加密）
- 同步配置：sync_interval、full_sync_completed
- 时间戳：created_at、updated_at、started_at
- 错误: error message，retry count, first error ts
- 执行的worker的配置：cn uuid

## 3. S3目录结构

```
s3://{bucket}/{prefix}/{account_id}/{db_id}/{table_id}/
├── manifest.json                    # 元数据清单（记录所有object和watermark）
├── object_list.meta                 # ObjectEntry列表序列化文件
├── {object_uuid_1}                  # TAE Object文件
├── {object_uuid_2}                  # TAE Object文件
├── ...
└── consumer_status.json             # 下游消费状态反馈
```

**说明**：
- 每个表一个独立目录，路径为 `{account_id}/{db_id}/{table_id}`
- `manifest.json`：记录当前watermark、object列表、版本号等元信息
- `object_list.meta`：序列化的ObjectEntry列表，包含CreateTS、DeleteTS、ObjectStats等
- `{object_uuid}`：TAE的实际数据块文件
- `consumer_status.json`：下游写入的消费状态，上游定期读取

---

## 4. 核心组件

### 4.1 UpstreamSyncExecutor（上游同步执行器）

**部署位置**：CN节点，每集群一个实例，CN启动时创建

**核心功能**：
1. 任务管理：启动时从`mo_s3_sync_tasks`加载所有`cluster_role='upstream'`的任务
2. 状态检查：执行前检查task的cn_uuid是否与当前CN一致，防止重复执行
3. 任务调度：根据系统表信息，定期（如每5秒）向TaskService提交Job
4. Job类型：
   - **UpstreamSyncJob**：同步数据到S3
   - **FeedbackMonitorJob**：监控下游消费状态

**传递给Job的信息**：
- task_id、table_id、db_name、table_name
- S3配置（endpoint、bucket、key_prefix等）
- current_watermark、full_sync_completed

---

### 4.2 DownstreamSyncExecutor（下游同步执行器）

**部署位置**：CN节点，每集群一个实例，CN启动时创建

**核心功能**：
1. 任务管理：启动时从`mo_s3_sync_tasks`加载所有`cluster_role='downstream'`的任务
2. 状态检查：执行前检查task的cn_uuid是否与当前CN一致
3. 任务调度：根据系统表信息，定期向TaskService提交Job
4. Job类型：
   - **DownstreamConsumeJob**：从S3消费数据并写入反馈

**传递给Job的信息**：
- task_id、table_id、db_name、table_name
- S3配置
- current_watermark

---

### 4.3 UpstreamSyncJob（上游同步任务）

**执行位置**：CN的TaskService

**核心功能**：
1. 状态检查：检查系统表中task的state和cn_uuid，确保未被其他CN执行
2. 读取ObjectList：
   - 从TN的catalog读取表的ObjectEntry列表
   - 首次同步：读取所有可见object
   - 增量同步：只读取CreateTS > watermark的aobj和cnobj
3. 复制Object：将object文件从源S3复制到目标S3的表目录
4. 生成元数据：
   - 序列化ObjectEntry列表到`object_list.meta`
   - 生成`manifest.json`，包含watermark、object数量等
5. 更新系统表：
   - 更新`mo_s3_sync_tasks`的watermark、updated_at
   - 首次完成后设置`full_sync_completed=true`

---

### 4.4 FeedbackMonitorJob（反馈监控任务）

**执行位置**：CN的TaskService

**核心功能**：
1. 状态检查：检查task的state和cn_uuid
2. 读取反馈：从S3读取`consumer_status.json`
3. 解析状态：解析下游的消费状态（ok/error）和error_message
4. 错误处理：
   - 如果状态为error，更新系统表state='paused'，记录error信息
   - 如果状态恢复为ok，自动将state改回'running'

---

### 4.5 DownstreamConsumeJob（下游消费任务）

**执行位置**：CN的TaskService

**核心功能**：
1. 状态检查：检查task的state和cn_uuid
2. 读取元数据：从S3读取`manifest.json`和`object_list.meta`
3. 检查watermark：如果S3的watermark <= 本地watermark，跳过本次执行
4. 下载Object：批量下载新的object文件到本地FileService
5. 应用到Catalog：
   - 开启新事务
   - 根据object_list.meta反序列化ObjectEntry
   - 创建ObjectEntry并关联到目标表
   - 提交事务
6. 更新系统表：更新watermark、updated_at
7. 写入反馈：
   - 成功：写入`consumer_status.json` {state: "ok", watermark: xxx, updated_at: xxx}
   - 失败：写入{state: "error", error_message: xxx, watermark: xxx}

---

### 4.6 S3GCManager（S3垃圾回收管理器）

**部署位置**：下游CN节点，作为独立的后台协程

**核心功能**：
1. 定期扫描：每小时检查S3中超过保留期（如24小时）的object文件
2. 安全验证：检查文件是否已被消费（watermark已超过该object的CreateTS）
3. 批量删除：删除过期的object文件和旧的object_list.meta
4. 保留文件：始终保留最新的manifest.json和object_list.meta

---

## 5. 关键设计要点

### 5.1 防止重复执行

- Executor发Job前检查系统表的cn_uuid是否与当前CN一致
- Job执行时再次检查state和cn_uuid，确保任务未被其他CN接管
- 使用乐观锁或version字段防止并发修改

### 5.2 Watermark机制

- 上游：扫描object后取max(CreateTS)作为新watermark
- 下游：事务提交成功后才更新watermark，保证一致性
- Watermark只增不减，用于断点续传

### 5.3 反馈机制

- 下游每次消费后写入`consumer_status.json`到S3
- 上游FeedbackMonitorJob定期读取，检测下游错误
- 下游出错时上游自动暂停，避免数据积压

### 5.4 故障恢复

- Executor崩溃：CN重启时重新加载任务，从watermark继续
- Job失败：TaskService自动重试，超过阈值后更新state='error'
- 事务失败：下游回滚事务，watermark不前进，反馈error给上游

---

## 6. 总结

本设计通过Executor+Job的架构实现基于S3的跨MO集群数据同步：

- **一张系统表**：`mo_s3_sync_tasks`记录所有任务信息和状态
- **简化的S3结构**：每表一个目录，包含manifest、object_list、objects和反馈文件
- **清晰的职责划分**：
  - Executor负责任务管理和调度
  - Job负责具体执行（读objectlist、复制object、更新状态）
- **可靠的同步机制**：
  - Watermark跟踪进度
  - 反馈机制监控下游状态
  - cn_uuid防止重复执行
  - 事务保证一致性

---

**End of Document**
