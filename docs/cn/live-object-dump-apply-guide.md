# Live Object Dump/Apply 使用文档

## 概述

`mo-tool live-dump` 是面向在线 MatrixOne 集群的 object 级 dump/apply 工具，支持按表、database、租户 account、cluster 粒度 dump 和 apply。

核心特点：

- 数据源是正在运行的集群。
- 输出是 MatrixOne object dump 包。
- apply 不走 `LOAD DATA`。
- dump 会过滤 tombstone，不在 dump 包中保留已删除的数据；过滤按 data block 生成 delete bitmap，不会把全表 tombstone rowid 一次性加载到内存。
- 目标端 object name/object id 会重新生成，不保留源端 object 名。

底层实现以表为最小单位。database、account、cluster 级别只是把范围展开成多张表并批量执行表级 dump/apply。

---

## 一、构建

```bash
make mo-tool
```

构建产物为：

```bash
./mo-tool
```

---

## 二、前置条件

### 2.1 源端

- 源 MatrixOne 集群在线。
- 执行用户有读取目标范围元数据和数据的权限。
- CN 可以把 dump 请求转发到 TN。

### 2.2 目标端

- 目标 MatrixOne 集群在线。
- apply 不依赖 debug mode，也不要求开启 debug-only 开关；正常启动的集群即可执行。
- 目标端能够读取 dump 包所在路径。
- 默认不允许覆盖已有目标表。

### 2.3 dump 包存储

dump 包可以写到：

- 本地目录
- MatrixOne fileservice 配置指定的对象存储
- 直接通过 S3/MinIO 参数指定的对象存储

---

## 三、list - 浏览在线集群元数据

`list` 用于在 dump 前查询 account、database、table 元数据。

### 3.1 命令

```bash
./mo-tool live-dump list [flags]
```

### 3.2 参数说明

| 参数 | 必需 | 说明 |
|------|------|------|
| `--source` | 是 | 源 MatrixOne 地址，格式 `<host>:<port>` |
| `--user` | 是 | 用户名 |
| `--password` | 否 | 密码 |
| `--type` | 否 | 列出类型：`tables` 默认、`databases`/`dbs`、`accounts` |
| `--account-id` | 否 | 按租户 ID 过滤 |
| `--database-id` | 否 | 按 database ID 过滤 |

### 3.3 示例

```bash
# 列出所有 database
./mo-tool live-dump list \
  --source 127.0.0.1:6001 \
  --user dump \
  --password 111 \
  --type databases

# 列出指定 database 下的表
./mo-tool live-dump list \
  --source 127.0.0.1:6001 \
  --user dump \
  --password 111 \
  --database-id 9001

# 列出所有 account
./mo-tool live-dump list \
  --source 127.0.0.1:6001 \
  --user dump \
  --password 111 \
  --type accounts
```

---

## 四、以表为单位 dump

### 4.1 命令

```bash
./mo-tool live-dump dump --table-id=<TABLE_ID> [选项]
```

### 4.2 参数说明

| 参数 | 必需 | 说明 |
|------|------|------|
| `--source` | 是 | 源 MatrixOne 地址 |
| `--user` | 是 | 用户名 |
| `--password` | 否 | 密码 |
| `--table-id` | 是 | MatrixOne 内部 table id，可通过 `live-dump list` 获取 |
| `-o` / `--output` | 是 | dump 包输出目录或远程路径 |
| `--out-fs-config` | 否 | 输出目标 MO TOML 配置文件 |
| `--out-fs-name` | 否 | 输出 fileservice 名称，默认 `SHARED` |
| `--out-s3` | 否 | 输出目标 S3 参数 |
| `--out-backend` | 否 | 输出 backend：`S3` 或 `MINIO` |

### 4.3 示例

```bash
./mo-tool live-dump dump \
  --source 127.0.0.1:6001 \
  --user dump \
  --password 111 \
  --table-id 272535 \
  -o ./dump_out
```

---

## 五、以 database 为单位 dump

### 5.1 命令

```bash
./mo-tool live-dump dump --database-id=<DB_ID> --output-dir=<DIR> [选项]
```

### 5.2 参数说明

| 参数 | 必需 | 说明 |
|------|------|------|
| `--source` | 是 | 源 MatrixOne 地址 |
| `--user` | 是 | 用户名 |
| `--password` | 否 | 密码 |
| `--database-id` | 是 | database ID |
| `--output-dir` | 是 | dump 包输出根目录 |
| `--jobs` | 否 | 并发 dump 表数，默认 `5` |

### 5.3 示例

```bash
./mo-tool live-dump dump \
  --source 127.0.0.1:6001 \
  --user dump \
  --password 111 \
  --database-id 9001 \
  --output-dir ./dump_out \
  --jobs 5
```

database dump 会枚举该库下所有支持恢复的普通表，并逐表执行底层 dump。

---

## 六、以 account 或 cluster 为单位 dump

### 6.1 account dump

```bash
./mo-tool live-dump dump \
  --source 127.0.0.1:6001 \
  --user dump \
  --password 111 \
  --account-id 7 \
  --output-dir ./dump_account_7 \
  --jobs 8
```

### 6.2 cluster dump

```bash
./mo-tool live-dump dump \
  --source 127.0.0.1:6001 \
  --user dump \
  --password 111 \
  --cluster \
  --output-dir ./dump_cluster \
  --jobs 8
```

account/cluster 级别会展开成多库多表任务。默认每张表使用自己的 snapshot；如果需要全局一致 snapshot，需要实现并启用统一 snapshot 模式。

---

## 七、dump 包目录结构

```text
<dump-root>/
├── manifest.json
└── tables/
    └── account_<account_id>/
        └── db_<database_id>/
            └── table_<table_id>/
                ├── table_manifest.json
                ├── schema
                ├── table
                ├── object_list
                └── objects/
                    ├── <dump-object-file-1>
                    ├── <dump-object-file-2>
                    └── ...
```

文件说明：

| 文件 | 说明 |
|------|------|
| `manifest.json` | 本次 dump 的全局信息，包括 dump id、范围、源集群、表列表 |
| `table_manifest.json` | 单表信息，包括 database/table 名称、ID、snapshot ts、统计信息 |
| `schema` | 表列元数据 object |
| `table` | 表元数据 object |
| `object_list` | data object 清单 |
| `objects/` | dump 出来的 object 数据 |

`object_list` 至少包含：

| 字段 | 说明 |
|------|------|
| `object_type` | data |
| `object_stats` | 源 object stats |
| `create_ts` | object 创建时间 |
| `delete_ts` | object 删除时间 |
| `is_persisted` | 源 object 是否已经持久化 |

---

## 八、输出到 S3/MinIO

### 8.1 通过 MO 配置文件

```bash
./mo-tool live-dump dump \
  --source 127.0.0.1:6001 \
  --user dump \
  --password 111 \
  --table-id 272535 \
  -o dump/table_272535 \
  --out-fs-config etc/launch-minio-local/tn.toml \
  --out-fs-name SHARED
```

### 8.2 直接指定 S3 参数

```bash
./mo-tool live-dump dump \
  --source 127.0.0.1:6001 \
  --user dump \
  --password 111 \
  --database-id 9001 \
  -o dump/db_9001 \
  --out-s3 bucket=mo-test,endpoint=http://127.0.0.1:9000,region=us-east-1,key-prefix=dumps/,key-id=minio,key-secret=minio123 \
  --out-backend MINIO
```

S3 参数使用逗号分隔的 `key=value` 格式：

| 参数 | 必需 | 说明 |
|------|------|------|
| `bucket` | 是 | bucket 名称 |
| `endpoint` | 是 | S3/MinIO endpoint |
| `key-prefix` | 是 | dump 包路径前缀 |
| `key-id` | 是 | access key id |
| `key-secret` | 是 | access key secret |
| `region` | 否 | region；MINIO 场景建议显式指定 |

---

## 九、apply

### 9.0 参数说明

apply 命令支持以下参数：

| 参数 | 必需 | 说明 |
|------|------|------|
| `--target` | 是 | 目标 MatrixOne 地址，格式 `<host>:<port>` |
| `--user` | 是 | 用户名 |
| `--password` | 否 | 密码 |
| `--from` | 是 | dump 包路径（本地目录或远程路径） |
| `--target-database` | 表级/database 级必需 | 目标 database 名称 |
| `--target-table` | 表级必需 | 目标 table 名称 |
| `--target-prefix` | 否 | account/cluster 级 apply 时，给所有恢复的 database/table 名加统一前缀。例如 `--target-prefix restored_` 会将 `mydb` → `restored_mydb`，`mytable` → `restored_mytable`。若指定了 `--target-database`/`--target-table`，则优先使用显式指定的名称，`--target-prefix` 对该表/库不生效 |

### 9.1 表级 apply

```bash
./mo-tool live-dump apply \
  --target 127.0.0.1:6001 \
  --user dump \
  --password 111 \
  --from ./dump_out \
  --target-database restored_db \
  --target-table restored_table
```

### 9.2 database apply

```bash
./mo-tool live-dump apply \
  --target 127.0.0.1:6001 \
  --user dump \
  --password 111 \
  --from ./dump_out \
  --target-database restored_db
```

### 9.3 account/cluster apply

```bash
./mo-tool live-dump apply \
  --target 127.0.0.1:6001 \
  --user dump \
  --password 111 \
  --from ./dump_account_7
```

```bash
./mo-tool live-dump apply \
  --target 127.0.0.1:6001 \
  --user dump \
  --password 111 \
  --from ./dump_cluster \
  --target-prefix restored_
```

### 9.4 apply 行为

apply 对每张表执行以下流程：

1. 读取 `schema`、`table`、`object_list`。
2. 创建目标 database；如果同名 database 已存在，直接失败。
3. 创建目标 table；如果同名 table 已存在，直接失败。
5. 读取 data object（dump 阶段已过滤 tombstone，仅包含可见行）。
6. 在目标端重新写新 object。
7. 将新 object 写入目标 S3/fileservice。
8. 注册新的 object stats。
9. 提交事务。

apply 不会把源端 object 原名复制到目标端。
