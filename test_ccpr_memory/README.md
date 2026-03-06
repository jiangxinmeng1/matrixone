# CCPR Memory Control Test Suite

测试CCPR内存控制在大数据量DML操作和索引创建场景下的表现。

## 文件说明

| 文件 | 说明 |
|------|------|
| `test_ccpr_memory.sql` | SQL测试脚本，包含各种DML和索引创建操作 |
| `run_test.sh` | Shell脚本，运行测试并收集metrics |
| `analyze_metrics.py` | Python脚本，分析metrics数据并生成报告 |
| `sample_1m.csv` | 本地测试数据文件（约100万行） |

## 测试内容

### Part 1: 大数据量DML操作

- **INSERT**: 10万、50万、全表插入
- **UPDATE**: 10万行更新、全表单列更新
- **DELETE**: 10万行删除、全表删除
- **CTAS**: 50万行、全表 Create Table As Select

### Part 2: IVF向量索引

- 同步IVF索引 (lists=100, 256)
- 异步IVF索引 (lists=512, 1024)

### Part 3: HNSW向量索引

- 同步HNSW索引 (M=16/24, ef=64/100)
- 异步HNSW索引 (M=32/48, ef=128/200)

### Part 4: 全文索引

- 10万行全文索引
- 50万行全文索引
- 全表全文索引

### Part 5: B树索引

- 单列索引
- 复合索引
- 唯一索引

## 使用方法

### 1. 准备环境

```bash
# 确保MatrixOne正在运行
# 确保sample_1m.csv文件在test_ccpr_memory目录下

# 配置环境变量（可选）
export MO_HOST=127.0.0.1
export MO_PORT=6001
export MO_USER=root
export MO_PASSWORD=111
export METRICS_PORT=7001
```

### 2. 运行测试

SQL脚本会自动创建数据库、加载本地数据文件并执行测试：

```bash
# 添加执行权限
chmod +x run_test.sh

# 进入测试目录（确保sample_1m.csv在此目录）
cd test_ccpr_memory

# 运行完整测试
./run_test.sh run

# 或运行单独测试
./run_test.sh insert      # 仅测试INSERT
./run_test.sh vector      # 仅测试向量索引
./run_test.sh fulltext    # 仅测试全文索引

# 仅收集当前metrics
./run_test.sh metrics
```

### 3. 手动加载数据（可选）

如果需要手动加载数据，可以执行：

```sql
-- 连接MatrixOne
mysql -h 127.0.0.1 -P 6001 -u root -p111

-- 创建数据库
CREATE DATABASE IF NOT EXISTS ccpr_memory_test;
USE ccpr_memory_test;

-- 创建表
CREATE TABLE IF NOT EXISTS ca_comprehensive_dataset (
    md5_id varchar(255) NOT NULL,
    question text DEFAULT NULL,
    answer json DEFAULT NULL,
    source_type varchar(255) DEFAULT NULL,
    content_type varchar(255) DEFAULT NULL,
    keyword varchar(255) DEFAULT NULL,
    question_vector vecf32(1024) DEFAULT NULL,
    allow_access varchar(511) DEFAULT NULL,
    allow_identities varchar(512) DEFAULT NULL,
    delete_flag int DEFAULT NULL,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP(),
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP(),
    PRIMARY KEY (md5_id)
);

-- 加载本地数据
LOAD DATA INFILE './sample_1m.csv' 
INTO TABLE ca_comprehensive_dataset 
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' 
IGNORE 1 LINES;
```

### 4. 分析结果

```bash
# 安装Python依赖（可选，用于生成图表）
pip install matplotlib

# 运行分析脚本
python analyze_metrics.py

# 或指定结果目录
python analyze_metrics.py test_results_20240101_120000
```

## 输出文件

测试完成后，在 `test_results_YYYYMMDD_HHMMSS/` 目录下生成：

| 文件 | 说明 |
|------|------|
| `metrics_*.txt` | 各阶段的CCPR metrics快照 |
| `memory_*.txt` | 各阶段的内存使用情况 |
| `metrics_monitor.csv` | 持续监控的metrics数据 |
| `timing.csv` | 各操作的耗时数据 |
| `sql_output.log` | SQL执行输出 |
| `report.md` | 测试报告 |
| `analysis_report.md` | 分析报告 |
| `analysis_data.json` | JSON格式的分析数据 |
| `memory_trend.png` | 内存趋势图（如安装matplotlib） |
| `pool_hit_rate.png` | 对象池命中率图 |
| `timing.png` | 操作耗时图 |

## 监控的Metrics

### 内存相关

```
mo_ccpr_memory_bytes{type="total"}           # 总内存使用
mo_ccpr_memory_bytes{type="object_content"}  # 对象内容内存
mo_ccpr_memory_bytes{type="decompress_buffer"} # 解压缓冲区内存
mo_ccpr_memory_bytes{type="sort_index"}      # 排序索引内存
mo_ccpr_memory_bytes{type="row_offset_map"}  # 行偏移映射内存
mo_ccpr_memory_alloc_total{type}             # 分配次数
mo_ccpr_memory_alloc_bytes_total{type}       # 总分配字节
mo_ccpr_memory_free_total{type}              # 释放次数
```

### 对象池相关

```
mo_ccpr_pool_total{type="get_chunk_job",result="hit"}
mo_ccpr_pool_total{type="get_chunk_job",result="miss"}
mo_ccpr_pool_total{type="write_object_job",result="hit"}
mo_ccpr_pool_total{type="filter_object_job",result="hit"}
mo_ccpr_pool_total{type="aobject_mapping",result="hit"}
```

### Job相关

```
mo_ccpr_job_total{type,status}               # Job计数
mo_ccpr_job_duration_seconds{type}           # Job耗时
mo_ccpr_running{type}                        # 运行中的Job数
mo_ccpr_queue_size{type}                     # 队列大小
```

## PromQL查询示例

```promql
# 当前总内存使用
mo_ccpr_memory_bytes{type="total"}

# 过去5分钟内存分配速率
rate(mo_ccpr_memory_alloc_bytes_total[5m])

# 对象池总命中率
sum(rate(mo_ccpr_pool_total{result="hit"}[5m])) / 
sum(rate(mo_ccpr_pool_total[5m]))

# 各类Job的P99延迟
histogram_quantile(0.99, rate(mo_ccpr_job_duration_seconds_bucket[5m]))

# 当前运行的Job数
sum(mo_ccpr_running)

# 队列积压情况
mo_ccpr_queue_size
```

## 注意事项

1. **数据文件**：确保 `sample_1m.csv` 文件位于 `test_ccpr_memory/` 目录下
2. **内存需求**：大数据量操作需要较大内存，建议 >= 8GB
3. **执行时间**：完整测试可能需要数十分钟到数小时
4. **CCPR场景**：这些测试主要用于验证CCPR同步场景下的内存控制

## 故障排除

### 1. 连接失败

```bash
# 检查MatrixOne状态
ps aux | grep mo-service

# 检查端口
netstat -tlnp | grep 6001
```

### 2. Metrics收集失败

```bash
# 检查metrics端口
curl http://127.0.0.1:7001/metrics | grep mo_ccpr

# 检查是否有CCPR相关metrics
curl http://127.0.0.1:7001/metrics | grep -c mo_ccpr
```

### 3. 数据加载失败

```bash
# 检查数据文件是否存在
ls -la sample_1m.csv

# 检查文件格式
head -5 sample_1m.csv
```

### 4. 测试超时

```bash
# 可以分步执行测试
./run_test.sh insert
./run_test.sh vector
./run_test.sh fulltext
```
