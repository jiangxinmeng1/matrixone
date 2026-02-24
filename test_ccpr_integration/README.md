# CCPR Integration Test Framework

跨集群发布/复制 (Cross-Cluster Publication/Replication) 集成测试框架。支持长时间运行（如跑一晚上）的全面功能测试。

init 权限
ddl 索引 alter truncate
dml
pause resume drop

ckp flush load object gc

安利向量 insert delete update
s3 insert/delete

alter truncate load


## 功能覆盖

### 1. 订阅级别测试
- **Table级别**: 单表订阅测试
- **Database级别**: 整库订阅测试
- **Account级别**: 账户级别订阅测试

### 2. 操作测试
- **PAUSE**: 暂停订阅
- **RESUME**: 恢复订阅
- **DROP**: 删除订阅
- **SHOW CCPR SUBSCRIPTIONS**: 查看订阅状态

### 3. 索引测试
- **次级索引**: 普通二级索引
- **唯一索引**: UNIQUE INDEX
- **全文索引**: FULLTEXT INDEX
- **向量索引**: IVFFLAT INDEX

### 4. ALTER TABLE测试
- ADD COLUMN
- DROP COLUMN
- MODIFY COLUMN
- RENAME COLUMN
- ADD INDEX
- DROP INDEX
- CHANGE COMMENT

### 5. 并发测试
- 多个下游账户同时订阅上游同一张表
- 并发写入与同步

### 6. 性能测试
- 持续数据插入
- 迭代延迟监控
- 吞吐量统计

## 快速开始

### 前置条件

1. 两个MatrixOne集群（上游和下游）已运行
2. Python 3.7+
3. pymysql库

```bash
pip install pymysql
```

### 运行测试

```bash
cd test_ccpr_integration

# 运行30分钟快速测试
python main.py --duration 1800

# 运行8小时过夜测试
python main.py --overnight

# 只运行场景测试
python main.py --scenarios-only --duration 3600

# 只运行性能测试
python main.py --performance-only --duration 7200
```

### 使用Shell脚本

```bash
# 给脚本执行权限
chmod +x run_overnight_test.sh

# 运行8小时测试
./run_overnight_test.sh

# 运行1小时测试
./run_overnight_test.sh -d 3600

# 自定义集群端口
./run_overnight_test.sh --upstream-port 6001 --downstream-port 6002
```

## 配置选项

### 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--overnight` | 运行过夜测试(8小时) | - |
| `--duration` | 测试时长(秒) | 1800 |
| `--upstream-host` | 上游集群地址 | 127.0.0.1 |
| `--upstream-port` | 上游集群端口 | 6001 |
| `--downstream-host` | 下游集群地址 | 127.0.0.1 |
| `--downstream-port` | 下游集群端口 | 6002 |
| `--user` | 数据库用户 | root |
| `--password` | 数据库密码 | 111 |
| `--sync-interval` | 同步间隔(秒) | 10 |
| `--concurrent-accounts` | 并发测试账户数 | 3 |
| `--log-file` | 日志文件路径 | ccpr_integration_test.log |
| `--report-file` | 报告文件路径 | ccpr_test_report.json |

### 禁用特定测试

```bash
# 跳过索引测试
python main.py --no-indexes

# 跳过并发测试
python main.py --no-concurrent

# 跳过alter table测试
python main.py --no-alter-table
```

### 环境变量

可以通过环境变量配置：

```bash
export UPSTREAM_HOST=192.168.1.100
export UPSTREAM_PORT=6001
export DOWNSTREAM_HOST=192.168.1.101
export DOWNSTREAM_PORT=6002
export DB_USER=root
export DB_PASSWORD=111
export TEST_DURATION=28800
export SYNC_INTERVAL=10

python main.py
```

## 测试输出

### 日志文件

测试过程中会生成详细日志：

```
2024-01-15 22:00:00 - INFO - === PHASE 1: Scenario Tests ===
2024-01-15 22:00:01 - INFO - [1/6] Running scenario: TableLevelSubscription
2024-01-15 22:00:15 - INFO - [✓] TableLevelSubscription: PASSED (14.2s)
...
```

### 报告文件

测试完成后生成JSON格式报告：

```json
{
  "mode": "overnight",
  "total_duration_seconds": 28800,
  "scenario_tests": {
    "iterations": 12,
    "total_passed": 68,
    "total_failed": 4,
    "pass_rate": 94.4
  },
  "performance_test": {
    "success": true,
    "total_rows_inserted": 125000,
    "total_iterations": 2160,
    "metrics": {
      "iteration_latency": {
        "avg": 10.2,
        "p95": 15.3
      }
    }
  },
  "overall_success": true
}
```

## 测试场景详解

### TableLevelScenario
测试单表级别的CCPR订阅：
1. 在上游创建表并插入数据
2. 创建表级别的publication
3. 在下游创建subscription
4. 持续插入数据并验证同步

### DatabaseLevelScenario
测试整库级别的CCPR订阅：
1. 在上游创建数据库和多种类型的表
2. 创建数据库级别的publication
3. 验证所有表的同步

### PauseResumeScenario
测试PAUSE/RESUME操作：
1. 创建订阅并等待初始同步
2. 执行PAUSE CCPR SUBSCRIPTION
3. 在上游插入数据（验证不同步）
4. 执行RESUME CCPR SUBSCRIPTION
5. 验证数据恢复同步

### IndexScenario
测试各种索引类型：
- 创建带有fulltext、ivfflat、unique、composite索引的表
- 验证索引在下游正确创建
- 测试索引功能（全文搜索、向量搜索）

### AlterTableScenario
测试ALTER TABLE操作的同步：
- ADD COLUMN
- CREATE INDEX
- ALTER TABLE COMMENT
- 验证结构变更正确同步

### ConcurrentMultiAccountScenario
测试多账户并发订阅：
1. 在下游创建多个账户
2. 每个账户订阅同一个上游数据库
3. 并发写入和同步
4. 验证所有账户数据一致

## 常见问题

### 连接失败
确保集群已启动且端口正确：
```bash
# 检查上游
mysql -h127.0.0.1 -P6001 -uroot -p111 -e "SELECT 1"

# 检查下游
mysql -h127.0.0.1 -P6002 -uroot -p111 -e "SELECT 1"
```

### 同步延迟过高
- 增加`sync_interval`
- 检查网络延迟
- 查看日志中的checkpoint触发情况

### 测试中断后清理
手动清理测试资源：
```sql
-- 在下游执行
DROP DATABASE IF EXISTS ccpr_test_%;
DROP ACCOUNT IF EXISTS ccpr_test_acc%;

-- 在上游执行
DROP PUBLICATION IF EXISTS pub_%;
DROP DATABASE IF EXISTS ccpr_test_%;
```

## 目录结构

```
test_ccpr_integration/
├── README.md              # 本文档
├── requirements.txt       # Python依赖
├── config.py              # 配置定义
├── db_utils.py            # 数据库工具
├── data_generator.py      # 测试数据生成
├── test_scenarios.py      # 测试场景
├── performance_test.py    # 性能测试
├── main.py                # 主入口
└── run_overnight_test.sh  # Shell启动脚本
```

## 扩展测试

添加新的测试场景：

1. 在`test_scenarios.py`中创建新类继承`BaseScenario`
2. 实现`setup()`, `run()`, `verify()`, `cleanup()`方法
3. 在`get_all_scenarios()`函数中注册新场景

```python
class MyNewScenario(BaseScenario):
    def __init__(self, config: TestConfig):
        super().__init__(config, "MyNewScenario")
    
    def setup(self) -> bool:
        # Setup code
        return True
    
    def run(self) -> bool:
        # Test code
        return True
    
    def verify(self) -> bool:
        # Verification code
        return True
```
