# CCPR集成测试计划

## 数据一致性检查方法

### 短测试检查流程
```
1. 上游执行DML
2. checkpoint
3. 读取下游mo_ccpr_log的iteration_lsn (记为lsn_before)
4. 循环等待: lsn变化 (lsn_current > lsn_before)
5. 直接读取上游表和下游表，对比COUNT/数据
```

### 长测试检查流程
```
1. PAUSE CCPR SUBSCRIPTION
2. sleep 10s (等待当前iteration完成)
3. 从下游mo_ccpr_log读取: task_id, iteration_lsn, watermark, error_message
4. 检查watermark落后程度 <= 10min
5. 检查error_message为空 (除非是预期错误)
6. 拼接snapshot name: ccpr_{task_id}_{iteration_lsn}
7. 上游: SELECT * FROM t {SNAPSHOT = 'ccpr_xxx'}
8. 下游: SELECT * FROM t (直接读)
9. 对比数据一致性
10. RESUME继续测试
```

### mo_ccpr_log关键字段
| 字段 | 说明 |
|------|------|
| task_id | 订阅任务ID |
| iteration_lsn | 当前迭代的LSN |
| watermark | 上游数据水位时间戳 |
| error_message | 错误信息 |
| state | 0=running, 1=error, 2=pause |

---

## 一、短时间测试 (Quick Test, ~30分钟)

### 1. 权限测试

#### 1.1 Account Level 权限
```
上游:
  CREATE ACCOUNT up_acc1, up_acc2   -- 上游账户
  CREATE PUBLICATION pub_acc ACCOUNT up_acc1
  
下游:
  CREATE ACCOUNT ds_acc1  -- 下游账户1
  CREATE ACCOUNT ds_acc2  -- 下游账户2
  
  Case 1: ds_acc1用up_acc1凭证订阅 -> 成功
    CREATE DATABASE db FROM 'mysql://up_acc1#user:pwd@host:port' PUBLICATION pub_acc
  Case 2: ds_acc2用up_acc2凭证订阅 -> 失败 (up_acc2无权限)
    CREATE DATABASE db FROM 'mysql://up_acc2#user:pwd@host:port' PUBLICATION pub_acc
```

#### 1.2 Database Level 权限
```
上游:
  CREATE ACCOUNT up_acc1
  CREATE DATABASE db1
  CREATE TABLE db1.t1, db1.t2
  CREATE PUBLICATION pub_db DATABASE db1 ACCOUNT up_acc1
  
下游:
  CREATE ACCOUNT ds_acc3, ds_acc4, ds_acc5
  
  Case 1: ds_acc3用up_acc1订阅整个db1 -> 成功
  Case 2: ds_acc4用up_acc1只订阅db1.t1 -> 成功 (部分订阅)
  Case 3: ds_acc5用up_acc1订阅不存在的表db1.t999 -> 失败
```

#### 1.3 Table Level 权限
```
上游:
  CREATE ACCOUNT up_acc1
  CREATE DATABASE db1
  CREATE TABLE db1.t1, db1.t2
  CREATE PUBLICATION pub_tbl TABLE db1.t1 ACCOUNT up_acc1
  
下游:
  CREATE ACCOUNT ds_acc6, ds_acc7, ds_acc8
  
  Case 1: ds_acc6用up_acc1订阅db1.t1 -> 成功
  Case 2: ds_acc7用up_acc1订阅db1.t2 -> 失败 (不在pub范围)
  Case 3: ds_acc8用up_acc1订阅整个db1 -> 失败 (pub只发布单表)
```

#### 1.4 ACCOUNT ALL 权限
```
上游:
  CREATE ACCOUNT up_acc1, up_acc2
  CREATE PUBLICATION pub_all DATABASE db1 ACCOUNT ALL
  
下游:
  CREATE ACCOUNT ds_acc9, ds_acc10
  
  Case 1: ds_acc9用up_acc1订阅 -> 成功
  Case 2: ds_acc10用up_acc2订阅 -> 成功
```

#### 1.5 多账户授权
```
上游:
  CREATE ACCOUNT up_acc1, up_acc2, up_acc3
  CREATE PUBLICATION pub_multi DATABASE db1 ACCOUNT up_acc1, up_acc2
  
下游:
  CREATE ACCOUNT ds_acc11, ds_acc12, ds_acc13
  
  Case 1: ds_acc11用up_acc1订阅 -> 成功
  Case 2: ds_acc12用up_acc2订阅 -> 成功
  Case 3: ds_acc13用up_acc3订阅 -> 失败 (up_acc3不在授权列表)
```

#### 1.6 部分订阅测试
```
场景A: 上游pub db, 下游订阅单表
  上游: CREATE PUBLICATION pub DATABASE db1 ACCOUNT ALL
  下游: CREATE TABLE db1.t1 FROM '...' PUBLICATION pub
  验证: 只同步t1, 不同步db1中其他表

场景B: 上游pub多表, 下游订阅部分表
  上游: CREATE PUBLICATION pub DATABASE db1 ACCOUNT ALL  (db1有t1,t2,t3)
  下游: 只订阅t1和t2
  验证: t1,t2同步, t3不同步
```

#### 1.7 删除Publication测试
```
1. 创建publication并订阅成功
2. 验证数据同步正常
3. 上游: DROP PUBLICATION pub_name
4. 上游: INSERT新数据
5. 等待sync_interval
6. 验证: 下游订阅进入error状态 或 数据不再同步
7. 检查mo_ccpr_log.error_message
```

### 2. DML测试

#### 2.1 测试表结构 (覆盖所有列类型)
```sql
CREATE TABLE all_types (
  -- 整数
  c_tinyint TINYINT, c_smallint SMALLINT, c_int INT, c_bigint BIGINT,
  c_utinyint TINYINT UNSIGNED, c_usmallint SMALLINT UNSIGNED,
  c_uint INT UNSIGNED, c_ubigint BIGINT UNSIGNED,
  -- 浮点/定点
  c_float FLOAT, c_double DOUBLE,
  c_decimal DECIMAL(18,4), c_decimal64 DECIMAL(18,0),
  -- 字符串
  c_char CHAR(50), c_varchar VARCHAR(200), c_text TEXT, c_blob BLOB,
  -- 时间
  c_date DATE, c_time TIME, c_datetime DATETIME, c_timestamp TIMESTAMP,
  -- 特殊
  c_bool BOOL, c_json JSON, c_uuid UUID, c_enum ENUM('a','b','c'),
  c_bit BIT(8),
  -- 向量
  c_vecf32 VECF32(8), c_vecf64 VECF64(8),
  -- 主键
  id BIGINT PRIMARY KEY
);
```

#### 2.2 索引类型覆盖
```sql
-- 普通索引
CREATE INDEX idx_int ON t(c_int);
-- 唯一索引
CREATE UNIQUE INDEX uidx_varchar ON t(c_varchar);
-- 组合索引
CREATE INDEX idx_composite ON t(c_int, c_varchar);
-- 全文索引
CREATE FULLTEXT INDEX ftidx ON t(c_text);
-- 向量索引
CREATE INDEX idx_vec USING IVFFLAT ON t(c_vecf32) LISTS=16 OP_TYPE 'vector_l2_ops';
-- Master索引 (用于fulltext)
CREATE INDEX idx_master USING MASTER ON t(c_text);
```

#### 2.3 Table Level DML测试
```
上游: pub TABLE db1.all_types
下游: 订阅 db1.all_types

测试:
  1. INSERT 100行 (覆盖所有列类型) -> 验证
  2. UPDATE 50行 -> 验证
  3. DELETE 30行 -> 验证
  4. LOAD 10万行 -> 验证
  5. DELETE 5万行 -> 验证
  6. TRUNCATE -> 验证
```

#### 2.4 Database Level DML测试 (部分表更新)
```
上游: 
  CREATE DATABASE db1
  CREATE TABLE db1.t1, db1.t2, db1.t3  -- 3张表
  pub DATABASE db1
  
下游: 订阅整个db1

测试:
  1. 只在t1插入数据 -> 验证t1同步, t2/t3不变
  2. 只在t2更新数据 -> 验证t2同步, t1/t3不变
  3. 只在t3删除数据 -> 验证t3同步, t1/t2不变
  4. t1 LOAD 10万行, t2/t3各插入100行 -> 验证全部同步
  5. TRUNCATE t1 -> 验证t1清空, t2/t3不变
```

#### 2.5 Account Level DML测试 (部分db/table更新)
```
上游:
  CREATE DATABASE db1, db2, db3
  db1: t1, t2
  db2: t3, t4
  db3: t5
  pub ACCOUNT level
  
下游: 订阅整个account

测试:
  1. 只在db1.t1插入 -> 验证db1.t1同步, 其他不变
  2. 只在db2更新 -> 验证db2同步, db1/db3不变
  3. 在db1/db2/db3各操作不同表 -> 验证各自同步
  4. db1.t1 LOAD 10万行, 其他表少量操作 -> 验证全部同步
  5. TRUNCATE db2.t3 -> 验证db2.t3清空, 其他不变
```

#### 2.6 验证流程
```python
def verify_sync(upstream, downstream, table):
    checkpoint(upstream)
    lsn_before = get_lsn(downstream)
    while get_lsn(downstream) == lsn_before:
        sleep(1)
    up_count = query(upstream, f"SELECT COUNT(*) FROM {table}")
    down_count = query(downstream, f"SELECT COUNT(*) FROM {table}")
    assert up_count == down_count
```

#### 2.7 索引表一致性检查
```
流程:
1. 从mo_tables获取table_id
   SELECT rel_id FROM mo_catalog.mo_tables 
   WHERE reldatabase='{db}' AND relname='{table}'

2. 从mo_indexes获取所有索引信息
   SELECT name, type, algo, algo_params, index_table_name 
   FROM mo_catalog.mo_indexes 
   WHERE table_id = {table_id}

3. 对每个索引，按(name, type, algo)匹配上下游
   上游索引: {name: 'idx_vec', algo: 'ivfflat', index_table_name: '__mo_idx_tbl_001'}
   下游索引: {name: 'idx_vec', algo: 'ivfflat', index_table_name: '__mo_idx_tbl_002'}
   
4. 检查索引表数据一致性
   上游: SELECT COUNT(*), checksum(*) FROM {up_index_table_name}
   下游: SELECT COUNT(*), checksum(*) FROM {down_index_table_name}
   断言: count和checksum一致
```

```python
def verify_index_tables(upstream, downstream, db, table):
    # 1. 获取上游table_id和索引
    up_table_id = query(upstream, f"""
        SELECT rel_id FROM mo_catalog.mo_tables 
        WHERE reldatabase='{db}' AND relname='{table}'
    """)[0][0]
    
    up_indexes = query(upstream, f"""
        SELECT name, type, algo, algo_params, index_table_name 
        FROM mo_catalog.mo_indexes WHERE table_id = {up_table_id}
    """)
    
    # 2. 获取下游table_id和索引
    down_table_id = query(downstream, f"""
        SELECT rel_id FROM mo_catalog.mo_tables 
        WHERE reldatabase='{db}' AND relname='{table}'
    """)[0][0]
    
    down_indexes = query(downstream, f"""
        SELECT name, type, algo, algo_params, index_table_name 
        FROM mo_catalog.mo_indexes WHERE table_id = {down_table_id}
    """)
    
    # 3. 按(name, type, algo)匹配
    for up_idx in up_indexes:
        name, type, algo = up_idx[0], up_idx[1], up_idx[2]
        up_idx_table = up_idx[4]
        
        # 找下游对应索引
        down_idx = find_by_key(down_indexes, name, type, algo)
        down_idx_table = down_idx[4]
        
        # 4. 检查索引表数据
        up_count = query(upstream, f"SELECT COUNT(*) FROM `{db}`.`{up_idx_table}`")[0][0]
        down_count = query(downstream, f"SELECT COUNT(*) FROM `{db}`.`{down_idx_table}`")[0][0]
        assert up_count == down_count, f"Index {name} count mismatch"
```

索引类型对应的索引表:
| 索引类型 | 索引表数量 | 说明 |
|----------|-----------|------|
| 普通/唯一索引 | 1 | 单个隐藏表 |
| FULLTEXT | 多个 | 倒排索引表 |
| IVFFLAT | 3个 | metadata/centroids/entries |
| MASTER | 1 | 主索引表 |

### 3. ALTER TABLE测试

#### 3.1 Inplace ALTER（原地修改，不重建表）
1. ADD INDEX -> `CREATE INDEX idx ON t(col)`
2. ADD UNIQUE INDEX -> `CREATE UNIQUE INDEX uidx ON t(col)`
3. ADD FULLTEXT INDEX -> `CREATE FULLTEXT INDEX ftidx ON t(col)`
4. DROP INDEX -> `DROP INDEX idx ON t`
5. ALTER INDEX VISIBILITY -> `ALTER TABLE t ALTER INDEX idx INVISIBLE`
6. ADD FOREIGN KEY -> `ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY...`
7. DROP FOREIGN KEY -> `ALTER TABLE t DROP FOREIGN KEY fk`
8. RENAME COLUMN -> `ALTER TABLE t RENAME COLUMN old TO new`
9. CHANGE COMMENT -> `ALTER TABLE t COMMENT = 'xxx'`

#### 3.2 Non-inplace ALTER（DROP+CREATE重建表）
1. ADD COLUMN -> `ALTER TABLE t ADD COLUMN col INT`
2. DROP COLUMN -> `ALTER TABLE t DROP COLUMN col`
3. MODIFY COLUMN -> `ALTER TABLE t MODIFY COLUMN col VARCHAR(200)`

### 4. 控制操作测试
1. PAUSE CCPR SUBSCRIPTION -> 验证state=2
2. 上游INSERT -> 验证下游不同步
3. RESUME CCPR SUBSCRIPTION -> 验证state=0, 数据同步
4. DROP CCPR SUBSCRIPTION -> 验证订阅删除

## 二、长时间测试 (Long Test, 8小时)

对account/db/table三种level分别执行：

### 阶段流程 (每个level)
```
Phase 1 (1-2h): 持续DML (insert/update/delete混合)
    ↓
[检查点] PAUSE -> 检查一致性 (用snapshot) -> RESUME
    ↓
ALTER: ADD COLUMN (non-inplace)
    ↓
Phase 2 (1h): 持续DML
    ↓
[检查点] PAUSE -> 检查一致性 -> RESUME
    ↓
ALTER: ADD INDEX (inplace)
    ↓
Phase 3 (1h): 持续DML
    ↓
[检查点] PAUSE -> 检查一致性 -> RESUME
    ↓
ALTER: ADD FULLTEXT INDEX (inplace)
    ↓
Phase 4 (1h): 持续DML
    ↓
[检查点] PAUSE -> 检查一致性 -> RESUME
    ↓
ALTER: RENAME COLUMN (inplace)
    ↓
Phase 5 (1h): 持续DML
    ↓
[检查点] PAUSE -> 检查一致性 -> RESUME
    ↓
ALTER: DROP COLUMN (non-inplace)
    ↓
Phase 6 (1h): 持续DML + 大批量LOAD
    ↓
ALTER: CHANGE COMMENT (inplace)
    ↓
[最终检查] PAUSE -> 检查一致性 -> 结束
```

### 每个检查点执行
1. `PAUSE CCPR SUBSCRIPTION {pub_name}`
2. `sleep 10s`
3. 查询: `SELECT task_id, iteration_lsn, watermark, error_message FROM mo_ccpr_log WHERE ...`
4. 断言: `now() - watermark <= 10min`
5. 断言: `error_message IS NULL`
6. 上游: `SELECT * FROM db.table {SNAPSHOT = 'ccpr_{task_id}_{lsn}'}`
7. 下游: `SELECT * FROM db.table`
8. 对比COUNT和抽样数据
9. `RESUME CCPR SUBSCRIPTION {pub_name}`

### ALTER类型全覆盖

#### Inplace ALTER (9种)
| # | 操作 | SQL |
|---|------|-----|
| 1 | ADD INDEX | `CREATE INDEX idx ON t(col)` |
| 2 | ADD UNIQUE INDEX | `CREATE UNIQUE INDEX uidx ON t(col)` |
| 3 | ADD FULLTEXT INDEX | `CREATE FULLTEXT INDEX ftidx ON t(col)` |
| 4 | DROP INDEX | `DROP INDEX idx ON t` |
| 5 | ALTER INDEX VISIBILITY | `ALTER TABLE t ALTER INDEX idx INVISIBLE` |
| 6 | ADD FOREIGN KEY | `ALTER TABLE t ADD CONSTRAINT fk...` |
| 7 | DROP FOREIGN KEY | `ALTER TABLE t DROP FOREIGN KEY fk` |
| 8 | RENAME COLUMN | `ALTER TABLE t RENAME COLUMN old TO new` |
| 9 | CHANGE COMMENT | `ALTER TABLE t COMMENT = 'xxx'` |

#### Non-inplace ALTER (3种)
| # | 操作 | SQL |
|---|------|-----|
| 1 | ADD COLUMN | `ALTER TABLE t ADD COLUMN col INT` |
| 2 | DROP COLUMN | `ALTER TABLE t DROP COLUMN col` |
| 3 | MODIFY COLUMN | `ALTER TABLE t MODIFY col VARCHAR(200)` |
