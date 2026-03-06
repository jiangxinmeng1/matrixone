-- ============================================================================
-- CCPR Memory Control Test Script
-- 测试大数据量DML操作和各种索引创建对CCPR内存的影响
-- 数据源: sample_1m.csv (本地文件，约100万行)
-- ============================================================================

-- ============================================================================
-- Part 0: 环境准备
-- ============================================================================

-- 创建测试数据库
DROP DATABASE IF EXISTS ccpr_memory_test;
CREATE DATABASE ccpr_memory_test;
USE ccpr_memory_test;

-- 创建源表
CREATE TABLE IF NOT EXISTS ca_comprehensive_dataset (
    md5_id varchar(255) NOT NULL,
    question text DEFAULT NULL,
    answer json DEFAULT NULL,
    source_type varchar(255) DEFAULT NULL,
    content_type varchar(255) DEFAULT NULL,
    keyword varchar(255) DEFAULT NULL,
    question_vector vecf32(1024) DEFAULT NULL COMMENT '摘要的向量集',
    allow_access varchar(511) DEFAULT NULL,
    allow_identities varchar(512) DEFAULT NULL,
    delete_flag int DEFAULT NULL,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP(),
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP(),
    PRIMARY KEY (md5_id),
    KEY idx_comprehensive_allow_access (allow_access),
    KEY idx_comprehensive_allow_identities (allow_identities),
    KEY idx_comprehensive_content_type (content_type)
);

-- 加载本地数据文件
LOAD DATA INFILE './sample_1m.csv' 
INTO TABLE ca_comprehensive_dataset 
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

-- 查看表行数
SELECT COUNT(*) as total_rows FROM ca_comprehensive_dataset;

-- ============================================================================
-- Part 1: 大数据量DML测试
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1.1 大批量INSERT测试
-- -----------------------------------------------------------------------------

-- 创建测试表用于INSERT测试
CREATE TABLE test_insert_100k LIKE ca_comprehensive_dataset;
CREATE TABLE test_insert_500k LIKE ca_comprehensive_dataset;
CREATE TABLE test_insert_all LIKE ca_comprehensive_dataset;

-- 插入10万行
SELECT NOW() as start_time, 'INSERT 100K rows' as operation;
INSERT INTO test_insert_100k 
SELECT * FROM ca_comprehensive_dataset LIMIT 100000;
SELECT NOW() as end_time, 'INSERT 100K rows completed' as operation;
SELECT COUNT(*) as inserted_rows FROM test_insert_100k;

-- 插入50万行
SELECT NOW() as start_time, 'INSERT 500K rows' as operation;
INSERT INTO test_insert_500k 
SELECT * FROM ca_comprehensive_dataset LIMIT 500000;
SELECT NOW() as end_time, 'INSERT 500K rows completed' as operation;
SELECT COUNT(*) as inserted_rows FROM test_insert_500k;

-- 插入全表
SELECT NOW() as start_time, 'INSERT ALL rows' as operation;
INSERT INTO test_insert_all 
SELECT * FROM ca_comprehensive_dataset;
SELECT NOW() as end_time, 'INSERT ALL rows completed' as operation;
SELECT COUNT(*) as inserted_rows FROM test_insert_all;

-- -----------------------------------------------------------------------------
-- 1.2 大批量UPDATE测试
-- -----------------------------------------------------------------------------

-- 创建UPDATE测试表
CREATE TABLE test_update_large LIKE ca_comprehensive_dataset;
INSERT INTO test_update_large SELECT * FROM ca_comprehensive_dataset;

SELECT COUNT(*) as update_table_rows FROM test_update_large;

-- 更新10万行
SELECT NOW() as start_time, 'UPDATE 100K rows' as operation;
UPDATE test_update_large 
SET keyword = CONCAT(keyword, '_updated'), 
    delete_flag = COALESCE(delete_flag, 0) + 1,
    updated_at = NOW()
WHERE md5_id IN (SELECT md5_id FROM test_update_large LIMIT 100000);
SELECT NOW() as end_time, 'UPDATE 100K rows completed' as operation;

-- 更新所有行的单列
SELECT NOW() as start_time, 'UPDATE all rows single column' as operation;
UPDATE test_update_large SET delete_flag = 0;
SELECT NOW() as end_time, 'UPDATE all rows completed' as operation;

-- -----------------------------------------------------------------------------
-- 1.3 大批量DELETE测试
-- -----------------------------------------------------------------------------

-- 创建DELETE测试表
CREATE TABLE test_delete_large LIKE ca_comprehensive_dataset;
INSERT INTO test_delete_large SELECT * FROM ca_comprehensive_dataset;

SELECT COUNT(*) as before_delete FROM test_delete_large;

-- 删除10万行
SELECT NOW() as start_time, 'DELETE 100K rows' as operation;
DELETE FROM test_delete_large 
WHERE md5_id IN (SELECT md5_id FROM test_delete_large LIMIT 100000);
SELECT NOW() as end_time, 'DELETE 100K rows completed' as operation;
SELECT COUNT(*) as after_delete FROM test_delete_large;

-- 删除剩余所有行
SELECT NOW() as start_time, 'DELETE remaining rows' as operation;
DELETE FROM test_delete_large;
SELECT NOW() as end_time, 'DELETE remaining completed' as operation;
SELECT COUNT(*) as final_count FROM test_delete_large;

-- -----------------------------------------------------------------------------
-- 1.4 CTAS (Create Table As Select) 测试
-- -----------------------------------------------------------------------------

SELECT NOW() as start_time, 'CTAS 500K rows' as operation;
CREATE TABLE test_ctas_500k AS 
SELECT * FROM ca_comprehensive_dataset LIMIT 500000;
SELECT NOW() as end_time, 'CTAS 500K rows completed' as operation;
SELECT COUNT(*) as ctas_rows FROM test_ctas_500k;

SELECT NOW() as start_time, 'CTAS ALL rows with transformation' as operation;
CREATE TABLE test_ctas_transform AS 
SELECT 
    md5_id,
    UPPER(question) as question_upper,
    source_type,
    content_type,
    keyword,
    question_vector,
    allow_access,
    delete_flag,
    created_at
FROM ca_comprehensive_dataset;
SELECT NOW() as end_time, 'CTAS transform completed' as operation;
SELECT COUNT(*) as ctas_transform_rows FROM test_ctas_transform;

-- ============================================================================
-- Part 2: 向量索引测试 (IVF-FLAT)
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 2.1 同步IVF索引创建
-- -----------------------------------------------------------------------------

-- 准备向量索引测试表 (10万行)
CREATE TABLE test_vector_ivf_sync_100k LIKE ca_comprehensive_dataset;
INSERT INTO test_vector_ivf_sync_100k 
SELECT * FROM ca_comprehensive_dataset 
WHERE question_vector IS NOT NULL 
LIMIT 100000;

SELECT COUNT(*) as vector_rows FROM test_vector_ivf_sync_100k WHERE question_vector IS NOT NULL;

-- 创建同步IVF-FLAT索引 (小规模)
SELECT NOW() as start_time, 'CREATE SYNC IVF INDEX (lists=100) on 100K rows' as operation;
CREATE INDEX idx_vector_ivf_sync_100 
ON test_vector_ivf_sync_100k(question_vector) 
USING IVFFLAT LISTS 100 OP_TYPE 'vector_l2_ops';
SELECT NOW() as end_time, 'SYNC IVF INDEX (lists=100) completed' as operation;

-- 查看索引状态
SHOW INDEX FROM test_vector_ivf_sync_100k;

-- 准备向量索引测试表 (50万行)
CREATE TABLE test_vector_ivf_sync_500k LIKE ca_comprehensive_dataset;
INSERT INTO test_vector_ivf_sync_500k 
SELECT * FROM ca_comprehensive_dataset 
WHERE question_vector IS NOT NULL 
LIMIT 500000;

SELECT NOW() as start_time, 'CREATE SYNC IVF INDEX (lists=256) on 500K rows' as operation;
CREATE INDEX idx_vector_ivf_sync_256 
ON test_vector_ivf_sync_500k(question_vector) 
USING IVFFLAT LISTS 256 OP_TYPE 'vector_l2_ops';
SELECT NOW() as end_time, 'SYNC IVF INDEX (lists=256) completed' as operation;

-- -----------------------------------------------------------------------------
-- 2.2 异步IVF索引创建
-- -----------------------------------------------------------------------------

-- 准备异步向量索引测试表 (全表)
CREATE TABLE test_vector_ivf_async LIKE ca_comprehensive_dataset;
INSERT INTO test_vector_ivf_async 
SELECT * FROM ca_comprehensive_dataset 
WHERE question_vector IS NOT NULL;

SELECT COUNT(*) as async_vector_rows FROM test_vector_ivf_async WHERE question_vector IS NOT NULL;

-- 创建异步IVF-FLAT索引
SELECT NOW() as start_time, 'CREATE ASYNC IVF INDEX (lists=512)' as operation;
CREATE INDEX idx_vector_ivf_async_512 
ON test_vector_ivf_async(question_vector) 
USING IVFFLAT LISTS 512 OP_TYPE 'vector_l2_ops' ASYNC;
SELECT NOW() as end_time, 'ASYNC IVF INDEX submitted' as operation;

-- 检查异步索引创建状态
SELECT SLEEP(5);
SHOW INDEX FROM test_vector_ivf_async;

-- 另一个异步IVF索引 (不同参数)
CREATE TABLE test_vector_ivf_async_2 LIKE ca_comprehensive_dataset;
INSERT INTO test_vector_ivf_async_2 
SELECT * FROM ca_comprehensive_dataset 
WHERE question_vector IS NOT NULL;

SELECT NOW() as start_time, 'CREATE ASYNC IVF INDEX (lists=1024)' as operation;
CREATE INDEX idx_vector_ivf_async_1024 
ON test_vector_ivf_async_2(question_vector) 
USING IVFFLAT LISTS 1024 OP_TYPE 'vector_l2_ops' ASYNC;
SELECT NOW() as end_time, 'ASYNC IVF INDEX (lists=1024) submitted' as operation;

-- ============================================================================
-- Part 3: HNSW向量索引测试
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 3.1 同步HNSW索引创建
-- -----------------------------------------------------------------------------

CREATE TABLE test_hnsw_sync_50k LIKE ca_comprehensive_dataset;
INSERT INTO test_hnsw_sync_50k 
SELECT * FROM ca_comprehensive_dataset 
WHERE question_vector IS NOT NULL 
LIMIT 50000;

SELECT NOW() as start_time, 'CREATE SYNC HNSW INDEX on 50K rows' as operation;
CREATE INDEX idx_hnsw_sync 
ON test_hnsw_sync_50k(question_vector) 
USING HNSW M 16 EF_CONSTRUCTION 64 OP_TYPE 'vector_l2_ops';
SELECT NOW() as end_time, 'SYNC HNSW INDEX completed' as operation;

SHOW INDEX FROM test_hnsw_sync_50k;

-- 中等规模同步HNSW
CREATE TABLE test_hnsw_sync_200k LIKE ca_comprehensive_dataset;
INSERT INTO test_hnsw_sync_200k 
SELECT * FROM ca_comprehensive_dataset 
WHERE question_vector IS NOT NULL 
LIMIT 200000;

SELECT NOW() as start_time, 'CREATE SYNC HNSW INDEX on 200K rows' as operation;
CREATE INDEX idx_hnsw_sync_200k 
ON test_hnsw_sync_200k(question_vector) 
USING HNSW M 24 EF_CONSTRUCTION 100 OP_TYPE 'vector_l2_ops';
SELECT NOW() as end_time, 'SYNC HNSW INDEX 200K completed' as operation;

-- -----------------------------------------------------------------------------
-- 3.2 异步HNSW索引创建
-- -----------------------------------------------------------------------------

CREATE TABLE test_hnsw_async LIKE ca_comprehensive_dataset;
INSERT INTO test_hnsw_async 
SELECT * FROM ca_comprehensive_dataset 
WHERE question_vector IS NOT NULL 
LIMIT 500000;

SELECT NOW() as start_time, 'CREATE ASYNC HNSW INDEX on 500K rows' as operation;
CREATE INDEX idx_hnsw_async 
ON test_hnsw_async(question_vector) 
USING HNSW M 32 EF_CONSTRUCTION 128 OP_TYPE 'vector_l2_ops' ASYNC;
SELECT NOW() as end_time, 'ASYNC HNSW INDEX submitted' as operation;

-- 全表异步HNSW索引
CREATE TABLE test_hnsw_async_all LIKE ca_comprehensive_dataset;
INSERT INTO test_hnsw_async_all 
SELECT * FROM ca_comprehensive_dataset 
WHERE question_vector IS NOT NULL;

SELECT NOW() as start_time, 'CREATE ASYNC HNSW INDEX on ALL rows' as operation;
CREATE INDEX idx_hnsw_async_all 
ON test_hnsw_async_all(question_vector) 
USING HNSW M 48 EF_CONSTRUCTION 200 OP_TYPE 'vector_l2_ops' ASYNC;
SELECT NOW() as end_time, 'ASYNC HNSW INDEX ALL submitted' as operation;

-- ============================================================================
-- Part 4: 全文索引测试
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 4.1 全文索引创建
-- -----------------------------------------------------------------------------

-- 准备全文索引测试表 (10万行)
CREATE TABLE test_fulltext_100k LIKE ca_comprehensive_dataset;
INSERT INTO test_fulltext_100k 
SELECT * FROM ca_comprehensive_dataset LIMIT 100000;

-- 创建全文索引 (question列)
SELECT NOW() as start_time, 'CREATE FULLTEXT INDEX on 100K rows' as operation;
CREATE FULLTEXT INDEX idx_fulltext_question ON test_fulltext_100k(question);
SELECT NOW() as end_time, 'FULLTEXT INDEX on 100K completed' as operation;

SHOW INDEX FROM test_fulltext_100k;

-- 中等规模全文索引 (50万行)
CREATE TABLE test_fulltext_500k LIKE ca_comprehensive_dataset;
INSERT INTO test_fulltext_500k 
SELECT * FROM ca_comprehensive_dataset LIMIT 500000;

SELECT NOW() as start_time, 'CREATE FULLTEXT INDEX on 500K rows' as operation;
CREATE FULLTEXT INDEX idx_fulltext_question_500k ON test_fulltext_500k(question);
SELECT NOW() as end_time, 'FULLTEXT INDEX on 500K completed' as operation;

-- 全表全文索引
CREATE TABLE test_fulltext_all LIKE ca_comprehensive_dataset;
INSERT INTO test_fulltext_all 
SELECT * FROM ca_comprehensive_dataset;

SELECT NOW() as start_time, 'CREATE FULLTEXT INDEX on ALL rows' as operation;
CREATE FULLTEXT INDEX idx_fulltext_question_all ON test_fulltext_all(question);
SELECT NOW() as end_time, 'FULLTEXT INDEX on ALL completed' as operation;

-- ============================================================================
-- Part 5: 复合索引测试
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 5.1 普通B树索引
-- -----------------------------------------------------------------------------

CREATE TABLE test_btree_index LIKE ca_comprehensive_dataset;
INSERT INTO test_btree_index 
SELECT * FROM ca_comprehensive_dataset;

SELECT COUNT(*) as btree_table_rows FROM test_btree_index;

-- 单列索引
SELECT NOW() as start_time, 'CREATE BTREE INDEX single column' as operation;
CREATE INDEX idx_keyword ON test_btree_index(keyword);
SELECT NOW() as end_time, 'BTREE INDEX single completed' as operation;

-- 复合索引
SELECT NOW() as start_time, 'CREATE BTREE INDEX composite' as operation;
CREATE INDEX idx_composite ON test_btree_index(source_type, content_type, delete_flag);
SELECT NOW() as end_time, 'BTREE INDEX composite completed' as operation;

-- 唯一索引
SELECT NOW() as start_time, 'CREATE UNIQUE INDEX' as operation;
CREATE UNIQUE INDEX idx_unique_md5 ON test_btree_index(md5_id, source_type);
SELECT NOW() as end_time, 'UNIQUE INDEX completed' as operation;

SHOW INDEX FROM test_btree_index;

-- ============================================================================
-- Part 6: 并发DML与索引创建混合测试
-- ============================================================================

-- 准备测试表
CREATE TABLE test_concurrent LIKE ca_comprehensive_dataset;
INSERT INTO test_concurrent 
SELECT * FROM ca_comprehensive_dataset 
WHERE question_vector IS NOT NULL 
LIMIT 200000;

SELECT COUNT(*) as concurrent_table_rows FROM test_concurrent;

-- 在创建索引的同时进行DML操作
-- 注意：需要在多个会话中并行执行

-- Session 1: 创建向量索引
SELECT NOW() as start_time, 'Concurrent test: CREATE VECTOR INDEX' as operation;
CREATE INDEX idx_concurrent_vector 
ON test_concurrent(question_vector) 
USING IVFFLAT LISTS 256 OP_TYPE 'vector_l2_ops' ASYNC;

-- Session 2: 同时进行INSERT (在另一个会话执行)
-- INSERT INTO test_concurrent SELECT * FROM ca_comprehensive_dataset LIMIT 10000;

-- Session 3: 同时进行UPDATE (在另一个会话执行)
-- UPDATE test_concurrent SET delete_flag = 1 WHERE delete_flag IS NULL LIMIT 5000;

-- ============================================================================
-- Part 7: 清理与统计
-- ============================================================================

-- 查看所有测试表
SHOW TABLES;

-- 查看各表大小
SELECT 
    table_name,
    table_rows,
    ROUND(data_length/1024/1024, 2) as data_mb,
    ROUND(index_length/1024/1024, 2) as index_mb
FROM information_schema.tables 
WHERE table_schema = 'ccpr_memory_test'
ORDER BY table_rows DESC;

-- 检查异步索引状态
SELECT SLEEP(10);
SHOW INDEX FROM test_vector_ivf_async;
SHOW INDEX FROM test_vector_ivf_async_2;
SHOW INDEX FROM test_hnsw_async;
SHOW INDEX FROM test_hnsw_async_all;

-- ============================================================================
-- Part 8: 可选清理
-- ============================================================================

-- 清理测试数据（谨慎执行）
-- DROP DATABASE IF EXISTS ccpr_memory_test;

SELECT NOW() as test_completed, 'All tests completed' as status;
