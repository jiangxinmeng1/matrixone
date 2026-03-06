#!/usr/bin/env python3
"""
CCPR Memory Test - 带订阅和一致性检查的大数据量测试

功能:
1. 在上游创建 publication，在下游创建 subscription
2. 对上游表执行各种大数据量 DML 和 DDL
3. 监控 watermark 变化，变化时检查上下游行数
4. 如果 error_message 是 nonretryable，停止脚本

用法:
    python ccpr_memory_test.py
    python ccpr_memory_test.py --duration 3600  # 1小时测试
"""

import os
import sys
import time
import logging
import argparse
import threading
import random
import pymysql
from datetime import datetime
from typing import Optional, Dict, Any, Tuple, List
from dataclasses import dataclass

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ccpr_memory_test.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# 配置
# =============================================================================

@dataclass
class ClusterConfig:
    host: str = "127.0.0.1"
    port: int = 6001
    user: str = "dump"
    password: str = "111"


@dataclass
class TestConfig:
    # 集群配置
    upstream: ClusterConfig = None
    downstream: ClusterConfig = None
    
    # 测试配置
    pub_name: str = "pub_memory_test"
    db_name: str = "ccpr_memory_test"
    table_name: str = "ca_comprehensive_dataset"
    data_file: str = "./sample_1m.csv"
    
    # Watermark 检查间隔
    watermark_check_interval: float = 5.0
    
    def __post_init__(self):
        if self.upstream is None:
            self.upstream = ClusterConfig(port=6001)
        if self.downstream is None:
            self.downstream = ClusterConfig(port=6002)


# =============================================================================
# 数据库工具
# =============================================================================

def get_connection(config: ClusterConfig) -> pymysql.Connection:
    """获取数据库连接"""
    return pymysql.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        autocommit=True,
        connect_timeout=30,
        read_timeout=3600,
        write_timeout=3600,
    )


def execute_sql(conn: pymysql.Connection, sql: str, fetch: bool = False) -> Optional[list]:
    """执行SQL"""
    with conn.cursor() as cursor:
        cursor.execute(sql)
        if fetch:
            return cursor.fetchall()
    return None


def execute_sql_safe(conn: pymysql.Connection, sql: str, fetch: bool = False) -> Tuple[bool, Optional[list], Optional[str]]:
    """安全执行SQL，返回 (success, result, error)"""
    try:
        result = execute_sql(conn, sql, fetch)
        return True, result, None
    except Exception as e:
        return False, None, str(e)


def get_table_count(conn: pymysql.Connection, db: str, table: str) -> int:
    """获取表行数"""
    result = execute_sql(conn, f"SELECT COUNT(*) FROM {db}.{table}", fetch=True)
    return result[0][0] if result else 0


# =============================================================================
# CCPR 测试类
# =============================================================================

class CCPRMemoryTest:
    def __init__(self, config: TestConfig):
        self.config = config
        self.upstream_conn: Optional[pymysql.Connection] = None
        self.downstream_conn: Optional[pymysql.Connection] = None
        self.stop_event = threading.Event()
        self.last_watermark: int = 0
        self.task_id: Optional[str] = None
        self.errors: List[str] = []
        self.watermark_thread: Optional[threading.Thread] = None
        self.dml_thread: Optional[threading.Thread] = None
        
        # DML/DDL 配置
        self.dml_interval: float = 0.5  # DML 操作间隔（秒）
        self.ddl_interval: int = 120    # DDL 操作间隔（秒，2分钟）
        self.test_duration: int = 3600  # 测试时长（秒，1小时）
        
        # 批量操作大小
        self.batch_insert_size: int = 10000   # 每次批量插入行数
        self.batch_update_size: int = 50000   # 每次批量更新行数
        self.batch_delete_size: int = 10000   # 每次批量删除行数
        
        # PK 追踪
        self.next_pk: int = 1
        self.pk_lock = threading.Lock()
        
        # 添加的列和索引追踪
        self.added_columns: List[str] = []
        self.added_indexes: List[str] = []
        
        # DDL 阶段计数
        self.ddl_phase: int = 0
        
    def connect(self) -> bool:
        """连接到上下游集群"""
        try:
            logger.info(f"Connecting to upstream: {self.config.upstream.host}:{self.config.upstream.port}")
            self.upstream_conn = get_connection(self.config.upstream)
            
            logger.info(f"Connecting to downstream: {self.config.downstream.host}:{self.config.downstream.port}")
            self.downstream_conn = get_connection(self.config.downstream)
            
            logger.info("Connected to both clusters")
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    def setup_ccpr(self) -> bool:
        """设置 CCPR: 创建数据库、表、Publication、Subscription"""
        try:
            db = self.config.db_name
            table = self.config.table_name
            pub = self.config.pub_name
            
            # 1. 在上游创建数据库和表
            logger.info(f"[Upstream] Creating database: {db}")
            execute_sql(self.upstream_conn, f"DROP DATABASE IF EXISTS {db}")
            execute_sql(self.upstream_conn, f"CREATE DATABASE {db}")
            
            logger.info(f"[Upstream] Creating table: {table}")
            create_table_sql = f"""
                CREATE TABLE {db}.{table} (
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
                )
            """
            execute_sql(self.upstream_conn, create_table_sql)
            
            # 2. 加载数据
            if os.path.exists(self.config.data_file):
                logger.info(f"[Upstream] Loading data from: {self.config.data_file}")
                # 使用绝对路径
                abs_path = os.path.abspath(self.config.data_file)
                load_sql = f"""
                    LOAD DATA INFILE '{abs_path}'
                    INTO TABLE {db}.{table}
                    FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\\n'
                    IGNORE 1 LINES
                """
                execute_sql(self.upstream_conn, load_sql)
                
                count = get_table_count(self.upstream_conn, db, table)
                logger.info(f"[Upstream] Loaded {count} rows")
            else:
                logger.warning(f"Data file not found: {self.config.data_file}")
                logger.info("[Upstream] Inserting sample data...")
                # 插入一些测试数据
                for i in range(1000):
                    execute_sql(self.upstream_conn, f"""
                        INSERT INTO {db}.{table} (md5_id, question, source_type, content_type, keyword)
                        VALUES ('md5_{i}', 'Question {i}', 'type_a', 'content_a', 'keyword_{i}')
                    """)
                logger.info("[Upstream] Inserted 1000 sample rows")
            
            # 3. 创建 Publication
            logger.info(f"[Upstream] Creating publication: {pub}")
            execute_sql_safe(self.upstream_conn, f"DROP PUBLICATION IF EXISTS {pub}")
            execute_sql(self.upstream_conn, 
                f"CREATE PUBLICATION {pub} DATABASE {db} TABLE {table} ACCOUNT ALL")
            
            # 4. 在下游创建 Subscription (DATABASE FROM)
            logger.info(f"[Downstream] Creating subscription...")
            execute_sql_safe(self.downstream_conn, f"DROP DATABASE IF EXISTS {db}")
            
            # 使用 sys 账户连接字符串
            conn_str = f"mysql://root:{self.config.upstream.password}@{self.config.upstream.host}:{self.config.upstream.port}"
            sub_sql = f"CREATE DATABASE {db} FROM '{conn_str}' PUBLICATION {pub}"
            execute_sql(self.downstream_conn, sub_sql)
            
            logger.info("[Downstream] Subscription created, waiting for initial sync...")
            time.sleep(10)
            
            # 5. 获取 task_id
            self._update_task_id()
            
            # 6. 检查初始同步
            up_count = get_table_count(self.upstream_conn, db, table)
            down_count = get_table_count(self.downstream_conn, db, table)
            logger.info(f"Initial sync check: upstream={up_count}, downstream={down_count}")
            
            return True
            
        except Exception as e:
            logger.error(f"Setup CCPR failed: {e}")
            self.errors.append(str(e))
            return False
    
    def _update_task_id(self):
        """更新 task_id"""
        try:
            result = execute_sql(self.downstream_conn,
                f"SELECT task_id FROM mo_catalog.mo_ccpr_log WHERE subscription_name='{self.config.pub_name}' AND drop_at IS NULL",
                fetch=True)
            if result and result[0][0]:
                self.task_id = result[0][0]
                logger.info(f"Task ID: {self.task_id}")
        except Exception as e:
            logger.warning(f"Failed to get task_id: {e}")
    
    def _check_ccpr_status(self) -> Tuple[bool, Dict[str, Any]]:
        """
        检查 CCPR 状态
        返回: (is_ok, status_dict)
        如果 error_message 包含 nonretryable，返回 is_ok=False
        """
        try:
            result = execute_sql(self.downstream_conn,
                f"""SELECT task_id, watermark, error_message, state 
                    FROM mo_catalog.mo_ccpr_log 
                    WHERE subscription_name='{self.config.pub_name}' AND drop_at IS NULL""",
                fetch=True)
            
            if not result:
                return True, {"error": "No CCPR task found"}
            
            row = result[0]
            status = {
                "task_id": row[0],
                "watermark": row[1] or 0,
                "error_message": row[2],
                "state": row[3],
            }
            
            # 检查 nonretryable 错误
            if status["error_message"]:
                error_lower = status["error_message"].lower()
                if "nonretryable" in error_lower or "non-retryable" in error_lower or "non retryable" in error_lower:
                    logger.error(f"NONRETRYABLE ERROR DETECTED: {status['error_message']}")
                    return False, status
            
            return True, status
            
        except Exception as e:
            return True, {"error": str(e)}
    
    def _check_row_counts(self) -> Tuple[int, int]:
        """检查上下游行数"""
        db = self.config.db_name
        table = self.config.table_name
        
        try:
            up_count = get_table_count(self.upstream_conn, db, table)
        except:
            up_count = -1
        
        try:
            down_count = get_table_count(self.downstream_conn, db, table)
        except:
            down_count = -1
        
        return up_count, down_count
    
    def _watermark_monitor(self):
        """Watermark 监控线程"""
        logger.info("[WatermarkMonitor] Started")
        
        while not self.stop_event.is_set():
            try:
                # 检查 CCPR 状态
                is_ok, status = self._check_ccpr_status()
                
                if not is_ok:
                    logger.error(f"[WatermarkMonitor] Nonretryable error detected!")
                    logger.error(f"[WatermarkMonitor] Error: {status.get('error_message')}")
                    self.errors.append(f"Nonretryable error: {status.get('error_message')}")
                    self.stop_event.set()
                    break
                
                current_watermark = status.get("watermark", 0)
                
                # 检查 watermark 变化
                if current_watermark != self.last_watermark and self.last_watermark != 0:
                    logger.info(f"[WatermarkMonitor] Watermark changed: {self.last_watermark} -> {current_watermark}")
                    
                    # 检查上下游行数
                    up_count, down_count = self._check_row_counts()
                    logger.info(f"[WatermarkMonitor] Row counts: upstream={up_count}, downstream={down_count}")
                    
                    if up_count >= 0 and down_count >= 0:
                        diff = abs(up_count - down_count)
                        if diff > 0:
                            logger.warning(f"[WatermarkMonitor] Row count difference: {diff}")
                
                self.last_watermark = current_watermark
                
            except Exception as e:
                logger.warning(f"[WatermarkMonitor] Error: {e}")
            
            # 等待检查间隔
            for _ in range(int(self.config.watermark_check_interval * 10)):
                if self.stop_event.is_set():
                    break
                time.sleep(0.1)
        
        logger.info("[WatermarkMonitor] Stopped")
    
    def start_watermark_monitor(self):
        """启动 watermark 监控"""
        self.watermark_thread = threading.Thread(target=self._watermark_monitor, daemon=True)
        self.watermark_thread.start()
        logger.info("Watermark monitor started")
    
    def stop_watermark_monitor(self):
        """停止 watermark 监控"""
        self.stop_event.set()
        if self.watermark_thread:
            self.watermark_thread.join(timeout=10)
        logger.info("Watermark monitor stopped")
    
    # =========================================================================
    # 大批量 DML 操作
    # =========================================================================
    
    def _batch_insert(self, count: int) -> int:
        """批量插入数据，返回实际插入行数"""
        db = self.config.db_name
        table = self.config.table_name
        
        with self.pk_lock:
            start_pk = self.next_pk
            self.next_pk += count
        
        # 使用 INSERT ... SELECT 生成数据
        # 每次生成 count 行数据
        inserted = 0
        batch_size = min(count, 5000)  # 每批最多5000行
        
        for batch_start in range(0, count, batch_size):
            batch_count = min(batch_size, count - batch_start)
            current_pk = start_pk + batch_start
            
            # 生成批量插入的 VALUES
            values_list = []
            for i in range(batch_count):
                pk = current_pk + i
                values_list.append(f"""(
                    'gen_md5_{pk}',
                    'Generated question {pk}',
                    NULL,
                    'gen_type',
                    'gen_content',
                    'keyword_{pk % 1000}',
                    NULL,
                    'access_{pk % 100}',
                    'identity_{pk % 50}',
                    {pk % 2},
                    NOW(),
                    NOW()
                )""")
            
            sql = f"""
                INSERT INTO {db}.{table} 
                (md5_id, question, answer, source_type, content_type, keyword, 
                 question_vector, allow_access, allow_identities, delete_flag, 
                 created_at, updated_at)
                VALUES {','.join(values_list)}
            """
            
            try:
                execute_sql(self.upstream_conn, sql)
                inserted += batch_count
            except Exception as e:
                logger.warning(f"Batch insert failed: {e}")
                break
        
        return inserted
    
    def _batch_update(self, count: int) -> int:
        """批量更新数据，返回实际更新行数"""
        db = self.config.db_name
        table = self.config.table_name
        
        try:
            # 更新随机的 count 行
            sql = f"""
                UPDATE {db}.{table}
                SET keyword = CONCAT(COALESCE(keyword, ''), '_upd'),
                    delete_flag = COALESCE(delete_flag, 0) + 1,
                    updated_at = NOW()
                LIMIT {count}
            """
            execute_sql(self.upstream_conn, sql)
            return count
        except Exception as e:
            logger.warning(f"Batch update failed: {e}")
            return 0
    
    def _batch_delete(self, count: int) -> int:
        """批量删除数据，返回实际删除行数"""
        db = self.config.db_name
        table = self.config.table_name
        
        try:
            # 先获取当前行数，确保不删光
            current_count = get_table_count(self.upstream_conn, db, table)
            if current_count < count + 10000:
                # 数据太少，跳过删除
                logger.info(f"Skipping delete: only {current_count} rows, need to keep minimum")
                return 0
            
            sql = f"DELETE FROM {db}.{table} LIMIT {count}"
            execute_sql(self.upstream_conn, sql)
            return count
        except Exception as e:
            logger.warning(f"Batch delete failed: {e}")
            return 0
    
    def _dml_worker(self):
        """DML 工作线程 - 持续执行大批量 DML 操作"""
        logger.info("[DML Worker] Started")
        
        # DML 操作轮换
        operations = [
            ("BATCH_INSERT", self.batch_insert_size),    # 10000 行
            ("BATCH_UPDATE", self.batch_update_size),    # 50000 行
            ("BATCH_INSERT", self.batch_insert_size),    # 10000 行
            ("BATCH_DELETE", self.batch_delete_size),    # 10000 行
            ("BATCH_UPDATE", self.batch_update_size // 2),  # 25000 行
            ("BATCH_INSERT", self.batch_insert_size * 2),   # 20000 行
        ]
        
        op_idx = 0
        total_inserted = 0
        total_updated = 0
        total_deleted = 0
        
        while not self.stop_event.is_set():
            op_name, op_count = operations[op_idx % len(operations)]
            op_idx += 1
            
            try:
                start = time.time()
                
                if op_name == "BATCH_INSERT":
                    rows = self._batch_insert(op_count)
                    total_inserted += rows
                    elapsed = time.time() - start
                    logger.info(f"[DML] INSERT {rows} rows in {elapsed:.2f}s (total inserted: {total_inserted})")
                    
                elif op_name == "BATCH_UPDATE":
                    rows = self._batch_update(op_count)
                    total_updated += rows
                    elapsed = time.time() - start
                    logger.info(f"[DML] UPDATE {rows} rows in {elapsed:.2f}s (total updated: {total_updated})")
                    
                elif op_name == "BATCH_DELETE":
                    rows = self._batch_delete(op_count)
                    total_deleted += rows
                    elapsed = time.time() - start
                    logger.info(f"[DML] DELETE {rows} rows in {elapsed:.2f}s (total deleted: {total_deleted})")
                    
            except Exception as e:
                logger.warning(f"[DML] Operation {op_name} failed: {e}")
            
            # 操作间隔
            time.sleep(self.dml_interval)
        
        logger.info(f"[DML Worker] Stopped. Total: inserted={total_inserted}, updated={total_updated}, deleted={total_deleted}")
    
    def start_dml_worker(self):
        """启动 DML 工作线程"""
        self.dml_thread = threading.Thread(target=self._dml_worker, daemon=True)
        self.dml_thread.start()
        logger.info("DML worker started")
    
    def stop_dml_worker(self):
        """停止 DML 工作线程"""
        if self.dml_thread:
            self.dml_thread.join(timeout=10)
        logger.info("DML worker stopped")
    
    # =========================================================================
    # DDL 操作 (ALTER TABLE, 索引创建)
    # =========================================================================
    
    def _execute_ddl_phase(self):
        """执行一个 DDL 阶段"""
        db = self.config.db_name
        table = self.config.table_name
        
        self.ddl_phase += 1
        phase = self.ddl_phase
        
        logger.info(f"\n{'='*60}")
        logger.info(f"DDL Phase {phase}")
        logger.info(f"{'='*60}")
        
        try:
            if phase == 1:
                # ADD COLUMN (VARCHAR)
                col_name = f"extra_col_{phase}"
                logger.info(f"[DDL {phase}] ALTER TABLE ADD COLUMN {col_name} VARCHAR(100)")
                execute_sql(self.upstream_conn, 
                    f"ALTER TABLE {db}.{table} ADD COLUMN {col_name} VARCHAR(100)")
                self.added_columns.append(col_name)
                
            elif phase == 2:
                # ADD COLUMN (INT)
                col_name = f"extra_int_{phase}"
                logger.info(f"[DDL {phase}] ALTER TABLE ADD COLUMN {col_name} INT DEFAULT 0")
                execute_sql(self.upstream_conn,
                    f"ALTER TABLE {db}.{table} ADD COLUMN {col_name} INT DEFAULT 0")
                self.added_columns.append(col_name)
                
            elif phase == 3:
                # CREATE INDEX (普通索引)
                idx_name = f"idx_extra_{phase}"
                if self.added_columns:
                    col = self.added_columns[0]
                    logger.info(f"[DDL {phase}] CREATE INDEX {idx_name} ON ({col})")
                    execute_sql(self.upstream_conn,
                        f"CREATE INDEX {idx_name} ON {db}.{table}({col})")
                    self.added_indexes.append(idx_name)
                    
            elif phase == 4:
                # ADD COLUMN (TEXT)
                col_name = f"extra_text_{phase}"
                logger.info(f"[DDL {phase}] ALTER TABLE ADD COLUMN {col_name} TEXT")
                execute_sql(self.upstream_conn,
                    f"ALTER TABLE {db}.{table} ADD COLUMN {col_name} TEXT")
                self.added_columns.append(col_name)
                
            elif phase == 5:
                # CREATE FULLTEXT INDEX
                if len(self.added_columns) >= 3:
                    col = self.added_columns[-1]  # 用最后添加的 TEXT 列
                    idx_name = f"idx_ft_{phase}"
                    logger.info(f"[DDL {phase}] CREATE FULLTEXT INDEX {idx_name} ON ({col})")
                    success, _, err = execute_sql_safe(self.upstream_conn,
                        f"CREATE FULLTEXT INDEX {idx_name} ON {db}.{table}({col})")
                    if success:
                        self.added_indexes.append(idx_name)
                    else:
                        logger.warning(f"  Fulltext index failed: {err}")
                        
            elif phase == 6:
                # MODIFY COLUMN (改变类型)
                if self.added_columns:
                    col = self.added_columns[0]
                    logger.info(f"[DDL {phase}] ALTER TABLE MODIFY COLUMN {col} VARCHAR(200)")
                    execute_sql(self.upstream_conn,
                        f"ALTER TABLE {db}.{table} MODIFY COLUMN {col} VARCHAR(200)")
                        
            elif phase == 7:
                # CREATE COMPOSITE INDEX
                idx_name = f"idx_composite_{phase}"
                logger.info(f"[DDL {phase}] CREATE INDEX {idx_name} ON (source_type, content_type)")
                execute_sql(self.upstream_conn,
                    f"CREATE INDEX {idx_name} ON {db}.{table}(source_type, content_type)")
                self.added_indexes.append(idx_name)
                
            elif phase == 8:
                # ADD COLUMN (TIMESTAMP)
                col_name = f"extra_ts_{phase}"
                logger.info(f"[DDL {phase}] ALTER TABLE ADD COLUMN {col_name} TIMESTAMP")
                execute_sql(self.upstream_conn,
                    f"ALTER TABLE {db}.{table} ADD COLUMN {col_name} TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                self.added_columns.append(col_name)
                
            elif phase == 9:
                # DROP INDEX
                if self.added_indexes:
                    idx = self.added_indexes.pop(0)
                    logger.info(f"[DDL {phase}] DROP INDEX {idx}")
                    execute_sql_safe(self.upstream_conn, f"DROP INDEX {idx} ON {db}.{table}")
                    
            elif phase == 10:
                # RENAME COLUMN
                if self.added_columns:
                    old_name = self.added_columns[0]
                    new_name = f"renamed_{phase}"
                    logger.info(f"[DDL {phase}] ALTER TABLE RENAME COLUMN {old_name} TO {new_name}")
                    execute_sql(self.upstream_conn,
                        f"ALTER TABLE {db}.{table} RENAME COLUMN {old_name} TO {new_name}")
                    self.added_columns[0] = new_name
                    
            elif phase == 11:
                # CREATE UNIQUE INDEX
                idx_name = f"idx_unique_{phase}"
                logger.info(f"[DDL {phase}] CREATE UNIQUE INDEX {idx_name}")
                # 使用 md5_id + source_type 组合（应该是唯一的）
                success, _, err = execute_sql_safe(self.upstream_conn,
                    f"CREATE UNIQUE INDEX {idx_name} ON {db}.{table}(md5_id, source_type)")
                if success:
                    self.added_indexes.append(idx_name)
                else:
                    logger.warning(f"  Unique index failed (may have duplicates): {err}")
                    
            elif phase == 12:
                # DROP COLUMN
                if len(self.added_columns) > 2:
                    col = self.added_columns.pop()
                    logger.info(f"[DDL {phase}] ALTER TABLE DROP COLUMN {col}")
                    execute_sql(self.upstream_conn, f"ALTER TABLE {db}.{table} DROP COLUMN {col}")
                    
            elif phase == 13:
                # CHANGE TABLE COMMENT
                comment = f"DDL test phase {phase} at {datetime.now()}"
                logger.info(f"[DDL {phase}] ALTER TABLE COMMENT = '{comment}'")
                execute_sql(self.upstream_conn, f"ALTER TABLE {db}.{table} COMMENT = '{comment}'")
                
            elif phase == 14:
                # CREATE IVF INDEX (异步)
                # 先创建一个新表，因为主表可能没有向量数据
                test_table = f"test_vector_{phase}"
                logger.info(f"[DDL {phase}] Creating vector table and IVF index...")
                
                execute_sql_safe(self.upstream_conn, f"DROP TABLE IF EXISTS {db}.{test_table}")
                execute_sql(self.upstream_conn, f"CREATE TABLE {db}.{test_table} LIKE {db}.{table}")
                execute_sql(self.upstream_conn, f"""
                    INSERT INTO {db}.{test_table}
                    SELECT * FROM {db}.{table} WHERE question_vector IS NOT NULL LIMIT 50000
                """)
                
                count = get_table_count(self.upstream_conn, db, test_table)
                if count > 0:
                    success, _, err = execute_sql_safe(self.upstream_conn, f"""
                        CREATE INDEX idx_ivf_{phase} ON {db}.{test_table}(question_vector)
                        USING IVFFLAT LISTS 128 OP_TYPE 'vector_l2_ops' ASYNC
                    """)
                    if success:
                        logger.info(f"  IVF index submitted on {count} rows")
                    else:
                        logger.warning(f"  IVF index failed: {err}")
                        
            elif phase == 15:
                # CREATE HNSW INDEX (异步)
                test_table = f"test_hnsw_{phase}"
                logger.info(f"[DDL {phase}] Creating HNSW index...")
                
                execute_sql_safe(self.upstream_conn, f"DROP TABLE IF EXISTS {db}.{test_table}")
                execute_sql(self.upstream_conn, f"CREATE TABLE {db}.{test_table} LIKE {db}.{table}")
                execute_sql(self.upstream_conn, f"""
                    INSERT INTO {db}.{test_table}
                    SELECT * FROM {db}.{table} WHERE question_vector IS NOT NULL LIMIT 30000
                """)
                
                count = get_table_count(self.upstream_conn, db, test_table)
                if count > 0:
                    success, _, err = execute_sql_safe(self.upstream_conn, f"""
                        CREATE INDEX idx_hnsw_{phase} ON {db}.{test_table}(question_vector)
                        USING HNSW M 16 EF_CONSTRUCTION 64 OP_TYPE 'vector_l2_ops' ASYNC
                    """)
                    if success:
                        logger.info(f"  HNSW index submitted on {count} rows")
                    else:
                        logger.warning(f"  HNSW index failed: {err}")
            
            else:
                # 循环回到 phase 1 类型的操作
                phase_type = (phase - 1) % 15 + 1
                logger.info(f"[DDL {phase}] Cycling back to phase type {phase_type}")
                self.ddl_phase = phase_type - 1  # 重置到对应阶段
                self._execute_ddl_phase()
                return
            
            logger.info(f"[DDL {phase}] Completed")
            
        except Exception as e:
            logger.error(f"[DDL {phase}] Failed: {e}")
            self.errors.append(f"DDL phase {phase} failed: {e}")
    
    def run_all_tests(self):
        """运行所有测试 - 持续 DML + 定期 DDL"""
        logger.info("\n" + "="*60)
        logger.info("Starting CCPR Memory Tests")
        logger.info(f"Test duration: {self.test_duration}s ({self.test_duration/60:.1f} min)")
        logger.info(f"DDL interval: {self.ddl_interval}s ({self.ddl_interval/60:.1f} min)")
        logger.info(f"Batch sizes: INSERT={self.batch_insert_size}, UPDATE={self.batch_update_size}, DELETE={self.batch_delete_size}")
        logger.info("="*60)
        
        # 启动 watermark 监控
        self.start_watermark_monitor()
        
        # 启动 DML 工作线程
        self.start_dml_worker()
        
        start_time = time.time()
        last_ddl_time = start_time
        
        try:
            while not self.stop_event.is_set():
                current_time = time.time()
                elapsed = current_time - start_time
                
                # 检查是否到达测试时长
                if elapsed >= self.test_duration:
                    logger.info(f"Test duration reached ({self.test_duration}s)")
                    break
                
                # 检查是否到了执行 DDL 的时间
                if current_time - last_ddl_time >= self.ddl_interval:
                    # 执行 DDL
                    self._execute_ddl_phase()
                    last_ddl_time = time.time()
                    
                    # DDL 后等待同步
                    time.sleep(10)
                    
                    # 检查上下游行数
                    db = self.config.db_name
                    table = self.config.table_name
                    up_count = get_table_count(self.upstream_conn, db, table)
                    down_count = get_table_count(self.downstream_conn, db, table)
                    logger.info(f"After DDL - Row counts: upstream={up_count}, downstream={down_count}")
                
                # 定期打印进度
                if int(elapsed) % 60 == 0 and int(elapsed) > 0:
                    remaining = self.test_duration - elapsed
                    logger.info(f"Progress: {elapsed/60:.1f} min elapsed, {remaining/60:.1f} min remaining")
                
                time.sleep(5)
            
            # 停止 DML 工作线程
            self.stop_event.set()
            self.stop_dml_worker()
            
            # 最终检查
            logger.info("\n" + "="*60)
            logger.info("Final Check")
            logger.info("="*60)
            
            # 等待同步完成
            logger.info("Waiting for final sync...")
            time.sleep(30)
            
            # 最终状态检查
            is_ok, status = self._check_ccpr_status()
            logger.info(f"Final CCPR status: {status}")
            
            # 检查各表行数
            db = self.config.db_name
            tables = execute_sql(self.upstream_conn, f"SHOW TABLES FROM {db}", fetch=True)
            
            logger.info("\nFinal table row counts:")
            for (tbl,) in tables:
                try:
                    up_count = get_table_count(self.upstream_conn, db, tbl)
                    down_count = get_table_count(self.downstream_conn, db, tbl)
                    match = "✓" if up_count == down_count else "✗"
                    logger.info(f"  {match} {tbl}: upstream={up_count}, downstream={down_count}")
                except Exception as e:
                    logger.warning(f"  ? {tbl}: error - {e}")
            
        finally:
            self.stop_event.set()
            self.stop_watermark_monitor()
            self.stop_dml_worker()
        
        if self.errors:
            logger.error("\n" + "="*60)
            logger.error("ERRORS DETECTED")
            logger.error("="*60)
            for err in self.errors:
                logger.error(f"  - {err}")
            return False
        
        logger.info("\n" + "="*60)
        logger.info("All tests completed successfully")
        logger.info("="*60)
        return True
    
    def close(self):
        """关闭连接（不清理数据）"""
        if self.upstream_conn:
            self.upstream_conn.close()
        if self.downstream_conn:
            self.downstream_conn.close()
        logger.info("Connections closed (data preserved)")


# =============================================================================
# 主函数
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="CCPR Memory Test with Subscription")
    
    # 集群配置
    parser.add_argument("--upstream-host", default="127.0.0.1")
    parser.add_argument("--upstream-port", type=int, default=6001)
    parser.add_argument("--downstream-host", default="127.0.0.1")
    parser.add_argument("--downstream-port", type=int, default=6002)
    parser.add_argument("--user", default="dump")
    parser.add_argument("--password", default="111")
    
    # 数据配置
    parser.add_argument("--data-file", default="./sample_1m.csv")
    parser.add_argument("--db-name", default="ccpr_memory_test")
    parser.add_argument("--pub-name", default="pub_memory_test")
    
    # 测试配置
    parser.add_argument("--duration", type=int, default=3600,
                       help="Test duration in seconds (default: 3600 = 1 hour)")
    parser.add_argument("--ddl-interval", type=int, default=120,
                       help="DDL operation interval in seconds (default: 120 = 2 min)")
    parser.add_argument("--dml-interval", type=float, default=0.5,
                       help="DML operation interval in seconds (default: 0.5)")
    
    # 批量操作大小
    parser.add_argument("--batch-insert", type=int, default=10000,
                       help="Batch insert size (default: 10000)")
    parser.add_argument("--batch-update", type=int, default=50000,
                       help="Batch update size (default: 50000)")
    parser.add_argument("--batch-delete", type=int, default=10000,
                       help="Batch delete size (default: 10000)")
    
    args = parser.parse_args()
    
    config = TestConfig()
    config.upstream = ClusterConfig(
        host=args.upstream_host,
        port=args.upstream_port,
        user=args.user,
        password=args.password
    )
    config.downstream = ClusterConfig(
        host=args.downstream_host,
        port=args.downstream_port,
        user=args.user,
        password=args.password
    )
    config.data_file = args.data_file
    config.db_name = args.db_name
    config.pub_name = args.pub_name
    
    test = CCPRMemoryTest(config)
    
    # 应用测试配置
    test.test_duration = args.duration
    test.ddl_interval = args.ddl_interval
    test.dml_interval = args.dml_interval
    test.batch_insert_size = args.batch_insert
    test.batch_update_size = args.batch_update
    test.batch_delete_size = args.batch_delete
    
    logger.info(f"Test Configuration:")
    logger.info(f"  Duration: {test.test_duration}s ({test.test_duration/60:.1f} min)")
    logger.info(f"  DDL interval: {test.ddl_interval}s ({test.ddl_interval/60:.1f} min)")
    logger.info(f"  DML interval: {test.dml_interval}s")
    logger.info(f"  Batch INSERT: {test.batch_insert_size} rows")
    logger.info(f"  Batch UPDATE: {test.batch_update_size} rows")
    logger.info(f"  Batch DELETE: {test.batch_delete_size} rows")
    
    try:
        # 连接
        if not test.connect():
            logger.error("Failed to connect")
            sys.exit(1)
        
        # 设置 CCPR
        if not test.setup_ccpr():
            logger.error("Failed to setup CCPR")
            sys.exit(1)
        
        # 运行测试
        success = test.run_all_tests()
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1)
    finally:
        test.close()


if __name__ == "__main__":
    main()
