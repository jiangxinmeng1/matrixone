#!/usr/bin/env python3
"""
CCPR Integration Test - Main Runner

按照TEST_PLAN.md实现的CCPR集成测试:
- 短测试: 权限测试、DML测试、ALTER TABLE测试、控制操作测试
- 长测试: 同时跑account/db/table三种level，6个阶段流程
"""

import os
import sys
import json
import time
import logging
import argparse
import threading
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum

from config import (
    TestConfig, ClusterConfig, SyncLevel, SubscriptionState,
    AccountConfig, load_config_from_env
)
from db_utils import (
    DatabaseConnection, execute_sql, execute_sql_safe, get_ccpr_status,
    trigger_checkpoint, get_table_count, verify_data_consistency,
    create_account, drop_account
)
from data_generator import (
    create_test_tables, random_string, escape_sql_string, random_text,
    BasicTable, FulltextTable, VectorTable, TableDefinition
)

logger = logging.getLogger(__name__)


# =============================================================================
# 测试账户配置
# =============================================================================

@dataclass
class CCPRTask:
    """CCPR任务配置"""
    level: SyncLevel
    name: str
    upstream_account: str
    downstream_account: str
    pub_name: str
    db_name: str
    table_name: Optional[str] = None  # 仅table level需要
    task_id: Optional[str] = None
    
    # 连接
    upstream_conn: Optional[DatabaseConnection] = None
    downstream_conn: Optional[DatabaseConnection] = None
    
    # 动态添加的列（用于ALTER测试）
    added_columns: List[str] = field(default_factory=list)
    added_indexes: List[str] = field(default_factory=list)
    
    # PK 追踪：已使用的PK和已删除的PK
    next_pk: int = 1
    deleted_pks: Set[int] = field(default_factory=set)
    active_pks: Set[int] = field(default_factory=set)  # 当前存活的PK
    pk_lock: Any = field(default=None)  # threading.Lock, 在 __post_init__ 中初始化
    
    def __post_init__(self):
        if self.pk_lock is None:
            self.pk_lock = threading.Lock()


class LongTestPhase(Enum):
    """长测试阶段"""
    PHASE1_DML_ADD_COLUMN = 1      # 持续DML -> ADD COLUMN (non-inplace)
    PHASE2_DML_ADD_INDEX = 2       # 持续DML -> ADD INDEX (inplace)
    PHASE3_DML_ADD_FULLTEXT = 3    # 持续DML -> ADD FULLTEXT INDEX (inplace)
    PHASE4_DML_RENAME_COLUMN = 4   # 持续DML -> RENAME COLUMN (inplace)
    PHASE5_DML_DROP_COLUMN = 5     # 持续DML -> DROP COLUMN (non-inplace)
    PHASE6_DML_LOAD_COMMENT = 6    # 持续DML + 大批量LOAD -> CHANGE COMMENT (inplace)
    PHASE7_DELETE_ALL = 7          # DELETE FROM 删除上游所有数据，再重新插入


# =============================================================================
# 测试基础设施
# =============================================================================

class CCPRLongTest:
    """
    CCPR长时间测试
    
    同时运行3种level的CCPR任务:
    - Account Level: 整个账户级别的订阅
    - Database Level: 数据库级别的订阅  
    - Table Level: 单表级别的订阅
    
    每种level在不同的上下游account下运行
    """
    
    def __init__(self, config: TestConfig):
        self.config = config
        self.tasks: List[CCPRTask] = []
        self.sys_upstream_conn: Optional[DatabaseConnection] = None
        self.sys_downstream_conn: Optional[DatabaseConnection] = None
        self.stop_event = threading.Event()
        self.errors: List[str] = []
        self.dml_threads: List[threading.Thread] = []
        self.watermark_monitor_thread: Optional[threading.Thread] = None
        self.last_watermarks: Dict[str, int] = {}  # task_name -> last_watermark
        
    def setup(self) -> bool:
        """初始化测试环境"""
        logger.info("="*60)
        logger.info("Setting up CCPR Long Test")
        logger.info("="*60)
        
        try:
            # 系统级连接（用于创建account）
            self.sys_upstream_conn = DatabaseConnection(self.config.upstream)
            self.sys_upstream_conn.connect()
            self.sys_downstream_conn = DatabaseConnection(self.config.downstream)
            self.sys_downstream_conn.connect()
            
            # 只运行table level测试
            levels = [
                # (SyncLevel.ACCOUNT, "account"),
                # (SyncLevel.DATABASE, "database"),
                (SyncLevel.TABLE, "table"),
            ]
            
            for level, level_name in levels:
                task = self._setup_level(level, level_name)
                if task:
                    self.tasks.append(task)
                    logger.info(f"[{level_name.upper()}] Task setup complete: {task.pub_name}")
                else:
                    logger.error(f"[{level_name.upper()}] Task setup failed, continuing with other tasks")
            
            if not self.tasks:
                logger.error("No tasks were created successfully")
                return False
            
            logger.info(f"Setup complete. {len(self.tasks)} CCPR tasks created.")
            return True
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            self.errors.append(str(e))
            return False
    
    def _setup_level(self, level: SyncLevel, level_name: str) -> Optional[CCPRTask]:
        """为指定level创建CCPR任务 - 每个任务使用不同的account"""
        suffix = random_string(6).lower()
        
        # 每个任务用不同的上下游account
        up_account = f"up_{level_name}_{suffix}"
        down_account = f"down_{level_name}_{suffix}"
        
        # 数据库、表、publication名
        db_name = f"ccpr_{level_name}_db_{suffix}"
        table_name = f"ccpr_{level_name}_tbl_{suffix}"
        pub_name = f"pub_{level_name}_{suffix}"
        
        try:
            # 1. 用sys在上游创建两个account：
            #    - up_account: 持有database/table
            #    - down_account: 用于publication授权（上游也要有这个account才能授权）
            logger.info(f"[{level_name}] Creating upstream accounts: {up_account}, {down_account}")
            create_account(self.sys_upstream_conn.get_connection(), up_account)
            create_account(self.sys_upstream_conn.get_connection(), down_account)
            
            # 2. 用sys在下游创建account（用于订阅）
            logger.info(f"[{level_name}] Creating downstream account: {down_account}")
            create_account(self.sys_downstream_conn.get_connection(), down_account)
            
            time.sleep(2)  # 等待account生效
            
            # 3. 连接上游account
            up_config = ClusterConfig(
                host=self.config.upstream.host,
                port=self.config.upstream.port,
                user="admin",
                password="111",
                account=up_account
            )
            up_conn = DatabaseConnection(up_config)
            up_conn.connect()
            
            # 4. 创建数据库和表
            logger.info(f"[{level_name}] Creating database: {db_name}")
            execute_sql(up_conn.get_connection(), f"CREATE DATABASE {db_name}")
            
            create_table_sql = f"""
                CREATE TABLE {db_name}.{table_name} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    c_int INT DEFAULT 0,
                    c_bigint BIGINT,
                    c_float FLOAT,
                    c_double DOUBLE,
                    c_decimal DECIMAL(18,4),
                    c_varchar VARCHAR(200),
                    c_text TEXT,
                    c_date DATE,
                    c_datetime DATETIME,
                    c_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    c_bool BOOL DEFAULT FALSE,
                    c_json JSON,
                    status VARCHAR(20) DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            execute_sql(up_conn.get_connection(), create_table_sql)
            logger.info(f"[{level_name}] Created table: {db_name}.{table_name}")
            
            # 创建初始索引
            execute_sql(up_conn.get_connection(), 
                f"CREATE INDEX idx_status ON {db_name}.{table_name} (status)")
            
            # 先创建 task 对象以便追踪 PK
            temp_task = CCPRTask(
                level=level,
                name=level_name,
                upstream_account=up_account,
                downstream_account=down_account,
                pub_name=pub_name,
                db_name=db_name,
                table_name=table_name,
            )
            
            # 插入初始数据，追踪 PK
            for i in range(100):
                self._insert_row(up_conn.get_connection(), db_name, table_name, task=temp_task)
            logger.info(f"[{level_name}] Inserted 100 initial rows, pks=[1..100]")
            
            # 5. 从tenant账户创建publication，发布给下游account
            # Account level: DATABASE *
            # Database level: DATABASE db_name
            # Table level: DATABASE db_name TABLE table_name
            if level == SyncLevel.ACCOUNT:
                pub_sql = f"CREATE PUBLICATION {pub_name} DATABASE * ACCOUNT {down_account}"
            elif level == SyncLevel.DATABASE:
                pub_sql = f"CREATE PUBLICATION {pub_name} DATABASE {db_name} ACCOUNT {down_account}"
            else:  # TABLE
                pub_sql = f"CREATE PUBLICATION {pub_name} DATABASE {db_name} TABLE {table_name} ACCOUNT {down_account}"
            
            # 从tenant账户执行（不是sys）
            execute_sql(up_conn.get_connection(), pub_sql)
            logger.info(f"[{level_name}] Created publication: {pub_sql}")
            
            # 6. 连接下游account，创建订阅
            down_config = ClusterConfig(
                host=self.config.downstream.host,
                port=self.config.downstream.port,
                user="admin",
                password="111",
                account=down_account
            )
            down_conn = DatabaseConnection(down_config)
            down_conn.connect()
            
            # 关键：连接字符串需要使用 down_account 的凭证（上游也创建了该账户）
            # 这样下游才能以被授权的账户身份访问 publication
            sub_conn_str = f"mysql://{down_account}#admin:111@{self.config.upstream.host}:{self.config.upstream.port}"
            
            if level == SyncLevel.ACCOUNT:
                # ACCOUNT level: 订阅到当前session的account
                sub_sql = f"CREATE ACCOUNT FROM '{sub_conn_str}' {up_account} PUBLICATION {pub_name}"
            elif level == SyncLevel.DATABASE:
                # DATABASE level: 订阅整个数据库
                sub_sql = f"CREATE DATABASE {db_name} FROM '{sub_conn_str}' {up_account} PUBLICATION {pub_name}"
            else:  # TABLE level
                # TABLE level: 先建库，再订阅表
                execute_sql(down_conn.get_connection(), f"CREATE DATABASE {db_name}")
                sub_sql = f"CREATE TABLE {db_name}.{table_name} FROM '{sub_conn_str}' {up_account} PUBLICATION {pub_name}"
            
            execute_sql(down_conn.get_connection(), sub_sql)
            logger.info(f"[{level_name}] Created subscription: {sub_sql}")
            
            # 等待初始同步 (用sys租户触发checkpoint)
            trigger_checkpoint(self.sys_upstream_conn.get_connection())
            time.sleep(15)
            
            # 更新 temp_task 的连接信息
            temp_task.upstream_conn = up_conn
            temp_task.downstream_conn = down_conn
            
            return temp_task
            
        except Exception as e:
            logger.error(f"[{level_name}] Setup failed: {e}")
            return None
    
    def _insert_row(self, conn, db_name: str, table_name: str, 
                    extra_columns: List[str] = None, task: CCPRTask = None) -> Optional[int]:
        """插入一行测试数据，使用显式 PK"""
        if task is None:
            # 向后兼容：没有task时使用AUTO_INCREMENT
            cols = ["c_int", "c_bigint", "c_float", "c_double", "c_decimal",
                    "c_varchar", "c_text", "c_date", "c_datetime", "c_bool", 
                    "c_json", "status"]
        else:
            # 使用显式 PK
            with task.pk_lock:
                pk = task.next_pk
                task.next_pk += 1
                task.active_pks.add(pk)
            
            cols = ["id", "c_int", "c_bigint", "c_float", "c_double", "c_decimal",
                    "c_varchar", "c_text", "c_date", "c_datetime", "c_bool", 
                    "c_json", "status"]
        
        json_val = json.dumps({"key": random_string(10)})
        
        if task is None:
            values = [
                str(random.randint(0, 100000)),
                str(random.randint(0, 10000000000)),
                str(round(random.uniform(-1000, 1000), 4)),
                str(round(random.uniform(-1000000, 1000000), 8)),
                str(round(random.uniform(-10000, 10000), 4)),
                f"'{escape_sql_string(random_string(50))}'",
                f"'{escape_sql_string(random_text(10, 30))}'",
                f"'{datetime.now().strftime('%Y-%m-%d')}'",
                f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'",
                str(random.choice([0, 1])),
                f"'{json_val}'",
                f"'{random.choice(['active', 'inactive', 'pending'])}'",
            ]
        else:
            values = [
                str(pk),  # 显式 PK
                str(random.randint(0, 100000)),
                str(random.randint(0, 10000000000)),
                str(round(random.uniform(-1000, 1000), 4)),
                str(round(random.uniform(-1000000, 1000000), 8)),
                str(round(random.uniform(-10000, 10000), 4)),
                f"'{escape_sql_string(random_string(50))}'",
                f"'{escape_sql_string(random_text(10, 30))}'",
                f"'{datetime.now().strftime('%Y-%m-%d')}'",
                f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'",
                str(random.choice([0, 1])),
                f"'{json_val}'",
                f"'{random.choice(['active', 'inactive', 'pending'])}'",
            ]
        
        # 添加额外的列值
        if extra_columns:
            for col in extra_columns:
                cols.append(col)
                values.append(f"'{random_string(20)}'")
        
        sql = f"INSERT INTO {db_name}.{table_name} ({', '.join(cols)}) VALUES ({', '.join(values)})"
        execute_sql(conn, sql)
        
        if task is not None:
            logger.info(f"[{task.name}] INSERT pk={pk}")
            return pk
        return None
    
    def _delete_rows_by_pk(self, conn, db_name: str, table_name: str, 
                           task: CCPRTask, count: int) -> List[int]:
        """删除指定数量的行，返回删除的 PK 列表"""
        deleted_pks = []
        
        with task.pk_lock:
            # 从 active_pks 中随机选择要删除的 PK
            available = list(task.active_pks)
            if len(available) == 0:
                return []
            
            to_delete = random.sample(available, min(count, len(available)))
            
            for pk in to_delete:
                task.active_pks.discard(pk)
                task.deleted_pks.add(pk)
                deleted_pks.append(pk)
        
        if deleted_pks:
            pk_list = ','.join(str(pk) for pk in deleted_pks)
            sql = f"DELETE FROM {db_name}.{table_name} WHERE id IN ({pk_list})"
            execute_sql(conn, sql)
            logger.info(f"[{task.name}] DELETE pks=[{pk_list}]")
        
        return deleted_pks
    
    def _update_rows_by_pk(self, conn, db_name: str, table_name: str,
                           task: CCPRTask, count: int) -> List[int]:
        """更新指定数量的行，返回更新的 PK 列表"""
        with task.pk_lock:
            available = list(task.active_pks)
            if len(available) == 0:
                return []
            
            to_update = random.sample(available, min(count, len(available)))
        
        if to_update:
            pk_list = ','.join(str(pk) for pk in to_update)
            sql = f"""
                UPDATE {db_name}.{table_name} 
                SET c_int = {random.randint(0, 100000)},
                    c_varchar = '{escape_sql_string(random_string(50))}',
                    status = '{random.choice(['active', 'inactive', 'pending'])}'
                WHERE id IN ({pk_list})
            """
            execute_sql(conn, sql)
            logger.info(f"[{task.name}] UPDATE pks=[{pk_list}]")
        
        return to_update
    
    def run(self, duration: int) -> bool:
        """运行长时间测试 - 每10分钟做一次ALTER TABLE"""
        logger.info("="*60)
        logger.info(f"Starting Long Test for {duration} seconds ({duration/3600:.1f} hours)")
        logger.info(f"ALTER TABLE will be executed every 10 minutes")
        logger.info("="*60)
        
        start_time = time.time()
        alter_interval = 10 * 60  # 每10分钟做一次ALTER
        
        phases = [
            LongTestPhase.PHASE1_DML_ADD_COLUMN,
            LongTestPhase.PHASE2_DML_ADD_INDEX,
            LongTestPhase.PHASE3_DML_ADD_FULLTEXT,
            LongTestPhase.PHASE4_DML_RENAME_COLUMN,
            LongTestPhase.PHASE5_DML_DROP_COLUMN,
            LongTestPhase.PHASE6_DML_LOAD_COMMENT,
            LongTestPhase.PHASE7_DELETE_ALL,
        ]
        
        try:
            # 启动DML线程（持续运行到测试结束）
            self._start_dml_threads()
            
            phase_idx = 0
            last_alter_time = start_time
            
            while time.time() - start_time < duration:
                current_time = time.time()
                
                # 每10秒触发一次checkpoint
                trigger_checkpoint(self.sys_upstream_conn.get_connection())
                
                # 检查是否到了执行ALTER的时间（每10分钟）
                if current_time - last_alter_time >= alter_interval:
                    phase = phases[phase_idx % len(phases)]
                    phase_idx += 1
                    
                    logger.info(f"\n{'='*60}")
                    logger.info(f"ALTER interval reached - executing {phase.name}")
                    logger.info(f"Elapsed: {(current_time - start_time)/60:.1f} min, "
                              f"ALTER count: {phase_idx}")
                    logger.info(f"{'='*60}")
                    
                    # 1. 停止DML线程
                    self._stop_dml_threads()
                    
                    # 2. 检查点：PAUSE -> 一致性检查 -> RESUME
                    logger.info(f"\n--- Checkpoint for {phase.name} ---")
                    for task in self.tasks:
                        self._checkpoint(task)
                    
                    # 3. 执行ALTER操作
                    logger.info(f"\n--- ALTER operations for {phase.name} ---")
                    for task in self.tasks:
                        self._execute_alter(task, phase)
                    
                    # 4. 等待ALTER同步
                    trigger_checkpoint(self.sys_upstream_conn.get_connection())
                    time.sleep(self.config.sync_interval + 5)
                    
                    # 5. 重启DML线程
                    self._start_dml_threads()
                    
                    last_alter_time = time.time()
                
                # 等待10秒后继续循环
                time.sleep(10)
            
            # 停止DML线程
            self._stop_dml_threads()
            
            # 最终检查
            logger.info("\n" + "="*60)
            logger.info("Final Checkpoint")
            logger.info("="*60)
            for task in self.tasks:
                self._checkpoint(task)
            
            return len(self.errors) == 0
            
        except KeyboardInterrupt:
            logger.info("Test interrupted by user")
            self._stop_dml_threads()
            return False
        except Exception as e:
            logger.error(f"Test failed: {e}")
            self.errors.append(str(e))
            self._stop_dml_threads()
            return False
    
    def _start_dml_threads(self):
        """启动DML线程"""
        self.stop_event.clear()
        self.dml_threads = []
        
        for task in self.tasks:
            t = threading.Thread(target=self._dml_worker, args=(task,), daemon=True)
            t.start()
            self.dml_threads.append(t)
        
        # 启动 watermark 监控线程
        self.watermark_monitor_thread = threading.Thread(
            target=self._watermark_monitor_worker, daemon=True)
        self.watermark_monitor_thread.start()
        
        logger.info(f"Started {len(self.dml_threads)} DML threads + 1 watermark monitor thread")
    
    def _stop_dml_threads(self):
        """停止DML线程"""
        self.stop_event.set()
        for t in self.dml_threads:
            t.join(timeout=10)
        self.dml_threads = []
        # 停止 watermark 监控线程
        if self.watermark_monitor_thread:
            self.watermark_monitor_thread.join(timeout=10)
            self.watermark_monitor_thread = None
        logger.info("Stopped DML threads and watermark monitor")
    
    def _dml_worker(self, task: CCPRTask):
        """DML工作线程 - 持续执行各种DML操作，使用显式 PK 追踪"""
        # DML操作类型列表，轮流执行
        dml_operations = [
            "SMALL_INSERT",      # 小数据插入 (1-5行)
            "SMALL_UPDATE",      # 小数据更新 (1-5行)
            "SMALL_DELETE",      # 小数据删除 (1-5行)
            "BATCH_INSERT",      # 批量插入 (50-100行)
            "BATCH_UPDATE",      # 批量更新 (10-50行)
            "BATCH_DELETE",      # 批量删除 (10-50行)
            "SMALL_INSERT",      # 再来一轮小操作
            "SMALL_UPDATE",
            "SMALL_DELETE",
        ]
        
        # 连接上游account
        up_config = ClusterConfig(
            host=self.config.upstream.host,
            port=self.config.upstream.port,
            user="admin",
            password="111",
            account=task.upstream_account
        )
        worker_conn = DatabaseConnection(up_config)
        try:
            worker_conn.connect()
        except Exception as e:
            logger.error(f"[{task.name}] DML worker failed to connect: {e}")
            return
        
        op_index = 0
        try:
            while not self.stop_event.is_set():
                op = dml_operations[op_index % len(dml_operations)]
                op_index += 1
                conn = worker_conn.get_connection()
                db = task.db_name
                tbl = task.table_name
                
                try:
                    if op == "SMALL_INSERT":
                        # 小数据插入 1-5行
                        count = random.randint(1, 5)
                        for _ in range(count):
                            self._insert_row(conn, db, tbl, task.added_columns, task)
                        
                    elif op == "SMALL_UPDATE":
                        # 小数据更新 1-5行
                        count = random.randint(1, 5)
                        self._update_rows_by_pk(conn, db, tbl, task, count)
                        
                    elif op == "SMALL_DELETE":
                        # 小数据删除 1-5行
                        count = random.randint(1, 5)
                        self._delete_rows_by_pk(conn, db, tbl, task, count)
                        
                    elif op == "BATCH_INSERT":
                        # 批量插入 50-100行
                        count = random.randint(50, 100)
                        inserted_pks = []
                        for _ in range(count):
                            pk = self._insert_row(conn, db, tbl, task.added_columns, task)
                            if pk:
                                inserted_pks.append(pk)
                        logger.info(f"[{task.name}] BATCH_INSERT: {len(inserted_pks)} rows, pks=[{inserted_pks[0]}..{inserted_pks[-1]}]")
                        
                    elif op == "BATCH_UPDATE":
                        # 批量更新 10-50行
                        count = random.randint(10, 50)
                        self._update_rows_by_pk(conn, db, tbl, task, count)
                        
                    elif op == "BATCH_DELETE":
                        # 批量删除 10-50行 (但保留至少100行)
                        with task.pk_lock:
                            active_count = len(task.active_pks)
                        
                        if active_count > 150:
                            count = min(random.randint(10, 50), active_count - 100)
                            self._delete_rows_by_pk(conn, db, tbl, task, count)
                        else:
                            # 数据太少，改为插入
                            inserted_pks = []
                            for _ in range(50):
                                pk = self._insert_row(conn, db, tbl, task.added_columns, task)
                                if pk:
                                    inserted_pks.append(pk)
                            logger.info(f"[{task.name}] BATCH_DELETE->INSERT: 50 rows (table too small), pks=[{inserted_pks[0]}..{inserted_pks[-1]}]")
                            
                except Exception as e:
                    logger.warning(f"[{task.name}] DML error ({op}): {e}")
                
                # 不等待，直接继续下一个操作
        finally:
            worker_conn.close()
    
    def _watermark_monitor_worker(self):
        """
        Watermark 监控线程 - 监控每个 task 的 watermark 变化，发现变化时进行数据一致性检查
        """
        logger.info("[WatermarkMonitor] Started")
        
        # 为监控线程创建独立的下游连接
        monitor_down_conn = DatabaseConnection(self.config.downstream)
        try:
            monitor_down_conn.connect()
        except Exception as e:
            logger.error(f"[WatermarkMonitor] Failed to connect: {e}")
            return
        
        try:
            while not self.stop_event.is_set():
                try:
                    for task in self.tasks:
                        # 查询当前 watermark
                        result = execute_sql(monitor_down_conn.get_connection(),
                            f"SELECT task_id, watermark FROM mo_catalog.mo_ccpr_log "
                            f"WHERE subscription_name='{task.pub_name}' AND drop_at IS NULL",
                            fetch=True)
                        
                        if not result or not result[0][0]:
                            continue
                        
                        task_id = result[0][0]
                        current_watermark = result[0][1] or 0
                        last_watermark = self.last_watermarks.get(task.name, 0)
                        
                        # 检查 watermark 是否有变化
                        if current_watermark != last_watermark and last_watermark != 0:
                            logger.info(f"[WatermarkMonitor] [{task.name}] Watermark changed: {last_watermark} -> {current_watermark}")
                            
                            # 执行数据一致性检查（不 PAUSE，直接用 snapshot 对比）
                            self._check_consistency_on_watermark_change(task, task_id, current_watermark)
                        
                        # 更新 last_watermark
                        self.last_watermarks[task.name] = current_watermark
                        
                except Exception as e:
                    logger.warning(f"[WatermarkMonitor] Error: {e}")
                
                # 每 5 秒检查一次
                for _ in range(50):  # 5秒，每0.1秒检查一次stop_event
                    if self.stop_event.is_set():
                        break
                    time.sleep(0.1)
        finally:
            monitor_down_conn.close()
            logger.info("[WatermarkMonitor] Stopped")
    
    def _check_consistency_on_watermark_change(self, task: CCPRTask, task_id: str, watermark: int):
        """
        Watermark 变化时的数据一致性检查（不 PAUSE，使用 snapshot 对比）
        """
        try:
            # 根据 watermark 查询对应的 snapshot
            snapshot_name = None
            try:
                snapshot_result = execute_sql(task.upstream_conn.get_connection(),
                    f"SELECT sname FROM mo_catalog.mo_snapshots WHERE ts = {watermark}",
                    fetch=True)
                if snapshot_result and snapshot_result[0][0]:
                    snapshot_name = snapshot_result[0][0]
                    logger.info(f"[WatermarkMonitor] [{task.name}] Found snapshot: {snapshot_name}")
            except Exception as e:
                logger.warning(f"[WatermarkMonitor] [{task.name}] Failed to query snapshot: {e}")
            
            if not snapshot_name:
                logger.warning(f"[WatermarkMonitor] [{task.name}] No snapshot found for watermark={watermark}, skipping check")
                return
            
            # 使用详细对比函数
            is_consistent, report = self._compare_data(task, snapshot_name)
            
            if not is_consistent:
                logger.error(f"[WatermarkMonitor] [{task.name}] {report}")
                logger.error(f"[WatermarkMonitor] [{task.name}] STOPPING TEST - Data mismatch detected!")
                
                # 打印 PK 追踪信息
                with task.pk_lock:
                    logger.error(f"[WatermarkMonitor] [{task.name}] PK tracking: next_pk={task.next_pk}, "
                               f"active={len(task.active_pks)}, deleted={len(task.deleted_pks)}")
                    logger.error(f"[WatermarkMonitor] [{task.name}] Active PKs (sample): {sorted(list(task.active_pks))[:50]}")
                    logger.error(f"[WatermarkMonitor] [{task.name}] Deleted PKs (sample): {sorted(list(task.deleted_pks))[:50]}")
                
                # 停止测试
                self.errors.append(f"[WatermarkMonitor] [{task.name}] {report}")
                raise SystemExit(f"[WatermarkMonitor] [{task.name}] {report}")
            else:
                logger.info(f"[WatermarkMonitor] [{task.name}] ✓ {report}")
                
        except SystemExit:
            raise
        except Exception as e:
            logger.warning(f"[WatermarkMonitor] [{task.name}] Check failed: {e}")
    
    def _compare_data(self, task: CCPRTask, snapshot_name: Optional[str] = None) -> Tuple[bool, str]:
        """
        比较上下游数据，返回 (是否一致, 差异描述)
        
        1. 查询上游所有数据（可选用 snapshot）
        2. 查询下游所有数据
        3. 对比每一行，列出差异
        """
        db = task.db_name
        tbl = task.table_name
        
        try:
            # 查询上游数据
            if snapshot_name:
                up_sql = f"SELECT id, c_int, c_varchar, status FROM {db}.{tbl} {{SNAPSHOT = '{snapshot_name}'}} ORDER BY id"
            else:
                up_sql = f"SELECT id, c_int, c_varchar, status FROM {db}.{tbl} ORDER BY id"
            
            up_rows = execute_sql(task.upstream_conn.get_connection(), up_sql, fetch=True)
            up_data = {row[0]: row for row in (up_rows or [])}
            
            # 查询下游数据
            down_sql = f"SELECT id, c_int, c_varchar, status FROM {db}.{tbl} ORDER BY id"
            down_rows = execute_sql(task.downstream_conn.get_connection(), down_sql, fetch=True)
            down_data = {row[0]: row for row in (down_rows or [])}
            
            # 对比
            all_pks = set(up_data.keys()) | set(down_data.keys())
            
            only_upstream = []
            only_downstream = []
            different_values = []
            
            for pk in sorted(all_pks):
                up_row = up_data.get(pk)
                down_row = down_data.get(pk)
                
                if up_row and not down_row:
                    only_upstream.append(pk)
                elif down_row and not up_row:
                    only_downstream.append(pk)
                elif up_row != down_row:
                    different_values.append((pk, up_row, down_row))
            
            # 生成报告
            if not only_upstream and not only_downstream and not different_values:
                return True, f"数据一致: {len(up_data)} 行"
            
            report_lines = [
                f"数据不一致! 上游={len(up_data)}行, 下游={len(down_data)}行",
            ]
            
            if only_upstream:
                report_lines.append(f"  仅上游存在 ({len(only_upstream)}行): pks={only_upstream[:20]}{'...' if len(only_upstream) > 20 else ''}")
            
            if only_downstream:
                report_lines.append(f"  仅下游存在 ({len(only_downstream)}行): pks={only_downstream[:20]}{'...' if len(only_downstream) > 20 else ''}")
            
            if different_values:
                report_lines.append(f"  值不同 ({len(different_values)}行):")
                for pk, up_row, down_row in different_values[:10]:
                    report_lines.append(f"    pk={pk}: 上游={up_row}, 下游={down_row}")
                if len(different_values) > 10:
                    report_lines.append(f"    ... 还有 {len(different_values) - 10} 行差异")
            
            return False, '\n'.join(report_lines)
            
        except Exception as e:
            return False, f"对比失败: {e}"
    
    def _checkpoint(self, task: CCPRTask) -> bool:
        """
        检查点流程：
        1. 查询task_id
        2. PAUSE CCPR SUBSCRIPTION 'task_id'
        3. sleep 10s
        4. 从下游mo_ccpr_log读取状态
        5. 检查watermark和error_message
        6. 用snapshot对比数据一致性
        7. RESUME CCPR SUBSCRIPTION 'task_id'
        """
        logger.info(f"[{task.name}] Starting checkpoint...")
        
        try:
            # 1. 查询task_id (用sys租户查询系统表)
            result = execute_sql(self.sys_downstream_conn.get_connection(),
                f"SELECT task_id FROM mo_catalog.mo_ccpr_log WHERE subscription_name='{task.pub_name}' AND drop_at IS NULL",
                fetch=True)
            if not result or not result[0][0]:
                logger.warning(f"[{task.name}] No task_id found for {task.pub_name}")
                return False
            task_id = result[0][0]
            logger.info(f"[{task.name}] Found task_id: {task_id}")
            
            # 2. PAUSE (使用task_id带引号，用sys租户)
            execute_sql(self.sys_downstream_conn.get_connection(),
                f"PAUSE CCPR SUBSCRIPTION '{task_id}'")
            logger.info(f"[{task.name}] Paused subscription")
            
            # 2. Sleep
            time.sleep(10)
            
            # 3. 读取状态 (用sys租户查询系统表)
            statuses = get_ccpr_status(self.sys_downstream_conn.get_connection(),
                subscription_name=task.pub_name)
            
            if statuses:
                status = statuses[0]
                logger.info(f"[{task.name}] Status: state={status.state}, "
                          f"lsn={status.iteration_lsn}, watermark={status.watermark}, error={status.error_message}")
                
                # 4. 检查error
                if status.error_message:
                    self.errors.append(f"[{task.name}] Error: {status.error_message}")
                    logger.error(f"[{task.name}] Subscription error: {status.error_message}")
                
                # 5. 数据一致性检查（使用snapshot）
                # 用watermark查询mo_snapshots表获取snapshot名称
                # 注意：CCPR的snapshot是在upstream account下创建的，和表在同一个account
                snapshot_name = None
                if status.watermark:
                    try:
                        snapshot_result = execute_sql(task.upstream_conn.get_connection(),
                            f"SELECT sname FROM mo_catalog.mo_snapshots WHERE ts = {status.watermark}",
                            fetch=True)
                        if snapshot_result and snapshot_result[0][0]:
                            snapshot_name = snapshot_result[0][0]
                            logger.info(f"[{task.name}] Found snapshot by watermark: {snapshot_name}")
                    except Exception as e:
                        logger.warning(f"[{task.name}] Failed to query snapshot by watermark: {e}")
                
                if not snapshot_name:
                    logger.warning(f"[{task.name}] No snapshot found for watermark={status.watermark}, using direct comparison")
                
                # 使用详细对比函数
                is_consistent, report = self._compare_data(task, snapshot_name)
                
                if not is_consistent:
                    logger.error(f"[{task.name}] {report}")
                    logger.error(f"[{task.name}] STOPPING TEST - Data mismatch detected!")
                    
                    # 打印 PK 追踪信息
                    with task.pk_lock:
                        logger.error(f"[{task.name}] PK tracking: next_pk={task.next_pk}, active={len(task.active_pks)}, deleted={len(task.deleted_pks)}")
                        logger.error(f"[{task.name}] Active PKs (sample): {sorted(list(task.active_pks))[:50]}")
                        logger.error(f"[{task.name}] Deleted PKs (sample): {sorted(list(task.deleted_pks))[:50]}")
                    
                    # 不resume，保持pause状态方便调试
                    raise SystemExit(f"[{task.name}] {report}")
                else:
                    logger.info(f"[{task.name}] {report}")
            
            # 7. RESUME (使用task_id带引号，用sys租户)
            execute_sql(self.sys_downstream_conn.get_connection(),
                f"RESUME CCPR SUBSCRIPTION '{task_id}'")
            logger.info(f"[{task.name}] Resumed subscription")
            
            return True
            
        except Exception as e:
            logger.error(f"[{task.name}] Checkpoint failed: {e}")
            self.errors.append(str(e))
            # 尝试resume (需要重新查询task_id，用sys租户)
            try:
                result = execute_sql(self.sys_downstream_conn.get_connection(),
                    f"SELECT task_id FROM mo_catalog.mo_ccpr_log WHERE subscription_name='{task.pub_name}' AND drop_at IS NULL",
                    fetch=True)
                if result and result[0][0]:
                    execute_sql(self.sys_downstream_conn.get_connection(),
                        f"RESUME CCPR SUBSCRIPTION '{result[0][0]}'")
            except:
                pass
            return False
    
    def _execute_alter(self, task: CCPRTask, phase: LongTestPhase):
        """执行ALTER操作"""
        conn = task.upstream_conn.get_connection()
        db = task.db_name
        tbl = task.table_name
        
        try:
            if phase == LongTestPhase.PHASE1_DML_ADD_COLUMN:
                # ADD COLUMN (non-inplace)
                col_name = f"extra_col_{random_string(4)}"
                execute_sql(conn, f"ALTER TABLE {db}.{tbl} ADD COLUMN {col_name} VARCHAR(100)")
                task.added_columns.append(col_name)
                logger.info(f"[{task.name}] Added column: {col_name}")
                
            elif phase == LongTestPhase.PHASE2_DML_ADD_INDEX:
                # ADD INDEX (inplace)
                idx_name = f"idx_{random_string(6)}"
                execute_sql(conn, f"CREATE INDEX {idx_name} ON {db}.{tbl} (c_int)")
                task.added_indexes.append(idx_name)
                logger.info(f"[{task.name}] Added index: {idx_name}")
                
            elif phase == LongTestPhase.PHASE3_DML_ADD_FULLTEXT:
                # ADD FULLTEXT INDEX (inplace)
                idx_name = f"ftidx_{random_string(6)}"
                execute_sql_safe(conn, 
                    f"CREATE FULLTEXT INDEX {idx_name} ON {db}.{tbl} (c_text)",
                    ignore_errors=True)
                task.added_indexes.append(idx_name)
                logger.info(f"[{task.name}] Added fulltext index: {idx_name}")
                
            elif phase == LongTestPhase.PHASE4_DML_RENAME_COLUMN:
                # RENAME COLUMN (inplace)
                if task.added_columns:
                    old_name = task.added_columns[0]
                    new_name = f"renamed_{random_string(4)}"
                    execute_sql(conn, 
                        f"ALTER TABLE {db}.{tbl} RENAME COLUMN {old_name} TO {new_name}")
                    task.added_columns[0] = new_name
                    logger.info(f"[{task.name}] Renamed column: {old_name} -> {new_name}")
                    
            elif phase == LongTestPhase.PHASE5_DML_DROP_COLUMN:
                # DROP COLUMN (non-inplace)
                if task.added_columns:
                    col_name = task.added_columns.pop()
                    execute_sql(conn, f"ALTER TABLE {db}.{tbl} DROP COLUMN {col_name}")
                    logger.info(f"[{task.name}] Dropped column: {col_name}")
                    
            elif phase == LongTestPhase.PHASE6_DML_LOAD_COMMENT:
                # 大批量LOAD + CHANGE COMMENT
                logger.info(f"[{task.name}] Bulk loading data...")
                inserted_pks = []
                for _ in range(100):
                    pk = self._insert_row(conn, db, tbl, task.added_columns, task)
                    if pk:
                        inserted_pks.append(pk)
                logger.info(f"[{task.name}] Bulk load complete, inserted pks=[{inserted_pks[0]}..{inserted_pks[-1]}]")
                
                comment = f"Long test completed at {datetime.now()}"
                execute_sql(conn, f"ALTER TABLE {db}.{tbl} COMMENT = '{comment}'")
                logger.info(f"[{task.name}] Changed comment")
                
            elif phase == LongTestPhase.PHASE7_DELETE_ALL:
                # DELETE FROM 删除上游所有数据
                with task.pk_lock:
                    old_active_count = len(task.active_pks)
                    task.deleted_pks.update(task.active_pks)
                    task.active_pks.clear()
                
                execute_sql(conn, f"DELETE FROM {db}.{tbl}")
                logger.info(f"[{task.name}] DELETE ALL: removed {old_active_count} rows")
                
                # 重新插入一批数据，确保后续阶段有数据可用
                inserted_pks = []
                for _ in range(100):
                    pk = self._insert_row(conn, db, tbl, task.added_columns, task)
                    if pk:
                        inserted_pks.append(pk)
                logger.info(f"[{task.name}] Re-inserted 100 rows after DELETE ALL, pks=[{inserted_pks[0]}..{inserted_pks[-1]}]")
                
        except Exception as e:
            logger.error(f"[{task.name}] ALTER failed in {phase.name}: {e}")
            self.errors.append(str(e))
    
    def cleanup(self):
        """清理测试资源"""
        logger.info("Cleaning up...")
        
        for task in self.tasks:
            try:
                # 关闭task连接
                if task.upstream_conn:
                    task.upstream_conn.close()
                if task.downstream_conn:
                    task.downstream_conn.close()
                
                # 用sys删除publication
                if self.sys_upstream_conn:
                    execute_sql_safe(self.sys_upstream_conn.get_connection(),
                        f"DROP PUBLICATION IF EXISTS {task.pub_name}", ignore_errors=True)
                
                # 删除上游account（会自动删除其下的database）
                if self.sys_upstream_conn and task.upstream_account != "sys":
                    drop_account(self.sys_upstream_conn.get_connection(), task.upstream_account)
                # 上游也创建了down_account用于授权，也要删除
                if self.sys_upstream_conn and task.downstream_account != "sys":
                    drop_account(self.sys_upstream_conn.get_connection(), task.downstream_account)
                
                # 删除下游account
                if self.sys_downstream_conn and task.downstream_account != "sys":
                    drop_account(self.sys_downstream_conn.get_connection(), task.downstream_account)
                    
            except Exception as e:
                logger.warning(f"Cleanup error for {task.name}: {e}")
        
        # 关闭sys连接
        if self.sys_upstream_conn:
            self.sys_upstream_conn.close()
        if self.sys_downstream_conn:
            self.sys_downstream_conn.close()
        
        logger.info("Cleanup complete")
    
    def get_report(self) -> Dict[str, Any]:
        """获取测试报告"""
        return {
            "test_type": "long_test",
            "tasks": [
                {
                    "level": task.level.value,
                    "name": task.name,
                    "upstream_account": task.upstream_account,
                    "downstream_account": task.downstream_account,
                    "db_name": task.db_name,
                    "table_name": task.table_name,
                }
                for task in self.tasks
            ],
            "errors": self.errors,
            "success": len(self.errors) == 0,
        }


# =============================================================================
# 主函数
# =============================================================================

def setup_logging(log_file: str, log_level: str):
    """配置日志"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('pymysql').setLevel(logging.WARNING)


def run_long_test(config: TestConfig, duration: int, no_cleanup: bool = False) -> Dict[str, Any]:
    """运行长时间测试"""
    test = CCPRLongTest(config)
    error_exit = False
    
    try:
        if not test.setup():
            return {"success": False, "error": "Setup failed", "errors": test.errors}
        
        success = test.run(duration)
        report = test.get_report()
        report["duration_seconds"] = duration
        report["success"] = success and len(test.errors) == 0
        
        return report
    except SystemExit as e:
        error_exit = True
        logger.error(f"Test stopped due to error: {e}")
        logger.info("Skipping cleanup to preserve environment for debugging")
        raise
    finally:
        if error_exit:
            logger.info("Error exit - cleanup skipped")
        elif not no_cleanup:
            test.cleanup()
        else:
            logger.info("Skipping cleanup (--no-cleanup specified)")


def main():
    parser = argparse.ArgumentParser(
        description="CCPR Integration Test Runner (按TEST_PLAN.md实现)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 运行长时间测试 (8小时)
  python main.py --long-test
  
  # 运行长时间测试 (自定义时长, 如5分钟)
  python main.py --long-test --duration 300
  
  # 运行短测试
  python main.py --quick-test
  
  # 自定义集群端口
  python main.py --long-test --upstream-port 6001 --downstream-port 6002
        """
    )
    
    # 测试模式
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--long-test", action="store_true",
                           help="运行长时间测试 (默认8小时)")
    mode_group.add_argument("--quick-test", action="store_true",
                           help="运行短测试 (调用quick_test.py)")
    
    # 时长
    parser.add_argument("--duration", type=int, default=8*60*60,
                       help="测试时长(秒), 默认8小时=28800秒")
    
    # 集群配置
    parser.add_argument("--upstream-host", default="127.0.0.1")
    parser.add_argument("--upstream-port", type=int, default=6001)
    parser.add_argument("--downstream-host", default="127.0.0.1")
    parser.add_argument("--downstream-port", type=int, default=6002)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="111")
    
    # 测试配置
    parser.add_argument("--sync-interval", type=int, default=10,
                       help="CCPR同步间隔(秒)")
    parser.add_argument("--insert-interval", type=float, default=2.0,
                       help="DML操作间隔(秒)")
    
    # 日志
    parser.add_argument("--log-file", default="ccpr_test.log")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--report-file", default="ccpr_report.json")
    
    # 清理控制
    parser.add_argument("--no-cleanup", action="store_true",
                       help="测试结束后不清理环境（保留账户和数据库）")
    
    args = parser.parse_args()
    
    # 构建配置
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
    config.sync_interval = args.sync_interval
    config.insert_interval = args.insert_interval
    
    # 配置日志
    setup_logging(args.log_file, args.log_level)
    
    logger.info("="*60)
    logger.info("CCPR Integration Test")
    logger.info("="*60)
    logger.info(f"Upstream: {config.upstream.host}:{config.upstream.port}")
    logger.info(f"Downstream: {config.downstream.host}:{config.downstream.port}")
    
    # 运行测试
    if args.quick_test:
        # 调用quick_test.py
        logger.info("Running quick test...")
        import quick_test
        quick_test.main()
        return
    
    # 默认运行长时间测试
    logger.info(f"Running long test for {args.duration} seconds ({args.duration/3600:.1f} hours)")
    if args.no_cleanup:
        logger.info("Cleanup disabled - environment will be preserved after test")
    results = run_long_test(config, args.duration, no_cleanup=args.no_cleanup)
    
    # 保存报告
    with open(args.report_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"Report saved to {args.report_file}")
    
    # 打印摘要
    logger.info("\n" + "="*60)
    logger.info("TEST SUMMARY")
    logger.info("="*60)
    logger.info(f"Success: {results.get('success', False)}")
    logger.info(f"Errors: {len(results.get('errors', []))}")
    
    if results.get('errors'):
        logger.error("Errors encountered:")
        for err in results['errors'][:10]:
            logger.error(f"  - {err}")
    
    sys.exit(0 if results.get('success') else 1)


if __name__ == "__main__":
    main()
