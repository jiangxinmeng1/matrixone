#!/usr/bin/env python3
"""
CCPR Integration Test - Database Utilities

Database connection management and SQL execution utilities.
"""

import pymysql
import time
import logging
from typing import Optional, List, Any, Tuple, Dict
from contextlib import contextmanager
from dataclasses import dataclass

from config import ClusterConfig, SubscriptionState, IterationState

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Wrapper for database connection with retry logic"""
    
    def __init__(self, config: ClusterConfig, database: Optional[str] = None,
                 autocommit: bool = True, max_retries: int = 3):
        self.config = config
        self.database = database
        self.autocommit = autocommit
        self.max_retries = max_retries
        self._conn: Optional[pymysql.Connection] = None
    
    def connect(self) -> pymysql.Connection:
        """Establish connection with retry"""
        for attempt in range(self.max_retries):
            try:
                self._conn = pymysql.connect(
                    host=self.config.host,
                    port=self.config.port,
                    user=self.config.get_user_string(),
                    password=self.config.password,
                    database=self.database,
                    autocommit=self.autocommit,
                    charset='utf8mb4',
                    connect_timeout=30,
                    read_timeout=300,
                    write_timeout=300
                )
                return self._conn
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
    
    def close(self):
        """Close connection"""
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
    
    def get_connection(self) -> pymysql.Connection:
        """Get or create connection"""
        if self._conn is None or not self._conn.open:
            self.connect()
        return self._conn
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def execute_sql(conn: pymysql.Connection, sql: str, fetch: bool = False,
                params: Optional[Tuple] = None) -> Optional[List[Any]]:
    """Execute SQL with error handling"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            if fetch:
                return cursor.fetchall()
        return None
    except Exception as e:
        logger.error(f"SQL execution failed: {sql[:200]}... Error: {e}")
        raise


def execute_sql_safe(conn: pymysql.Connection, sql: str, fetch: bool = False,
                     ignore_errors: bool = False) -> Optional[List[Any]]:
    """Execute SQL with error handling, optionally ignoring errors"""
    try:
        return execute_sql(conn, sql, fetch)
    except Exception as e:
        if not ignore_errors:
            logger.error(f"SQL failed: {sql[:100]}... - {e}")
        return None


@dataclass
class CCPRStatus:
    """CCPR subscription status"""
    task_id: str
    subscription_name: str
    db_name: str
    table_name: str
    state: SubscriptionState
    iteration_state: IterationState
    iteration_lsn: int
    error_message: Optional[str]
    created_at: Any
    sync_level: str


def get_ccpr_status(conn: pymysql.Connection, task_id: Optional[str] = None,
                    subscription_name: Optional[str] = None) -> List[CCPRStatus]:
    """Query mo_ccpr_log for subscription status"""
    sql = """
        SELECT task_id, subscription_name, db_name, table_name, state, 
               iteration_state, iteration_lsn, error_message, created_at, sync_level
        FROM mo_catalog.mo_ccpr_log
        WHERE drop_at IS NULL
    """
    if task_id:
        sql += f" AND task_id = '{task_id}'"
    if subscription_name:
        sql += f" AND subscription_name = '{subscription_name}'"
    sql += " ORDER BY created_at DESC"
    
    results = execute_sql(conn, sql, fetch=True)
    statuses = []
    
    if results:
        for row in results:
            try:
                statuses.append(CCPRStatus(
                    task_id=row[0],
                    subscription_name=row[1],
                    db_name=row[2] or "",
                    table_name=row[3] or "",
                    state=SubscriptionState(row[4]) if row[4] is not None else SubscriptionState.RUNNING,
                    iteration_state=IterationState(row[5]) if row[5] is not None else IterationState.PENDING,
                    iteration_lsn=row[6] or 0,
                    error_message=row[7],
                    created_at=row[8],
                    sync_level=row[9] or "table"
                ))
            except Exception as e:
                logger.warning(f"Failed to parse CCPR status row: {e}")
    
    return statuses


def wait_for_iteration_complete(conn: pymysql.Connection, task_id: str,
                                 timeout: int = 120, poll_interval: float = 2.0) -> bool:
    """Wait for an iteration to complete"""
    start_time = time.time()
    initial_lsn = None
    
    while time.time() - start_time < timeout:
        statuses = get_ccpr_status(conn, task_id=task_id)
        if statuses:
            status = statuses[0]
            if initial_lsn is None:
                initial_lsn = status.iteration_lsn
            
            if status.state == SubscriptionState.ERROR:
                logger.error(f"Subscription {task_id} in error state: {status.error_message}")
                return False
            
            if status.iteration_state == IterationState.COMPLETED:
                if status.iteration_lsn > initial_lsn:
                    return True
        
        time.sleep(poll_interval)
    
    logger.warning(f"Timeout waiting for iteration to complete for task {task_id}")
    return False


def trigger_checkpoint(conn: pymysql.Connection) -> bool:
    """Trigger checkpoint on cluster"""
    try:
        execute_sql(conn, "SELECT mo_ctl('dn', 'checkpoint', '')")
        logger.debug("Checkpoint triggered")
        return True
    except Exception as e:
        logger.warning(f"Failed to trigger checkpoint: {e}")
        return False


def get_table_count(conn: pymysql.Connection, db_name: str, table_name: str) -> int:
    """Get row count of a table"""
    try:
        result = execute_sql(conn, f"SELECT COUNT(*) FROM `{db_name}`.`{table_name}`", fetch=True)
        return result[0][0] if result else 0
    except Exception as e:
        logger.warning(f"Failed to get count for {db_name}.{table_name}: {e}")
        return -1


def verify_data_consistency(upstream_conn: pymysql.Connection,
                            downstream_conn: pymysql.Connection,
                            db_name: str, table_name: str) -> Tuple[bool, str]:
    """Verify data consistency between upstream and downstream"""
    upstream_count = get_table_count(upstream_conn, db_name, table_name)
    downstream_count = get_table_count(downstream_conn, db_name, table_name)
    
    if upstream_count < 0 or downstream_count < 0:
        return False, f"Failed to get counts"
    
    if upstream_count != downstream_count:
        return False, f"Count mismatch: upstream={upstream_count}, downstream={downstream_count}"
    
    # For more thorough verification, compare checksums
    try:
        # Get primary key column
        pk_result = execute_sql(upstream_conn, f"""
            SELECT attname FROM mo_catalog.mo_columns 
            WHERE att_database = '{db_name}' AND att_relname = '{table_name}'
            AND att_constraint_type = 'p'
            LIMIT 1
        """, fetch=True)
        
        if pk_result:
            pk_col = pk_result[0][0]
            # Compare min/max of PK
            upstream_minmax = execute_sql(upstream_conn, 
                f"SELECT MIN(`{pk_col}`), MAX(`{pk_col}`) FROM `{db_name}`.`{table_name}`", fetch=True)
            downstream_minmax = execute_sql(downstream_conn,
                f"SELECT MIN(`{pk_col}`), MAX(`{pk_col}`) FROM `{db_name}`.`{table_name}`", fetch=True)
            
            if upstream_minmax != downstream_minmax:
                return False, f"PK range mismatch"
    except Exception as e:
        logger.debug(f"Extended verification failed: {e}")
    
    return True, f"Counts match: {upstream_count}"


def create_account(conn: pymysql.Connection, account_name: str, 
                   admin_user: str = "admin", admin_password: str = "111") -> bool:
    """Create a new account"""
    try:
        execute_sql(conn, f"DROP ACCOUNT IF EXISTS {account_name}")
        execute_sql(conn, f"CREATE ACCOUNT {account_name} ADMIN_NAME '{admin_user}' IDENTIFIED BY '{admin_password}'")
        logger.info(f"Created account: {account_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to create account {account_name}: {e}")
        return False


def drop_account(conn: pymysql.Connection, account_name: str) -> bool:
    """Drop an account"""
    try:
        execute_sql(conn, f"DROP ACCOUNT IF EXISTS {account_name}")
        logger.info(f"Dropped account: {account_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to drop account {account_name}: {e}")
        return False
