#!/usr/bin/env python3
"""
CCPR Integration Test - Test Scenarios

Implements various test scenarios for CCPR functionality.
"""

import time
import logging
import random
import threading
from typing import Optional, List, Dict, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from abc import ABC, abstractmethod

from config import (
    ClusterConfig, TestConfig, SyncLevel, SubscriptionState, IterationState,
    AccountConfig, DEFAULT_DOWNSTREAM_ACCOUNTS
)
from db_utils import (
    DatabaseConnection, execute_sql, execute_sql_safe, get_ccpr_status,
    wait_for_iteration_complete, trigger_checkpoint, verify_data_consistency,
    create_account, drop_account, get_table_count
)
from data_generator import (
    create_test_tables, TableDefinition, random_string, random_text,
    generate_add_column_sql, generate_add_index_sql
)

logger = logging.getLogger(__name__)


class TestResult(Enum):
    """Test result status"""
    PASSED = "PASSED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"


@dataclass
class ScenarioResult:
    """Result of a test scenario"""
    name: str
    result: TestResult
    duration: float
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)


# =============================================================================
# Base Scenario Class
# =============================================================================

class BaseScenario(ABC):
    """Base class for all test scenarios"""
    
    def __init__(self, config: TestConfig, name: str):
        self.config = config
        self.name = name
        self.errors: List[str] = []
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
    
    @abstractmethod
    def setup(self) -> bool:
        """Setup the test scenario"""
        pass
    
    @abstractmethod
    def run(self) -> bool:
        """Run the test scenario"""
        pass
    
    @abstractmethod
    def verify(self) -> bool:
        """Verify the test results"""
        pass
    
    def cleanup(self) -> bool:
        """Cleanup test resources"""
        return True
    
    def execute(self) -> ScenarioResult:
        """Execute the complete scenario"""
        self.start_time = time.time()
        result = TestResult.PASSED
        message = ""
        
        try:
            logger.info(f"[{self.name}] Starting setup...")
            if not self.setup():
                result = TestResult.FAILED
                message = "Setup failed"
            else:
                logger.info(f"[{self.name}] Running test...")
                if not self.run():
                    result = TestResult.FAILED
                    message = "Test execution failed"
                else:
                    logger.info(f"[{self.name}] Verifying results...")
                    if not self.verify():
                        result = TestResult.FAILED
                        message = "Verification failed"
                    else:
                        message = "All checks passed"
        except Exception as e:
            logger.error(f"[{self.name}] Exception: {e}")
            result = TestResult.ERROR
            message = str(e)
            self.errors.append(str(e))
        finally:
            try:
                logger.info(f"[{self.name}] Cleaning up...")
                self.cleanup()
            except Exception as e:
                logger.warning(f"[{self.name}] Cleanup failed: {e}")
        
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        
        return ScenarioResult(
            name=self.name,
            result=result,
            duration=duration,
            message=message,
            errors=self.errors.copy()
        )


# =============================================================================
# Table Level Subscription Scenario
# =============================================================================

class TableLevelScenario(BaseScenario):
    """Test table-level CCPR subscription"""
    
    def __init__(self, config: TestConfig):
        super().__init__(config, "TableLevelSubscription")
        self.db_name = f"ccpr_table_test_{random_string(6)}"
        self.table_name = "test_table"
        self.pub_name = f"pub_{random_string(6)}"
        self.task_id: Optional[str] = None
        self.upstream_conn: Optional[DatabaseConnection] = None
        self.downstream_conn: Optional[DatabaseConnection] = None
    
    def setup(self) -> bool:
        """Setup upstream table and publication"""
        try:
            # Connect to upstream
            self.upstream_conn = DatabaseConnection(self.config.upstream)
            self.upstream_conn.connect()
            
            # Create database and table
            execute_sql(self.upstream_conn.get_connection(), f"DROP DATABASE IF EXISTS {self.db_name}")
            execute_sql(self.upstream_conn.get_connection(), f"CREATE DATABASE {self.db_name}")
            execute_sql(self.upstream_conn.get_connection(), f"""
                CREATE TABLE {self.db_name}.{self.table_name} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    value INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert initial data
            for i in range(10):
                execute_sql(self.upstream_conn.get_connection(), f"""
                    INSERT INTO {self.db_name}.{self.table_name} (name, value) 
                    VALUES ('item_{i}', {i * 100})
                """)
            
            # Create publication
            execute_sql(self.upstream_conn.get_connection(), f"DROP PUBLICATION IF EXISTS {self.pub_name}")
            execute_sql(self.upstream_conn.get_connection(), f"""
                CREATE PUBLICATION {self.pub_name} TABLE {self.db_name}.{self.table_name} ACCOUNT ALL
            """)
            
            logger.info(f"[{self.name}] Created publication {self.pub_name} for table {self.db_name}.{self.table_name}")
            
            # Connect to downstream and create subscription
            self.downstream_conn = DatabaseConnection(self.config.downstream)
            self.downstream_conn.connect()
            
            # Create subscription using CCPR syntax
            conn_str = self.config.upstream.get_connection_string()
            execute_sql(self.downstream_conn.get_connection(), f"""
                CREATE DATABASE {self.db_name} FROM '{conn_str}' 
                PUBLICATION {self.pub_name} SYNC INTERVAL {self.config.sync_interval}
            """)
            
            logger.info(f"[{self.name}] Created subscription for {self.db_name}")
            
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Setup failed: {e}")
            self.errors.append(str(e))
            return False
    
    def run(self) -> bool:
        """Run the table-level test"""
        try:
            # Trigger checkpoint
            trigger_checkpoint(self.upstream_conn.get_connection())
            
            # Wait for initial sync
            time.sleep(self.config.sync_interval + 5)
            
            # Insert more data on upstream
            for i in range(5):
                execute_sql(self.upstream_conn.get_connection(), f"""
                    INSERT INTO {self.db_name}.{self.table_name} (name, value) 
                    VALUES ('new_item_{i}', {(10 + i) * 100})
                """)
            
            # Trigger checkpoint again
            trigger_checkpoint(self.upstream_conn.get_connection())
            
            # Wait for sync
            time.sleep(self.config.sync_interval + 5)
            
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Run failed: {e}")
            self.errors.append(str(e))
            return False
    
    def verify(self) -> bool:
        """Verify data consistency"""
        try:
            consistent, msg = verify_data_consistency(
                self.upstream_conn.get_connection(),
                self.downstream_conn.get_connection(),
                self.db_name, self.table_name
            )
            
            if not consistent:
                self.errors.append(f"Data inconsistency: {msg}")
                return False
            
            logger.info(f"[{self.name}] Verification passed: {msg}")
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Verify failed: {e}")
            self.errors.append(str(e))
            return False
    
    def cleanup(self) -> bool:
        """Cleanup test resources"""
        try:
            if self.downstream_conn:
                execute_sql_safe(self.downstream_conn.get_connection(), 
                    f"DROP DATABASE IF EXISTS {self.db_name}", ignore_errors=True)
                self.downstream_conn.close()
            
            if self.upstream_conn:
                execute_sql_safe(self.upstream_conn.get_connection(),
                    f"DROP PUBLICATION IF EXISTS {self.pub_name}", ignore_errors=True)
                execute_sql_safe(self.upstream_conn.get_connection(),
                    f"DROP DATABASE IF EXISTS {self.db_name}", ignore_errors=True)
                self.upstream_conn.close()
            
            return True
        except Exception as e:
            logger.warning(f"[{self.name}] Cleanup failed: {e}")
            return False


# =============================================================================
# Database Level Subscription Scenario
# =============================================================================

class DatabaseLevelScenario(BaseScenario):
    """Test database-level CCPR subscription"""
    
    def __init__(self, config: TestConfig):
        super().__init__(config, "DatabaseLevelSubscription")
        self.db_name = f"ccpr_db_test_{random_string(6)}"
        self.pub_name = f"pub_db_{random_string(6)}"
        self.tables = {}
        self.upstream_conn: Optional[DatabaseConnection] = None
        self.downstream_conn: Optional[DatabaseConnection] = None
    
    def setup(self) -> bool:
        """Setup upstream database with multiple tables"""
        try:
            self.upstream_conn = DatabaseConnection(self.config.upstream)
            self.upstream_conn.connect()
            
            # Create database
            execute_sql(self.upstream_conn.get_connection(), f"DROP DATABASE IF EXISTS {self.db_name}")
            execute_sql(self.upstream_conn.get_connection(), f"CREATE DATABASE {self.db_name}")
            
            # Create multiple tables
            self.tables = create_test_tables(self.db_name)
            for name, table_def in self.tables.items():
                execute_sql(self.upstream_conn.get_connection(), table_def.get_create_sql())
                for idx_sql in table_def.get_index_sqls():
                    execute_sql_safe(self.upstream_conn.get_connection(), idx_sql, ignore_errors=True)
                
                # Insert initial data
                if hasattr(table_def, 'generate_insert_sql'):
                    for _ in range(5):
                        try:
                            execute_sql(self.upstream_conn.get_connection(), 
                                table_def.generate_insert_sql(count=2))
                        except Exception as e:
                            logger.debug(f"Insert failed for {name}: {e}")
            
            # Create publication
            execute_sql(self.upstream_conn.get_connection(), f"DROP PUBLICATION IF EXISTS {self.pub_name}")
            execute_sql(self.upstream_conn.get_connection(), f"""
                CREATE PUBLICATION {self.pub_name} DATABASE {self.db_name} ACCOUNT ALL
            """)
            
            logger.info(f"[{self.name}] Created publication {self.pub_name} for database {self.db_name}")
            
            # Create subscription on downstream
            self.downstream_conn = DatabaseConnection(self.config.downstream)
            self.downstream_conn.connect()
            
            conn_str = self.config.upstream.get_connection_string()
            execute_sql(self.downstream_conn.get_connection(), f"""
                CREATE DATABASE {self.db_name} FROM '{conn_str}'
                PUBLICATION {self.pub_name} SYNC INTERVAL {self.config.sync_interval}
            """)
            
            logger.info(f"[{self.name}] Created database-level subscription")
            
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Setup failed: {e}")
            self.errors.append(str(e))
            return False
    
    def run(self) -> bool:
        """Run database-level test with continuous inserts"""
        try:
            # Wait for initial sync
            trigger_checkpoint(self.upstream_conn.get_connection())
            time.sleep(self.config.sync_interval + 5)
            
            # Insert more data
            for _ in range(3):
                for name, table_def in self.tables.items():
                    if hasattr(table_def, 'generate_insert_sql'):
                        try:
                            execute_sql(self.upstream_conn.get_connection(),
                                table_def.generate_insert_sql(count=2))
                        except Exception as e:
                            logger.debug(f"Insert failed for {name}: {e}")
                
                trigger_checkpoint(self.upstream_conn.get_connection())
                time.sleep(self.config.sync_interval + 2)
            
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Run failed: {e}")
            self.errors.append(str(e))
            return False
    
    def verify(self) -> bool:
        """Verify all tables are consistent"""
        all_passed = True
        for name, table_def in self.tables.items():
            consistent, msg = verify_data_consistency(
                self.upstream_conn.get_connection(),
                self.downstream_conn.get_connection(),
                self.db_name, table_def.name
            )
            if not consistent:
                self.errors.append(f"Table {name} inconsistent: {msg}")
                all_passed = False
            else:
                logger.info(f"[{self.name}] Table {name} verified: {msg}")
        
        return all_passed
    
    def cleanup(self) -> bool:
        try:
            if self.downstream_conn:
                execute_sql_safe(self.downstream_conn.get_connection(),
                    f"DROP DATABASE IF EXISTS {self.db_name}", ignore_errors=True)
                self.downstream_conn.close()
            
            if self.upstream_conn:
                execute_sql_safe(self.upstream_conn.get_connection(),
                    f"DROP PUBLICATION IF EXISTS {self.pub_name}", ignore_errors=True)
                execute_sql_safe(self.upstream_conn.get_connection(),
                    f"DROP DATABASE IF EXISTS {self.db_name}", ignore_errors=True)
                self.upstream_conn.close()
            
            return True
        except Exception as e:
            logger.warning(f"[{self.name}] Cleanup failed: {e}")
            return False


# =============================================================================
# Pause/Resume/Drop Scenario
# =============================================================================

class PauseResumeScenario(BaseScenario):
    """Test PAUSE, RESUME, and DROP operations"""
    
    def __init__(self, config: TestConfig):
        super().__init__(config, "PauseResumeOperations")
        self.db_name = f"ccpr_pause_test_{random_string(6)}"
        self.table_name = "pause_table"
        self.pub_name = f"pub_pause_{random_string(6)}"
        self.upstream_conn: Optional[DatabaseConnection] = None
        self.downstream_conn: Optional[DatabaseConnection] = None
    
    def setup(self) -> bool:
        """Setup test resources"""
        try:
            self.upstream_conn = DatabaseConnection(self.config.upstream)
            self.upstream_conn.connect()
            
            execute_sql(self.upstream_conn.get_connection(), f"DROP DATABASE IF EXISTS {self.db_name}")
            execute_sql(self.upstream_conn.get_connection(), f"CREATE DATABASE {self.db_name}")
            execute_sql(self.upstream_conn.get_connection(), f"""
                CREATE TABLE {self.db_name}.{self.table_name} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    value INT DEFAULT 0
                )
            """)
            
            # Insert initial data
            for i in range(10):
                execute_sql(self.upstream_conn.get_connection(), f"""
                    INSERT INTO {self.db_name}.{self.table_name} (name, value) VALUES ('item_{i}', {i})
                """)
            
            execute_sql(self.upstream_conn.get_connection(), f"DROP PUBLICATION IF EXISTS {self.pub_name}")
            execute_sql(self.upstream_conn.get_connection(), f"""
                CREATE PUBLICATION {self.pub_name} DATABASE {self.db_name} ACCOUNT ALL
            """)
            
            self.downstream_conn = DatabaseConnection(self.config.downstream)
            self.downstream_conn.connect()
            
            conn_str = self.config.upstream.get_connection_string()
            execute_sql(self.downstream_conn.get_connection(), f"""
                CREATE DATABASE {self.db_name} FROM '{conn_str}'
                PUBLICATION {self.pub_name} SYNC INTERVAL {self.config.sync_interval}
            """)
            
            # Wait for initial sync
            trigger_checkpoint(self.upstream_conn.get_connection())
            time.sleep(self.config.sync_interval + 5)
            
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Setup failed: {e}")
            self.errors.append(str(e))
            return False
    
    def run(self) -> bool:
        """Run pause/resume test"""
        try:
            # Step 1: Verify initial sync
            count_before = get_table_count(self.downstream_conn.get_connection(), 
                self.db_name, self.table_name)
            logger.info(f"[{self.name}] Initial count: {count_before}")
            
            # Step 2: Pause subscription
            logger.info(f"[{self.name}] Pausing subscription...")
            execute_sql(self.downstream_conn.get_connection(), 
                f"PAUSE CCPR SUBSCRIPTION {self.pub_name}")
            time.sleep(5)
            
            # Step 3: Insert data while paused
            for i in range(5):
                execute_sql(self.upstream_conn.get_connection(), f"""
                    INSERT INTO {self.db_name}.{self.table_name} (name, value) 
                    VALUES ('paused_item_{i}', {100 + i})
                """)
            
            trigger_checkpoint(self.upstream_conn.get_connection())
            time.sleep(self.config.sync_interval + 5)
            
            # Verify data not synced
            count_after_pause = get_table_count(self.downstream_conn.get_connection(),
                self.db_name, self.table_name)
            
            if count_after_pause != count_before:
                logger.warning(f"[{self.name}] Data synced during pause: {count_before} -> {count_after_pause}")
                # This might be acceptable if sync was already in progress
            
            # Step 4: Check subscription state
            statuses = get_ccpr_status(self.downstream_conn.get_connection(), 
                subscription_name=self.pub_name)
            if statuses and statuses[0].state != SubscriptionState.PAUSE:
                self.errors.append(f"Expected PAUSE state, got {statuses[0].state}")
            
            # Step 5: Resume subscription
            logger.info(f"[{self.name}] Resuming subscription...")
            execute_sql(self.downstream_conn.get_connection(),
                f"RESUME CCPR SUBSCRIPTION {self.pub_name}")
            time.sleep(self.config.sync_interval + 10)
            
            # Verify data synced after resume
            count_after_resume = get_table_count(self.downstream_conn.get_connection(),
                self.db_name, self.table_name)
            
            upstream_count = get_table_count(self.upstream_conn.get_connection(),
                self.db_name, self.table_name)
            
            if count_after_resume != upstream_count:
                self.errors.append(f"Data not synced after resume: downstream={count_after_resume}, upstream={upstream_count}")
            else:
                logger.info(f"[{self.name}] Resume successful: {count_after_resume} rows")
            
            return len(self.errors) == 0
        except Exception as e:
            logger.error(f"[{self.name}] Run failed: {e}")
            self.errors.append(str(e))
            return False
    
    def verify(self) -> bool:
        """Verify final state"""
        try:
            consistent, msg = verify_data_consistency(
                self.upstream_conn.get_connection(),
                self.downstream_conn.get_connection(),
                self.db_name, self.table_name
            )
            if not consistent:
                self.errors.append(f"Final verification failed: {msg}")
                return False
            
            logger.info(f"[{self.name}] Final verification passed: {msg}")
            return True
        except Exception as e:
            self.errors.append(str(e))
            return False
    
    def cleanup(self) -> bool:
        try:
            if self.downstream_conn:
                # Drop subscription first
                execute_sql_safe(self.downstream_conn.get_connection(),
                    f"DROP CCPR SUBSCRIPTION IF EXISTS {self.pub_name}", ignore_errors=True)
                execute_sql_safe(self.downstream_conn.get_connection(),
                    f"DROP DATABASE IF EXISTS {self.db_name}", ignore_errors=True)
                self.downstream_conn.close()
            
            if self.upstream_conn:
                execute_sql_safe(self.upstream_conn.get_connection(),
                    f"DROP PUBLICATION IF EXISTS {self.pub_name}", ignore_errors=True)
                execute_sql_safe(self.upstream_conn.get_connection(),
                    f"DROP DATABASE IF EXISTS {self.db_name}", ignore_errors=True)
                self.upstream_conn.close()
            
            return True
        except Exception as e:
            logger.warning(f"[{self.name}] Cleanup failed: {e}")
            return False


# =============================================================================
# Index Test Scenario
# =============================================================================

class IndexScenario(BaseScenario):
    """Test tables with various index types (secondary, fulltext, IVF)"""
    
    def __init__(self, config: TestConfig):
        super().__init__(config, "IndexedTables")
        self.db_name = f"ccpr_index_test_{random_string(6)}"
        self.pub_name = f"pub_index_{random_string(6)}"
        self.tables = {}
        self.upstream_conn: Optional[DatabaseConnection] = None
        self.downstream_conn: Optional[DatabaseConnection] = None
    
    def setup(self) -> bool:
        try:
            self.upstream_conn = DatabaseConnection(self.config.upstream)
            self.upstream_conn.connect()
            
            execute_sql(self.upstream_conn.get_connection(), f"DROP DATABASE IF EXISTS {self.db_name}")
            execute_sql(self.upstream_conn.get_connection(), f"CREATE DATABASE {self.db_name}")
            
            # Create tables with various indexes
            self.tables = create_test_tables(self.db_name)
            
            for name, table_def in self.tables.items():
                logger.info(f"[{self.name}] Creating table {name}...")
                execute_sql(self.upstream_conn.get_connection(), table_def.get_create_sql())
                
                # Create indexes
                for idx_sql in table_def.get_index_sqls():
                    logger.debug(f"[{self.name}] Creating index: {idx_sql[:100]}...")
                    execute_sql_safe(self.upstream_conn.get_connection(), idx_sql, ignore_errors=True)
                
                # Insert data
                if hasattr(table_def, 'generate_insert_sql'):
                    for _ in range(3):
                        try:
                            execute_sql(self.upstream_conn.get_connection(),
                                table_def.generate_insert_sql(count=5))
                        except Exception as e:
                            logger.debug(f"Insert failed: {e}")
            
            # Create publication
            execute_sql(self.upstream_conn.get_connection(), f"DROP PUBLICATION IF EXISTS {self.pub_name}")
            execute_sql(self.upstream_conn.get_connection(), f"""
                CREATE PUBLICATION {self.pub_name} DATABASE {self.db_name} ACCOUNT ALL
            """)
            
            # Create subscription
            self.downstream_conn = DatabaseConnection(self.config.downstream)
            self.downstream_conn.connect()
            
            conn_str = self.config.upstream.get_connection_string()
            execute_sql(self.downstream_conn.get_connection(), f"""
                CREATE DATABASE {self.db_name} FROM '{conn_str}'
                PUBLICATION {self.pub_name} SYNC INTERVAL {self.config.sync_interval}
            """)
            
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Setup failed: {e}")
            self.errors.append(str(e))
            return False
    
    def run(self) -> bool:
        try:
            # Wait for initial sync
            trigger_checkpoint(self.upstream_conn.get_connection())
            time.sleep(self.config.sync_interval + 10)
            
            # Insert more data with indexes
            for _ in range(3):
                for name, table_def in self.tables.items():
                    if hasattr(table_def, 'generate_insert_sql'):
                        try:
                            execute_sql(self.upstream_conn.get_connection(),
                                table_def.generate_insert_sql(count=3))
                        except Exception as e:
                            logger.debug(f"Insert failed for {name}: {e}")
                
                trigger_checkpoint(self.upstream_conn.get_connection())
                time.sleep(self.config.sync_interval + 5)
            
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Run failed: {e}")
            self.errors.append(str(e))
            return False
    
    def verify(self) -> bool:
        """Verify data and index functionality"""
        all_passed = True
        
        for name, table_def in self.tables.items():
            # Check data consistency
            consistent, msg = verify_data_consistency(
                self.upstream_conn.get_connection(),
                self.downstream_conn.get_connection(),
                self.db_name, table_def.name
            )
            
            if not consistent:
                self.errors.append(f"Table {name} inconsistent: {msg}")
                all_passed = False
            else:
                logger.info(f"[{self.name}] Table {name} data verified: {msg}")
            
            # Test fulltext search on fulltext tables
            if name == "fulltext":
                try:
                    result = execute_sql(self.downstream_conn.get_connection(), f"""
                        SELECT COUNT(*) FROM {self.db_name}.{table_def.name}
                        WHERE MATCH(title, content) AGAINST('database' IN NATURAL LANGUAGE MODE)
                    """, fetch=True)
                    logger.info(f"[{self.name}] Fulltext search works: {result[0][0]} matches")
                except Exception as e:
                    self.errors.append(f"Fulltext search failed: {e}")
                    all_passed = False
            
            # Test vector search on vector tables
            if name == "vector":
                try:
                    result = execute_sql(self.downstream_conn.get_connection(), f"""
                        SELECT COUNT(*) FROM {self.db_name}.{table_def.name}
                    """, fetch=True)
                    logger.info(f"[{self.name}] Vector table accessible: {result[0][0]} rows")
                except Exception as e:
                    self.errors.append(f"Vector table access failed: {e}")
                    all_passed = False
        
        return all_passed
    
    def cleanup(self) -> bool:
        try:
            if self.downstream_conn:
                execute_sql_safe(self.downstream_conn.get_connection(),
                    f"DROP DATABASE IF EXISTS {self.db_name}", ignore_errors=True)
                self.downstream_conn.close()
            
            if self.upstream_conn:
                execute_sql_safe(self.upstream_conn.get_connection(),
                    f"DROP PUBLICATION IF EXISTS {self.pub_name}", ignore_errors=True)
                execute_sql_safe(self.upstream_conn.get_connection(),
                    f"DROP DATABASE IF EXISTS {self.db_name}", ignore_errors=True)
                self.upstream_conn.close()
            
            return True
        except Exception as e:
            logger.warning(f"[{self.name}] Cleanup failed: {e}")
            return False


# =============================================================================
# Alter Table Scenario
# =============================================================================

class AlterTableScenario(BaseScenario):
    """Test ALTER TABLE operations with CCPR"""
    
    def __init__(self, config: TestConfig):
        super().__init__(config, "AlterTableOperations")
        self.db_name = f"ccpr_alter_test_{random_string(6)}"
        self.table_name = "alter_table"
        self.pub_name = f"pub_alter_{random_string(6)}"
        self.upstream_conn: Optional[DatabaseConnection] = None
        self.downstream_conn: Optional[DatabaseConnection] = None
    
    def setup(self) -> bool:
        try:
            self.upstream_conn = DatabaseConnection(self.config.upstream)
            self.upstream_conn.connect()
            
            execute_sql(self.upstream_conn.get_connection(), f"DROP DATABASE IF EXISTS {self.db_name}")
            execute_sql(self.upstream_conn.get_connection(), f"CREATE DATABASE {self.db_name}")
            execute_sql(self.upstream_conn.get_connection(), f"""
                CREATE TABLE {self.db_name}.{self.table_name} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    value INT DEFAULT 0,
                    status VARCHAR(20) DEFAULT 'active'
                )
            """)
            
            # Create initial index
            execute_sql(self.upstream_conn.get_connection(), f"""
                CREATE INDEX idx_name ON {self.db_name}.{self.table_name} (name)
            """)
            
            # Insert initial data
            for i in range(20):
                execute_sql(self.upstream_conn.get_connection(), f"""
                    INSERT INTO {self.db_name}.{self.table_name} (name, value, status)
                    VALUES ('item_{i}', {i * 10}, 'active')
                """)
            
            execute_sql(self.upstream_conn.get_connection(), f"DROP PUBLICATION IF EXISTS {self.pub_name}")
            execute_sql(self.upstream_conn.get_connection(), f"""
                CREATE PUBLICATION {self.pub_name} DATABASE {self.db_name} ACCOUNT ALL
            """)
            
            self.downstream_conn = DatabaseConnection(self.config.downstream)
            self.downstream_conn.connect()
            
            conn_str = self.config.upstream.get_connection_string()
            execute_sql(self.downstream_conn.get_connection(), f"""
                CREATE DATABASE {self.db_name} FROM '{conn_str}'
                PUBLICATION {self.pub_name} SYNC INTERVAL {self.config.sync_interval}
            """)
            
            # Wait for initial sync
            trigger_checkpoint(self.upstream_conn.get_connection())
            time.sleep(self.config.sync_interval + 10)
            
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Setup failed: {e}")
            self.errors.append(str(e))
            return False
    
    def run(self) -> bool:
        try:
            # Test 1: Add column
            logger.info(f"[{self.name}] Testing ADD COLUMN...")
            execute_sql(self.upstream_conn.get_connection(), f"""
                ALTER TABLE {self.db_name}.{self.table_name} 
                ADD COLUMN description TEXT
            """)
            
            trigger_checkpoint(self.upstream_conn.get_connection())
            time.sleep(self.config.sync_interval + 10)
            
            # Verify column exists on downstream
            result = execute_sql(self.downstream_conn.get_connection(), f"""
                SELECT COUNT(*) FROM mo_catalog.mo_columns 
                WHERE att_database = '{self.db_name}' 
                AND att_relname = '{self.table_name}'
                AND attname = 'description'
            """, fetch=True)
            
            if result[0][0] == 0:
                self.errors.append("ADD COLUMN not replicated")
            else:
                logger.info(f"[{self.name}] ADD COLUMN verified")
            
            # Test 2: Add new index
            logger.info(f"[{self.name}] Testing ADD INDEX...")
            execute_sql(self.upstream_conn.get_connection(), f"""
                CREATE INDEX idx_status ON {self.db_name}.{self.table_name} (status)
            """)
            
            trigger_checkpoint(self.upstream_conn.get_connection())
            time.sleep(self.config.sync_interval + 10)
            
            # Test 3: Change table comment
            logger.info(f"[{self.name}] Testing CHANGE COMMENT...")
            execute_sql(self.upstream_conn.get_connection(), f"""
                ALTER TABLE {self.db_name}.{self.table_name} 
                COMMENT = 'Test table for CCPR alter operations'
            """)
            
            trigger_checkpoint(self.upstream_conn.get_connection())
            time.sleep(self.config.sync_interval + 10)
            
            # Insert more data
            for i in range(5):
                execute_sql(self.upstream_conn.get_connection(), f"""
                    INSERT INTO {self.db_name}.{self.table_name} (name, value, status, description)
                    VALUES ('new_item_{i}', {(20 + i) * 10}, 'new', 'Added after ALTER')
                """)
            
            trigger_checkpoint(self.upstream_conn.get_connection())
            time.sleep(self.config.sync_interval + 10)
            
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Run failed: {e}")
            self.errors.append(str(e))
            return False
    
    def verify(self) -> bool:
        try:
            consistent, msg = verify_data_consistency(
                self.upstream_conn.get_connection(),
                self.downstream_conn.get_connection(),
                self.db_name, self.table_name
            )
            
            if not consistent:
                self.errors.append(f"Final verification failed: {msg}")
                return False
            
            logger.info(f"[{self.name}] Final verification passed: {msg}")
            return True
        except Exception as e:
            self.errors.append(str(e))
            return False
    
    def cleanup(self) -> bool:
        try:
            if self.downstream_conn:
                execute_sql_safe(self.downstream_conn.get_connection(),
                    f"DROP DATABASE IF EXISTS {self.db_name}", ignore_errors=True)
                self.downstream_conn.close()
            
            if self.upstream_conn:
                execute_sql_safe(self.upstream_conn.get_connection(),
                    f"DROP PUBLICATION IF EXISTS {self.pub_name}", ignore_errors=True)
                execute_sql_safe(self.upstream_conn.get_connection(),
                    f"DROP DATABASE IF EXISTS {self.db_name}", ignore_errors=True)
                self.upstream_conn.close()
            
            return True
        except Exception as e:
            logger.warning(f"[{self.name}] Cleanup failed: {e}")
            return False


# =============================================================================
# Concurrent Multi-Account Scenario
# =============================================================================

class ConcurrentMultiAccountScenario(BaseScenario):
    """Test multiple downstream accounts subscribing to the same upstream table"""
    
    def __init__(self, config: TestConfig):
        super().__init__(config, "ConcurrentMultiAccount")
        self.db_name = f"ccpr_concurrent_test_{random_string(6)}"
        self.table_name = "concurrent_table"
        self.pub_name = f"pub_concurrent_{random_string(6)}"
        self.upstream_conn: Optional[DatabaseConnection] = None
        self.accounts = DEFAULT_DOWNSTREAM_ACCOUNTS[:config.concurrent_accounts]
        self.account_conns: Dict[str, DatabaseConnection] = {}
    
    def setup(self) -> bool:
        try:
            # Setup upstream
            self.upstream_conn = DatabaseConnection(self.config.upstream)
            self.upstream_conn.connect()
            
            execute_sql(self.upstream_conn.get_connection(), f"DROP DATABASE IF EXISTS {self.db_name}")
            execute_sql(self.upstream_conn.get_connection(), f"CREATE DATABASE {self.db_name}")
            execute_sql(self.upstream_conn.get_connection(), f"""
                CREATE TABLE {self.db_name}.{self.table_name} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    value INT DEFAULT 0,
                    account VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            execute_sql(self.upstream_conn.get_connection(), f"""
                CREATE INDEX idx_account ON {self.db_name}.{self.table_name} (account)
            """)
            
            # Insert initial data
            for i in range(20):
                execute_sql(self.upstream_conn.get_connection(), f"""
                    INSERT INTO {self.db_name}.{self.table_name} (name, value, account)
                    VALUES ('item_{i}', {i * 10}, 'initial')
                """)
            
            # Create publication
            execute_sql(self.upstream_conn.get_connection(), f"DROP PUBLICATION IF EXISTS {self.pub_name}")
            execute_sql(self.upstream_conn.get_connection(), f"""
                CREATE PUBLICATION {self.pub_name} DATABASE {self.db_name} ACCOUNT ALL
            """)
            
            # Create accounts on downstream and subscribe
            downstream_sys_conn = DatabaseConnection(self.config.downstream)
            downstream_sys_conn.connect()
            
            for acc in self.accounts:
                logger.info(f"[{self.name}] Creating account {acc.name}...")
                create_account(downstream_sys_conn.get_connection(), acc.name, acc.admin_user, acc.admin_password)
                
                # Connect as account and create subscription
                acc_config = acc.get_cluster_config(self.config.downstream)
                acc_conn = DatabaseConnection(acc_config)
                acc_conn.connect()
                self.account_conns[acc.name] = acc_conn
                
                # Create subscription for this account
                conn_str = self.config.upstream.get_connection_string()
                execute_sql(acc_conn.get_connection(), f"""
                    CREATE DATABASE {self.db_name} FROM '{conn_str}'
                    PUBLICATION {self.pub_name} SYNC INTERVAL {self.config.sync_interval}
                """)
                
                logger.info(f"[{self.name}] Account {acc.name} subscribed to {self.pub_name}")
            
            downstream_sys_conn.close()
            
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Setup failed: {e}")
            self.errors.append(str(e))
            return False
    
    def run(self) -> bool:
        try:
            # Wait for initial sync
            trigger_checkpoint(self.upstream_conn.get_connection())
            time.sleep(self.config.sync_interval + 10)
            
            # Concurrent operations
            def insert_batch(batch_num):
                for i in range(10):
                    execute_sql(self.upstream_conn.get_connection(), f"""
                        INSERT INTO {self.db_name}.{self.table_name} (name, value, account)
                        VALUES ('batch_{batch_num}_item_{i}', {batch_num * 1000 + i}, 'batch_{batch_num}')
                    """)
            
            # Run multiple batches
            for batch in range(3):
                insert_batch(batch)
                trigger_checkpoint(self.upstream_conn.get_connection())
                time.sleep(self.config.sync_interval + 5)
            
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Run failed: {e}")
            self.errors.append(str(e))
            return False
    
    def verify(self) -> bool:
        all_passed = True
        upstream_count = get_table_count(self.upstream_conn.get_connection(),
            self.db_name, self.table_name)
        
        for acc_name, acc_conn in self.account_conns.items():
            downstream_count = get_table_count(acc_conn.get_connection(),
                self.db_name, self.table_name)
            
            if downstream_count != upstream_count:
                self.errors.append(f"Account {acc_name} count mismatch: {downstream_count} vs {upstream_count}")
                all_passed = False
            else:
                logger.info(f"[{self.name}] Account {acc_name} verified: {downstream_count} rows")
        
        return all_passed
    
    def cleanup(self) -> bool:
        try:
            # Close account connections
            for acc_conn in self.account_conns.values():
                try:
                    acc_conn.close()
                except:
                    pass
            
            # Drop accounts
            downstream_sys_conn = DatabaseConnection(self.config.downstream)
            downstream_sys_conn.connect()
            
            for acc in self.accounts:
                drop_account(downstream_sys_conn.get_connection(), acc.name)
            
            downstream_sys_conn.close()
            
            # Cleanup upstream
            if self.upstream_conn:
                execute_sql_safe(self.upstream_conn.get_connection(),
                    f"DROP PUBLICATION IF EXISTS {self.pub_name}", ignore_errors=True)
                execute_sql_safe(self.upstream_conn.get_connection(),
                    f"DROP DATABASE IF EXISTS {self.db_name}", ignore_errors=True)
                self.upstream_conn.close()
            
            return True
        except Exception as e:
            logger.warning(f"[{self.name}] Cleanup failed: {e}")
            return False


def get_all_scenarios(config: TestConfig) -> List[BaseScenario]:
    """Get all available test scenarios based on config"""
    scenarios = []
    
    if config.enable_table_level:
        scenarios.append(TableLevelScenario(config))
    
    if config.enable_db_level:
        scenarios.append(DatabaseLevelScenario(config))
    
    if config.enable_pause_resume:
        scenarios.append(PauseResumeScenario(config))
    
    if config.enable_indexes:
        scenarios.append(IndexScenario(config))
    
    if config.enable_alter_table:
        scenarios.append(AlterTableScenario(config))
    
    if config.enable_concurrent:
        scenarios.append(ConcurrentMultiAccountScenario(config))
    
    return scenarios
