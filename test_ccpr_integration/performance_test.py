#!/usr/bin/env python3
"""
CCPR Integration Test - Performance Testing

Long-running performance test for CCPR replication.
"""

import time
import threading
import logging
import random
import statistics
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Tuple
from collections import deque

from config import TestConfig, SubscriptionState, IterationState
from db_utils import (
    DatabaseConnection, execute_sql, execute_sql_safe, get_ccpr_status,
    trigger_checkpoint, get_table_count, verify_data_consistency
)
from data_generator import (
    create_test_tables, random_string, escape_sql_string, random_text, random_vector
)

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Performance metrics collected during test"""
    iteration_latencies: List[float] = field(default_factory=list)  # Time between iterations
    sync_latencies: List[float] = field(default_factory=list)      # Time to sync data
    insert_counts: List[int] = field(default_factory=list)         # Inserts per batch
    throughput_samples: List[float] = field(default_factory=list)  # Rows synced per second
    errors: List[Tuple[datetime, str]] = field(default_factory=list)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics"""
        summary = {
            "total_iterations": len(self.iteration_latencies),
            "total_errors": len(self.errors),
        }
        
        if self.iteration_latencies:
            summary["iteration_latency"] = {
                "min": min(self.iteration_latencies),
                "max": max(self.iteration_latencies),
                "avg": statistics.mean(self.iteration_latencies),
                "median": statistics.median(self.iteration_latencies),
                "p95": sorted(self.iteration_latencies)[int(len(self.iteration_latencies) * 0.95)] if len(self.iteration_latencies) > 20 else max(self.iteration_latencies),
            }
        
        if self.sync_latencies:
            summary["sync_latency"] = {
                "min": min(self.sync_latencies),
                "max": max(self.sync_latencies),
                "avg": statistics.mean(self.sync_latencies),
                "median": statistics.median(self.sync_latencies),
            }
        
        if self.throughput_samples:
            summary["throughput_rows_per_sec"] = {
                "min": min(self.throughput_samples),
                "max": max(self.throughput_samples),
                "avg": statistics.mean(self.throughput_samples),
            }
        
        return summary


class DataInserter(threading.Thread):
    """Background thread for continuous data insertion"""
    
    def __init__(self, config: TestConfig, db_name: str, tables: Dict,
                 stop_event: threading.Event):
        super().__init__(daemon=True)
        self.config = config
        self.db_name = db_name
        self.tables = tables
        self.stop_event = stop_event
        
        self.insert_count = 0
        self.total_rows = 0
        self.last_insert_time: Optional[datetime] = None
        self.lock = threading.Lock()
        self.errors: List[str] = []
    
    def run(self):
        logger.info("Data inserter thread started")
        conn = DatabaseConnection(self.config.upstream, database=self.db_name)
        
        try:
            conn.connect()
            while not self.stop_event.is_set():
                batch_rows = self._insert_batch(conn.get_connection())
                
                with self.lock:
                    self.insert_count += 1
                    self.total_rows += batch_rows
                    self.last_insert_time = datetime.now()
                
                self.stop_event.wait(self.config.insert_interval)
        except Exception as e:
            logger.error(f"Data inserter error: {e}")
            self.errors.append(str(e))
        finally:
            conn.close()
        
        logger.info(f"Data inserter stopped. Total batches: {self.insert_count}, total rows: {self.total_rows}")
    
    def _insert_batch(self, conn) -> int:
        """Insert a batch of data into random tables"""
        batch_rows = 0
        batch_size = random.randint(1, self.config.perf_batch_size)
        
        for name, table_def in self.tables.items():
            if hasattr(table_def, 'generate_insert_sql'):
                try:
                    execute_sql(conn, table_def.generate_insert_sql(count=batch_size))
                    batch_rows += batch_size
                except Exception as e:
                    logger.debug(f"Insert failed for {name}: {e}")
        
        return batch_rows
    
    def get_stats(self) -> Tuple[int, int, Optional[datetime]]:
        """Get current stats"""
        with self.lock:
            return self.insert_count, self.total_rows, self.last_insert_time


class IterationMonitor(threading.Thread):
    """Background thread for monitoring iteration status"""
    
    def __init__(self, config: TestConfig, subscription_name: str,
                 stop_event: threading.Event, metrics: PerformanceMetrics):
        super().__init__(daemon=True)
        self.config = config
        self.subscription_name = subscription_name
        self.stop_event = stop_event
        self.metrics = metrics
        
        self.last_iteration_lsn = 0
        self.last_iteration_time: Optional[datetime] = None
        self.iteration_count = 0
        self.lock = threading.Lock()
    
    def run(self):
        logger.info("Iteration monitor thread started")
        conn = DatabaseConnection(self.config.downstream)
        
        try:
            conn.connect()
            while not self.stop_event.is_set():
                try:
                    statuses = get_ccpr_status(conn.get_connection(),
                        subscription_name=self.subscription_name)
                    
                    if statuses:
                        status = statuses[0]
                        current_lsn = status.iteration_lsn
                        
                        if current_lsn > self.last_iteration_lsn:
                            now = datetime.now()
                            
                            if self.last_iteration_time:
                                latency = (now - self.last_iteration_time).total_seconds()
                                self.metrics.iteration_latencies.append(latency)
                            
                            with self.lock:
                                self.last_iteration_lsn = current_lsn
                                self.last_iteration_time = now
                                self.iteration_count += 1
                            
                            logger.debug(f"Iteration {self.iteration_count}: LSN={current_lsn}")
                        
                        if status.state == SubscriptionState.ERROR:
                            error_msg = f"Subscription error: {status.error_message}"
                            self.metrics.errors.append((datetime.now(), error_msg))
                            logger.error(error_msg)
                
                except Exception as e:
                    logger.debug(f"Monitor check failed: {e}")
                
                self.stop_event.wait(5)  # Check every 5 seconds
        finally:
            conn.close()
        
        logger.info(f"Iteration monitor stopped. Total iterations: {self.iteration_count}")
    
    def get_stats(self) -> Tuple[int, int]:
        """Get current stats"""
        with self.lock:
            return self.iteration_count, self.last_iteration_lsn


class PerformanceTest:
    """Long-running performance test"""
    
    def __init__(self, config: TestConfig):
        self.config = config
        self.db_name = f"ccpr_perf_test_{random_string(6)}"
        self.pub_name = f"pub_perf_{random_string(6)}"
        self.tables = {}
        self.metrics = PerformanceMetrics()
        
        self.upstream_conn: Optional[DatabaseConnection] = None
        self.downstream_conn: Optional[DatabaseConnection] = None
        self.stop_event = threading.Event()
        self.inserter: Optional[DataInserter] = None
        self.monitor: Optional[IterationMonitor] = None
    
    def setup(self) -> bool:
        """Setup test environment"""
        try:
            logger.info("Setting up performance test...")
            
            # Connect to upstream
            self.upstream_conn = DatabaseConnection(self.config.upstream)
            self.upstream_conn.connect()
            
            # Create database
            execute_sql(self.upstream_conn.get_connection(), f"DROP DATABASE IF EXISTS {self.db_name}")
            execute_sql(self.upstream_conn.get_connection(), f"CREATE DATABASE {self.db_name}")
            
            # Create tables
            self.tables = create_test_tables(self.db_name)
            for name, table_def in self.tables.items():
                logger.info(f"Creating table {name}...")
                execute_sql(self.upstream_conn.get_connection(), table_def.get_create_sql())
                
                for idx_sql in table_def.get_index_sqls():
                    execute_sql_safe(self.upstream_conn.get_connection(), idx_sql, ignore_errors=True)
                
                # Insert initial data
                if hasattr(table_def, 'generate_insert_sql'):
                    for _ in range(5):
                        try:
                            execute_sql(self.upstream_conn.get_connection(),
                                table_def.generate_insert_sql(count=10))
                        except Exception as e:
                            logger.debug(f"Initial insert failed: {e}")
            
            # Create publication
            execute_sql(self.upstream_conn.get_connection(), f"DROP PUBLICATION IF EXISTS {self.pub_name}")
            execute_sql(self.upstream_conn.get_connection(), f"""
                CREATE PUBLICATION {self.pub_name} DATABASE {self.db_name} ACCOUNT ALL
            """)
            
            # Connect to downstream
            self.downstream_conn = DatabaseConnection(self.config.downstream)
            self.downstream_conn.connect()
            
            # Create subscription
            conn_str = self.config.upstream.get_connection_string()
            execute_sql(self.downstream_conn.get_connection(), f"""
                CREATE DATABASE {self.db_name} FROM '{conn_str}'
                PUBLICATION {self.pub_name} SYNC INTERVAL {self.config.sync_interval}
            """)
            
            logger.info("Performance test setup complete")
            return True
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            return False
    
    def run(self, duration: Optional[int] = None) -> bool:
        """Run the performance test"""
        test_duration = duration or self.config.test_duration
        
        try:
            logger.info(f"Starting performance test for {test_duration} seconds ({test_duration/3600:.1f} hours)")
            
            # Start inserter thread
            self.inserter = DataInserter(
                self.config, self.db_name, self.tables, self.stop_event)
            self.inserter.start()
            
            # Start monitor thread
            self.monitor = IterationMonitor(
                self.config, self.pub_name, self.stop_event, self.metrics)
            self.monitor.start()
            
            # Main loop with periodic reporting
            start_time = time.time()
            last_report = start_time
            last_checkpoint = start_time
            
            while time.time() - start_time < test_duration:
                current_time = time.time()
                elapsed = current_time - start_time
                
                # Periodic checkpoint
                if current_time - last_checkpoint >= self.config.sync_interval:
                    trigger_checkpoint(self.upstream_conn.get_connection())
                    last_checkpoint = current_time
                
                # Periodic status report
                if current_time - last_report >= self.config.report_interval:
                    self._report_status(elapsed, test_duration)
                    last_report = current_time
                
                # Check for too many errors
                if len(self.metrics.errors) >= self.config.max_errors:
                    logger.error("Too many errors, stopping test")
                    break
                
                time.sleep(5)
            
            logger.info("Performance test duration complete")
            return True
            
        except KeyboardInterrupt:
            logger.info("Test interrupted by user")
            return True
        except Exception as e:
            logger.error(f"Performance test failed: {e}")
            self.metrics.errors.append((datetime.now(), str(e)))
            return False
        finally:
            self.stop_event.set()
            if self.inserter:
                self.inserter.join(timeout=10)
            if self.monitor:
                self.monitor.join(timeout=10)
    
    def _report_status(self, elapsed: float, total_duration: float):
        """Print periodic status report"""
        insert_batches, total_rows, _ = self.inserter.get_stats() if self.inserter else (0, 0, None)
        iterations, lsn = self.monitor.get_stats() if self.monitor else (0, 0)
        
        remaining = total_duration - elapsed
        remaining_str = str(timedelta(seconds=int(remaining)))
        
        logger.info(f"=== Status Report ===")
        logger.info(f"Elapsed: {timedelta(seconds=int(elapsed))} | Remaining: {remaining_str}")
        logger.info(f"Insert batches: {insert_batches} | Total rows inserted: {total_rows}")
        logger.info(f"Iterations completed: {iterations} | Current LSN: {lsn}")
        logger.info(f"Errors: {len(self.metrics.errors)}")
        
        # Calculate throughput
        if elapsed > 0 and iterations > 0:
            avg_latency = statistics.mean(self.metrics.iteration_latencies) if self.metrics.iteration_latencies else 0
            logger.info(f"Avg iteration latency: {avg_latency:.2f}s")
    
    def verify(self) -> bool:
        """Verify final data consistency"""
        logger.info("Verifying data consistency...")
        
        # Final checkpoint and wait
        trigger_checkpoint(self.upstream_conn.get_connection())
        time.sleep(self.config.sync_interval + 10)
        
        all_passed = True
        for name, table_def in self.tables.items():
            consistent, msg = verify_data_consistency(
                self.upstream_conn.get_connection(),
                self.downstream_conn.get_connection(),
                self.db_name, table_def.name
            )
            
            if not consistent:
                logger.error(f"Table {name} inconsistent: {msg}")
                all_passed = False
            else:
                logger.info(f"Table {name} verified: {msg}")
        
        return all_passed
    
    def cleanup(self):
        """Cleanup test resources"""
        logger.info("Cleaning up performance test resources...")
        
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
        except Exception as e:
            logger.warning(f"Cleanup failed: {e}")
    
    def get_report(self) -> Dict[str, Any]:
        """Get final test report"""
        insert_batches, total_rows, _ = self.inserter.get_stats() if self.inserter else (0, 0, None)
        iterations, lsn = self.monitor.get_stats() if self.monitor else (0, 0)
        
        return {
            "test_name": "PerformanceTest",
            "database": self.db_name,
            "publication": self.pub_name,
            "total_insert_batches": insert_batches,
            "total_rows_inserted": total_rows,
            "total_iterations": iterations,
            "final_lsn": lsn,
            "metrics": self.metrics.get_summary(),
            "tables": list(self.tables.keys()),
            "config": {
                "test_duration": self.config.test_duration,
                "sync_interval": self.config.sync_interval,
                "insert_interval": self.config.insert_interval,
            }
        }


def run_performance_test(config: TestConfig, duration: Optional[int] = None) -> Dict[str, Any]:
    """Run the performance test and return report"""
    test = PerformanceTest(config)
    
    try:
        if not test.setup():
            return {"success": False, "error": "Setup failed"}
        
        if not test.run(duration):
            return {"success": False, "error": "Test run failed", **test.get_report()}
        
        verified = test.verify()
        report = test.get_report()
        report["success"] = verified
        report["verified"] = verified
        
        return report
    finally:
        test.cleanup()
