#!/usr/bin/env python3
"""
CCPR Integration Test Configuration

Configuration for Cross-Cluster Publication/Replication (CCPR) integration tests.
"""

import os
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from enum import Enum


class SyncLevel(Enum):
    """CCPR subscription sync levels"""
    TABLE = "table"
    DATABASE = "database"
    ACCOUNT = "account"


class SubscriptionState(Enum):
    """CCPR subscription states from mo_ccpr_log"""
    RUNNING = 0
    ERROR = 1
    PAUSE = 2
    DROPPED = 3


class IterationState(Enum):
    """CCPR iteration states"""
    PENDING = 0
    RUNNING = 1
    COMPLETED = 2
    ERROR = 3
    CANCELED = 4


@dataclass
class ClusterConfig:
    """Configuration for a MatrixOne cluster connection"""
    host: str = "127.0.0.1"
    port: int = 6001
    user: str = "root"
    password: str = "111"
    account: Optional[str] = None  # For tenant account connection
    
    def get_connection_string(self) -> str:
        """Get MySQL connection string for CCPR subscription"""
        if self.account:
            return f"mysql://{self.account}#{self.user}:{self.password}@{self.host}:{self.port}"
        return f"mysql://{self.user}:{self.password}@{self.host}:{self.port}"
    
    def get_user_string(self) -> str:
        """Get user string for pymysql connection"""
        if self.account:
            return f"{self.account}#{self.user}"
        return self.user


@dataclass
class TestConfig:
    """Main test configuration"""
    # Cluster configurations
    upstream: ClusterConfig = field(default_factory=lambda: ClusterConfig(port=6001))
    downstream: ClusterConfig = field(default_factory=lambda: ClusterConfig(port=6002))
    
    # Test duration (in seconds)
    # 8 hours for overnight test
    test_duration: int = int(os.environ.get("TEST_DURATION", 8 * 60 * 60))
    
    # Sync interval for subscriptions (in seconds)
    sync_interval: int = int(os.environ.get("SYNC_INTERVAL", 10))
    
    # Data insertion interval (in seconds)
    insert_interval: float = float(os.environ.get("INSERT_INTERVAL", 2.0))
    
    # Number of concurrent downstream accounts
    concurrent_accounts: int = int(os.environ.get("CONCURRENT_ACCOUNTS", 3))
    
    # Whether to trigger checkpoint before iteration
    checkpoint_before_iteration: bool = True
    
    # Test scenarios to run
    enable_table_level: bool = True
    enable_db_level: bool = True
    enable_account_level: bool = True
    enable_pause_resume: bool = True
    enable_indexes: bool = True
    enable_alter_table: bool = True
    enable_concurrent: bool = True
    enable_performance: bool = True
    
    # Performance test settings
    perf_batch_size: int = 100
    perf_max_rows_per_table: int = 100000
    
    # Error tolerance (how many errors before stopping)
    max_errors: int = 100
    
    # Logging
    log_level: str = "INFO"
    log_file: str = "ccpr_integration_test.log"
    
    # Report settings
    report_interval: int = 60  # Report status every 60 seconds
    final_report_file: str = "ccpr_test_report.json"


@dataclass  
class AccountConfig:
    """Configuration for a test account"""
    name: str
    admin_user: str = "admin"
    admin_password: str = "111"
    
    def get_cluster_config(self, base_config: ClusterConfig) -> ClusterConfig:
        """Get cluster config for this account"""
        return ClusterConfig(
            host=base_config.host,
            port=base_config.port,
            user=self.admin_user,
            password=self.admin_password,
            account=self.name
        )


# Default test accounts for concurrent testing
DEFAULT_DOWNSTREAM_ACCOUNTS = [
    AccountConfig(name="ccpr_test_acc1"),
    AccountConfig(name="ccpr_test_acc2"),
    AccountConfig(name="ccpr_test_acc3"),
]


# Index types to test
INDEX_TYPES = [
    "SECONDARY",  # Normal secondary index
    "UNIQUE",     # Unique index
    "FULLTEXT",   # Fulltext index
    "IVFFLAT",    # IVF vector index
]


# Alter table operations to test
ALTER_TABLE_OPERATIONS = [
    "ADD_COLUMN",
    "DROP_COLUMN",
    "MODIFY_COLUMN",
    "RENAME_COLUMN",
    "ADD_INDEX",
    "DROP_INDEX",
    "ADD_FOREIGN_KEY",
    "DROP_FOREIGN_KEY",
    "CHANGE_COMMENT",
]


def load_config_from_env() -> TestConfig:
    """Load configuration from environment variables"""
    config = TestConfig()
    
    # Override from environment
    if os.environ.get("UPSTREAM_HOST"):
        config.upstream.host = os.environ["UPSTREAM_HOST"]
    if os.environ.get("UPSTREAM_PORT"):
        config.upstream.port = int(os.environ["UPSTREAM_PORT"])
    if os.environ.get("UPSTREAM_USER"):
        config.upstream.user = os.environ["UPSTREAM_USER"]
    if os.environ.get("UPSTREAM_PASSWORD"):
        config.upstream.password = os.environ["UPSTREAM_PASSWORD"]
        
    if os.environ.get("DOWNSTREAM_HOST"):
        config.downstream.host = os.environ["DOWNSTREAM_HOST"]
    if os.environ.get("DOWNSTREAM_PORT"):
        config.downstream.port = int(os.environ["DOWNSTREAM_PORT"])
    if os.environ.get("DOWNSTREAM_USER"):
        config.downstream.user = os.environ["DOWNSTREAM_USER"]
    if os.environ.get("DOWNSTREAM_PASSWORD"):
        config.downstream.password = os.environ["DOWNSTREAM_PASSWORD"]
    
    return config
