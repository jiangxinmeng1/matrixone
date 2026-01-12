#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script for Cross-Cluster Physical Subscription with checkpoint operations.

This script:
- Sets up both cluster1 and cluster2
- Creates accounts, databases, and publications in cluster1
- Creates subscriptions in cluster2
- Updates publications in cluster1 before creating subscriptions
- Creates table with secondary index
- Continuously inserts/deletes data in upstream for 5 minutes
- Periodically creates checkpoints in upstream (every 10 seconds)
- Checks downstream synchronization
- Verifies data consistency between upstream and downstream
"""

import sys
import os
import time
import threading
from datetime import datetime

# Add clients/python to path to import matrixone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'clients', 'python'))

try:
    from matrixone import Client
    from matrixone.exceptions import MatrixOneError
except ImportError as e:
    print("=" * 80)
    print("ERROR: Missing required Python dependencies!")
    print("=" * 80)
    print(f"\nError: {e}")
    print("\nTo install dependencies, run:")
    print("  cd clients/python")
    print("  pip3 install -r requirements.txt")
    print("=" * 80)
    sys.exit(1)


# Configuration
CLUSTER1_HOST = '127.0.0.1'
CLUSTER1_PORT = 6001
CLUSTER2_HOST = '127.0.0.1'
CLUSTER2_PORT = 6002
SYS_USER = 'root'
SYS_PASSWORD = '111'

# Account configuration
CLUSTER1_ACCOUNT = 'cluster1_account'
CLUSTER2_ACCOUNT = 'cluster2_account'
ACCOUNT_ADMIN = 'admin'
ACCOUNT_PASSWORD = '111'

# Test configuration
TEST_DB_NAME = 'test_db'  # Database name in upstream (cluster1)
TEST_TABLE_NAME = 'test_table'
PUBLICATION_NAME = 'test_pub'
SUBSCRIPTION_DB_NAME = 'test_db'  # Use same name as upstream for subscription

# Test timing configuration
DATA_INSERT_DURATION = 300  # 5 minutes in seconds
CHECKPOINT_INTERVAL = 10  # 10 seconds
SYNC_WAIT_INTERVAL = 5  # Wait 5 seconds between sync checks


class Colors:
    """ANSI color codes for terminal output"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def print_header(msg):
    """Print a header message"""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 80}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{msg}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 80}{Colors.ENDC}\n")


def print_success(msg):
    """Print a success message"""
    print(f"{Colors.OKGREEN}✓ {msg}{Colors.ENDC}")


def print_error(msg):
    """Print an error message"""
    print(f"{Colors.FAIL}✗ {msg}{Colors.ENDC}")


def print_warning(msg):
    """Print a warning message"""
    print(f"{Colors.WARNING}⚠ {msg}{Colors.ENDC}")


def print_info(msg):
    """Print an info message"""
    print(f"{Colors.OKCYAN}ℹ {msg}{Colors.ENDC}")


def cleanup_account(client, account_name):
    """Drop account if exists"""
    try:
        client.execute(f"DROP ACCOUNT IF EXISTS `{account_name}`")
        print_success(f"Dropped account {account_name} (if existed)")
    except Exception as e:
        print_warning(f"Error dropping account {account_name}: {e}")


def cleanup_database(client, db_name):
    """Drop database if exists"""
    try:
        client.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
        print_success(f"Dropped database {db_name} (if existed)")
    except Exception as e:
        print_warning(f"Error dropping database {db_name}: {e}")


def cleanup_publication(client, pub_name):
    """Drop publication if exists"""
    try:
        client.execute(f"DROP PUBLICATION IF EXISTS `{pub_name}`")
        print_success(f"Dropped publication {pub_name} (if existed)")
    except Exception as e:
        print_warning(f"Error dropping publication {pub_name}: {e}")


def get_connection_string(cluster_account, admin_user, password, host, port):
    """Generate connection string for FROM clause"""
    return f"mysql://{cluster_account}#{admin_user}:{password}@{host}:{port}"


def setup_cluster1_publication(cluster1_account_client, db_name, pub_name, account_name):
    """Setup publication in cluster1 and update it"""
    print_info(f"Setting up publication {pub_name} in cluster1")
    
    # Cleanup existing publication
    cleanup_publication(cluster1_account_client, pub_name)
    
    # Create publication
    try:
        cluster1_account_client.execute(
            f"CREATE PUBLICATION `{pub_name}` DATABASE `{db_name}` ACCOUNT `{account_name}`"
        )
        print_success(f"Created publication {pub_name} in cluster1")
    except Exception as e:
        print_error(f"Failed to create publication {pub_name}: {e}")
        raise
    
    # Update publication to ensure it's properly configured
    try:
        cluster1_account_client.execute(
            f"ALTER PUBLICATION `{pub_name}` DATABASE `{db_name}` ACCOUNT `{account_name}`"
        )
        print_success(f"Updated publication {pub_name} in cluster1")
    except Exception as e:
        print_warning(f"Failed to update publication {pub_name}: {e}")
        # Don't raise, as publication was created successfully


def create_checkpoint(client):
    """Create checkpoint in upstream cluster"""
    try:
        result = client.execute("SELECT mo_ctl('dn','checkpoint','')")
        print_info("Checkpoint created successfully")
        return True
    except Exception as e:
        print_warning(f"Failed to create checkpoint: {e}")
        return False


def get_table_count(client, db_name, table_name):
    """Get row count from table"""
    try:
        client.execute(f"USE `{db_name}`")
        result = client.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        rows = result.fetchall()
        if rows and len(rows) > 0:
            return int(rows[0][0])
        return 0
    except Exception as e:
        print_warning(f"Failed to get table count: {e}")
        return -1


def get_table_data(client, db_name, table_name, limit=100):
    """Get table data for comparison"""
    try:
        client.execute(f"USE `{db_name}`")
        result = client.execute(f"SELECT * FROM `{table_name}` ORDER BY id LIMIT {limit}")
        rows = result.fetchall()
        return rows
    except Exception as e:
        print_warning(f"Failed to get table data: {e}")
        return None


def check_data_consistency(upstream_client, downstream_client, db_name, table_name):
    """Check if upstream and downstream data are consistent"""
    print_info("Checking data consistency between upstream and downstream...")
    
    upstream_count = get_table_count(upstream_client, db_name, table_name)
    downstream_count = get_table_count(downstream_client, db_name, table_name)
    
    print_info(f"Upstream count: {upstream_count}, Downstream count: {downstream_count}")
    
    if upstream_count == -1 or downstream_count == -1:
        print_error("Failed to get row counts")
        return False
    
    if upstream_count != downstream_count:
        print_error(f"Row count mismatch: upstream={upstream_count}, downstream={downstream_count}")
        return False
    
    # Compare actual data
    upstream_data = get_table_data(upstream_client, db_name, table_name)
    downstream_data = get_table_data(downstream_client, db_name, table_name)
    
    if upstream_data is None or downstream_data is None:
        print_warning("Failed to get table data for comparison")
        return False
    
    if len(upstream_data) != len(downstream_data):
        print_error(f"Data length mismatch: upstream={len(upstream_data)}, downstream={len(downstream_data)}")
        return False
    
    # Compare each row
    for i, (up_row, down_row) in enumerate(zip(upstream_data, downstream_data)):
        if up_row != down_row:
            print_error(f"Row {i} mismatch: upstream={up_row}, downstream={down_row}")
            return False
    
    print_success("Data consistency check passed!")
    return True


def checkpoint_worker(client, interval, stop_event):
    """Background worker to create checkpoints periodically"""
    while not stop_event.is_set():
        time.sleep(interval)
        if not stop_event.is_set():
            create_checkpoint(client)


def insert_delete_worker(client, db_name, table_name, duration, stop_event):
    """Background worker to continuously insert and delete data"""
    start_time = time.time()
    insert_id = 1
    
    print_info(f"Starting insert/delete operations for {duration} seconds...")
    
    while not stop_event.is_set() and (time.time() - start_time) < duration:
        try:
            # Insert data
            client.execute(f"USE `{db_name}`")
            client.execute(
                f"INSERT INTO `{table_name}` (id, name) VALUES ({insert_id}, 'name_{insert_id}')"
            )
            
            # Delete some old data (keep only recent 1000 rows)
            if insert_id > 1000:
                delete_id = insert_id - 1000
                client.execute(f"DELETE FROM `{table_name}` WHERE id = {delete_id}")
            
            insert_id += 1
            
            # Small delay to avoid overwhelming the system
            time.sleep(0.1)
            
        except Exception as e:
            print_warning(f"Error during insert/delete: {e}")
            time.sleep(1)
    
    print_info(f"Insert/delete operations completed. Total inserts: {insert_id - 1}")


def wait_for_sync(downstream_sys_client, db_name, pub_name, expected_iterations=2):
    """Wait for downstream to sync, checking iteration count"""
    print_info(f"Waiting for downstream to sync {expected_iterations} iterations...")
    
    # Query mo_ccpr_log to check iteration state
    try:
        downstream_sys_client.execute("USE mo_catalog")
        result = downstream_sys_client.execute(
            f"SELECT iteration_lsn, iteration_state FROM mo_catalog.mo_ccpr_log "
            f"WHERE db_name = '{db_name}' AND subscription_name = '{pub_name}' "
            f"ORDER BY iteration_lsn DESC LIMIT 1"
        )
        rows = result.fetchall()
        
        if not rows:
            print_warning("No subscription log found")
            return False
        
        initial_lsn = int(rows[0][0]) if rows[0][0] else 0
        print_info(f"Initial iteration LSN: {initial_lsn}")
        
        # Wait for expected iterations
        target_lsn = initial_lsn + expected_iterations
        max_wait_time = 300  # 5 minutes max wait
        start_time = time.time()
        
        while (time.time() - start_time) < max_wait_time:
            time.sleep(SYNC_WAIT_INTERVAL)
            
            result = downstream_sys_client.execute(
                f"SELECT iteration_lsn, iteration_state FROM mo_catalog.mo_ccpr_log "
                f"WHERE db_name = '{db_name}' AND subscription_name = '{pub_name}' "
                f"ORDER BY iteration_lsn DESC LIMIT 1"
            )
            rows = result.fetchall()
            
            if rows:
                current_lsn = int(rows[0][0]) if rows[0][0] else 0
                state = rows[0][1] if len(rows[0]) > 1 else None
                print_info(f"Current iteration LSN: {current_lsn}, State: {state}, Target: {target_lsn}")
                
                if current_lsn >= target_lsn:
                    print_success(f"Downstream synced to LSN {current_lsn}")
                    return True
        
        print_warning("Timeout waiting for downstream sync")
        return False
        
    except Exception as e:
        print_warning(f"Error checking sync status: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main test function"""
    print_header("Cross-Cluster Physical Subscription Setup")
    
    cluster1_sys_client = Client()
    cluster2_sys_client = Client()
    cluster1_account_client = Client()
    cluster2_account_client = Client()
    
    try:
        # Connect to clusters as sys users
        print_info("Connecting to clusters...")
        cluster1_sys_client.connect(
            host=CLUSTER1_HOST, port=CLUSTER1_PORT,
            user=SYS_USER, password=SYS_PASSWORD, database=''
        )
        cluster2_sys_client.connect(
            host=CLUSTER2_HOST, port=CLUSTER2_PORT,
            user=SYS_USER, password=SYS_PASSWORD, database=''
        )
        print_success("Connected to both clusters as sys users")
        
        # Setup accounts
        print_info("Setting up accounts...")
        cleanup_account(cluster1_sys_client, CLUSTER1_ACCOUNT)
        cleanup_account(cluster2_sys_client, CLUSTER2_ACCOUNT)
        
        cluster1_sys_client.execute(
            f"CREATE ACCOUNT {CLUSTER1_ACCOUNT} ADMIN_NAME '{ACCOUNT_ADMIN}' IDENTIFIED BY '{ACCOUNT_PASSWORD}'"
        )
        cluster2_sys_client.execute(
            f"CREATE ACCOUNT {CLUSTER2_ACCOUNT} ADMIN_NAME '{ACCOUNT_ADMIN}' IDENTIFIED BY '{ACCOUNT_PASSWORD}'"
        )
        print_success("Created accounts in both clusters")
        
        # Connect as account users
        print_info("Connecting as account users...")
        cluster1_account_client.connect(
            host=CLUSTER1_HOST, port=CLUSTER1_PORT,
            user=f"{CLUSTER1_ACCOUNT}#{ACCOUNT_ADMIN}", password=ACCOUNT_PASSWORD, database=''
        )
        cluster2_account_client.connect(
            host=CLUSTER2_HOST, port=CLUSTER2_PORT,
            user=f"{CLUSTER2_ACCOUNT}#{ACCOUNT_ADMIN}", password=ACCOUNT_PASSWORD, database=''
        )
        print_success("Connected as account users")
        
        # Setup upstream (cluster1): create database and table with secondary index
        print_info("Setting up upstream database in cluster1...")
        cleanup_database(cluster1_account_client, TEST_DB_NAME)
        cluster1_account_client.execute(f"CREATE DATABASE `{TEST_DB_NAME}`")
        cluster1_account_client.execute(f"USE `{TEST_DB_NAME}`")
        cluster1_account_client.execute(
            f"CREATE TABLE `{TEST_TABLE_NAME}` ("
            f"id INT PRIMARY KEY, "
            f"name VARCHAR(100), "
            f"INDEX idx_name (name)"
            f")"
        )
        print_success("Created database and table with secondary index in cluster1")
        
        # Create and update publication in cluster1
        setup_cluster1_publication(
            cluster1_account_client, TEST_DB_NAME, PUBLICATION_NAME, CLUSTER1_ACCOUNT
        )
        
        # Get connection string
        conn_str = get_connection_string(
            CLUSTER1_ACCOUNT, ACCOUNT_ADMIN, ACCOUNT_PASSWORD,
            CLUSTER1_HOST, CLUSTER1_PORT
        )
        print_info(f"Connection string: {conn_str}")
        
        # Create subscription in cluster2
        print_info("Creating subscription in cluster2...")
        cleanup_database(cluster2_account_client, SUBSCRIPTION_DB_NAME)
        
        try:
            cluster2_account_client.execute(
                f"CREATE DATABASE `{SUBSCRIPTION_DB_NAME}` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME}`"
            )
            print_success(f"Created subscription database {SUBSCRIPTION_DB_NAME} in cluster2")
        except Exception as e:
            print_error(f"Failed to create subscription: {e}")
            raise
        
        print_header("Setup Complete")
        print_success("Cross-cluster subscription setup completed successfully!")
        print_info(f"Publication: {PUBLICATION_NAME} in cluster1")
        print_info(f"Subscription: {SUBSCRIPTION_DB_NAME} in cluster2")
        
        # Start checkpoint worker thread (use sys client for checkpoint)
        print_info("Starting checkpoint worker (every 10 seconds)...")
        checkpoint_stop_event = threading.Event()
        checkpoint_thread = threading.Thread(
            target=checkpoint_worker,
            args=(cluster1_sys_client, CHECKPOINT_INTERVAL, checkpoint_stop_event),
            daemon=True
        )
        checkpoint_thread.start()
        print_success("Checkpoint worker started")
        
        # Start insert/delete worker thread
        print_info("Starting insert/delete operations in upstream...")
        insert_stop_event = threading.Event()
        insert_thread = threading.Thread(
            target=insert_delete_worker,
            args=(cluster1_account_client, TEST_DB_NAME, TEST_TABLE_NAME, DATA_INSERT_DURATION, insert_stop_event),
            daemon=True
        )
        insert_thread.start()
        print_success("Insert/delete worker started")
        
        # Wait for insert/delete operations to complete
        print_info(f"Waiting for {DATA_INSERT_DURATION} seconds of insert/delete operations...")
        insert_thread.join()
        insert_stop_event.set()
        print_success("Insert/delete operations completed")
        
        # Create final checkpoint
        print_info("Creating final checkpoint...")
        create_checkpoint(cluster1_sys_client)
        time.sleep(2)  # Wait a bit for checkpoint to complete
        
        # Stop checkpoint worker
        checkpoint_stop_event.set()
        checkpoint_thread.join(timeout=5)
        print_success("Checkpoint worker stopped")
        
        # Wait for downstream to sync (2 more iterations)
        print_info("Waiting for downstream to sync 2 more iterations...")
        if wait_for_sync(cluster2_sys_client, SUBSCRIPTION_DB_NAME, PUBLICATION_NAME, expected_iterations=2):
            print_success("Downstream sync completed")
        else:
            print_warning("Downstream sync may not be complete, but continuing with consistency check")
        
        # Check data consistency
        print_header("Data Consistency Check")
        if check_data_consistency(
            cluster1_account_client,
            cluster2_account_client,
            TEST_DB_NAME,
            TEST_TABLE_NAME
        ):
            print_success("✓ All tests passed! Data is consistent between upstream and downstream.")
        else:
            print_error("✗ Data consistency check failed!")
            sys.exit(1)
        
    except Exception as e:
        print_error(f"Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        try:
            cluster1_account_client.disconnect()
            cluster2_account_client.disconnect()
            cluster1_sys_client.disconnect()
            cluster2_sys_client.disconnect()
        except:
            pass


if __name__ == "__main__":
    main()
