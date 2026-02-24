#!/usr/bin/env python3
"""
CCPR Integration Test - Main Runner

Entry point for running CCPR integration tests.
Supports overnight testing with comprehensive scenario coverage.
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from config import TestConfig, load_config_from_env, ClusterConfig
from test_scenarios import (
    get_all_scenarios, BaseScenario, ScenarioResult, TestResult,
    TableLevelScenario, DatabaseLevelScenario, PauseResumeScenario,
    IndexScenario, AlterTableScenario, ConcurrentMultiAccountScenario
)
from performance_test import run_performance_test, PerformanceTest


def setup_logging(config: TestConfig):
    """Setup logging configuration"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)
    
    # Setup root logger
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(config.log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Reduce noise from some loggers
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('pymysql').setLevel(logging.WARNING)


def run_scenario_tests(config: TestConfig) -> List[ScenarioResult]:
    """Run all scenario tests"""
    logger = logging.getLogger(__name__)
    scenarios = get_all_scenarios(config)
    results = []
    
    logger.info(f"Running {len(scenarios)} test scenarios...")
    
    for i, scenario in enumerate(scenarios, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"[{i}/{len(scenarios)}] Running scenario: {scenario.name}")
        logger.info(f"{'='*60}")
        
        result = scenario.execute()
        results.append(result)
        
        status_icon = "✓" if result.result == TestResult.PASSED else "✗"
        logger.info(f"[{status_icon}] {scenario.name}: {result.result.value} ({result.duration:.1f}s)")
        
        if result.errors:
            for err in result.errors:
                logger.error(f"  Error: {err}")
    
    return results


def run_continuous_test(config: TestConfig, duration: int) -> Dict[str, Any]:
    """Run continuous integration test for specified duration"""
    logger = logging.getLogger(__name__)
    
    start_time = datetime.now()
    end_time = start_time + timedelta(seconds=duration)
    
    all_results: List[ScenarioResult] = []
    iteration = 0
    total_passed = 0
    total_failed = 0
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Starting continuous test")
    logger.info(f"Duration: {timedelta(seconds=duration)}")
    logger.info(f"Start time: {start_time}")
    logger.info(f"Estimated end time: {end_time}")
    logger.info(f"{'='*60}\n")
    
    try:
        while datetime.now() < end_time:
            iteration += 1
            logger.info(f"\n{'='*60}")
            logger.info(f"ITERATION {iteration}")
            logger.info(f"Time remaining: {end_time - datetime.now()}")
            logger.info(f"{'='*60}\n")
            
            # Run all scenarios
            results = run_scenario_tests(config)
            all_results.extend(results)
            
            # Count results
            for r in results:
                if r.result == TestResult.PASSED:
                    total_passed += 1
                else:
                    total_failed += 1
            
            # Print iteration summary
            logger.info(f"\nIteration {iteration} Summary:")
            logger.info(f"  Passed: {sum(1 for r in results if r.result == TestResult.PASSED)}/{len(results)}")
            logger.info(f"  Total Passed: {total_passed}, Total Failed: {total_failed}")
            
            # Check if we should stop due to too many errors
            if total_failed >= config.max_errors:
                logger.error(f"Too many failures ({total_failed}), stopping test")
                break
            
            # Small delay between iterations
            if datetime.now() < end_time:
                time.sleep(30)
    
    except KeyboardInterrupt:
        logger.info("\nTest interrupted by user")
    
    actual_duration = (datetime.now() - start_time).total_seconds()
    
    return {
        "mode": "continuous",
        "iterations": iteration,
        "total_scenarios_run": len(all_results),
        "total_passed": total_passed,
        "total_failed": total_failed,
        "pass_rate": total_passed / len(all_results) * 100 if all_results else 0,
        "duration_seconds": actual_duration,
        "duration_human": str(timedelta(seconds=int(actual_duration))),
        "start_time": start_time.isoformat(),
        "end_time": datetime.now().isoformat(),
    }


def run_overnight_test(config: TestConfig) -> Dict[str, Any]:
    """Run overnight test combining scenarios and performance test"""
    logger = logging.getLogger(__name__)
    
    total_duration = config.test_duration
    scenario_duration = min(total_duration // 4, 2 * 60 * 60)  # Max 2 hours for scenarios
    perf_duration = total_duration - scenario_duration
    
    logger.info(f"\n{'='*60}")
    logger.info(f"OVERNIGHT TEST")
    logger.info(f"Total duration: {timedelta(seconds=total_duration)}")
    logger.info(f"Scenario tests: {timedelta(seconds=scenario_duration)}")
    logger.info(f"Performance test: {timedelta(seconds=perf_duration)}")
    logger.info(f"{'='*60}\n")
    
    results = {
        "mode": "overnight",
        "total_duration_seconds": total_duration,
        "start_time": datetime.now().isoformat(),
    }
    
    # Phase 1: Run scenario tests
    logger.info("\n=== PHASE 1: Scenario Tests ===\n")
    scenario_results = run_continuous_test(config, scenario_duration)
    results["scenario_tests"] = scenario_results
    
    # Phase 2: Run performance test
    logger.info("\n=== PHASE 2: Performance Test ===\n")
    perf_results = run_performance_test(config, perf_duration)
    results["performance_test"] = perf_results
    
    results["end_time"] = datetime.now().isoformat()
    actual_duration = (datetime.fromisoformat(results["end_time"]) - 
                       datetime.fromisoformat(results["start_time"])).total_seconds()
    results["actual_duration_seconds"] = actual_duration
    results["actual_duration_human"] = str(timedelta(seconds=int(actual_duration)))
    
    # Calculate overall success
    scenario_success = scenario_results.get("pass_rate", 0) >= 80
    perf_success = perf_results.get("success", False)
    results["overall_success"] = scenario_success and perf_success
    
    return results


def print_summary(results: Dict[str, Any]):
    """Print final test summary"""
    logger = logging.getLogger(__name__)
    
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    
    mode = results.get("mode", "unknown")
    
    if mode == "overnight":
        scenario_results = results.get("scenario_tests", {})
        perf_results = results.get("performance_test", {})
        
        logger.info(f"Mode: Overnight Test")
        logger.info(f"Duration: {results.get('actual_duration_human', 'N/A')}")
        logger.info(f"\nScenario Tests:")
        logger.info(f"  Iterations: {scenario_results.get('iterations', 'N/A')}")
        logger.info(f"  Pass Rate: {scenario_results.get('pass_rate', 0):.1f}%")
        
        logger.info(f"\nPerformance Test:")
        logger.info(f"  Success: {perf_results.get('success', False)}")
        logger.info(f"  Total Rows: {perf_results.get('total_rows_inserted', 'N/A')}")
        logger.info(f"  Iterations: {perf_results.get('total_iterations', 'N/A')}")
        
        if perf_results.get("metrics"):
            metrics = perf_results["metrics"]
            if "iteration_latency" in metrics:
                logger.info(f"  Avg Iteration Latency: {metrics['iteration_latency'].get('avg', 'N/A'):.2f}s")
        
        overall = "PASSED ✓" if results.get("overall_success") else "FAILED ✗"
        logger.info(f"\nOVERALL RESULT: {overall}")
    
    elif mode == "continuous":
        logger.info(f"Mode: Continuous Test")
        logger.info(f"Duration: {results.get('duration_human', 'N/A')}")
        logger.info(f"Iterations: {results.get('iterations', 'N/A')}")
        logger.info(f"Pass Rate: {results.get('pass_rate', 0):.1f}%")
        
        success = results.get("pass_rate", 0) >= 80
        overall = "PASSED ✓" if success else "FAILED ✗"
        logger.info(f"\nOVERALL RESULT: {overall}")
    
    else:
        logger.info(f"Results: {json.dumps(results, indent=2)}")


def save_report(results: Dict[str, Any], filename: str):
    """Save test report to file"""
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"Report saved to {filename}")


def main():
    parser = argparse.ArgumentParser(
        description="CCPR Integration Test Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run overnight test (8 hours)
  python main.py --overnight

  # Run quick test (30 minutes)
  python main.py --duration 1800

  # Run only scenario tests
  python main.py --scenarios-only

  # Run only performance test
  python main.py --performance-only --duration 3600

  # Run with custom cluster ports
  python main.py --upstream-port 6001 --downstream-port 6002
        """
    )
    
    # Test mode options
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--overnight", action="store_true",
                           help="Run overnight test (8 hours by default)")
    mode_group.add_argument("--scenarios-only", action="store_true",
                           help="Run only scenario tests")
    mode_group.add_argument("--performance-only", action="store_true",
                           help="Run only performance test")
    
    # Duration options
    parser.add_argument("--duration", type=int, default=None,
                       help="Test duration in seconds (default: 8 hours for overnight)")
    
    # Cluster configuration
    parser.add_argument("--upstream-host", default="127.0.0.1",
                       help="Upstream cluster host")
    parser.add_argument("--upstream-port", type=int, default=6001,
                       help="Upstream cluster port")
    parser.add_argument("--downstream-host", default="127.0.0.1",
                       help="Downstream cluster host")
    parser.add_argument("--downstream-port", type=int, default=6002,
                       help="Downstream cluster port")
    parser.add_argument("--user", default="root",
                       help="Database user")
    parser.add_argument("--password", default="111",
                       help="Database password")
    
    # Test configuration
    parser.add_argument("--sync-interval", type=int, default=10,
                       help="Sync interval for subscriptions (seconds)")
    parser.add_argument("--insert-interval", type=float, default=2.0,
                       help="Data insertion interval (seconds)")
    parser.add_argument("--concurrent-accounts", type=int, default=3,
                       help="Number of concurrent accounts for testing")
    parser.add_argument("--max-errors", type=int, default=100,
                       help="Maximum errors before stopping")
    
    # Output options
    parser.add_argument("--log-file", default="ccpr_integration_test.log",
                       help="Log file path")
    parser.add_argument("--report-file", default="ccpr_test_report.json",
                       help="Report file path")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Log level")
    
    # Scenario toggles
    parser.add_argument("--no-table-level", action="store_true",
                       help="Skip table-level tests")
    parser.add_argument("--no-db-level", action="store_true",
                       help="Skip database-level tests")
    parser.add_argument("--no-pause-resume", action="store_true",
                       help="Skip pause/resume tests")
    parser.add_argument("--no-indexes", action="store_true",
                       help="Skip index tests")
    parser.add_argument("--no-alter-table", action="store_true",
                       help="Skip alter table tests")
    parser.add_argument("--no-concurrent", action="store_true",
                       help="Skip concurrent tests")
    
    args = parser.parse_args()
    
    # Build configuration
    config = TestConfig()
    
    # Override cluster config
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
    
    # Test settings
    config.sync_interval = args.sync_interval
    config.insert_interval = args.insert_interval
    config.concurrent_accounts = args.concurrent_accounts
    config.max_errors = args.max_errors
    config.log_file = args.log_file
    config.log_level = args.log_level
    config.final_report_file = args.report_file
    
    # Duration
    if args.duration:
        config.test_duration = args.duration
    elif not args.overnight:
        config.test_duration = 30 * 60  # 30 minutes default
    
    # Scenario toggles
    config.enable_table_level = not args.no_table_level
    config.enable_db_level = not args.no_db_level
    config.enable_pause_resume = not args.no_pause_resume
    config.enable_indexes = not args.no_indexes
    config.enable_alter_table = not args.no_alter_table
    config.enable_concurrent = not args.no_concurrent
    
    # Setup logging
    setup_logging(config)
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("CCPR Integration Test Runner")
    logger.info("=" * 60)
    logger.info(f"Upstream: {config.upstream.host}:{config.upstream.port}")
    logger.info(f"Downstream: {config.downstream.host}:{config.downstream.port}")
    logger.info(f"Duration: {timedelta(seconds=config.test_duration)}")
    logger.info("=" * 60)
    
    try:
        if args.overnight:
            results = run_overnight_test(config)
        elif args.scenarios_only:
            results = run_continuous_test(config, config.test_duration)
        elif args.performance_only:
            results = run_performance_test(config, config.test_duration)
        else:
            # Default: run overnight test
            results = run_overnight_test(config)
        
        # Print summary and save report
        print_summary(results)
        save_report(results, config.final_report_file)
        
        # Exit with appropriate code
        success = results.get("overall_success", False) or results.get("success", False)
        if not success and results.get("pass_rate"):
            success = results["pass_rate"] >= 80
        
        sys.exit(0 if success else 1)
    
    except KeyboardInterrupt:
        logger.info("\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
