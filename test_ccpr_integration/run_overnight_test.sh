#!/bin/bash
#
# CCPR Integration Overnight Test Runner
#
# This script runs the CCPR integration test overnight.
# It handles setup, execution, and cleanup.
#

set -e

# Default configuration
UPSTREAM_HOST="${UPSTREAM_HOST:-127.0.0.1}"
UPSTREAM_PORT="${UPSTREAM_PORT:-6001}"
DOWNSTREAM_HOST="${DOWNSTREAM_HOST:-127.0.0.1}"
DOWNSTREAM_PORT="${DOWNSTREAM_PORT:-6002}"
USER="${DB_USER:-root}"
PASSWORD="${DB_PASSWORD:-111}"

# Test duration (8 hours by default)
DURATION="${TEST_DURATION:-28800}"

# Sync interval (10 seconds by default)
SYNC_INTERVAL="${SYNC_INTERVAL:-10}"

# Output directory
OUTPUT_DIR="${OUTPUT_DIR:-./test_output}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${OUTPUT_DIR}/ccpr_test_${TIMESTAMP}.log"
REPORT_FILE="${OUTPUT_DIR}/ccpr_report_${TIMESTAMP}.json"

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}========================================${NC}"
}

print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        print_error "Python3 is not installed"
        exit 1
    fi
    print_info "Python3: $(python3 --version)"
    
    # Check pymysql
    if ! python3 -c "import pymysql" 2>/dev/null; then
        print_info "Installing pymysql..."
        pip3 install pymysql
    fi
    print_info "pymysql: installed"
    
    # Check connectivity
    print_info "Checking cluster connectivity..."
    
    if ! python3 -c "
import pymysql
try:
    conn = pymysql.connect(host='$UPSTREAM_HOST', port=$UPSTREAM_PORT, user='$USER', password='$PASSWORD', connect_timeout=5)
    conn.close()
    print('Upstream: OK')
except Exception as e:
    print(f'Upstream: FAILED - {e}')
    exit(1)
" ; then
        print_error "Cannot connect to upstream cluster"
        exit 1
    fi
    
    if ! python3 -c "
import pymysql
try:
    conn = pymysql.connect(host='$DOWNSTREAM_HOST', port=$DOWNSTREAM_PORT, user='$USER', password='$PASSWORD', connect_timeout=5)
    conn.close()
    print('Downstream: OK')
except Exception as e:
    print(f'Downstream: FAILED - {e}')
    exit(1)
" ; then
        print_error "Cannot connect to downstream cluster"
        exit 1
    fi
    
    echo ""
}

setup_output_dir() {
    mkdir -p "$OUTPUT_DIR"
    print_info "Output directory: $OUTPUT_DIR"
    print_info "Log file: $LOG_FILE"
    print_info "Report file: $REPORT_FILE"
}

run_test() {
    print_header "Starting CCPR Integration Test"
    
    DURATION_HOURS=$(echo "scale=1; $DURATION / 3600" | bc)
    print_info "Duration: $DURATION seconds ($DURATION_HOURS hours)"
    print_info "Start time: $(date)"
    print_info "Expected end time: $(date -d "+$DURATION seconds")"
    
    echo ""
    print_info "Running test..."
    
    cd "$SCRIPT_DIR"
    
    python3 main.py \
        --overnight \
        --duration "$DURATION" \
        --upstream-host "$UPSTREAM_HOST" \
        --upstream-port "$UPSTREAM_PORT" \
        --downstream-host "$DOWNSTREAM_HOST" \
        --downstream-port "$DOWNSTREAM_PORT" \
        --user "$USER" \
        --password "$PASSWORD" \
        --sync-interval "$SYNC_INTERVAL" \
        --log-file "$LOG_FILE" \
        --report-file "$REPORT_FILE" \
        --log-level INFO \
        2>&1 | tee -a "$LOG_FILE"
    
    EXIT_CODE=${PIPESTATUS[0]}
    
    return $EXIT_CODE
}

print_summary() {
    print_header "Test Complete"
    print_info "End time: $(date)"
    print_info "Log file: $LOG_FILE"
    print_info "Report file: $REPORT_FILE"
    
    if [ -f "$REPORT_FILE" ]; then
        echo ""
        print_info "Report summary:"
        python3 -c "
import json
with open('$REPORT_FILE') as f:
    data = json.load(f)
    if 'overall_success' in data:
        print(f'  Overall Success: {data[\"overall_success\"]}')
    if 'scenario_tests' in data:
        st = data['scenario_tests']
        print(f'  Scenario Pass Rate: {st.get(\"pass_rate\", \"N/A\"):.1f}%')
    if 'performance_test' in data:
        pt = data['performance_test']
        print(f'  Performance Test: {\"PASSED\" if pt.get(\"success\") else \"FAILED\"}')
"
    fi
}

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help              Show this help message"
    echo "  -d, --duration SECONDS  Test duration in seconds (default: 28800 = 8 hours)"
    echo "  --upstream-host HOST    Upstream cluster host (default: 127.0.0.1)"
    echo "  --upstream-port PORT    Upstream cluster port (default: 6001)"
    echo "  --downstream-host HOST  Downstream cluster host (default: 127.0.0.1)"
    echo "  --downstream-port PORT  Downstream cluster port (default: 6002)"
    echo "  --user USER             Database user (default: root)"
    echo "  --password PASSWORD     Database password (default: 111)"
    echo "  --output-dir DIR        Output directory (default: ./test_output)"
    echo ""
    echo "Environment variables:"
    echo "  UPSTREAM_HOST, UPSTREAM_PORT, DOWNSTREAM_HOST, DOWNSTREAM_PORT"
    echo "  DB_USER, DB_PASSWORD, TEST_DURATION, SYNC_INTERVAL, OUTPUT_DIR"
    echo ""
    echo "Examples:"
    echo "  # Run 8-hour overnight test"
    echo "  $0"
    echo ""
    echo "  # Run 1-hour test"
    echo "  $0 -d 3600"
    echo ""
    echo "  # Run with custom cluster ports"
    echo "  $0 --upstream-port 6001 --downstream-port 6002"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -d|--duration)
            DURATION="$2"
            shift 2
            ;;
        --upstream-host)
            UPSTREAM_HOST="$2"
            shift 2
            ;;
        --upstream-port)
            UPSTREAM_PORT="$2"
            shift 2
            ;;
        --downstream-host)
            DOWNSTREAM_HOST="$2"
            shift 2
            ;;
        --downstream-port)
            DOWNSTREAM_PORT="$2"
            shift 2
            ;;
        --user)
            USER="$2"
            shift 2
            ;;
        --password)
            PASSWORD="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Main execution
main() {
    print_header "CCPR Integration Test"
    
    check_prerequisites
    setup_output_dir
    
    if run_test; then
        print_summary
        exit 0
    else
        print_summary
        print_error "Test failed!"
        exit 1
    fi
}

main
