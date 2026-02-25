#!/bin/bash
#
# CCPR Integration Long Test Runner
#
# 按TEST_PLAN.md运行长时间测试:
# - 同时跑account/db/table三种level
# - 上下游都在不同account下
# - 6个阶段流程: DML + checkpoint + ALTER
#

set -e

# 默认配置
UPSTREAM_HOST="${UPSTREAM_HOST:-127.0.0.1}"
UPSTREAM_PORT="${UPSTREAM_PORT:-6001}"
DOWNSTREAM_HOST="${DOWNSTREAM_HOST:-127.0.0.1}"
DOWNSTREAM_PORT="${DOWNSTREAM_PORT:-6002}"
USER="${DB_USER:-root}"
PASSWORD="${DB_PASSWORD:-111}"

# 测试时长 (默认8小时)
DURATION="${TEST_DURATION:-28800}"

# 同步间隔 (默认10秒)
SYNC_INTERVAL="${SYNC_INTERVAL:-10}"

# 输出目录
OUTPUT_DIR="${OUTPUT_DIR:-./test_output}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${OUTPUT_DIR}/ccpr_long_test_${TIMESTAMP}.log"
REPORT_FILE="${OUTPUT_DIR}/ccpr_report_${TIMESTAMP}.json"

# 脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

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
    
    # 检查Python
    if ! command -v python3 &> /dev/null; then
        print_error "Python3 is not installed"
        exit 1
    fi
    print_info "Python3: $(python3 --version)"
    
    # 检查pymysql
    if ! python3 -c "import pymysql" 2>/dev/null; then
        print_info "Installing pymysql..."
        pip3 install pymysql || pip install pymysql
    fi
    print_info "pymysql: installed"
    
    # 检查连接
    print_info "Checking cluster connectivity..."
    
    python3 -c "
import pymysql
try:
    conn = pymysql.connect(host='$UPSTREAM_HOST', port=$UPSTREAM_PORT, user='$USER', password='$PASSWORD', connect_timeout=5)
    conn.close()
    print('Upstream: OK')
except Exception as e:
    print(f'Upstream: FAILED - {e}')
    exit(1)
"
    
    python3 -c "
import pymysql
try:
    conn = pymysql.connect(host='$DOWNSTREAM_HOST', port=$DOWNSTREAM_PORT, user='$USER', password='$PASSWORD', connect_timeout=5)
    conn.close()
    print('Downstream: OK')
except Exception as e:
    print(f'Downstream: FAILED - {e}')
    exit(1)
"
    echo ""
}

setup_output_dir() {
    mkdir -p "$OUTPUT_DIR"
    print_info "Output directory: $OUTPUT_DIR"
    print_info "Log file: $LOG_FILE"
    print_info "Report file: $REPORT_FILE"
}

run_test() {
    print_header "Starting CCPR Long Test"
    
    DURATION_HOURS=$(echo "scale=2; $DURATION / 3600" | bc)
    print_info "Duration: $DURATION seconds ($DURATION_HOURS hours)"
    print_info "Sync interval: $SYNC_INTERVAL seconds"
    print_info "Start time: $(date)"
    print_info "Expected end time: $(date -d "+$DURATION seconds" 2>/dev/null || date -v+${DURATION}S 2>/dev/null || echo "N/A")"
    
    echo ""
    print_info "Running long test (3 CCPR tasks: account/db/table levels)..."
    
    cd "$SCRIPT_DIR"
    
    python3 main.py \
        --long-test \
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
    
    return ${PIPESTATUS[0]}
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
    print(f'  Success: {data.get(\"success\", False)}')
    print(f'  Errors: {len(data.get(\"errors\", []))}')
    if 'tasks' in data:
        print(f'  Tasks:')
        for task in data['tasks']:
            print(f'    - {task[\"level\"]}: {task[\"name\"]}')
"
    fi
}

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "按TEST_PLAN.md运行CCPR长时间测试"
    echo ""
    echo "测试内容:"
    echo "  - 同时运行3个CCPR任务: account/database/table level"
    echo "  - 上下游分别在不同的account下运行"
    echo "  - 6个阶段: 持续DML + checkpoint + ALTER操作"
    echo ""
    echo "Options:"
    echo "  -h, --help              显示帮助"
    echo "  -d, --duration SECONDS  测试时长(秒), 默认28800=8小时"
    echo "  --upstream-host HOST    上游集群地址 (默认: 127.0.0.1)"
    echo "  --upstream-port PORT    上游集群端口 (默认: 6001)"
    echo "  --downstream-host HOST  下游集群地址 (默认: 127.0.0.1)"
    echo "  --downstream-port PORT  下游集群端口 (默认: 6002)"
    echo "  --user USER             数据库用户 (默认: root)"
    echo "  --password PASSWORD     数据库密码 (默认: 111)"
    echo "  --sync-interval SECS    CCPR同步间隔 (默认: 10)"
    echo "  --output-dir DIR        输出目录 (默认: ./test_output)"
    echo ""
    echo "Environment variables:"
    echo "  UPSTREAM_HOST, UPSTREAM_PORT, DOWNSTREAM_HOST, DOWNSTREAM_PORT"
    echo "  DB_USER, DB_PASSWORD, TEST_DURATION, SYNC_INTERVAL, OUTPUT_DIR"
    echo ""
    echo "Examples:"
    echo "  # 运行8小时长测试"
    echo "  $0"
    echo ""
    echo "  # 运行5分钟测试 (试运行)"
    echo "  $0 -d 300"
    echo ""
    echo "  # 自定义同步间隔为5分钟"
    echo "  $0 -d 3600 --sync-interval 300"
}

# 解析参数
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
        --sync-interval)
            SYNC_INTERVAL="$2"
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

# 主流程
main() {
    print_header "CCPR Long Test Runner"
    print_info "按TEST_PLAN.md运行: 同时跑account/db/table三种level"
    
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
