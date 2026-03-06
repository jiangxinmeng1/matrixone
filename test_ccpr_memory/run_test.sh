#!/bin/bash

# ============================================================================
# CCPR Memory Test Runner
# 运行CCPR内存控制测试并收集metrics
# ============================================================================

set -e

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

# 配置
MO_HOST="${MO_HOST:-127.0.0.1}"
MO_PORT="${MO_PORT:-6001}"
MO_PORT_DOWN="${MO_PORT_DOWN:-6002}"
MO_USER="${MO_USER:-root}"
MO_PASSWORD="${MO_PASSWORD:-111}"
METRICS_PORT="${METRICS_PORT:-7001}"
METRICS_PORT_DOWN="${METRICS_PORT_DOWN:-7002}"
DATA_FILE="${DATA_FILE:-sample_1m.csv}"
DATA_LINES="${DATA_LINES:-1000000}"

# 集群启动脚本
CLUSTER_SCRIPT="${ROOT_DIR}/start-two-clusters-minio.sh"
CLUSTER_LOG="${SCRIPT_DIR}/cluster_startup.log"

# 等待MySQL连接的超时时间（秒）
MYSQL_WAIT_TIMEOUT="${MYSQL_WAIT_TIMEOUT:-300}"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
}

log_section() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

# 检查MySQL是否可连接 (指定端口)
check_mysql_connection_port() {
    local port="$1"
    mysql -h "$MO_HOST" -P "$port" -u "$MO_USER" -p"$MO_PASSWORD" -e "SELECT 1" &>/dev/null
    return $?
}

# 检查上游MySQL
check_mysql_connection() {
    check_mysql_connection_port "$MO_PORT"
    return $?
}

# 检查下游MySQL
check_mysql_connection_downstream() {
    check_mysql_connection_port "$MO_PORT_DOWN"
    return $?
}

# 等待MySQL可连接
wait_for_mysql() {
    local timeout="$1"
    local start_time=$(date +%s)
    local elapsed=0
    local upstream_ready=false
    local downstream_ready=false
    
    log_info "Waiting for MySQL connections..."
    log_info "  Upstream: ${MO_HOST}:${MO_PORT}"
    log_info "  Downstream: ${MO_HOST}:${MO_PORT_DOWN}"
    
    while [ $elapsed -lt $timeout ]; do
        # 检查上游
        if [ "$upstream_ready" = false ] && check_mysql_connection; then
            log_info "Upstream MySQL is ready! (${MO_HOST}:${MO_PORT})"
            upstream_ready=true
        fi
        
        # 检查下游
        if [ "$downstream_ready" = false ] && check_mysql_connection_downstream; then
            log_info "Downstream MySQL is ready! (${MO_HOST}:${MO_PORT_DOWN})"
            downstream_ready=true
        fi
        
        # 两个都就绪
        if [ "$upstream_ready" = true ] && [ "$downstream_ready" = true ]; then
            log_info "Both clusters are ready! (waited ${elapsed}s)"
            return 0
        fi
        
        sleep 2
        elapsed=$(($(date +%s) - start_time))
        
        # 每30秒打印一次进度
        if [ $((elapsed % 30)) -eq 0 ] && [ $elapsed -gt 0 ]; then
            log_info "Still waiting... up=${upstream_ready}, down=${downstream_ready} (${elapsed}s / ${timeout}s)"
        fi
    done
    
    log_error "Timeout waiting for MySQL connections after ${timeout}s"
    log_error "  Upstream ready: $upstream_ready"
    log_error "  Downstream ready: $downstream_ready"
    return 1
}

# 启动集群
start_cluster() {
    log_section "Starting MatrixOne Clusters"
    
    # 检查启动脚本是否存在
    if [ ! -f "$CLUSTER_SCRIPT" ]; then
        log_error "Cluster startup script not found: $CLUSTER_SCRIPT"
        exit 1
    fi
    
    # 检查是否已经有集群在运行
    if check_mysql_connection; then
        log_info "MySQL is already accessible, skipping cluster startup"
        return 0
    fi
    
    # 检查是否有mo-service进程在运行
    if pgrep -f "mo-service" > /dev/null; then
        log_warn "mo-service processes found, but MySQL not accessible"
        log_warn "You may need to manually check the cluster status"
        read -p "Continue waiting for MySQL? [Y/n]: " response
        if [ "$response" = "n" ] || [ "$response" = "N" ]; then
            exit 1
        fi
    else
        # 启动集群（后台运行）
        log_info "Starting clusters with: $CLUSTER_SCRIPT"
        log_info "Cluster log: $CLUSTER_LOG"
        
        cd "$ROOT_DIR"
        nohup "$CLUSTER_SCRIPT" > "$CLUSTER_LOG" 2>&1 &
        CLUSTER_PID=$!
        
        log_info "Cluster startup script started with PID: $CLUSTER_PID"
        
        # 等待几秒让脚本开始执行
        sleep 5
        
        # 检查进程是否还在运行
        if ! kill -0 $CLUSTER_PID 2>/dev/null; then
            log_error "Cluster startup script exited unexpectedly"
            log_error "Check log: $CLUSTER_LOG"
            tail -50 "$CLUSTER_LOG" 2>/dev/null || true
            exit 1
        fi
        
        cd "$SCRIPT_DIR"
    fi
    
    # 等待MySQL可连接
    if ! wait_for_mysql "$MYSQL_WAIT_TIMEOUT"; then
        log_error "Failed to connect to MySQL"
        log_error "Cluster startup log (last 100 lines):"
        tail -100 "$CLUSTER_LOG" 2>/dev/null || true
        exit 1
    fi
    
    log_info "Cluster is ready!"
}

# 停止集群
stop_cluster() {
    log_section "Stopping MatrixOne Clusters"
    
    # 查找并杀死mo-service进程
    if pgrep -f "mo-service" > /dev/null; then
        log_info "Stopping mo-service processes..."
        pkill -f "mo-service" || true
        sleep 3
        
        # 强制杀死（如果还在运行）
        if pgrep -f "mo-service" > /dev/null; then
            log_warn "Force killing mo-service processes..."
            pkill -9 -f "mo-service" || true
        fi
        
        log_info "mo-service processes stopped"
    else
        log_info "No mo-service processes found"
    fi
    
    # 杀死minio进程（如果有）
    if pgrep -f "minio" > /dev/null; then
        log_info "Stopping minio processes..."
        pkill -f "minio" || true
        sleep 2
        log_info "minio processes stopped"
    fi
}

# 下载数据文件
download_data() {
    log_section "Downloading Test Data"
    
    if [ -f "$DATA_FILE" ]; then
        log_info "Data file already exists: $DATA_FILE"
        local line_count=$(wc -l < "$DATA_FILE")
        log_info "File has $line_count lines"
        
        read -p "Re-download? [y/N]: " response
        if [ "$response" != "y" ] && [ "$response" != "Y" ]; then
            log_info "Using existing data file"
            return 0
        fi
    fi
    
    log_info "Downloading data from COS..."
    log_info "Output file: $DATA_FILE"
    log_info "Max lines: $DATA_LINES"
    
    # 检查Python和依赖
    if ! command -v python3 &> /dev/null; then
        log_error "python3 not found. Please install Python 3."
        exit 1
    fi
    
    # 检查cos-python-sdk-v5
    if ! python3 -c "import qcloud_cos" &> /dev/null; then
        log_warn "qcloud_cos not installed. Installing..."
        pip install cos-python-sdk-v5
    fi
    
    # 运行下载脚本
    python3 "$SCRIPT_DIR/download_data.py" -o "$DATA_FILE" -n "$DATA_LINES" --force
    
    if [ -f "$DATA_FILE" ]; then
        log_info "Data downloaded successfully"
        local file_size=$(du -h "$DATA_FILE" | cut -f1)
        log_info "File size: $file_size"
    else
        log_error "Failed to download data"
        exit 1
    fi
}

# 创建输出目录
OUTPUT_DIR="test_results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR"

log_info "Output directory: $OUTPUT_DIR"

# 收集CCPR metrics函数
collect_metrics() {
    local label="$1"
    local output_file="$OUTPUT_DIR/metrics_${label}.txt"
    
    log_info "Collecting metrics: $label"
    
    curl -s "http://${MO_HOST}:${METRICS_PORT}/metrics" | grep "^mo_ccpr" > "$output_file" 2>/dev/null || true
    
    if [ -s "$output_file" ]; then
        log_info "Metrics saved to $output_file"
    else
        log_warn "No CCPR metrics collected"
    fi
}

# 收集内存相关metrics
collect_memory_metrics() {
    local label="$1"
    local output_file="$OUTPUT_DIR/memory_${label}.txt"
    
    log_info "Collecting memory metrics: $label"
    
    {
        echo "=== CCPR Memory Metrics at $label ==="
        echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
        echo ""
        
        curl -s "http://${MO_HOST}:${METRICS_PORT}/metrics" 2>/dev/null | grep -E "^mo_ccpr_memory|^mo_ccpr_pool" || echo "No memory metrics found"
        
        echo ""
        echo "=== System Memory ==="
        free -h 2>/dev/null || echo "free command not available"
        
    } > "$output_file"
}

# 执行SQL并记录时间
run_sql() {
    local sql="$1"
    local description="$2"
    
    log_info "Running: $description"
    
    local start_time=$(date +%s.%N)
    
    mysql -h "$MO_HOST" -P "$MO_PORT" -u "$MO_USER" -p"$MO_PASSWORD" -e "$sql" 2>&1 || {
        log_error "SQL execution failed: $description"
        return 1
    }
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    
    log_info "Completed in ${duration}s: $description"
    
    echo "$description,$duration" >> "$OUTPUT_DIR/timing.csv"
}

# 执行SQL文件
run_sql_file() {
    local sql_file="$1"
    local description="$2"
    
    log_info "Running SQL file: $sql_file"
    
    local start_time=$(date +%s.%N)
    
    mysql -h "$MO_HOST" -P "$MO_PORT" -u "$MO_USER" -p"$MO_PASSWORD" < "$sql_file" 2>&1 | tee "$OUTPUT_DIR/sql_output.log" || {
        log_error "SQL file execution failed: $sql_file"
        return 1
    }
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    
    log_info "SQL file completed in ${duration}s"
}

# 监控metrics (后台运行)
start_metrics_monitor() {
    log_info "Starting metrics monitor..."
    
    local monitor_file="$OUTPUT_DIR/metrics_monitor.csv"
    echo "timestamp,memory_total,memory_object_content,memory_decompress,pool_hit_rate" > "$monitor_file"
    
    while true; do
        local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
        local metrics=$(curl -s "http://${MO_HOST}:${METRICS_PORT}/metrics" 2>/dev/null)
        
        local mem_total=$(echo "$metrics" | grep 'mo_ccpr_memory_bytes{type="total"}' | awk '{print $2}' || echo "0")
        local mem_object=$(echo "$metrics" | grep 'mo_ccpr_memory_bytes{type="object_content"}' | awk '{print $2}' || echo "0")
        local mem_decompress=$(echo "$metrics" | grep 'mo_ccpr_memory_bytes{type="decompress_buffer"}' | awk '{print $2}' || echo "0")
        
        local pool_hit=$(echo "$metrics" | grep 'mo_ccpr_pool_total{.*result="hit"}' | awk '{sum+=$2} END {print sum}' || echo "0")
        local pool_miss=$(echo "$metrics" | grep 'mo_ccpr_pool_total{.*result="miss"}' | awk '{sum+=$2} END {print sum}' || echo "0")
        local pool_total=$((pool_hit + pool_miss))
        local hit_rate="0"
        if [ "$pool_total" -gt 0 ]; then
            hit_rate=$(echo "scale=4; $pool_hit / $pool_total" | bc)
        fi
        
        echo "$timestamp,$mem_total,$mem_object,$mem_decompress,$hit_rate" >> "$monitor_file"
        
        sleep 5
    done
}

# 停止metrics监控
stop_metrics_monitor() {
    log_info "Stopping metrics monitor..."
    pkill -f "metrics_monitor" 2>/dev/null || true
}

# 生成测试报告
generate_report() {
    log_section "Generating Test Report"
    
    local report_file="$OUTPUT_DIR/report.md"
    
    {
        echo "# CCPR Memory Test Report"
        echo ""
        echo "**Generated:** $(date '+%Y-%m-%d %H:%M:%S')"
        echo "**Host:** $MO_HOST:$MO_PORT"
        echo "**Data File:** $DATA_FILE"
        echo ""
        
        echo "## Test Timing Summary"
        echo ""
        echo "| Operation | Duration (s) |"
        echo "|-----------|--------------|"
        if [ -f "$OUTPUT_DIR/timing.csv" ]; then
            while IFS=',' read -r op duration; do
                echo "| $op | $duration |"
            done < "$OUTPUT_DIR/timing.csv"
        fi
        echo ""
        
        echo "## Memory Metrics Summary"
        echo ""
        if [ -f "$OUTPUT_DIR/memory_final.txt" ]; then
            cat "$OUTPUT_DIR/memory_final.txt"
        fi
        echo ""
        
        echo "## CCPR Metrics"
        echo ""
        echo "\`\`\`"
        if [ -f "$OUTPUT_DIR/metrics_final.txt" ]; then
            cat "$OUTPUT_DIR/metrics_final.txt"
        fi
        echo "\`\`\`"
        
    } > "$report_file"
    
    log_info "Report generated: $report_file"
}

# 运行 CCPR 测试 (Python脚本，带订阅和watermark监控)
run_ccpr_test() {
    log_section "Running CCPR Memory Test (with subscription)"
    
    # 检查 Python 脚本
    CCPR_SCRIPT="$SCRIPT_DIR/ccpr_memory_test.py"
    if [ ! -f "$CCPR_SCRIPT" ]; then
        log_error "CCPR test script not found: $CCPR_SCRIPT"
        exit 1
    fi
    
    # 检查 pymysql
    if ! python3 -c "import pymysql" &> /dev/null; then
        log_warn "pymysql not installed. Installing..."
        pip install pymysql
    fi
    
    # 运行 CCPR 测试
    log_info "Starting CCPR test..."
    log_info "  Upstream: ${MO_HOST}:${MO_PORT}"
    log_info "  Downstream: ${MO_HOST}:${MO_PORT_DOWN:-6002}"
    log_info "  Data file: ${DATA_FILE}"
    
    python3 "$CCPR_SCRIPT" \
        --upstream-host "$MO_HOST" \
        --upstream-port "$MO_PORT" \
        --downstream-host "$MO_HOST" \
        --downstream-port "${MO_PORT_DOWN:-6002}" \
        --user "$MO_USER" \
        --password "$MO_PASSWORD" \
        --data-file "$DATA_FILE" \
        2>&1 | tee "$OUTPUT_DIR/ccpr_test.log"
    
    CCPR_EXIT_CODE=${PIPESTATUS[0]}
    
    if [ $CCPR_EXIT_CODE -ne 0 ]; then
        log_error "CCPR test failed with exit code: $CCPR_EXIT_CODE"
        log_error "Check log: $OUTPUT_DIR/ccpr_test.log"
        return 1
    fi
    
    log_info "CCPR test completed successfully"
    return 0
}

# 主函数
main() {
    log_section "CCPR Memory Test Started"
    
    # 启动集群并等待连接
    start_cluster
    
    # 下载数据
    download_data
    
    # 初始化CSV文件
    echo "operation,duration_seconds" > "$OUTPUT_DIR/timing.csv"
    
    # 收集初始metrics
    collect_metrics "initial"
    collect_memory_metrics "initial"
    
    # 启动metrics监控 (后台)
    start_metrics_monitor &
    MONITOR_PID=$!
    log_info "Metrics monitor started with PID: $MONITOR_PID"
    
    # 运行 CCPR 测试 (Python脚本，包含订阅和watermark监控)
    log_section "Running CCPR Test with Subscription"
    
    local test_start=$(date +%s)
    
    if ! run_ccpr_test; then
        log_error "CCPR test failed!"
        # 收集失败时的metrics
        collect_metrics "on_failure"
        collect_memory_metrics "on_failure"
    fi
    
    local test_end=$(date +%s)
    local test_duration=$((test_end - test_start))
    log_info "Test duration: ${test_duration}s"
    
    # 收集最终metrics
    collect_metrics "final"
    collect_memory_metrics "final"
    
    # 停止监控
    kill $MONITOR_PID 2>/dev/null || true
    
    # 生成报告
    generate_report
    
    log_section "Test Completed"
    log_info "Results saved to: $OUTPUT_DIR"
    log_info "View report: cat $OUTPUT_DIR/report.md"
    log_info "View CCPR log: cat $OUTPUT_DIR/ccpr_test.log"
}

# 单独运行 CCPR 测试（假设集群已启动）
run_ccpr_only() {
    download_data
    
    log_section "Running CCPR Test Only"
    
    # 初始化输出目录
    echo "operation,duration_seconds" > "$OUTPUT_DIR/timing.csv"
    
    collect_metrics "initial"
    collect_memory_metrics "initial"
    
    # 启动metrics监控
    start_metrics_monitor &
    MONITOR_PID=$!
    
    # 运行 CCPR 测试
    run_ccpr_test
    
    # 收集最终metrics
    collect_metrics "final"
    collect_memory_metrics "final"
    
    # 停止监控
    kill $MONITOR_PID 2>/dev/null || true
    
    generate_report
}

# 帮助信息
show_help() {
    echo "CCPR Memory Test Runner"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  run|all       Run full CCPR test (default) - starts cluster, downloads data, runs CCPR test"
    echo "  start         Start clusters only and wait for MySQL connection"
    echo "  stop          Stop all running clusters"
    echo "  download      Download test data only"
    echo "  ccpr          Run CCPR test only (assumes cluster is running)"
    echo "  metrics       Collect current metrics only"
    echo "  status        Check cluster and MySQL status"
    echo "  help          Show this help message"
    echo ""
    echo "Environment Variables:"
    echo "  MO_HOST            MatrixOne host (default: 127.0.0.1)"
    echo "  MO_PORT            Upstream cluster MySQL port (default: 6001)"
    echo "  MO_PORT_DOWN       Downstream cluster MySQL port (default: 6002)"
    echo "  MO_USER            MatrixOne user (default: root)"
    echo "  MO_PASSWORD        MatrixOne password (default: 111)"
    echo "  METRICS_PORT       Upstream metrics port (default: 7001)"
    echo "  METRICS_PORT_DOWN  Downstream metrics port (default: 7002)"
    echo "  DATA_FILE          Data file name (default: sample_1m.csv)"
    echo "  DATA_LINES         Number of lines to download (default: 1000000)"
    echo "  MYSQL_WAIT_TIMEOUT Timeout for MySQL connection (default: 300s)"
    echo ""
    echo "Test Flow:"
    echo "  1. Start two MatrixOne clusters (upstream: 6001, downstream: 6002)"
    echo "  2. Download test data from COS"
    echo "  3. Create publication on upstream, subscription on downstream"
    echo "  4. Run DML tests (INSERT, UPDATE, DELETE) with CCPR sync"
    echo "  5. Run index tests (IVF, HNSW, Fulltext, B-tree)"
    echo "  6. Monitor watermark changes and check row counts"
    echo "  7. Stop on nonretryable errors (preserve data for debugging)"
    echo ""
    echo "Examples:"
    echo "  $0                 # Full test (same as ./run_test.sh run)"
    echo "  $0 start           # Start clusters only"
    echo "  $0 download        # Download data only"
    echo "  $0 ccpr            # Run CCPR test (cluster must be running)"
    echo "  $0 status          # Check cluster status"
    echo "  $0 stop            # Stop clusters"
}

# 检查状态
check_status() {
    log_section "Cluster Status"
    
    # 检查mo-service进程
    echo "=== mo-service processes ==="
    if pgrep -f "mo-service" > /dev/null; then
        pgrep -af "mo-service" | head -10
    else
        echo "No mo-service processes found"
    fi
    echo ""
    
    # 检查minio进程
    echo "=== minio processes ==="
    if pgrep -f "minio" > /dev/null; then
        pgrep -af "minio" | head -5
    else
        echo "No minio processes found"
    fi
    echo ""
    
    # 检查MySQL连接 - 上游
    echo "=== Upstream MySQL (${MO_HOST}:${MO_PORT}) ==="
    if check_mysql_connection; then
        log_info "Upstream MySQL is accessible"
        mysql -h "$MO_HOST" -P "$MO_PORT" -u "$MO_USER" -p"$MO_PASSWORD" -e "SELECT VERSION();" 2>/dev/null || true
    else
        log_warn "Upstream MySQL is NOT accessible"
    fi
    echo ""
    
    # 检查MySQL连接 - 下游
    echo "=== Downstream MySQL (${MO_HOST}:${MO_PORT_DOWN}) ==="
    if check_mysql_connection_downstream; then
        log_info "Downstream MySQL is accessible"
        mysql -h "$MO_HOST" -P "$MO_PORT_DOWN" -u "$MO_USER" -p"$MO_PASSWORD" -e "SELECT VERSION();" 2>/dev/null || true
    else
        log_warn "Downstream MySQL is NOT accessible"
    fi
    echo ""
    
    # 检查CCPR状态
    echo "=== CCPR Status ==="
    if check_mysql_connection_downstream; then
        mysql -h "$MO_HOST" -P "$MO_PORT_DOWN" -u "$MO_USER" -p"$MO_PASSWORD" \
            -e "SELECT subscription_name, state, watermark, error_message FROM mo_catalog.mo_ccpr_log WHERE drop_at IS NULL;" 2>/dev/null || \
            echo "No CCPR subscriptions found or unable to query"
    fi
    echo ""
    
    # 检查端口
    echo "=== Listening Ports ==="
    netstat -tlnp 2>/dev/null | grep -E ":(6001|6002|7001|7002|9000|9001|9002|9003) " || \
    ss -tlnp 2>/dev/null | grep -E ":(6001|6002|7001|7002|9000|9001|9002|9003) " || \
    echo "Unable to check ports"
}

# 解析命令行参数
case "${1:-run}" in
    run|all)
        main
        ;;
    start)
        start_cluster
        ;;
    stop)
        stop_cluster
        ;;
    download)
        download_data
        ;;
    ccpr)
        run_ccpr_only
        ;;
    metrics)
        collect_metrics "manual"
        collect_memory_metrics "manual"
        ;;
    status)
        check_status
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        log_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac
