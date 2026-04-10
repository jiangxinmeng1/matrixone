#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

# 兼容 docker compose V2 插件和 V1 独立命令
if docker compose version &>/dev/null; then
  DC="docker compose"
elif docker-compose version &>/dev/null; then
  DC="docker-compose"
else
  echo "错误: 未找到 docker compose 或 docker-compose" >&2
  exit 1
fi

CN1_SQL_PORT=${CN1_SQL_PORT:-6010}
CN2_SQL_PORT=${CN2_SQL_PORT:-6011}
CN3_SQL_PORT=${CN3_SQL_PORT:-6012}
LOG_PORT=${LOG_PORT:-32010}

# 项目根目录（start.sh 在 etc/launch-custom-docker/ 下）
MO_ROOT="$(cd ../.. && pwd)"
IMAGE_NAME=${IMAGE_NAME:-mo-custom:latest}

case "${1:-up}" in
  up)
    echo "=== Building MatrixOne from source ==="
    docker build --build-arg GOPROXY="${GOPROXY:-https://goproxy.cn,direct}" -t "$IMAGE_NAME" -f "$MO_ROOT/optools/images/Dockerfile" "$MO_ROOT"
    echo "=== Build done ==="
    echo ""
    echo "=== MatrixOne Custom Cluster (3 CN) ==="
    echo "  Image:    $IMAGE_NAME"
    echo "  CN1 port: $CN1_SQL_PORT"
    echo "  CN2 port: $CN2_SQL_PORT"
    echo "  CN3 port: $CN3_SQL_PORT"
    echo "  LOG port: $LOG_PORT"
    echo "  Resources:"
    echo "    LogService: 2c / 2G"
    echo "    TN(DN):     5c / 15G"
    echo "    CN x3:      14c / 25G each"
    echo "========================================="
    export CN1_SQL_PORT CN2_SQL_PORT CN3_SQL_PORT LOG_PORT IMAGE_NAME
    $DC up -d
    echo ""
    echo "连接任意 CN:"
    echo "  mysql -h 127.0.0.1 -P ${CN1_SQL_PORT} -u root -p111"
    echo "  mysql -h 127.0.0.1 -P ${CN2_SQL_PORT} -u root -p111"
    echo "  mysql -h 127.0.0.1 -P ${CN3_SQL_PORT} -u root -p111"
    ;;
  down)
    $DC down
    ;;
  logs)
    $DC logs -f ${2:-}
    ;;
  ps)
    $DC ps
    ;;
  restart)
    $DC restart ${2:-}
    ;;
  clean)
    $DC down
    rm -rf mo-data logs
    echo "已清理所有数据"
    ;;
  *)
    echo "用法: $0 {up|down|logs|ps|restart|clean}"
    echo ""
    echo "环境变量:"
    echo "  CN1_SQL_PORT=6010   CN1端口"
    echo "  CN2_SQL_PORT=6011   CN2端口"
    echo "  CN3_SQL_PORT=6012   CN3端口"
    echo "  LOG_PORT=32010      LogService端口"
    echo "  IMAGE_NAME=...      镜像名"
    ;;
esac
