#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

CN1_SQL_PORT=${CN1_SQL_PORT:-6010}
CN2_SQL_PORT=${CN2_SQL_PORT:-6011}
CN3_SQL_PORT=${CN3_SQL_PORT:-6012}
LOG_PORT=${LOG_PORT:-32010}
IMAGE_NAME=${IMAGE_NAME:-matrixorigin/matrixone:latest}

case "${1:-up}" in
  up)
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
    docker compose up -d
    echo ""
    echo "连接任意 CN:"
    echo "  mysql -h 127.0.0.1 -P ${CN1_SQL_PORT} -u root -p111"
    echo "  mysql -h 127.0.0.1 -P ${CN2_SQL_PORT} -u root -p111"
    echo "  mysql -h 127.0.0.1 -P ${CN3_SQL_PORT} -u root -p111"
    ;;
  down)
    docker compose down
    ;;
  logs)
    docker compose logs -f ${2:-}
    ;;
  ps)
    docker compose ps
    ;;
  restart)
    docker compose restart ${2:-}
    ;;
  clean)
    docker compose down
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
