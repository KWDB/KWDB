#!/bin/bash
# perf-capture.sh — 性能抓取
#
# 用法: bash perf-capture.sh -p <pid> -c <client_script> [-o output] [-f freq] [-g callgraph]
#
# 事件列表由 perf-event-check.sh 自动检测。
# 示例: bash perf-capture.sh -p 40589 -c ./do.sh
#       bash perf-capture.sh -p 1234 -c ./workload.sh -o my.data -f 99 -g fp

set -e

usage() {
  echo "用法: $0 -p <pid> -c <client_script> [-o output] [-f freq] [-g callgraph]"
  echo ""
  echo "必需参数:"
  echo "  -p  服务器进程 PID"
  echo "  -c  客户端脚本路径"
  echo ""
  echo "可选参数:"
  echo "  -o  perf.data 输出路径 (默认: ./perf.data)"
  echo "  -f  采样频率 Hz     (默认: 49)"
  echo "  -g  调用栈方式       (默认: dwarf)"
  exit 1
}

# ---- 默认值 ----
OUTPUT_FILE="./perf.data"
FREQ=49
CALL_GRAPH=dwarf

# ---- 参数解析 ----
while getopts "p:c:o:f:g:h" opt; do
  case $opt in
    p) SERVER_PID="$OPTARG" ;;
    c) CLIENT_SCRIPT="$OPTARG" ;;
    o) OUTPUT_FILE="$OPTARG" ;;
    f) FREQ="$OPTARG" ;;
    g) CALL_GRAPH="$OPTARG" ;;
    h) usage ;;
    *) usage ;;
  esac
done

if [ -z "$SERVER_PID" ] || [ -z "$CLIENT_SCRIPT" ]; then
  echo "[perf] ERROR: -p and -c are required" >&2
  usage
fi

if [ ! -f "$CLIENT_SCRIPT" ]; then
  echo "[perf] ERROR: client script not found: $CLIENT_SCRIPT" >&2
  exit 1
fi

# ---- 自动检测可用事件 ----
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CHECK_SCRIPT="${SCRIPT_DIR}/perf-event-check.sh"

if [ -f "$CHECK_SCRIPT" ]; then
  EVENTS=$(bash "$CHECK_SCRIPT" | awk -F= '/^available=/{print $2}')
else
  echo "[perf] ERROR: cannot find perf-event-check.sh at $CHECK_SCRIPT" >&2
  exit 1
fi

if [ -z "$EVENTS" ]; then
  echo "[perf] ERROR: no available events found" >&2
  exit 1
fi

echo "[perf] Starting capture on PID $SERVER_PID"
echo "[perf] Events: $EVENTS"
echo "[perf] Output: $OUTPUT_FILE"
echo "[perf] Freq: ${FREQ}Hz, Call graph: $CALL_GRAPH"

perf record -e "$EVENTS" -p "$SERVER_PID" -F "$FREQ" -g --call-graph "$CALL_GRAPH" -o "$OUTPUT_FILE" &
PERF_PID=$!

sleep 0.5
if ! kill -0 $PERF_PID 2>/dev/null; then
  echo "[perf] ERROR: perf record failed to start" >&2
  exit 1
fi

echo "[perf] Profiler ready (PID $PERF_PID), waiting 1.5s for warmup..."
sleep 1.5

echo "[perf] Running client script: $CLIENT_SCRIPT"
bash "$CLIENT_SCRIPT"

echo "[perf] Client script done, stopping profiler..."
kill -INT $PERF_PID 2>/dev/null || true
wait $PERF_PID 2>/dev/null || true

echo "[perf] Capture complete. Data saved to $OUTPUT_FILE"
