#!/bin/bash
# perf-event-check.sh — 检查 perf 硬件事件的可用性
#
# 用法: bash perf-event-check.sh [event1,event2,...]
# 自动检测 NUMA 节点数，多 NUMA 时追加 node-load-misses,node-store-misses。
# 输出每个事件的可用状态，最后汇总 available= 和 missing= 列表。

# ---- NUMA 检测 ----
NUMA_NODES=$(ls -d /sys/devices/system/node/node[0-9]* 2>/dev/null | wc -l)
if [ "$NUMA_NODES" -gt 1 ]; then
  NUMA_EVENTS=",node-load-misses,node-store-misses"
else
  NUMA_EVENTS=""
fi

DESIRED="${1:-cycles,instructions,cache-misses,cache-references,branch-misses,branches}${NUMA_EVENTS}"

AVAILABLE=""
MISSING=""

echo "=== Perf Event Availability Check ==="
echo "NUMA nodes: ${NUMA_NODES:-unknown}"
echo ""

for ev in $(echo "$DESIRED" | tr ',' ' '); do
  output=$(perf stat -e "$ev" true 2>&1)
  rc=$?
  if [ $rc -ne 0 ] || echo "$output" | grep -q "<not supported>"; then
    echo "  [MISS] $ev"
    MISSING="${MISSING}${MISSING:+,}${ev}"
  else
    echo "  [OK]   $ev"
    AVAILABLE="${AVAILABLE}${AVAILABLE:+,}${ev}"
  fi
done

echo ""
echo "available=${AVAILABLE:-}"
echo "missing=${MISSING:-}"
