#!/bin/bash
# Setup FlameGraph — clone if not present
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FLAMEGRAPH_DIR="${SCRIPT_DIR}/FlameGraph"

if [ -d "$FLAMEGRAPH_DIR" ]; then
    echo "[flamegraph] FlameGraph already exists at $FLAMEGRAPH_DIR"
else
    echo "[flamegraph] Cloning FlameGraph..."
    git clone --depth 1 https://gitee.com/trisolarans/FlameGraph "$FLAMEGRAPH_DIR"
    echo "[flamegraph] Done: $FLAMEGRAPH_DIR"
fi

STACKCOLLAPSE="${FLAMEGRAPH_DIR}/stackcollapse-perf.pl"
FLAMEGRAPH="${FLAMEGRAPH_DIR}/flamegraph.pl"

if [ ! -x "$STACKCOLLAPSE" ] || [ ! -x "$FLAMEGRAPH" ]; then
    echo "[flamegraph] ERROR: FlameGraph scripts not found" >&2
    exit 1
fi
