#!/bin/bash
# Generate per-event flame graphs from perf.data
#
# Usage: bash flamegraph-generate.sh -i <perf.data> [-d <output_dir>] [-r <dso=debug> ...]
set -e

usage() {
    echo "Usage: $0 -i <perf.data> [-d <output_dir>] [-r <dso=debug> ...]"
    echo "  -i  perf.data path"
    echo "  -d  output directory (default: .)"
    echo "  -r  DSO-to-debug mapping: <dso>=<debug> (repeatable)"
    echo "      Example: -r kwbase=/build/bin/kwbase.debug -r libkwdbts2.so=/build/lib/libkwdbts2.debug"
    exit 1
}

OUTPUT_DIR="."
declare -a DEBUG_MAPS=()

while getopts "i:d:r:h" opt; do
    case $opt in
        i) PERF_DATA="$OPTARG" ;;
        d) OUTPUT_DIR="$OPTARG" ;;
        r) DEBUG_MAPS+=("$OPTARG") ;;
        h) usage ;;
        *) usage ;;
    esac
done

if [ -z "${PERF_DATA:-}" ] || [ ! -f "$PERF_DATA" ]; then
    echo "[flamegraph] ERROR: perf.data not found: ${PERF_DATA:-<not set>}" >&2
    usage
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
bash "$SCRIPT_DIR/flamegraph-setup.sh"

STACKCOLLAPSE="${SCRIPT_DIR}/FlameGraph/stackcollapse-perf.pl"
FLAMEGRAPH="${SCRIPT_DIR}/FlameGraph/flamegraph.pl"

PARENT_DIR="$OUTPUT_DIR"
OUTPUT_DIR="$OUTPUT_DIR/flamegraphs"
mkdir -p "$OUTPUT_DIR"

echo "[flamegraph] Detecting events in $PERF_DATA..."

# Normalize perf evlist output to one event selector per line
raw_events=$(perf evlist -i "$PERF_DATA" 2>/dev/null | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | grep -v '^$\|^#')

if [ -z "$raw_events" ]; then
    echo "[flamegraph] ERROR: no events found in $PERF_DATA" >&2
    exit 1
fi

# Group raw event selectors by logical event name
#   cpu_core/cycles/    -> cycles
#   cpu_atom/cycles/    -> cycles
#   cache-misses        -> cache-misses
declare -A event_groups
ordered=()

while IFS= read -r raw; do
    [ -z "$raw" ] && continue
    logical=$(echo "$raw" | sed 's/^cpu_core\///;s/^cpu_atom\///;s/\/$//;s/:.*//')
    [ -z "$logical" ] && continue
    [ "$logical" = "dummy" ] && continue
    [ "$logical" = "instructions" ] && continue
    if [ -z "${event_groups[$logical]:-}" ]; then
        event_groups[$logical]="$raw"
        ordered+=("$logical")
    else
        event_groups[$logical]="${event_groups[$logical]} $raw"
    fi
done <<< "$raw_events"

if [ ${#ordered[@]} -eq 0 ]; then
    echo "[flamegraph] ERROR: no analyzable events found" >&2
    exit 1
fi

echo "[flamegraph] Found ${#ordered[@]} event type(s), generating flame graphs..."

# Run perf script once, save to temp file
TMP_SCRIPT=$(mktemp)
trap "rm -f $TMP_SCRIPT" EXIT

if [ ${#DEBUG_MAPS[@]} -gt 0 ]; then
    RESOLVE="$SCRIPT_DIR/flamegraph-resolve/flamegraph-resolve"
    if [ ! -x "$RESOLVE" ]; then
        echo "[flamegraph] Building flamegraph-resolve..."
        (cd "$SCRIPT_DIR/flamegraph-resolve" && go build -o flamegraph-resolve .)
    fi
    echo "[flamegraph] Extracting stacks and resolving [unknown] symbols..."
    perf script -i "$PERF_DATA" | "$RESOLVE" "${DEBUG_MAPS[@]}" > "$TMP_SCRIPT"
else
    echo "[flamegraph] Extracting stacks from $PERF_DATA..."
    perf script -i "$PERF_DATA" > "$TMP_SCRIPT"
fi

# Merge folded stacks: sums counts for identical stacks across multiple folded files
merge_folded() {
    awk '
    {
        match($0, / [^ ]+$/)
        stack = substr($0, 1, RSTART - 1)
        count = substr($0, RSTART + 1)
        sums[stack] += count
    }
    END {
        for (s in sums) print s, sums[s]
    }' "$@"
}

for logical in "${ordered[@]}"; do
    output_svg="$OUTPUT_DIR/flamegraph_${logical}.svg"
    title="${logical} Flame Graph"

    # Fold each raw event within this logical group
    folded_files=()
    for raw in ${event_groups[$logical]}; do
        folded_tmp=$(mktemp)
        folded_files+=("$folded_tmp")
        "$STACKCOLLAPSE" --event-filter="$raw" "$TMP_SCRIPT" > "$folded_tmp"
    done

    # Merge if multiple raw events (hybrid CPU), otherwise use as-is
    if [ ${#folded_files[@]} -eq 1 ]; then
        folded="${folded_files[0]}"
    else
        folded=$(mktemp)
        merge_folded "${folded_files[@]}" > "$folded"
    fi

    if [ ! -s "$folded" ]; then
        echo "[flamegraph]   $logical: SKIP (no samples)"
    else
        echo "[flamegraph]   $logical -> $(basename "$output_svg")"
        "$FLAMEGRAPH" --title "$title" --width 1200 < "$folded" > "$output_svg"
        [ "$logical" = "cycles" ] && cp "$output_svg" "$PARENT_DIR/flamegraph.svg"
    fi

    rm -f "$folded" "${folded_files[@]}"
done

echo "[flamegraph] Done: ${#ordered[@]} event type(s) processed, output in $OUTPUT_DIR"
