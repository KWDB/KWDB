---
name: perf-event-profiling
description: Use when profiling a running server process with Linux perf — capturing performance events (cycles, cache-misses, instructions) while a client script triggers the workload, then analyzing perf.data for hot functions and hardware event metrics. Profiling and analysis can be invoked independently or combined.
---

# Perf Event Profiling

## When to Use

- **Profiling only**: PID + client script → capture perf.data
- **Analysis only**: existing `perf.data` → analyze and write report
- **Combined** (default): PID + client script → capture then analyze end-to-end

## Modes

| Mode | Trigger | Flow |
|------|---------|------|
| **Profiling** | PID + client script | Prerequisites → event detection → capture → perf.data |
| **Analysis** | perf.data path provided by caller | Analysis → source-level analysis → write report |
| **Combined** | PID + client script, no perf.data | Profiling then Analysis (full pipeline) |

These modes are independent. An orchestrator (e.g. `kwdb-perf-investigation`) may
call profiling and analysis as separate steps. Profiling is called first to produce
`perf.data`; analysis is called later with that path to produce the report.

## Output Language

**All analysis output and findings MUST be in Chinese.** Commands and technical terms remain in English.

## Profiling

Applies to **Profiling** and **Combined** modes.

### Prerequisites

Check toolchain availability and perf permissions before anything else:

```bash
which perf addr2line readelf objdump
cat /proc/sys/kernel/perf_event_paranoid
```

| Tool | Purpose |
|------|---------|
| `perf` | Capture and report |
| `addr2line` | Map addresses to source lines |
| `readelf` | Check debug info / build-id |
| `objdump` | Disassemble when `perf annotate` isn't enough |

If any binary is missing, prompt the user to install it (usually `linux-tools-common` / `binutils`).

perf_event_paranoid:

| Value | Meaning |
|-------|---------|
| ≤0 | Full access |
| 1 | CPU events allowed, raw tracepoint restricted |
| ≥2 | No hardware event access |

If the value is ≥2, prompt the user:

```
perf_event_paranoid = X, cannot capture hardware events.
Run: sudo sysctl kernel.perf_event_paranoid=-1
```

### Event Selection

Default set (covers IPC, cache, and branch prediction):

```
cycles,instructions,cache-misses,cache-references,branch-misses,branches
```

| Event | Measures |
|-------|----------|
| `cycles` | CPU cycles — where time is spent |
| `instructions` | Instructions retired — for IPC = instructions / cycles |
| `cache-misses` | Cache lines missed — primary bottleneck indicator |
| `cache-references` | Total cache references — for miss rate = misses / references |
| `branch-misses` | Mispredicted branches — virtual calls / unpredictable control flow |
| `branches` | Total branch instructions — for miss rate = misses / branches |

Keep the event count minimal: more events → more PMU multiplexing → less accurate sampling. 6 events is the max before multiplexing becomes a problem on most CPUs.

On multi-NUMA systems, `node-load-misses` and `node-store-misses` are automatically appended to detect cross-NUMA memory access bottlenecks.

### Event Availability Check

Virtualized environments and older CPUs may lack certain events. Check **before capture**.

Run `scripts/perf-event-check.sh` which detects NUMA topology, checks all events, and outputs `available=...` and `missing=...` lines.

**Critical Events:** If `cycles` or `instructions` is missing, stop and ask the user to check the environment — these are essential for IPC computation. For other events, continue with the available subset and note the missing ones in the report.

### Capture

Run `scripts/perf-capture.sh` directly with arguments:

```bash
bash scripts/perf-capture.sh -p <pid> -c <client_script> [-o output] [-f freq] [-g callgraph]
```

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `-p` | yes | — | Server PID to profile |
| `-c` | yes | — | Client workload script (file path, not a command string) |
| `-o` | no | `./perf.data` | Output file |
| `-f` | no | `49` | Sampling frequency (Hz) |
| `-g` | no | `dwarf` | Call graph method (dwarf/fp) |

Events are auto-detected via `perf-event-check.sh`. Capture procedure: event detection → background perf → 2s warmup → run client script → SIGINT perf.

## Analysis

Applies to **Analysis** and **Combined** modes. In Analysis-only mode, the
`perf.data` path is provided by the caller — replace `perf.data` in commands
below with the actual path.

### Top Hot Functions

First pass — get self cost for all symbols, resolved or not:

```bash
perf report --stdio -i perf.data --no-children --sort symbol
```

`--no-children` shows self overhead only. **Do NOT use `-U` — it hides ALL unresolved symbols,
not just kernel. On release builds this silently drops major hotspots.**

Extract the top 3 entries. If any are unresolved (`0x...` or `[unknown]`), resolve them
with `addr2line` against the matching `.debug` file:

```bash
DEBUG_FILE=<build_dir>/<dso>.debug
for addr in <0xaddr1> <0xaddr2> <0xaddr3>; do
  addr2line -e "$DEBUG_FILE" -f -C -p $addr
done
```

`-f` prints function name, `-C` demangles, `-p` shows source location inline.

If `addr2line` returns `??`, the `.debug` file's build-id doesn't match — go to
[Step 2](#step-2-find-the-matching-debug-file) to find the correct one.

Once the top functions are identified (resolved or via addr2line), use `--children`
to trace callers.

### Key Metrics

Compute from event counts in `perf report --stdio`:

| Metric | Formula | Good | Bad |
|--------|---------|------|-----|
| IPC | instructions / cycles | > 1.0 | < 0.5 (stalls) |
| Cache Miss Rate | cache-misses / (cache-misses + cache-references) | < 1% | > 5% |
| Branch Miss Rate | branch-misses / (branch-misses + branches) | < 1% | > 3% |

Get event counts via `perf report --stat -i perf.data` and `perf report --stdio -i perf.data --show-total-period`.

### Hybrid CPU PMU Handling

On hybrid CPUs (Intel Alder Lake+, ARM big.LITTLE), events are captured separately on `cpu_atom` and `cpu_core`. `perf report --stat` shows per-PMU counts. **Sum atom + core counts for each event type before computing metrics.** Use `perf evlist -i perf.data` to confirm PMU assignments.

### Summary Template

```
## Perf Analysis Summary
**Events:** <list>  **Duration:** <duration>

### Top Hot Functions
1. func_a — XX%
2. func_b — XX%
3. func_c — XX%

### Key Metrics
- IPC: X.XX  Cache miss rate: X.X%
```

### Flame Graph

**Applies to Analysis / Combined modes.** Generate per-event flame graphs for
visual hotspot overview. Each event in `perf.data` produces its own SVG:

```bash
bash scripts/flamegraph-setup.sh     # one-time: clones FlameGraph repo
bash scripts/flamegraph-generate.sh -i perf.data -d <output_dir> \
    -r <dso>=<debug> [-r <dso>=<debug> ...]
```

The `-r` flag resolves `[unknown]` symbols via `addr2line` against a `.debug` file.
[Step 2](#step-2-find-the-matching-debug-file) covers finding the correct `.debug` file
for each DSO. Multiple `-r` flags can be provided — one per DSO with unresolved symbols.

Example:
```bash
bash scripts/flamegraph-generate.sh -i perf.data -d analysis/ \
    -r kwbase=/build/bin/kwbase.debug \
    -r libkwdbts2.so=/build/lib/libkwdbts2.debug
```

When `-r` is not provided, the script runs without symbol resolution (same as before).
The resolve step adds ~5-10s for typical workloads (2M lines, ~4K unique addresses).

The generate script uses `perf evlist` to detect event types, groups hybrid-PMU
events (e.g. `cpu_core/cycles/` + `cpu_atom/cycles/` → `cycles`), then runs
`perf script` → optional `flamegraph-resolve` → `stackcollapse-perf.pl` →
`flamegraph.pl` for each event type. Events with zero samples are skipped.

Output files under `<output_dir>/flamegraphs/flamegraph_<event>.svg` (one per
event type). The cycles flame graph is also copied to `<output_dir>/flamegraph.svg`
as the primary flame graph. `instructions` is skipped (redundant with cycles +
metrics). In orchestrator-driven sessions, output to `$SESSION_DIR/`.

## Source-Level Analysis

Applies to **Analysis** and **Combined** modes. Follow the steps in order.
Skip/reorder only with a documented reason.

### Step 1: Identify the DSO for Each Hot Function

`perf.data` records the path of every DSO that had samples. Extract with `perf buildid-list -i perf.data`. In `perf report --stdio`, the DSO column (`[.]` = userspace) shows which DSO owns each symbol. For functions with self overhead > 2%, note its DSO — this determines the debug file in Step 2.

If the buildid-list path no longer exists (binary redeployed after capture), find the DSO by name and verify build-id:
```bash
find <project_root> -name "<dso_name>"
readelf -n <found_path> | grep "Build ID"
```

### Step 2: Find the Matching .debug File

When `perf report` shows `[unknown]` for a hot function, find the debug file for its DSO:

```bash
find <project_root> -name "<dso_name>.debug"
```

**Build ID must match** — otherwise `addr2line` resolves to wrong functions.

```bash
perf buildid-list -i perf.data | grep <dso_name>     # recorded build-id
readelf -n <found.debug> | grep "Build ID"            # debug file build-id
```

The hex strings must be identical.

### Step 3: Resolve [unknown] Addresses with addr2line

`perf report` does NOT load symbols from the `.debug` file. Use `addr2line` directly:

```bash
DEBUG_FILE=<path_to_matching.debug>
addr2line -e "$DEBUG_FILE" -f -C -p <hex_addr>
```

Flags: `-f` function names, `-C` demangle, `-p` inline source location.

If `addr2line` returns `??`, the build-id doesn't match — go back to Step 2.
If it returns `go.go:?`, the address is a pure Go function (Go DWARF limitation) —
use the function name to search source code instead.


### Step 4: Per-Instruction Hotspot Drilldown

Use the full demangled signature copied exactly from `perf report --stdio --sort symbol` output:

```bash
perf annotate --stdio --group -i perf.data "namespace::Class::method(args...)"
```

If `perf annotate` reports "no samples", the symbol name is incomplete — use the full demangled signature from `perf report`.

Within the annotate output, the hottest instruction has the highest sample %. PMU skid (~2-5 instructions) means samples often land _after_ the real bottleneck — a hot `mov` following a `cmp (mem)`/`mov (mem)`/`call*` points to the earlier memory-access instruction as the true hotspot.

### Step 5: Map Hot Addresses to Source Lines with addr2line

Extract the top 3-5 hot addresses from annotate output (sampled instruction + preceding load/cmp):

```bash
DEBUG_FILE=<build_dir>/<correct_dso>.debug
for addr in 0x80988f 0x809893 0x809897; do
  addr2line -e "$DEBUG_FILE" -f -C -i $addr
done
```

Flags: `-f` function names, `-C` demangle, `-i` inline chain.

**Verify:** the function name from `addr2line` must match the hot function from `perf report`. If it prints an unrelated function (e.g., Go runtime's `proto.equalAny` instead of C++ `GetBlockItem`), wrong debug file — go back to Step 1.

### Step 6: Read the Source and Correlate

`addr2line` gives exact file:line for each hot address. Read those source files.

**6a. Assembly → source pattern match**

| Assembly Pattern | Typical Source Code | Likely Issue |
|------------------|-------------------|--------------|
| `cmp 0x6c(%rax),...` with hot successor | `if (ptr->field == ...)` | Data just read from cold memory; pointer-chasing cache miss |
| `mov 0x38(%r8),%r11` with high samples | `cur->field` struct deref | Sequential dereference of fields from cold mmap data |
| `call *0x8(%rax)` (indirect call) | Virtual method call | vtable lookup + I-cache miss |
| Hot in `_M_realloc_insert` | `vec.push_back(x)` in loop | Missing `reserve()` |
| Hot in `_M_dispose` | `shared_ptr` dtor / reassign | shared_ptr copied by value |
| Hot in `__lll_lock_wait` | `std::lock_guard` / `pthread_mutex_lock` | Lock contention |

**6b. Read the hot function source** — for each top function (> 2% self), read its body at the `addr2line` file:line locations.

**6c. Walk the call chain** — use `perf report --children` to see callers. Check: can callers batch calls? Are large objects passed by value through multiple levels? Is the same data fetched independently at different levels?

**6d. Correlate metrics to code**

| Metric | Value | Code-Level Question |
|--------|-------|---------------------|
| Cache miss rate | > 5% | Walking linked/pointer structures? Can fields be colocated? |
| IPC | < 0.5 | Serially dependent loads? Can software prefetch help? |
| LLC-load-misses | > 100M | Working set > L3? Can access be partitioned or prefetched? |

### Step 7: Report per-function

```
### Hotspot: <func_name> (<self_overhead>%)

**Source:** <file>:<line> (DSO: <library.so>)

**Bottleneck:** <source line + what happens at CPU level>

**Root Cause:** <why — based on both metrics and code pattern>

**Suggestion:** <concrete code change>
```

## Output

Applies to **Analysis** and **Combined** modes. When analysis completes, write the
full report (Summary Template + per-function hotspots from Step 7) to the path
requested by the caller. In orchestrator-driven sessions this is
`<session_dir>/analysis_report.md`.

## Quick Reference

### Capture

| Task | Command |
|------|---------|
| External-trigger capture | Use `perf-capture.sh` template (background + kill -INT) |
| List available events | `perf list` |

### Analysis

| Task | Command |
|------|---------|
| Hot functions (self) | `perf report --stdio -i perf.data --no-children --sort symbol` |
| Hot functions (callchain) | `perf report --stdio -i perf.data` |
| Assembly drilldown | `perf annotate --stdio --group -i perf.data "<full_signature>"` |
| Per-event sample/event counts | `perf report --stat -i perf.data` |
| List captured PMU events | `perf evlist -i perf.data` |
| Show raw event counts | `perf report --stdio -i perf.data --show-total-period` |
| Check if stripped | `readelf -S <binary> \| grep debug` |
| Map addr to source | `addr2line -e <debug_file> -f -C -i <addr>` |

### Common Events

| Event | Measures |
|-------|----------|
| `cycles` | CPU cycles |
| `instructions` | Instructions retired |
| `cache-misses` | Cache lines missed (all levels) |
| `cache-references` | Total cache references |
| `branch-misses` | Mispredicted branches |
| `branches` | Total branch instructions |

When deeper analysis is needed, add `LLC-load-misses` for last-level cache profiling.
