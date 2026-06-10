---
name: tsbs-benchmark
description: |
  Use when running KWDB local TSBS benchmark workflows. Covers
  two entry points: the full-pipeline qa/run_tsbs_test.sh for standard
  benchmarks, and the scripts under .agents/skills/tsbs-benchmark/scripts/
  for workload setup (called by the kwdb-perf-investigation orchestrator).

  Do not use for general JDBC tests, SQL correctness tests, or non-TSBS
  performance issues.
---

# KWDB TSBS Benchmark Skill

This skill covers the integrated TSBS benchmark workflow in the KWDB repository.
There are two entry points; choose based on the user's goal:

| Entry point | Purpose | When to use |
|-------------|---------|-------------|
| `qa/run_tsbs_test.sh` | Full-pipeline one-shot script | Standard TSBS benchmarks: write+query full pipeline, automatic result collection and threshold comparison |
| `.agents/skills/tsbs-benchmark/scripts/` | Workload setup scripts | Called by `kwdb-perf-investigation` orchestrator for perf profiling setup. Single-node only. Do NOT use for standalone benchmarks or absolute latency/throughput numbers. |

## Entry A: Full-pipeline script `qa/run_tsbs_test.sh`

### Usage

```bash
cd <KWDB_REPO_ROOT>
UPDATE_THRESHOLD=true ./qa/run_tsbs_test.sh <topology> <scales>
```

- `topology`: `1n` (single node) or `3c` (3-node cluster)
- `scales`: comma-separated scale list, e.g. `100` or `4000,100000`

### Two run modes

**Generate threshold (baseline)** — `UPDATE_THRESHOLD=true`:

```bash
UPDATE_THRESHOLD=true ./qa/run_tsbs_test.sh 1n 100
```

Writes results to `qa/tsbs_test/tsbs_result/threshold/` as the baseline for future
validation. Use for the first run or when updating baselines.

**Validation mode** (default):

```bash
./qa/run_tsbs_test.sh 1n 100
```

Runs the benchmark and compares against existing thresholds. Fails if results
exceed threshold bounds. Suitable for regression detection.

### Execution flow

1. `qa/tsbs_test/setup.sh` — ensures TSBS binaries are ready (auto-clones `kwdb-tsbs` and compiles into `qa/tsbs_test/bin/` if missing)
2. `qa/util/setup_basic_v2.sh <topology>` — starts the KWDB cluster (deploys to `install/deploy/`)
3. `qa/tsbs_test/excute_tsbs_test.sh` — per scale in sequence: generate data → load → generate queries → run queries → record results
4. `qa/util/stop_basic_v2.sh <topology>` — stops KWDB

### Result directory

```
qa/tsbs_test/tsbs_result/
├── threshold/                          # Threshold baselines
└── kwdb/<timestamp>_<branch>_scale<scale>_.../
    ├── load_result/<case>_<format>_scale_<scale>.log   # Write results
    └── query_result/<format>_scale<scale>_<case>_<query_type>_worker<W>.log  # Query results
```

## Entry B: Perf helper scripts `scripts/`

Entry B is called by the `kwdb-perf-investigation` orchestrator for workload
setup before perf profiling. Single-node only. Do NOT rely on it for absolute
latency or throughput numbers, and there is no threshold comparison.

### Prerequisites

`utils.sh` validates that `install/bin/kwbase` exists and is a Release build
(via `go version -m` checking `build.typ=Release`). If the script dies with
a non-Release error, rebuild before retrying:

```bash
cd <KWDB_REPO_ROOT>
make BUILD_TYPE=Release install -j
```

### Script architecture

| Script | Sources | Purpose |
|--------|---------|---------|
| `setup.sh` | `utils.sh` | Calls `qa/tsbs_test/setup.sh` to ensure TSBS binaries exist. Run once at the very beginning, before kwbase starts. |
| `utils.sh` | — | Shared environment variables and functions. Sourced by all other scripts. Does NOT call `qa/tsbs_test/setup.sh`. |
| `start_single_node.sh` | `utils.sh` | Starts single-node kwbase and applies cluster settings. |
| `generate_wrapper.sh` | `utils.sh` | Generates data files and writes a self-contained workload wrapper script. |

When Entry B is chosen, the agent automatically:
1. **Setup environment** — `setup.sh` (ensures TSBS binaries; also cleans up stale kwbase processes before starting)
2. **Start KWDB** — `start_single_node.sh`
3. **Generate wrapper** — `generate_wrapper.sh <scale> [load] [query_types...]`

The agent does NOT execute any write or query operations. Those are exposed as
functions in the generated wrapper for the downstream consumer to call.

### generate_wrapper.sh

```bash
bash .agents/skills/tsbs-benchmark/scripts/generate_wrapper.sh <scale> [load] [query_types...] [-o output_dir]
```

Generates load/query data files and writes a self-contained wrapper script
to `-o output_dir` (default `/tmp`). The literal keyword `load` means "include
write workload"; all other args are treated as query types.

Examples:
```bash
# Write + single query type, custom output dir
bash .agents/skills/tsbs-benchmark/scripts/generate_wrapper.sh 100 load high-cpu-all -o /path/to/session

# Query only, multiple types
bash .agents/skills/tsbs-benchmark/scripts/generate_wrapper.sh 100 single-groupby-1-1-1 high-cpu-all

# Write only
bash .agents/skills/tsbs-benchmark/scripts/generate_wrapper.sh 100 load
```

### Generated Wrapper Script

The wrapper exposes two standard functions with unified names,
regardless of which specific operations were requested:

| Function | Purpose |
|----------|---------|
| `do_load` | Run `tsbs_load_kwdb` (only present when `load` was requested) |
| `do_query` | Run all requested query types via `tsbs_run_queries_kwdb` |

All data file paths are hardcoded inside the wrapper. A consuming skill only
needs to `source <wrapper_path>` and call `do_load` / `do_query` — no
knowledge of scale, query types, or file paths required.

```bash
source <wrapper_path>
do_load
do_query
```

When called from the orchestrator, the wrapper is written to the session dir
(e.g. `tsbs-profile/<timestamp>/tsbs_workload.sh`).

### Setup report

After completing setup, the agent MUST output the following structured report.
This report is the contract for downstream skills (perf, orchestrator) to
consume.

```markdown
## TSBS Setup Report

- **KWDB PID**: <pid>  (read from tsbs-benchmark/data/tsbs_n1/kwbase.pid)
- **Wrapper**: <session_dir>/tsbs_workload.sh

### Workload functions

| Function | Source |
|----------|--------|
| `do_load` | `<session_dir>/tsbs_workload.sh` |
| `do_query` | `<session_dir>/tsbs_workload.sh` |

Only functions that were requested at generation time will be present in the
wrapper. The orchestrator decides which function to trace.
```

The PID MUST be read from the pid file specified by `--pid-file`, not from
`ps -ef` or process inspection.

## Task routing

| Entry point | When |
|-------------|------|
| Entry A | Default. Standard TSBS benchmarks (性能测试, 对比, 回归). |
| Entry B | When the caller (e.g. `kwdb-perf-investigation` orchestrator) explicitly requests Entry B. Not triggered by user keywords alone. |

Entry B's responsibility ends once KWDB is started and the workload wrapper is
generated. It does NOT execute `do_load` or `do_query` — those are invoked by the
downstream consumer of the workflow wrapper.

When the user's goal and parameters are ambiguous, confirm before running:
- Write only, query only, or full pipeline?
- Which scale? (Formal tests require the user to specify; never default to a small value.)
- Need comparison against a baseline / pre-change / specific commit?
- Case: `cpu-only` or `iot`?

## Parameters

Defaults are defined in `qa/tsbs_test/tsbs_env.sh`. Override via environment variables.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `tsbs_case` | `cpu-only` | Use case: `cpu-only` or `iot` |
| `format` | `kwdb` | Data format |
| `insert_type` | `prepare` | Write mode: `insert`, `prepare`, `prepare-extend`, `prepareiot` |
| `load_workers` | `12` | Write concurrency |
| `load_batchsizes` | `1000` | Write batch size |
| `wal` | `3` | WAL level |
| `replica_mode` | `1` | Replica mode |
| `query_times` | `30` | Queries generated/executed per query type |
| `query_workers` | `8` | Query concurrency (worker count) |
| `parallel_degree` | `8` | KWDB query parallel degree |

### Scale ↔ Scenario mapping

Users may refer to these by their Chinese colloquial names ("场景一", "场景二", etc.):

| Scenario | Scale |
|----------|-------|
| Scenario 1 (场景一) | 100 |
| Scenario 2 (场景二) | 4000 |
| Scenario 3 (场景三) | 100000 |
| Scenario 4 (场景四) | 1000000 |

### Query types

`single-groupby-1-1-1`, `single-groupby-1-1-12`, `single-groupby-1-8-1`,
`single-groupby-5-1-1`, `single-groupby-5-1-12`, `single-groupby-5-8-1`,
`cpu-max-all-1`, `cpu-max-all-8`, `double-groupby-1`, `double-groupby-5`,
`double-groupby-all`, `high-cpu-all`, `high-cpu-1`, `lastpoint`,
`groupby-orderby-limit`

## Runtime monitoring

**Entry A** runs end-to-end automatically. Only the exit code and result directory
matter.

**Entry B** runs step by step. After each step, check stdout to confirm progress.
Before declaring a stall, check:
- Whether the relevant process exists (`tsbs_generate_data`, `tsbs_load_kwdb`, `tsbs_run_queries_kwdb`, `kwbase`)
- Whether data file sizes are still growing
- Whether disk space is sufficient

## Interpreting results

### Write results

Look for the `mean rate` line in the log:
```text
mean rate <N> rows/sec
```
`rows/sec` is the primary write throughput metric.

### Query results

Look for the statistics line in the log:
```text
min: ... ms, med: ... ms, mean: ... ms, max: ... ms, stddev: ... ms, sum: ... sec, count: ...
```

- `mean`: average latency across `count` queries for this query type
- `count`: must equal `query_times`; if not, that query type's results are invalid

When summarizing query results, report `mean` and `count` per query type. Use
min, max, stddev as supplementary data.

### Before-after comparison

Requirements for valid comparisons:
- Same machine, same entry point, same build type (both release), same parameters
- Write comparison: `change_pct = (current - baseline) / baseline * 100`
- Query comparison: compare mean per query type
- Single-run conclusions MUST be labeled "single observation"; for stable conclusions, run at least 3 times per group and report mean and variance

## Troubleshooting

### Missing TSBS binaries

Entry A handles this automatically (`qa/tsbs_test/setup.sh`). For Entry B, run:
```bash
bash .agents/skills/tsbs-benchmark/scripts/setup.sh
```

### Port / address conflict

```bash
ps -ef | grep kwbase
```
For Entry B, override the address:
```bash
host_ip=127.0.0.1 listenport=36257 bash .agents/skills/tsbs-benchmark/scripts/start_single_node.sh
```

### Write / query failures

- Verify that `insert_type` is compatible with `tsbs_case`
- Reproduce with a small scale (e.g. 100) first
- Entry A: check that threshold files exist under `qa/tsbs_test/tsbs_result/`

## Output requirements

After completing a TSBS task, report:

1. Entry point used and the exact command
2. `kwbase` build type (debug/release) and commit/branch
3. Parameters: scale, tsbs_case, insert_type, wal, load_workers, query_times, etc.
4. Write results: rows/sec, or note if unavailable
5. Query results: mean and count per query type (if applicable)
6. Any failures encountered and at which stage
7. What was NOT verified

Do NOT claim "performance passed" or "no regression" unless compared against an
explicit baseline under identical parameters and environment.
