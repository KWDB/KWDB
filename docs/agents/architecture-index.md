# Architecture Index

This document is the agent-facing architecture map for the repository.
It answers "where should I look first?" without duplicating full design documents.

## Top-Level Module Index

### `common/`

- Purpose: shared C/C++ infrastructure reused by engine-side components.
- Key responsibilities: logging, error stacks and code mapping, threading, tracing, latches, memory helpers, and common utilities.
- Common change types: log behavior, error propagation, shared utility changes, low-level runtime fixes.
- High-risk areas: process-wide logging behavior, error JSON shape, thread lifecycle, and low-level helpers used by many modules.
- Key directories:
  - `common/src/log/`
  - `common/src/error/`
  - `common/src/thread/`
  - `common/src/trace/`

### `kwbase/`

- Purpose: Go-side database logic and user-facing database surface.
- Key responsibilities: SQL, optimizer/planner, executor, KV/server, CLI, workload, settings, jobs, test utilities, and operational surfaces.
- Common change types: SQL semantics, DDL/DML behavior, server settings, CLI behavior, logic tests.
- High-risk areas: SQL compatibility, storage and KV behavior, cluster version semantics, upgrade paths, and public error surfaces.
- Key directories:
  - `kwbase/pkg/sql/`
  - `kwbase/pkg/kv/`
  - `kwbase/pkg/server/`
  - `kwbase/pkg/cli/`
  - `kwbase/pkg/testutils/`
  - `kwbase/pkg/workload/`

### `kwdbts2/`

- Purpose: time-series and engine-side implementation in C/C++.
- Key responsibilities: engine, storage, execution, statistics, mmap/brpc dependencies, and shared TS runtime.
- Common change types: storage behavior, execution pipeline changes, engine optimizations, TS query/runtime fixes.
- High-risk areas: persistence, memory ownership, concurrency, TS execution correctness, and performance-sensitive hot paths.
- Key directories:
  - `kwdbts2/engine/`
  - `kwdbts2/storage/`
  - `kwdbts2/exec/`
  - `kwdbts2/statistic/`
  - `kwdbts2/ts_engine/`
  - `kwdbts2/common/`

### `qa/`

- Purpose: test harnesses and regression/performance tooling.
- Key responsibilities: integration entry scripts, local regression orchestration, TSBS runs, perf harness, utility setup/teardown scripts.
- Key directories:
  - `qa/Integration/`
  - `qa/performance/`
  - `qa/perf_test/`
  - `qa/tsbs_test/`
  - `qa/util/`
- Common entrypoints:
  - `qa/run_test_local_v2.sh`
  - `qa/run_test_v2.sh`
  - `qa/run_tsbs_test.sh`

## Common Task Routing

- SQL semantic change: start in `kwbase/pkg/sql/`, then inspect related logic tests and server/session code.
- Server or CLI behavior change: inspect `kwbase/pkg/server/` or `kwbase/pkg/cli/`.
- Time-series execution or storage change: inspect `kwdbts2/exec/`, `kwdbts2/storage/`, `kwdbts2/engine/`, and related tests.
- Logging or error behavior change: inspect `common/src/log/` and `common/src/error/` first.
- Regression or integration issue: inspect `qa/Integration/`, `qa/util/`, and the matching `make regression-test` entry.
- Performance issue: inspect the runtime module plus `qa/perf_test/` and `qa/tsbs_test/`.

## Review Boundaries

- Cross-module semantic changes across `kwbase` and `kwdbts2` need explicit compatibility review.
- Changes under `common/src/log/` or `common/src/error/` can affect multiple subsystems and should be treated as shared-surface edits.
- Changes that alter SQL output, protocol behavior, error codes, or persisted behavior need broader validation than a local unit test.
- Performance changes should be validated with workload evidence, not just code inspection.

## Related References

- root operating guide: `AGENTS.md`
- test and verification guide: `docs/agents/testing-flow.md`
- logging guide: `docs/agents/logging.md`
- error handling guide: `docs/agents/errors.md`
- PR guide: `docs/agents/pr-guide.md`
