# KWDB Agent Guide

This file is the default operating guide for AI coding agents working in this repository.

More specific `AGENTS.md` files may exist in subdirectories. The nearest file to the changed code takes precedence for that subtree.

Normative wording in this guide: **MUST** means required; **SHOULD** means
recommended unless there is a concrete reason to deviate; **MAY** means
optional.

## 1. Mission

- Preserve correctness, compatibility, and operability before pursuing speed.
- Prefer small, reviewable diffs over broad rewrites.
- Treat SQL semantics, time-series behavior, persistence safety, concurrency, and performance regressions as first-class risks.
- Never claim completion without verification evidence.

## 2. Repository Facts

- The root `Makefile` is the primary build and test entrypoint.
- Some repository build/test flows assume the checkout path matches `.../src/gitee.com/kwbasedb`.
- Some `kwbase` build/test flows still rely on `GO111MODULE=off`.
- Main code areas:
  - `common/`: shared C/C++ infrastructure such as logging, error handling, threading, tracing, and memory utilities
  - `kwbase/`: Go-side SQL, KV, server, CLI, workload, and test utilities
  - `kwdbts2/`: time-series engine, storage, execution, statistics, and related C/C++ components
  - `qa/`: integration, regression, TSBS, and performance test harnesses
  - `cluster_start/`, `kaiwudb_install/`: deployment and environment scripts

Deeper module guidance lives in `docs/agents/architecture-index.md`. Build and
verification command details live in `docs/agents/testing-flow.md`.


## 3. Working Rules

- Do not invent APIs, flags, protocol details, error codes, or file locations. Verify first.
- Do not change cross-module behavior unless the task explicitly requires it.
- Do not manually edit generated files unless the owning workflow requires it.
- Do not weaken tests to make a change pass.
- Match existing file-local and module-local patterns before introducing a new abstraction.
- Comments should explain intent, constraints, or tradeoffs, not restate obvious code.
- Preserve observability when changing retries, background work, storage paths, state transitions, or failure handling.
- If a task is ambiguous, reduce scope or record assumptions explicitly in the output.

## 4. Task Inputs and External Specs

Detailed bug, feature, and performance specs should live outside the repository
in the team's approved system, such as issue trackers, wikis, or design doc
pages.

Before substantial work, identify the authoritative external task artifact,
copy its canonical URL into your issue description and keep it echoed in pull
requests, and summarize what it covers (no pasted requirement dumps unless the
project explicitly allows archival copies).

If no formal artifact exists yet, create a concise task brief in the work item
or review thread instead of committing a volatile spec file into the repository.

These external task artifacts should be reviewed and accepted by accountable
subsystem owners **before implementation begins**.

- Use the project's normal ownership routing: tracked assignees, reviewer list,
  or `CODEOWNERS` rules when present.
- If ownership is ambiguous, escalate to module maintainers and record who
  approved the interpretation in the tracking issue before coding starts.

The repository should not become the storage layer
for draft task plans or draft specs.

Every substantial task should still define:

- goal
- scope
- invariants
- validation plan
- rollback or mitigation notes

## 5. Verification Rules

- Always run the narrowest verification that proves the claim.
- Expand verification when touching shared code, compatibility logic, concurrency, storage, logging, error surfaces, or performance-sensitive paths.
- For bug fixes, verify both the failing path and the nearest unaffected path.
- For performance work, record workload, environment, and before/after results.

Use `docs/agents/testing-flow.md` as the source of truth for concrete
verification commands and test-scope decisions.

## 6. Logging and Error Handling

If a change affects logs or errors, review these files before editing behavior:

- `docs/agents/logging.md`
- `docs/agents/errors.md`

Preserve compatibility when changing:

- log severity meaning or rendering
- log message shape used by operators or scripts
- error-code to message mapping
- error-code to `PgCode` mapping
- JSON error field names returned to callers

## 7. PR and Change Summary Rules

Every substantial change should include:

- what changed
- why it changed
- what was verified
- known risks or unverified areas

If the work is intended for review or merge, also follow `docs/agents/pr-guide.md`.

## 8. Agent Resources

- architecture and module map: `docs/agents/architecture-index.md`
- agent docs overview: `docs/agents/README.md`
- CI and release guidance: `docs/agents/ci-release-guide.md`
- component-specific index: `docs/agents/components/README.md`
- testing and verification flow: `docs/agents/testing-flow.md`
- logging guidance: `docs/agents/logging.md`
- error handling guidance: `docs/agents/errors.md`
- resource index: `docs/agents/resource-index.md`
- repository-local skills index: `.agents/skills/README.md`
- Current available skills (listed in .agents/skills) :
    - `kwdb-perf-investigation/`: TSBS-based performance investigation — routes user intent and coordinates tsbs-benchmark + perf-event-profiling pipelines for hotspot analysis
    - `perf-event-profiling/`: Linux perf profiling for running server processes — capture and analyze perf.data for hot functions, metrics (IPC, cache misses), and source-level hotspot drilldown
    - `tsbs-benchmark/`: KWDB local TSBS benchmark workflows — full-pipeline write+query benchmarks with threshold comparison
    - `gitee-pr-review/`: Pull Request review on Gitee kwdb/kwdb repository — batch PR status and AI-generated code review reports
    - `kwdb-document-writing/`: Functional specification and design document writing for KWDB database features
- Go / C++ coding style: `docs/agents/coding-style-go-cpp.md`

## 9. Output Contract

When finishing work, report:

1. Files changed (paths).
2. Verification run: exact commands and outcomes, or explicitly what was **not**
   exercised locally.
3. Notable assumptions.
4. Remaining risks or follow-up areas.

Agents **SHOULD NOT** claim completion without evidence aligned with
`docs/agents/testing-flow.md`.

## 10. Go and C++ coding style

- Because KWDB is a complex system, code SHOULD remain maintainable for future readers with basic KWDB familiarity.
- Follow existing package-local conventions first and keep style consistent with nearby files.
- Code SHOULD be self-documenting through clear naming and structure.
  - Example: when implementing a well-known algorithm, naming SHOULD be clear enough to make the approach recognizable; if naming alone may not make intent obvious, add a brief comment.
- Keep error handling actionable and contextual; avoid silently swallowing errors.
- For new source files (for example *.go or *.cpp), include the standard KWDB license header (Shanghai Yunxi Technology Co copyright + Mulan PSL v2) by copying from a nearby file and updating year if needed.
- Comments SHOULD explain non-obvious intent, constraints, invariants, concurrency guarantees, SQL/compatibility contracts, or important performance trade-offs, and SHOULD NOT restate what the code already makes clear.

## 11. Maintenance Rules

- Keep this file short and stable.
- Move long procedures to `docs/agents/*`.
- Introduce repository-local skills only after a workflow is stable, repeated,
  and clearly repository-specific.
- Keep detailed task specs and volatile project plans outside the repository.
- Prefer child `AGENTS.md` files for module-specific rules instead of growing the root file indefinitely.
