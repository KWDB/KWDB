# Repository-Local Skills

This directory contains optional, repository-local agent skill packages
(`SKILL.md` trees consumed by tooling).

## Available Skills

### kwdb-document-writing
Functional specification and design document writing for KWDB database features.

  - Focus: requirement clarification, codebase analysis, template-based document generation with Chinese language output.
  - Guardrails: evidence-first, code-referenced designs, and template-compliant document structure.
  - Pattern: `Generator` for functional specs and design docs with integrated code analysis.
  - Coverage: new features, feature enhancements, performance optimizations, bug fixes, and architecture adjustments.
  - **File:** [.agents/skills/kwdb-document-writing/SKILL.md](kwdb-document-writing/SKILL.md)


### kwdb-unit-test-guidelines
Go unit test writing and guidelines for KWDB repository.

  - Focus: file naming, test conventions, leak detection, table-driven tests, error assertions, and test execution patterns.
  - Guardrails: leaktest enforcement, table-driven patterns, proper error assertions, and existing style matching.
  - Pattern: `Tool Wrapper` for KWDB-specific test knowledge plus `Generator` for test file templates.
  - Coverage: pure function tests, server tests, cluster tests, benchmarks, and kwdbts2 Google Test conventions.
  - **File:** [.agents/skills/kwdb-unit-test-guidelines/SKILL.md](kwdb-unit-test-guidelines/SKILL.md)


### kwdb-refactor
Code refactoring for the KWDB database repository (Go/C++).

  - Focus: planning multi-file refactors, reducing method complexity, extracting functions, eliminating code smells, and applying safe design patterns.
  - Guardrails: behavior-preserving changes, verification with "failed=0", preservation of SQL semantics/time-series behavior/concurrency safety/error codes/logging.
  - Pattern: `Generator` for refactor plans plus `Tool Wrapper` for KWDB-specific considerations (error handling, logging, testing flow).
  - Coverage: long functions, duplicated code, nested conditionals, large switch statements, dead code, primitive obsession, and other code smells in Go and C++ code.
  - **File:** [.agents/skills/kwdb-refactor/SKILL.md](kwdb-refactor/SKILL.md)


### kwdb-code-review
KWDB-specific code review for C++ (common/ + kwdbts2/) and Go (kwbase/) changes.

  - Guardrails: review-first (no auto-fix), evidence-based findings, severity-graded (P0–P3), and verification expectations per change type.
  - Pattern: `Tool Wrapper` for KWDB review knowledge (language-specific + cross-cutting references) plus `Generator` for structured review output.
  - Coverage: git diff changes across all KWDB modules, including cross-module boundary changes, error code/log format changes, and build system changes.
  - **File:** [.agents/skills/kwdb-code-review/SKILL.md](kwdb-code-review/SKILL.md)


### kwdb-perf-investigation

TSBS-based performance investigation orchestrator. Routes user intent and
coordinates multi-skill pipelines: dispatches to `tsbs-benchmark` for standard
benchmarks, and combines `tsbs-benchmark` + `perf-event-profiling` for hotspot
analysis. Currently only supports TSBS-based performance analysis.

- **File:** [.agents/skills/kwdb-perf-investigation/SKILL.md](kwdb-perf-investigation/SKILL.md)

### perf-event-profiling

Linux perf profiling for running server processes. Captures performance events
(cycles, cache-misses, instructions) while a client script triggers the
workload, then analyzes `perf.data` for hot functions, key metrics (IPC, cache
miss rate), and source-level hotspot drilldown with `addr2line`. Supports
profiling-only, analysis-only, and combined modes.

- **File:** [.agents/skills/perf-event-profiling/SKILL.md](perf-event-profiling/SKILL.md)

### tsbs-benchmark

KWDB local TSBS benchmark workflows. Two entry points: `qa/run_tsbs_test.sh`
for full-pipeline write+query benchmarks with threshold comparison, and
`.agents/skills/tsbs-benchmark/scripts/` for workload setup (called by the
`kwdb-perf-investigation` orchestrator for perf profiling).

- **File:** [.agents/skills/tsbs-benchmark/SKILL.md](tsbs-benchmark/SKILL.md)

### kwdb-ci-failure-triage

First-pass CI failure triage for this repository. Classifies failure class,
routes to the most likely module, identifies the first files to inspect, and
recommends the smallest reproduction command.

- Guardrails: triage-only, evidence-first, and no host-specific environment assumptions.
- Pattern: `Tool Wrapper` for KWDB-specific triage knowledge plus `Generator` for a fixed triage report shape.
- Coverage: `kwdbts2` gtests, `kwbase` Go tests, SQL logic tests, build or link failures, and `qa` regression harness failures.
- **File:** [.agents/skills/kwdb-ci-failure-triage/SKILL.md](kwdb-ci-failure-triage/SKILL.md)
