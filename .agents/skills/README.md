# Repository-Local Skills

This directory contains optional, repository-local agent skill packages
(`SKILL.md` trees consumed by tooling).

## Available Skills

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

### ai-pr-kb-usage

Branch-local PR AI / KB usage recorder and renderer. Start metric capture as
soon as AI-assisted work begins on a local branch, keep raw data out of git,
and render or refresh the `## AI / KB Usage` PR section.

- Guardrails: keep raw data out of git, aggregate only at PR level, and do not
  capture prompts or session traces.
- **File:** [.agents/skills/ai-pr-kb-usage/SKILL.md](ai-pr-kb-usage/SKILL.md)
