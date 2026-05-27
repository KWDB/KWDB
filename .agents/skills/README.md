# Repository-Local Skills

This directory is reserved for optional, repository-local agent skill packages
when the project chooses to add them (`SKILL.md` trees consumed by tooling).

## Available Skills

- `kwdb-ci-failure-triage/`
  - First-pass CI failure triage for this repository.
  - Focus: classify failure class, route to the most likely module, identify the first files to inspect, and recommend the smallest reproduction command.
  - Guardrails: triage-only, evidence-first, and no host-specific environment assumptions.
  - Pattern: `Tool Wrapper` for KWDB-specific triage knowledge plus `Generator` for a fixed triage report shape.
  - Coverage: `kwdbts2` gtests, `kwbase` Go tests, SQL logic tests, build or link failures, and `qa` regression harness failures.
