# Agent Resource Index

This file is the stable map of agent-facing resources in this repository.

## Core Files

- `AGENTS.md`: repository-wide operating rules and stable verification expectations
- `.agents/skills/README.md`: placeholder layout for optional local skills

## External Task Artifacts

- Detailed bug, feature, and performance specs should live outside the repo.
- Use the issue tracker, wiki, or another team-approved system as the source of truth.
- Link the authoritative external artifact from the issue, PR, or change summary.

## Reference Docs

- `docs/agents/README.md`: overview of the agent-facing doc and skill layout
- `docs/agents/*-zh.md`: optional Chinese-language editions; English sources
  remain the OSS default
- `docs/agents/architecture-index.md`: module map and task routing hints
- `docs/agents/coding-style-go-cpp.md`: Go/C++ norms for KWDB-maintained OSS code
- `docs/agents/ci-release-guide.md`: Makefile/script verification surfaces plus merge checks
- `docs/agents/testing-flow.md`: verification levels and common commands
- `docs/agents/logging.md`: shared logging constraints and review checklist
- `docs/agents/errors.md`: shared error-code and compatibility constraints
- `docs/agents/pr-guide.md`: review-ready summary expectations
- `docs/agents/components/README.md`: component-specific note layout

## Existing Repository References

- `README.md`: repository introduction, build prerequisites, and high-level setup
- `Makefile`: root build, lint, unit-test, logic-test, and regression entrypoints
- `kwbase/CONTRIBUTING.md`: points to the upstream KaiwuDB contribution process
- `qa/perf_test/README.md`: current perf harness notes
- `qa/run_test_local_v2.sh`: local regression entry script
- `qa/run_tsbs_test.sh`: TSBS test entry script

## Skills directory

- `.agents/skills/README.md`: optional skills placeholder

## Module-Specific References

- `common/src/log/`: shared log implementation
- `common/src/error/`: shared error implementation
- `kwbase/pkg/`: Go-side feature and SQL implementation
- `kwdbts2/`: time-series and engine-side implementation
- `qa/`: regression and performance harnesses
