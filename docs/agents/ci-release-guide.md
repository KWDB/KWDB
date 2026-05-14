# CI and Release Guide

This repository has strong local build and test entrypoints, but it does not
currently expose a single in-repo CI definition for hosted runners. Treat this
document as the contract for what CI and release automation should validate,
regardless of whether the platform is Gitee, Jenkins, or another system.

## Current Verification Surfaces

- `Makefile` for build and test entrypoints
- `qa/run_test_local_v2.sh` and `qa/run_test_v2.sh` for regression flows
- `qa/run_tsbs_test.sh` and `qa/perf_test/*` for performance validation

## Current Gaps

- No repository-visible hosted CI pipeline definition was found during review.
- No repository-local agent skill existed for CI failure triage before this update.
- Release-readiness checks were implied across docs, but not collected in one place.

## Recommended Pipeline Shape

| Stage | Input | Checks | Output |
| --- | --- | --- | --- |
| Metadata | PR or change request | issue link, external requirement link, changed paths, owner | traceable work item |
| Build | source diff | build or compile target for affected area | build logs, artifacts |
| Focused tests | changed module | nearest unit, package, or targeted regression tests | pass/fail evidence |
| Shared-surface tests | shared code changes | broadened tests for logging, errors, SQL semantics, or storage | confidence on blast radius |
| Nightly or gated regression | merge candidate | heavier integration, logic, or HA suites | release confidence signal |
| Perf gate | perf-sensitive changes only | before/after benchmark or TSBS evidence | benchmark diff |
| Release gate | approved candidate | smoke plan, rollback plan, operator notes | go/no-go decision |

## Minimum Expectations by Change Type

- Shared logging or error changes: one focused test plus one broader validation target
- SQL or planner changes: targeted Go or logic tests plus semantic regression coverage
- TS engine or storage changes: focused engine tests plus integration coverage
- QA harness changes: run the narrowest affected script flow and record exact arguments
- Perf changes: benchmark before and after under the same workload

## Recommended Tooling

- Gitee integration for issue and PR context linking
- archived CI artifacts for logs, benchmarks, and flaky test evidence
- `benchstat` or equivalent diff tooling for Go benchmarks
- perf evidence retention for TSBS and flame graph outputs when relevant
- an AI triage worker that summarizes failing CI stages before humans inspect raw logs

## Release Checklist

- external requirement and approval link present
- verification commands and results attached
- operator-visible behavior changes summarized
- monitoring or smoke checks identified
- rollback trigger and rollback action documented

## What Not to Do

- Do not rerun failing CI blindly without classifying the failure.
- Do not merge perf-sensitive changes without workload evidence.
- Do not treat an AI-generated PR summary as a substitute for verification output.
