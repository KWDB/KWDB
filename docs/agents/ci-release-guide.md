# CI and Release Guide

This repository has strong local build and test entrypoints. Hosted CI manifests,
if any, are maintained outside this tree depending on infrastructure.

Treat this note as factual mapping between code changes and the repository's own
verification entrypoints (`Makefile`/scripts).

For concrete build and verification commands, see [testing-flow.md](testing-flow.md).

## Verification Surfaces

- `Makefile` for build and test entrypoints
- `qa/run_test_local_v2.sh` and `qa/run_test_v2.sh` for regression flows
- `qa/run_tsbs_test.sh` and `qa/perf_test/*` for performance validation

## Minimum Expectations by Change Type

- Shared logging or error changes: one focused test plus one broader validation target
- SQL or planner changes: targeted Go or logic tests plus semantic regression coverage
- TS engine or storage changes: focused engine tests plus integration coverage
- QA harness changes: run the narrowest affected script flow and record exact arguments
- Perf changes: benchmark before and after under the same workload

## Release Checklist

- external requirement link present where required by project policy
- verification commands and results attached
- operator-visible behavior changes summarized
- monitoring or smoke checks identified
- rollback trigger and rollback action documented

## What Not to Do

- Do not rerun failing CI blindly without classifying the failure.
- Do not merge perf-sensitive changes without workload evidence.
- Do not treat an AI-generated PR summary as a substitute for verification output.
