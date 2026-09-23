# Testing Flow

This document defines the default verification workflow for agents and contributors.

For quality gates and release criteria, see [ci-release-guide.md](ci-release-guide.md).

## Principles

- Prove the specific claim with the smallest reliable check first.
- Expand test scope when touching shared code or sensitive behavior.
- Never replace a failing regression with a weaker assertion.
- Read the actual output before reporting success.

## Common Build and Verification Commands

From the repository root, the main build and verification commands are:

### Build Commands

- `make` or `make all` – build the entire project (alias for `build`)
- `make build` – build via CMake; configurable with:
  - `BUILD_TYPE=Debug` (default) or `Release`
  - `WITH_ASAN=ON` for address sanitizer
  - `WITH_TESTS=ON` to build tests (for `kwdbts2-test`)
  - `PROTOBUF_DIR=...` to specify protobuf path
- `make install` – install artifacts to `install/` directory
- `make clean` – clean all build and test files

For detailed build prerequisites and environment setup, see the root [README.md](../../README.md).

### Verification Commands

- `make cpplint`
- `make kwdbts2-test`
- `make kwbase-test`
- `make test`
- `make test-logic`
- `make regression-test TEST_NAME=... T='...'`
- `make regression-test-ha`

Additional scripts commonly used for regression or performance validation:

- `qa/run_test_local_v2.sh <test-filter> <sql-filter> <topologies...>`
- `qa/run_tsbs_test.sh 1n`
- `qa/run_tsbs_test.sh 3c`

## Verification Levels

### Level 1: Focused Local Check

Use when:

- the change is local to one file or one narrow module
- no cross-module contract changed
- no known concurrency, storage, or compatibility risk exists

Typical evidence:

- targeted unit test
- focused package or module test
- lint for the touched area when relevant

### Level 2: Module-Level Check

Use when:

- the change affects a shared module
- a module boundary or public behavior changed
- logging or error behavior changed

Typical evidence:

- module test suite
- targeted regression coverage
- related lint or logic test

### Level 3: Cross-Module or Sensitive Check

Use when:

- SQL semantics changed
- storage semantics changed
- compatibility behavior changed
- concurrency behavior changed
- performance-sensitive paths changed

Typical evidence:

- focused tests
- module tests
- logic test or regression test
- benchmark or profile comparison when relevant

## What to Run for Common Changes

- `common/src/log/*` or `common/src/error/*`: run the nearest unit tests plus at least one broader test target that exercises the affected surface.
- `kwdbts2/*`: start with `make kwdbts2-test`; expand to regression coverage if behavior crosses module boundaries.
- `kwbase/pkg/sql/*` or other SQL-facing logic: run targeted Go tests if available, then `make test-logic` or other matching logic/regression coverage for semantic changes.
- QA script or regression harness changes: run the narrowest affected script flow and record the exact invocation.
- Build-system or shared wiring changes: run at least `make build` and the most relevant test target for the touched module.

## Bug Fix Verification

- capture the failing scenario first when possible
- add or update a regression test if the failure is reproducible in automation
- verify the fix on the failing path
- verify the nearest unaffected path
- if the bug involved logging or error shape, verify those outputs too

## Performance Verification

- record baseline command, dataset, topology, and environment
- use the repository's existing harnesses when possible, especially `qa/run_tsbs_test.sh` or `qa/perf_test/*`
- compare before and after with the same workload
- report variance, sample count, and caveats
- if the optimization changes SQL plans, keep the relevant `EXPLAIN` or `EXPLAIN ANALYZE` evidence with the task record

## Reporting Format

Every change summary should include:

- command(s) run
- scope of verification
- result
- known gaps
