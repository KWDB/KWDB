# KWDB CI Failure Triage Playbook

Use this file to map raw CI evidence to a likely module and the narrowest next command.

## Decision Cues

| Signal | Leading class | First module guess |
| --- | --- | --- |
| `make kwdbts2-test`, `ctest`, `gtest`, `.cpp`, `ASSERT_EQ`, `EXPECT_*` under `kwdbts2/` | `kwdbts2-cpp-test` | matching subtree under `kwdbts2/` |
| `make kwbase-test`, `go test`, `panic:`, `--- FAIL: Test...`, stack under `kwbase/pkg/` | `kwbase-go-test` | package from the first failing stack frame |
| `make test-logic`, `TestLogic/`, `logictest`, `logic_test/`, `execbuilder/testdata/` | `kwbase-logic-test` | `kwbase/pkg/sql/` or `kwbase/pkg/sql/opt/exec/execbuilder/` |
| `fatal error: ... No such file`, `undefined reference`, `ld returned 1`, `cannot find -l`, `third_party`, `protoc` | `build-or-link` | source subtree named in the first compiler or linker error |
| `make regression-test`, `qa/run_test_local_v2.sh`, `TEST_v2_`, missing `*_passed`, failing `.sql` case | `qa-regression` | matching `qa/Integration/` suite plus referenced SQL and utility scripts |
| `Current source path isn't match GOPATH rule`, `grep: invalid option -- P`, `command not found`, `No space left on device`, `Killed`, `permission denied` | `environment-or-infra` | environment, shell, path, or machine resource setup |

## Repository Map

- `common/`
  - shared C/C++ runtime and helpers
  - inspect when failures mention logging, error handling, threading, latches, tracing, or shared utilities
- `kwdbts2/`
  - engine-side C/C++ execution, storage, and gtests
  - inspect `exec/`, `storage/`, `ts_engine/`, `engine/`, `common/`, and matching `tests/`
- `kwbase/`
  - Go SQL, server, CLI, KV, optimizer, and logic tests
  - inspect `pkg/sql/`, `pkg/server/`, `pkg/cli/`, `pkg/testutils/`, and relevant testdata
- `qa/`
  - regression harnesses, topology scripts, and SQL suites
  - inspect `qa/Integration/`, `qa/util/`, `qa/performance/`, `qa/tsbs_test/`

## Class: `kwdbts2-cpp-test`

### Typical Signals

- `ctest --output-on-failure`
- gtest suite or case names such as `HashTableTest.SpillAndCombineAfterAbandonedTuple`
- assertions in `kwdbts2/*/tests/*.cpp`
- `KStatus::FAIL` or `ASSERT_*` / `EXPECT_*` output from C++

### High-Probability Module Routing

- test path under `kwdbts2/exec/tests/` -> inspect `kwdbts2/exec/`
- test path under `kwdbts2/storage/tests/` -> inspect `kwdbts2/storage/`
- test path under `kwdbts2/ts_engine/tests/` -> inspect `kwdbts2/ts_engine/`
- failures mentioning shared allocators, status, or utilities may also require `kwdbts2/common/` or top-level `common/`

### Inspect First

- the failing test source file
- sibling headers and implementation files in the matching subtree
- helper headers referenced by the test
- for `kwdbts2/exec/tests/*.cpp`, inspect:
  - `kwdbts2/exec/include/` headers matching the tested type or function
  - `kwdbts2/exec/src/` implementation files with the same stem or owning operator family
  - tuple, batch, or status helper headers pulled into the failing test
- for `kwdbts2/storage/tests/*.cpp`, inspect:
  - matching files under `kwdbts2/storage/include/` and `kwdbts2/storage/src/`
  - nearby WAL, snapshot, or iterator helpers named in the first assertion or stack
- for `kwdbts2/ts_engine/tests/*.cpp`, inspect:
  - matching files under `kwdbts2/ts_engine/include/` and `kwdbts2/ts_engine/src/`
  - query, planner, or chunk-processing helpers referenced by the failing case
- recent shared runtime files only if the failure shape suggests allocator, status, or concurrency behavior

### Minimal Repro Commands

Start narrow and only widen if the test name is unknown.

```bash
make kwdbts2-test
cd build-kwdbts2-test && ctest --output-on-failure -R '^<ctest-name>$'
cd build-kwdbts2-test/tests/<ctest-name>.dir && ./<ctest-name> --gtest_filter=<suite>.<case>
```

Notes:

- KWDB's `kwdbts2` CMake wiring derives the `ctest` name from the relative path by replacing `/` with `_` and appending the filename without `.cpp`.
- Example: `kwdbts2/exec/tests/ee_hash_table_test.cpp` becomes `exec_tests_ee_hash_table_test`.

### Environment vs Code

Lean code or test issue when:

- the same assertion fails deterministically at the same source line
- the binary builds and most peer tests pass
- the failure is logical, such as wrong `KStatus`, wrong row count, or wrong hash-table state

Lean environment issue when:

- the test never starts
- the binary is missing
- the error is allocator failure, OOM, permission, or toolchain incompatibility without a stable test assertion

## Class: `kwbase-go-test`

### Typical Signals

- `make kwbase-test`
- `go test` output with `--- FAIL: Test...`
- Go panic stack traces under `kwbase/pkg/...`
- failures under server, SQL, CLI, KV, or workload packages

### High-Probability Module Routing

- first failing stack frame under `kwbase/pkg/sql/` -> SQL semantics, planner, executor, logic helpers
- under `kwbase/pkg/server/` -> server startup, admin APIs, telemetry, status, cluster wiring
- under `kwbase/pkg/cli/` -> CLI behavior, interactive tests, flags, debug tools

### Inspect First

- the first non-test frame in the panic or failure stack
- the failing package test file
- adjacent helper files used by the failing package
- package-local helpers in the same directory before widening to other `kwbase/pkg/` subtrees

### Minimal Repro Commands

```bash
make kwbase-test
cd kwbase && make test PKG=./pkg/<package> TESTS='<TestName>' TESTTIMEOUT=45m
cd kwbase && GOPATH=$(pwd | sed -nE 's!(.*)/src/gitee.com/kwbasedb/kwbase!\1!p') make test PKG=./pkg/<package> TESTS='<TestName>' TESTTIMEOUT=45m
```

Use the second form only when you need to restate `GOPATH` explicitly in a copied snippet.

### Environment vs Code

Lean code issue when the same package test fails reproducibly on the same test function. Lean environment issue when the failure is bootstrapping, missing native libs, path setup, or process resource exhaustion before the package logic runs.

## Class: `kwbase-logic-test`

### Typical Signals

- `make test-logic`
- `TestLogic/...`
- output mismatch in `pkg/sql/logictest/testdata/logic_test/*`
- optimizer or execbuilder mismatches in `pkg/sql/opt/exec/execbuilder/testdata/*`

### High-Probability Module Routing

- base SQL logic files -> `kwbase/pkg/sql/`
- opt execbuilder failures -> `kwbase/pkg/sql/opt/exec/execbuilder/`
- parser or semantic mismatches -> first stack or plan frame under `kwbase/pkg/sql/` and related subpackages

### Inspect First

- failing logic test file
- exact subtest number or statement block
- nearest implementation package that owns the SQL feature
- the owning files under `kwbase/pkg/sql/` or `kwbase/pkg/sql/opt/` that correspond to the first non-test frame, plan node, or feature keyword in the failing statement

### Minimal Repro Commands

```bash
make test-logic
cd kwbase && make testlogic FILES='<logic-file>' SUBTESTS='<subtest-regex>' TESTCONFIG=<config> TESTTIMEOUT=45m
cd kwbase && make testoptlogic FILES='<logic-file>' SUBTESTS='<subtest-regex>' TESTCONFIG=<config> TESTTIMEOUT=45m
```

Examples:

```bash
cd kwbase && make testlogic FILES='fk' SUBTESTS='20042|20045' TESTCONFIG=local TESTTIMEOUT=45m
cd kwbase && make testoptlogic FILES='upsert' SUBTESTS='^365$' TESTTIMEOUT=45m
```

### Missing Evidence to Request

- exact logic file name
- exact subtest number
- expected versus actual output block
- if available, the nearest `EXPLAIN` or plan text

## Class: `build-or-link`

### Typical Signals

- compiler errors such as missing headers, type mismatches, template instantiation failures
- linker errors such as `undefined reference`, duplicate symbol, missing `-l` library
- protobuf or generated-file issues
- `third_party` dependency wiring errors

### High-Probability Module Routing

- use the first failing compile or link line, not the final summary line
- route by source file path first, then by target:
  - `kwdbts2/*` and `common/*` -> C/C++ module issue
  - `kwbase/pkg/*` -> Go package or generated Go surface
  - `third_party/*` or generated artifacts -> dependency or generator path

### Inspect First

- first compiler or linker diagnostic
- target definition in `Makefile` or relevant `CMakeLists.txt`
- owning source file and included header
- generator or vendored subtree if the failure originates there
- if the first line is a linker error, inspect the target's source list and the translation unit that should define the missing symbol before widening further

### Minimal Repro Commands

```bash
make build
make kwdbts2-test
make kwbase-test
```

Pick the narrowest target that still triggers the first failing compile or link step. Do not widen to `make test` unless the failing target is unknown.

### Missing Evidence to Request

- first compiler or linker error line
- failing target name
- full undefined symbol or missing header text

## Class: `qa-regression`

### Typical Signals

- `make regression-test`
- `qa/run_test_local_v2.sh`
- `TEST_v2_integration_*`
- a suite log that does not end with `<suite>_passed`
- failure attached to a specific `.sql` file or topology

### High-Probability Module Routing

- suite shell script under `qa/Integration/`
- referenced SQL case under `qa/Integration/basic_v3/` or `qa/Integration/cluster_v3/`
- utility scripts under `qa/util/` when the harness fails before SQL execution

### Inspect First

- suite script named by `TEST_NAME`
- failing SQL file named by `T`
- the generated suite log path printed by `qa/run_test_local_v2.sh`
- helper scripts in `qa/util/` used by that suite
- topology-specific setup or teardown helpers when the suite fails before SQL execution

### Minimal Repro Commands

```bash
make regression-test TEST_NAME=TEST_v2_integration_basic_v2.sh T='bug_50539.sql' TEST_TOPOLOGIES='1n'
make regression-test TEST_NAME=TEST_v2_integration_distribute_v2.sh T='*.sql' TEST_TOPOLOGIES='5c'
cd qa && ./run_test_local_v2.sh TEST_v2_integration_basic_v2.sh 'bug_50539.sql' 1n
```

### Environment vs Code

Lean code or SQL issue when:

- the suite starts normally and a specific SQL case fails consistently
- the harness reaches result comparison or SQL execution

Lean environment or harness issue when:

- cluster setup, cleanup, or topology scripts fail before SQL execution
- the suite log shows missing binaries, port conflicts, permissions, or machine resource failures

## Class: `environment-or-infra`

### Common Indicators

- GOPATH path mismatch from the root `Makefile`
- shell incompatibility such as unsupported `grep -Po`
- missing `protoc`, compiler, or tool binaries
- `Killed`, `cannot allocate memory`, `No space left on device`
- permission errors, missing checkout paths, or branch sync issues

### Discriminating Next Steps

- rerun the narrowest failing command unchanged on the same commit
- confirm whether the failure happens before compiling, before the test binary starts, or inside a stable assertion
- compare with a known-good target on the same machine:
  - `make build`
  - `make kwdbts2-test`
  - `make kwbase-test`
  - `make test-logic`

If a single targeted test fails while the environment can still build and run adjacent tests, lean back toward code or test issue.
