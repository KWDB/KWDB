# Failure Classification Checklist

Use this checklist before writing the final report. Pick the smallest class that fits the evidence best.

## Step 1: Normalize The Evidence

Extract these fields if present:

- failing command or target
- narrowed repro
- failing test, suite, package, or script name
- first decisive error line
- file path or file:line
- stack frame, panic text, assertion text, undefined symbol, or missing header
- environment clues such as missing tool, GOPATH mismatch, OOM, disk full, permission issue
- nearby passing signals such as build succeeded, peer tests passed, or one-case isolation

If the input is only code paths, a bug theory, or "something in this area is failing" without runtime output, stop here and request the minimum missing failure evidence.
If both the failing command and a decisive failure line or log excerpt are missing, classify as `unknown-needs-more-logs`.

## Step 2: Choose The Leading Class

### `kwdbts2-cpp-test`

Choose this when one or more are true:

- the command is `make kwdbts2-test`
- the output comes from `ctest` or a gtest binary
- the failure references `kwdbts2/*/tests/*.cpp`
- the decisive line is a C++ assertion such as `ASSERT_*`, `EXPECT_*`, or wrong `KStatus`

### `kwbase-go-test`

Choose this when one or more are true:

- the command is `make kwbase-test`
- the output comes from `go test`
- the decisive line is `--- FAIL: Test...`
- the first useful stack frame is under `kwbase/pkg/`

### `kwbase-logic-test`

Choose this when one or more are true:

- the command is `make test-logic`
- the output contains `TestLogic/`
- the failure points to `pkg/sql/logictest/testdata/logic_test/`
- the failure points to `pkg/sql/opt/exec/execbuilder/testdata/`

### `build-or-link`

Choose this when one or more are true:

- the failure is a compiler error before tests run
- the decisive line is `undefined reference`, missing header, duplicate symbol, or missing library
- the output points to `third_party`, generated artifacts, protobuf, or target wiring

### `qa-regression`

Choose this when one or more are true:

- the command is `make regression-test`
- the output comes from `qa/run_test_local_v2.sh`
- the failure references `TEST_v2_integration_*`
- the suite log lacks the `<suite>_passed` marker

### `environment-or-infra`

Choose this when one or more are true:

- the failure happens before code-specific assertions or stack frames
- the decisive line is path mismatch, missing binary, missing tool, permission error, OOM, disk full, shell incompatibility, or branch sync issue
- rerunning on the same commit would primarily validate machine state rather than code behavior

### `unknown-needs-more-logs`

Choose this only when the available evidence does not reliably place the failure into one of the classes above.

## Step 3: Decide Whether To Ask For More Evidence

Ask for more evidence only when classification or routing would otherwise be guesswork.

Preferred single follow-up items:

1. full failing command
2. first failing file and line
3. last 50 to 100 log lines around the decisive failure
4. a narrower repro command if the current command is too broad to be actionable

## Step 4: Prepare The Final Report

After classification:

- route with `references/triage-playbook.md`
- separate environment suspicion from code or test suspicion using `references/environment-baseline.md` when relevant
- fill `assets/triage-report-template.md`
