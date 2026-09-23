# Example Triage Cases

## Example 1: `exec_tests_ee_hash_table_test`

### Input

```text
command: make kwdbts2-test
rerun: ctest --rerun-failed --output-on-failure
ctest name: exec_tests_ee_hash_table_test
gtest case: HashTableTest.SpillAndCombineAfterAbandonedTuple
file: kwdbts2/exec/tests/ee_hash_table_test.cpp:141
symptom: ht.FindOrCreateGroupsAndAddTuple(...) returned 0, expected KStatus::SUCCESS == 1
context: 123 tests ran, 122 passed, only this case failed on arm64
```

### Expected Triage

```text
Failure class: kwdbts2-cpp-test
Likely module: kwdbts2/exec/ with the failing test in kwdbts2/exec/tests/ee_hash_table_test.cpp; second-order shared surface is kwdbts2/common/ or common/ only if allocator or status helpers appear in the trace
Why this is the leading hypothesis: the failure is a deterministic gtest assertion in a single exec-side test case after the build succeeded and 122 peer tests passed, so this looks more like code or test logic than environment setup
Inspect first:
- kwdbts2/exec/tests/ee_hash_table_test.cpp
- kwdbts2/exec/include/ee_hash_table.h
- kwdbts2/exec/include/ee_hash_table_tuple_data.h
- matching implementation files under kwdbts2/exec/
Minimal repro:
- cd build-kwdbts2-test && ctest --output-on-failure -R '^exec_tests_ee_hash_table_test$'
- cd build-kwdbts2-test/tests/exec_tests_ee_hash_table_test.dir && ./exec_tests_ee_hash_table_test --gtest_filter=HashTableTest.SpillAndCombineAfterAbandonedTuple
Environment vs code: current evidence leans code or test issue, not environment, because the machine already builds successfully and the failure is a stable assertion at one source line
Missing evidence: if deeper triage is needed, capture the first stack or logging around FindOrCreateGroupsAndAddTuple and any recent edits under kwdbts2/exec/
Confidence: high
```

## Example 2: `kwbase` logic test mismatch

### Input

```text
command: make test-logic
failure: --- FAIL: TestLogic/local/fk: TestLogic/local/fk/20042
path hint: kwbase/pkg/sql/logictest/testdata/logic_test/fk
symptom: result mismatch between expected and actual rows
```

### Expected Triage

```text
Failure class: kwbase-logic-test
Likely module: kwbase/pkg/sql/ and the logic file kwbase/pkg/sql/logictest/testdata/logic_test/fk
Why this is the leading hypothesis: the failure is already narrowed to one logic test file and one subtest number, which usually points to SQL semantics or planner behavior rather than harness setup
Inspect first:
- kwbase/pkg/sql/logictest/testdata/logic_test/fk
- the statement block for subtest 20042
- the first non-test stack or plan frame under kwbase/pkg/sql/
Minimal repro:
- cd kwbase && make testlogic FILES='fk' SUBTESTS='20042' TESTCONFIG=local TESTTIMEOUT=45m
Environment vs code: lean code issue unless the log shows server startup or native library failures before the SQL statement runs
Missing evidence: expected output block, actual output block, and if available an EXPLAIN or stack frame for the statement
Confidence: medium
```

## Example 3: QA regression suite failure

### Input

```text
command: make regression-test TEST_NAME=TEST_v2_integration_basic_v2.sh T='bug_50539.sql' TEST_TOPOLOGIES='1n'
symptom: TEST_v2_integration_basic_v2.sh Failed, can't find TEST_v2_integration_basic_v2.sh_passed on the last line
```

### Expected Triage

```text
Failure class: qa-regression
Likely module: qa/Integration/TEST_v2_integration_basic_v2.sh plus qa/Integration/basic_v3/bug_50539.sql; if the suite dies before SQL execution, inspect qa/util/
Why this is the leading hypothesis: the failure is at the regression harness level and already identifies both the suite and SQL filter
Inspect first:
- qa/Integration/TEST_v2_integration_basic_v2.sh
- qa/Integration/basic_v3/bug_50539.sql
- the per-suite log path printed by qa/run_test_local_v2.sh
- qa/util/kwbase_exec_basic_v2.sh and sibling helpers if setup failed early
Minimal repro:
- make regression-test TEST_NAME=TEST_v2_integration_basic_v2.sh T='bug_50539.sql' TEST_TOPOLOGIES='1n'
- cd qa && ./run_test_local_v2.sh TEST_v2_integration_basic_v2.sh 'bug_50539.sql' 1n
Environment vs code: if cluster setup or binary launch fails, lean environment or harness; if one SQL case fails after cluster startup, lean code or SQL logic
Missing evidence: last 80 lines of the suite log and the first failing SQL statement or harness step
Confidence: medium
```

## Example 4: No failure logs, plus a tempting local diff

### Input

```text
guess: this probably comes from kwdbts2/exec/
paths: kwdbts2/exec/tests/ee_hash_table_test.cpp and kwdbts2/exec/include/ee_hash_table.h
workspace clue: there is an uncommitted local diff under kwdbts2/exec/src/ that looks like a possible fix
request: inspect the code and tell me the root cause
runtime evidence: none
```

### Correct Triage Behavior

```text
Failure class: unknown-needs-more-logs
Evidence basis: none
Likely module: kwdbts2/exec/ is a plausible area, but current evidence is not sufficient to classify or route confidently
Why this is the leading hypothesis: the input contains only code locations, a guess, and a tempting local diff, but no failing command, decisive line, stack, assertion, or runtime output. This skill is evidence-first and triage-only, so neither static inspection nor local uncommitted changes are enough.
Inspect first:
- assets/triage-input-template.md
- references/preflight-checklist.md
- references/classification-checklist.md
Minimal repro:
- request the full failing command
- or request the last 50 to 100 log lines around the decisive failure
Environment vs code: ambiguous; the missing runtime evidence is the blocker
Missing evidence:
- full failing command or target
- first decisive error line
Confidence: high
Blocker: full failing command or decisive log excerpt
```

### Incorrect Behavior

```text
- claim a root cause directly from the source files
- claim a root cause from an uncommitted local diff or suspected fix
- start suggesting patches or code edits
- treat "likely module" as proof and convert triage into static debugging
```
