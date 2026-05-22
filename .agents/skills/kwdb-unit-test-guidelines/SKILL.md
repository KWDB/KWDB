---
name: kwdb-unit-test-guidelines
description: MUST USE this skill for ANY Go unit test task in KWDB — even a simple "generate a _test.go" or "add tests" request. Covers file naming, 单元测试 conventions, package foo vs foo_test, leaktest goroutine detection, table-driven give/want tests with t.Run, testutils.IsError error assertions, testutils.SucceedsSoon async helpers, skip.UnderShort/UnderRace, TestMain + serverutils.InitTestServerFactory, StartServer for in-process SQL tests, testcluster.New for multi-node cluster tests, benchmark templates, testdata directories, and Makefile targets (test/testshort/testrace/stress/bench). Trigger whenever user says 单元测试/单测/测试文件/测试/unit test, references _test.go files, asks about testing conventions, or needs to write/generate/add tests for any Go code under kwbase/. Even for trivial one-file requests, the skill enforces leaktest, table-driven patterns, and proper error assertions. C++ unit test guidelines are a TODO.
---

# KWDB Unit Test Guidelines

This skill covers writing unit tests in the KWDB repository. It is organized by
language: **Go** tests are covered in full below; **C++** test guidelines are
under development.

When asked to write or modify tests, use the workflow in this skill. The
conventions here are flexible — follow them unless the surrounding package
already uses a different established pattern. When in doubt, match the
neighbouring test files.

---

## Go Unit Test Workflow

Follow these steps when generating or modifying Go tests:

### Step 1 — Identify the package type

Determine what kind of test is needed by reading the package under test:

| Signal | Test type | Template |
|--------|-----------|----------|
| No imports of `server`, `serverutils`, `testcluster`; pure functions/methods | Pure function test | Template A |
| Package imports `serverutils` or has a `main_test.go` with `TestMain`; may start a single test server | Server test | Template B |
| Package uses `testcluster.New()` or multiple `TestServer`s | Cluster test | Template C |

Open the existing `*_test.go` files in the package to confirm the local pattern.
Some packages mix types — pick the lightest type that covers what you need.

### Step 2 — Check existing test patterns

Before writing anything, read at least one existing `*_test.go` from the same
package. Note:

- White-box (`package foo`) vs black-box (`package foo_test`)
- Whether `testify/require` is already used, or `t.Fatal`/`t.Error` is preferred
- Whether `leaktest` is already imported
- Whether there is a `main_test.go` with `TestMain`

Match the existing style. Don't introduce a new assertion library unless the
package already uses it.

### Step 3 — Select and fill in the template

Pick one of the three templates below. The code blocks are **skeletons** — copy
them and replace the placeholders with real test logic.

Full runnable examples live under `references/`:

| Template | Reference file |
|----------|---------------|
| A: Pure function test | `references/pure_function_test.go` |
| B: Server test | `references/server_test.go` |
| C: Cluster test | `references/cluster_test.go` |

### Step 4 — Fill in test logic

- Use table-driven tests with `t.Run()` for multiple cases
- Name table fields to separate input from expected (prefer `give`/`want` or the
  local convention)
- Use `require.*` for fatal assertions, `assert.*` for non-fatal when testify is
  available
- Match errors with `testutils.IsError(err, regex)` — write regex patterns
  specific enough to catch the right error but not fragile to message rewording
- For async conditions, use `testutils.SucceedsSoon(t, fn)` instead of `time.Sleep`

### Step 5 — Self-check against the checklist

After writing the test, verify every item in the [Self-Checklist](#self-checklist).

### Step 6 — Run verification

```bash
# Run only the package you changed:
cd kwbase && go test -v -run "TestXxx" ./pkg/your/package/

# Run the whole package:
cd kwbase && go test -v ./pkg/your/package/

# Quick smoke test across all packages (excludes slow tests):
make testshort

# With race detector:
make testrace PKG=./pkg/your/package/

# Stress test to catch flakiness:
make stress PKG=./pkg/your/package/ TESTS=TestXxx
```

Always run at least the focused package test before claiming completion.

---

## Core Conventions

### File naming and package

- Test files end with `_test.go` and sit alongside the code they test
- **White-box tests:** use `package foo` when testing unexported internals
- **Black-box tests:** use `package foo_test` when testing only the public API
  (required when there are import cycles in test)

```go
// White-box: keys_test.go
package keys

// Black-box: settings_test.go
package settings_test
```

### Test function naming

- Functions begin with `Test`: `func TestValidateAddrs(t *testing.T)`
- Benchmarks begin with `Benchmark`: `func BenchmarkString(b *testing.B)`
- Name describes what is being tested, not how

### Imports

Imports in test files follow standard + third-party groups, same as non-test code:

```go
import (
    "testing"

    "gitee.com/kwbasedb/kwbase/pkg/testutils"
    "gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
    "github.com/stretchr/testify/require"
)
```

### Leak detection

Every test function that spawns goroutines (directly or through a server) must
start with:

```go
func TestSomething(t *testing.T) {
    defer leaktest.AfterTest(t)()
    // ...
}
```

Many packages auto-generate leak checks via `main_test.go`:

```go
//go:generate ../util/leaktest/add-leaktest.sh *_test.go
```

When adding a new test file to a package that has this `go:generate` line, just
add `defer leaktest.AfterTest(t)()` at the top of each test function. The
generate script handles the rest.

---

## Template A: Pure Function Test

Use when the code under test has no server dependency — pure logic, data
structures, encoding, validation, settings, etc.

```go
package foo

import (
    "testing"

    "gitee.com/kwbasedb/kwbase/pkg/testutils"
    "gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
    "github.com/stretchr/testify/require"
)

func TestThing(t *testing.T) {
    defer leaktest.AfterTest(t)()

    testCases := []struct {
        give string
        want string
    }{
        {give: "input1", want: "expected1"},
        {give: "input2", want: "expected2"},
    }

    for _, tc := range testCases {
        t.Run(tc.give, func(t *testing.T) {
            got := DoThing(tc.give)
            if got != tc.want {
                t.Errorf("DoThing(%q) = %q, want %q", tc.give, got, tc.want)
            }
        })
    }
}

func TestThingError(t *testing.T) {
    defer leaktest.AfterTest(t)()

    _, err := DoThing("bad-input")
    if !testutils.IsError(err, "expected error pattern") {
        t.Fatalf("expected error matching %q, got %v", "expected error pattern", err)
    }
}
```

Add `require` imports when testify is already used in the package.

For benchmarking the function:

```go
var benchmarkSink int  // prevent compiler optimizations

func BenchmarkThing(b *testing.B) {
    for i := 0; i < b.N; i++ {
        benchmarkSink = DoThing("bench-input")
    }
}
```

---

## Template B: Server Test (with TestMain)

Use when the test needs a running KV/SQL server. This requires a `main_test.go`
file if one doesn't already exist.

**main_test.go** (create if missing):

```go
package foo_test

import (
    "os"
    "testing"

    "gitee.com/kwbasedb/kwbase/pkg/security"
    "gitee.com/kwbasedb/kwbase/pkg/security/securitytest"
    "gitee.com/kwbasedb/kwbase/pkg/server"
    "gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
    "gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
)

func TestMain(m *testing.M) {
    security.SetAssetLoader(securitytest.EmbeddedAssets)
    serverutils.InitTestServerFactory(server.TestServerFactory)
    serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
    os.Exit(m.Run())
}

//go:generate ../../util/leaktest/add-leaktest.sh *_test.go
```

Adjust the relative path in `go:generate` based on directory depth from the
package to `pkg/util/leaktest/`.

**Test file:**

```go
package foo_test

import (
    "context"
    "testing"

    "gitee.com/kwbasedb/kwbase/pkg/base"
    "gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
    "gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
    "github.com/stretchr/testify/require"
)

func TestWithServer(t *testing.T) {
    defer leaktest.AfterTest(t)()

    s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
    defer s.Stopper().Stop(context.Background())

    _, err := db.Exec("SELECT 1")
    require.NoError(t, err)
}
```

Key points:
- `serverutils.StartServer` returns `(TestServerInterface, *gosql.DB, Config)`
- Always `defer s.Stopper().Stop(context.Background())` to clean up
- Use `base.TestServerArgs{}` to configure the server
- The `db` handle is a standard `database/sql` connection — use it for SQL
  queries in tests

---

## Template C: Cluster Test

Use when testing distributed behavior (raft, replication, range operations).
Extends Template B with a multi-node cluster.

```go
package foo_test

import (
    "context"
    "testing"

    "gitee.com/kwbasedb/kwbase/pkg/base"
    "gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
    "gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
    "github.com/stretchr/testify/require"
)

func TestWithCluster(t *testing.T) {
    defer leaktest.AfterTest(t)()

    tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
    defer tc.Stopper().Stop(context.Background())

    db := tc.ServerConn(0)
    _, err := db.Exec("CREATE TABLE t (id INT PRIMARY KEY)")
    require.NoError(t, err)
}
```

Key points:
- `testcluster.StartTestCluster(t, nodes, args)` starts `nodes` servers
- `tc.ServerConn(n)` returns the `*gosql.DB` for node `n`
- Always `defer tc.Stopper().Stop(context.Background())`
- Shut down servers before inspecting cluster state to avoid races

---

## Key Test Utilities

### Error assertions

```go
// Match error message against a regex. Empty regex means "expect nil error".
testutils.IsError(err, "expected substring or regex")

// For roachpb.Error values:
testutils.IsPError(pErr, "expected regex")
```

### Async conditions

```go
// Retry fn with exponential backoff for up to 45s. Fatal on timeout.
testutils.SucceedsSoon(t, func() error {
    val := getSomething()
    if val != expected {
        return errors.New("not ready yet")
    }
    return nil
})
```

### Skipping tests

```go
// Skip under -short flag (for slow tests):
skip.UnderShort(t)

// Skip under race detector:
skip.UnderRace(t)

// Skip with issue tracking:
skip.WithIssue(t, 12345, "flaky until KWDB-12345 is fixed")

// Skip and mark as not-a-tracked-skip:
skip.IgnoreLint(t, "only relevant under race")
```

### Subtest helpers

```go
// Run a subtest with both true and false values:
testutils.RunTrueAndFalse(t, "feature-flag", func(t *testing.T, enabled bool) {
    // test body
})
```

A full API reference is in `references/testutils-api-cheatsheet.md`.

---

## Testdata

When tests need fixture files (SQL scripts, JSON payloads, binary data), place
them in a `testdata/` directory next to the test file:

```
pkg/your/module/
├── your_test.go
└── testdata/
    └── fixture.sql
```

Load at test time with standard `os`/`io` calls — no `//go:embed`:

```go
data, err := ioutil.ReadFile(filepath.Join("testdata", "fixture.sql"))
require.NoError(t, err)
```

---

## Makefile Commands

From repository root (`cd kwbase` first for Go-only targets):

| Command | What it does |
|---------|-------------|
| `make test PKG=./pkg/foo/ TESTS=TestBar` | Run specific test |
| `make test PKG=./pkg/foo/` | Run all tests in package |
| `make testshort` | All packages with `-short` flag |
| `make testrace PKG=./pkg/foo/` | With race detector |
| `make bench PKG=./pkg/foo/` | Run benchmarks |
| `make benchshort PKG=./pkg/foo/` | Quick benchmark smoke test |
| `make stress PKG=./pkg/foo/ TESTS=TestBar` | Run test repeatedly to find flakes |

Variables you can override:
- `PKG` — which package(s), defaults to `./pkg/...`
- `TESTS` — `-run` regex, defaults to `.` (all)
- `TESTFLAGS` — extra flags passed to `go test`
- `TESTTIMEOUT` — timeout, defaults to `30m`

---

## Self-Checklist

After writing a test, confirm each item:

- [ ] File named `*_test.go` in the same directory as the tested code
- [ ] Package name correct (`package foo` for white-box, `package foo_test` for black-box)
- [ ] Test function named `TestXxx` with `*testing.T` parameter
- [ ] `defer leaktest.AfterTest(t)()` at the start of each test function
- [ ] If package has `TestMain`, verified `serverutils.InitTestServerFactory` is called
- [ ] Table-driven with `t.Run()` sub-tests for multi-case functions
- [ ] `require.*` for fatal assertions, `assert.*` for non-fatal (or match local convention)
- [ ] Error matching via `testutils.IsError(err, regex)`, not raw string comparison
- [ ] No hardcoded paths; use `testdata/` directory or temporary directories
- [ ] Async checks use `testutils.SucceedsSoon`, not `time.Sleep`
- [ ] Slow or race-sensitive tests use `skip.UnderShort(t)` / `skip.UnderRace(t)`
- [ ] Test actually ran locally and passed before claiming completion

---

## C++ Unit Test Guidelines

**TODO** — This section is under development by another team member. For now,
follow the existing C++ test patterns in the repository (`kwdbts2/`,
`common/src/`). Run C++ tests via `make kwdbts2-test` and `make cpplint` from
the repository root.
