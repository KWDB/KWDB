# testutils API Cheatsheet

Quick reference for the most commonly used test utility functions in
`kwbase/pkg/testutils/` and its sub-packages.

## Error Assertions

| Function | Signature | Notes |
|----------|-----------|-------|
| `IsError` | `func IsError(err error, re string) bool` | Match error message against regex. Empty `re` means expect nil error. Uses `pgerror.FullError` to unwrap. |
| `IsPError` | `func IsPError(pErr *roachpb.Error, re string) bool` | Same as `IsError` but for `roachpb.Error` values. |

```go
if !testutils.IsError(err, "table .* does not exist") {
    t.Fatalf("expected table-not-found error, got %v", err)
}
```

## Async / Retry

| Function | Signature | Notes |
|----------|-----------|-------|
| `SucceedsSoon` | `func SucceedsSoon(t testing.TB, fn func() error)` | Retry with exponential backoff up to 45s. `t.Fatal` on timeout. |
| `SucceedsSoonError` | `func SucceedsSoonError(fn func() error) error` | Same but returns error instead of calling Fatal. |

```go
testutils.SucceedsSoon(t, func() error {
    if getCounter() < 10 {
        return errors.New("counter not reached yet")
    }
    return nil
})
```

## Subtest Helpers

| Function | Signature | Notes |
|----------|-----------|-------|
| `RunTrueAndFalse` | `func RunTrueAndFalse(t *testing.T, name string, fn func(t *testing.T, val bool))` | Runs subtest with `true` and `false` values. |

```go
testutils.RunTrueAndFalse(t, "feature-flag", func(t *testing.T, enabled bool) {
    // test with both boolean values
})
```

## Skip Helpers (`testutils/skip`)

| Function | Signature | Notes |
|----------|-----------|-------|
| `UnderShort` | `func UnderShort(t SkippableTest, args ...interface{})` | Skip if `-short` is set. |
| `UnderRace` | `func UnderRace(t SkippableTest, args ...interface{})` | Skip if race detector is enabled. |
| `WithIssue` | `func WithIssue(t SkippableTest, githubIssueID int, args ...interface{})` | Skip with link to issue tracker. |
| `IgnoreLint` | `func IgnoreLint(t SkippableTest, args ...interface{})` | Skip and mark as not a tracked skip. |
| `IgnoreLintf` | `func IgnoreLintf(t SkippableTest, format string, args ...interface{})` | Like `IgnoreLint` with format string. |

```go
func TestSlowThing(t *testing.T) {
    skip.UnderShort(t, "this test takes several minutes")
    // ...
}
```

## Server / Cluster (`testutils/serverutils`, `testutils/testcluster`)

| Function | Signature | Notes |
|----------|-----------|-------|
| `InitTestServerFactory` | `func InitTestServerFactory(factory TestServerFactory)` | Called in `TestMain`. Register the server factory. |
| `InitTestClusterFactory` | `func InitTestClusterFactory(factory TestClusterFactory)` | Called in `TestMain`. Register the cluster factory. |
| `StartServer` | `func StartServer(t testing.TB, params base.TestServerArgs) (TestServerInterface, *gosql.DB, Config)` | Start single-node in-process server. |
| `StartTestCluster` | `func StartTestCluster(t testing.TB, nodes int, args base.TestClusterArgs) TestCluster` | Start multi-node cluster. |
| `tc.ServerConn(n)` | `func (tc TestCluster) ServerConn(idx int) *gosql.DB` | Get SQL connection for node `idx`. |

## SQL Runner (`testutils/sqlutils`)

| Function | Signature | Notes |
|----------|-----------|-------|
| `MakeSQLRunner` | `func MakeSQLRunner(db *gosql.DB) *SQLRunner` | Create a SQL runner for the test. |
| `r.Exec` | `func (sr *SQLRunner) Exec(t testing.TB, query string, args ...interface{})` | Run a statement; fail test on error. |
| `r.CheckQueryResults` | `func (sr *SQLRunner) CheckQueryResults(t testing.TB, query string, expected [][]string)` | Run query and verify results match expected rows. |

## Leak Detection (`util/leaktest`)

| Function | Signature | Notes |
|----------|-----------|-------|
| `AfterTest` | `func AfterTest(t testing.TB) func()` | Returns a cleanup function. Call as `defer leaktest.AfterTest(t)()`. |

```go
func TestFoo(t *testing.T) {
    defer leaktest.AfterTest(t)()
    // ...
}
```

Auto-generation via `go:generate`:
```go
//go:generate ../util/leaktest/add-leaktest.sh *_test.go
```

## Common Import Paths

```go
"gitee.com/kwbasedb/kwbase/pkg/testutils"
"gitee.com/kwbasedb/kwbase/pkg/testutils/skip"
"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
```
