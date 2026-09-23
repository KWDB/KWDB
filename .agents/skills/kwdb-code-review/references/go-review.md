# Go Code Review Guide for KWDB

> Principle-based review guide for KWDB Go code (kwbase/). Focuses on what to watch for, not prescriptive patterns.

## Table of Contents

- [Error Handling](#error-handling)
- [Concurrency](#concurrency)
- [Context](#context)
- [Testing](#testing)
- [Interface & Package Design](#interface--package-design)
- [Common Pitfalls](#common-pitfalls)
- [Quick Checklist](#quick-checklist)

---

## Error Handling

### Key Principles

1. **Use `cockroachdb/errors`** — KWDB uses `github.com/cockroachdb/errors` throughout. Look for `errors.Wrapf`, `errors.Wrap`, `errors.Newf`, `errors.AssertionFailedf`. The standard library `errors` package is rarely used directly. Raw `fmt.Errorf` with `%v` instead of `%w` breaks the error chain.

2. **SQL-layer errors need PG codes** — SQL errors must use `pgerror` with PostgreSQL-compatible error codes (`pgcode.*`). Without it, the SQL client won't get a proper error classification.

3. **Don't silently swallow errors** — Patterns like `result, _ := fn()` are red flags. Errors should either be handled (with recovery logic) or propagated (with context).

4. **Don't both log and return** — Either log the error (at the boundary where context is complete) or return it (wrapped with context). Doing both clutters logs and makes debugging harder.

5. **Sentinel errors** — Package-level `var errXxx = errors.New(...)` and `var XxxError = pgerror.New(...)` are the common pattern. Use `errors.Is()` to check them (not `==`), since they may be wrapped.

6. **Assertion failures** — Use `errors.AssertionFailedf` for internal invariants that should never be violated. This adds stack traces and Sentry reporting.

### What to look for

- [ ] Missing `%w` in error wrapping (breaks `errors.Is`/`errors.As`)
- [ ] SQL errors without `pgerror` / PG error codes
- [ ] `result, _ := fn()` — silently discarded errors
- [ ] Errors logged *and* returned in the same function
- [ ] Direct error comparison (`==`) instead of `errors.Is`

---

## Concurrency

### Key Principles

1. **Use `Stopper` for goroutine lifecycle** — Background goroutines should be managed through `Stopper.RunWorker`, `RunTask`, or `RunAsyncTask`. Bare `go func()` is suspicious unless there's a clear exit mechanism (context cancellation, channel close).

2. **Protect shared state with `syncutil.Mutex`** — The project uses `syncutil.Mutex` (with deadlock detection) wrapped in an anonymous struct alongside the fields it protects. Look for the `mu struct { syncutil.Mutex; ... }` pattern. Lock ordering should be consistent across the codebase.

3. **Use `ctxgroup` for errgroup** — The project uses `util/ctxgroup` which wraps `errgroup` with explicit context passing. This prevents using a closed context after `Wait()`.

4. **Avoid blocking while holding locks** — I/O, network calls, and long computations should not be done while holding a mutex.

### What to look for

- [ ] Bare `go func()` without stopper or exit mechanism — potential goroutine leak
- [ ] Shared mutable state without synchronization
- [ ] Inconsistent lock ordering — potential deadlock
- [ ] I/O or blocking operations inside critical sections
- [ ] Raw `errgroup.Group` instead of `ctxgroup.Group`

---

## Context

### Key Principles

1. **Context is the first parameter** — Never store context in a struct. It should flow as the first function parameter.

2. **Propagate, don't create** — Don't call `context.Background()` mid-call-chain. Pass the received context through, annotating it as needed.

3. **`AmbientContext` for server components** — Server structs embed `log.AmbientContext` and use `AnnotateCtx(ctx)` to add log tags and tracing.

4. **Always defer cancel** — Every `context.WithCancel` / `WithTimeout` must have its cancel function deferred to avoid resource leaks.

### What to look for

- [ ] Context stored in struct fields
- [ ] `context.Background()` used in the middle of a call chain
- [ ] Missing `defer cancel()` after `context.WithCancel` / `WithTimeout`
- [ ] Long-running operations that don't check `ctx.Done()`

---

## Testing

### Key Principles

1. **Leak detection is mandatory** — Every test function must have `defer leaktest.AfterTest(t)()`. The `add-leaktest.sh` generate script automates this.

2. **Test server pattern** — Integration tests use `serverutils.StartServer` which returns `(TestServerInterface, *gosql.DB, *kv.DB)`. Clean up with `defer s.Stopper().Stop(ctx)`.

3. **`TestingKnobs` for injection** — Test hooks are injected through `base.TestingKnobs`. Each subsystem defines its own knobs struct (e.g., `SchemaChangerTestingKnobs`).

4. **Table-driven tests** — Standard Go table-driven tests are used throughout. Error paths and boundary conditions should be tested.

### What to look for

- [ ] Missing `defer leaktest.AfterTest(t)()`
- [ ] Tests without `TestMain` setup that need a test server
- [ ] Missing error path or boundary condition tests
- [ ] Tests weakened to make changes pass (never do this)

---

## Interface & Package Design

### Key Principles

1. **Small interfaces at the consumer** — Interfaces are typically defined near their use site with 1-3 methods. Large interfaces are rare and often have compile-time compliance checks.

2. **Compile-time checks** — `var _ Interface = &Type{}` assertions verify interface compliance at compile time.

3. **Package naming** — Packages are named by function, not by pattern (no `utils`, `common`, `helpers` at the Go level).

4. **`internal/` for restricted access** — The `internal/` package convention is used to limit visibility.

### What to look for

- [ ] Interfaces with too many methods (consider splitting)
- [ ] Missing compile-time interface compliance checks
- [ ] `interface{}` abuse where generics or specific types would work
- [ ] Import cycles (usually indicate poor package boundaries)

---

## Common Pitfalls

Watch for these well-known Go traps, especially in concurrent code:

- **Loop variable capture** (Go < 1.22): goroutines inside loops may capture the same variable
- **Nil interface trap**: returning a nil pointer typed as an `error` interface produces a non-nil `error`
- **Slice sharing**: sub-slices share the underlying array; unintended mutations can occur
- **defer in loops**: defers only run when the function returns, not per-iteration
- **Map without make**: writing to a nil map panics

---

## Quick Checklist

### Error Handling
- [ ] `cockroachdb/errors` used (not standard `errors` or raw `fmt.Errorf`)
- [ ] SQL errors use `pgerror` with PG error codes
- [ ] Errors wrapped with `%w` (not `%v`)
- [ ] No silently swallowed errors
- [ ] Internal assertions use `errors.AssertionFailedf`

### Concurrency
- [ ] Goroutines managed through `Stopper` (not bare `go func()`)
- [ ] Shared state protected with `syncutil.Mutex`
- [ ] Lock ordering is consistent
- [ ] `ctxgroup` used instead of bare `errgroup`

### Context
- [ ] Context is first parameter, not stored in structs
- [ ] Cancel functions deferred
- [ ] Background tasks respond to context cancellation

### Testing
- [ ] `defer leaktest.AfterTest(t)()` present in every test
- [ ] Error paths and edge cases tested
- [ ] No tests weakened to make changes pass
