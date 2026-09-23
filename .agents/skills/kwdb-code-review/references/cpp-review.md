# C++ Code Review Guide for KWDB

> Principle-based review guide for KWDB C++ code (common/ + kwdbts2/). Focuses on what to watch for, not prescriptive patterns that may not hold universally.

## Table of Contents

- [Memory & Resource Safety](#memory--resource-safety)
- [Error Handling](#error-handling-1)
- [Concurrency](#concurrency-1)
- [Performance](#performance)
- [API Design](#api-design)
- [Common Anti-Patterns](#common-anti-patterns)
- [Quick Checklist](#quick-checklist-1)

---

## Memory & Resource Safety

### Key Principles

1. **Ownership should be clear** — The project uses a mix of raw pointers, `shared_ptr`, and custom wrappers (`TsSliceGuard`). The key question is not "which pattern" but "is ownership unambiguous?" If a function allocates memory, the caller must know who frees it and when.

2. **Resource acquisition should be paired with release** — Whether using RAII, goto-cleanup, or explicit close/free, every acquisition path (including error paths) must have a corresponding release. This is especially critical in the storage engine where file handles and memory-mapped regions are involved.

3. **Watch for raw `new`/`delete`** — The project generally avoids C++ exceptions, so `new(std::nothrow)` (or the `KNEW` macro) is used instead of plain `new` to avoid `std::bad_alloc`. Check that allocation failures are handled.

4. **On-disk struct layouts are sacred** — Structs that map to on-disk format (e.g., `TsEntitySegmentBlockItem`, `TsEntityItem`) often have `static_assert` for size verification. Changing these structs changes the storage format and requires migration.

### What to look for

- [ ] Allocation without matching deallocation on all paths
- [ ] `new` without `std::nothrow` (or `KNEW`) — potential unhandled exception
- [ ] Mismatched allocator/deallocator pairs (e.g., `k_malloc` + `delete`)
- [ ] On-disk struct size changes without `static_assert` update
- [ ] Raw pointer ownership without documentation of who owns it

---

## Error Handling

### Key Principles

1. **No C++ exceptions** — KWDB C++ uses `KStatus` return codes (SUCCESS/FAIL), not exceptions. Every function that can fail should return `KStatus` and callers must check it. Look for `[[nodiscard]]` on `KStatus`-returning functions.

2. **`PUSH_ERR` for error context** — When an operation fails, use `PUSH_ERR` (or typed variants `PUSH_ERR_0` through `PUSH_ERR_3`) to push errors onto the context-local error stack (`KwdbErrorStack`). The stack stores up to 8 errors and serializes to JSON with fields `{Code, Message, Action, Reason}`.

3. **Error codes are an external contract** — `KwdbErrCode` → `PgCode` mappings in `er_cfg.cpp` are externally visible. Modifying existing mappings can break downstream consumers. Adding a new error code may require a corresponding `PgCode` mapping.

4. **Don't silently discard failures** — Ignoring a `KStatus` return (especially `FAIL`) is equivalent to swallowing an exception. If the return value truly doesn't matter, document why.

### What to look for

- [ ] `KStatus` return values ignored by callers
- [ ] Missing `PUSH_ERR` after detecting a failure
- [ ] Existing `KwdbErrCode` mappings modified without compatibility review
- [ ] JSON error field shape changed (`Code`/`Message`/`Action`/`Reason`)
- [ ] `KStatus`-returning functions missing `[[nodiscard]]`

---

## Concurrency

### Key Principles

1. **Use project-specific synchronization** — The project uses `KLatch` (mutex wrapper), `KRWLatch` (rwlock wrapper), and `SpinMutex` (for short critical sections). These are instrumented with debug tracking and LATCH_TYPES enum IDs for observability. Standard `std::mutex` / `std::shared_mutex` also appear in some subsystems.

2. **Lock ordering matters** — With dozens of named latches across the system, inconsistent lock ordering is a deadlock risk. Changes that introduce new locks or change lock acquisition order deserve extra scrutiny.

3. **Atomic operations need correct memory ordering** — `std::atomic` is used for counters and flags. Pay attention to `memory_order_acquire`/`release` pairs — incorrect ordering can cause subtle bugs on weak-memory platforms.

4. **Thread pool tasks should be cancellable** — The `KWDBDynamicThreadPool` supports cooperative cancellation via `IsCancel()` checkpoints. Long-running tasks should check for cancellation periodically.

5. **`volatile` is not synchronization** — `volatile` does not provide atomicity or memory ordering. If it appears in a concurrency context, it's likely wrong.

### What to look for

- [ ] Shared data without latch/mutex protection
- [ ] Inconsistent lock ordering (new locks added without considering existing order)
- [ ] I/O or blocking while holding a lock
- [ ] `volatile` used for thread synchronization
- [ ] Missing memory ordering on atomic operations
- [ ] Thread pool tasks that never check `IsCancel()`

---

## Performance

### Key Principles

1. **Hot paths avoid allocation** — The time-series engine's data path is performance-critical. Look for repeated allocations in loops, unnecessary copies, and suboptimal data structure choices.

2. **Branch prediction hints** — `LIKELY`/`UNLIKELY` macros (`__builtin_expect`) are used on hot paths to guide branch prediction. Missing them on obvious hot/cold branches may be a missed optimization (but not a correctness issue).

3. **Container pre-allocation** — `reserve()` should be used when the final size of a `std::vector` is known in advance.

4. **String concatenation in loops** — Repeated `+=` on `std::string` causes repeated reallocation. Pre-calculate total size and `reserve()` first.

5. **Lock contention** — Critical sections should be as short as possible. Coarse-grained locks on hot paths can become scalability bottlenecks.

### What to look for

- [ ] Repeated allocations inside loops on hot paths
- [ ] Large objects passed by value instead of const reference
- [ ] Coarse-grained locking on hot paths
- [ ] Algorithmic O(n²) where O(n log n) or O(n) is possible
- [ ] Missing `LIKELY`/`UNLIKELY` on obvious hot/cold branches

---

## API Design

### Key Principles

1. **Const correctness** — Input parameters should be `const` references or pointers. Methods that don't modify state should be `const`.

2. **Context parameter** — Many functions take `kwdbContext_p ctx` as a parameter for error stack, tracing, and frame tracking. Not every function needs it (utility functions, simple accessors), but entry points and I/O functions typically do.

3. **`override` on virtual functions** — All overridden virtual functions should use the `override` specifier. Missing it can hide bugs during refactoring.

4. **`explicit` on single-argument constructors** — Prevents unintended implicit conversions.

5. **Object slicing** — Passing derived objects by value to a base parameter slices the object. Use references or pointers.

### What to look for

- [ ] Non-const reference parameters that aren't output parameters
- [ ] Missing `override` on virtual function overrides
- [ ] Missing `explicit` on single-argument constructors
- [ ] Object slicing (passing derived by value as base)
- [ ] Missing const on methods that don't mutate state

---

## Common Anti-Patterns

Watch for these in KWDB C++ code:

- **Ignoring `KStatus` return values** — The most common C++ bug in this codebase
- **Error logged but not propagated** — `LOG_ERROR` followed by continuing as if nothing happened
- **Raw pointer ownership unclear** — Who frees this? When? Under what error conditions?
- **Mutex held across I/O** — Blocks other threads unnecessarily
- **Inconsistent header guard style** — Some use `#pragma once`, others use `#ifndef`; mixing in the same module is confusing
- **Missing error handling on allocation** — Not checking if `KNEW`/`malloc` returned `nullptr`

---

## Quick Checklist

### Memory & Resources
- [ ] Ownership is clear (raw pointer or smart pointer, documented if ambiguous)
- [ ] Allocation failures handled (nothrow new or nullptr check)
- [ ] On-disk struct sizes not changed without migration plan
- [ ] Resource cleanup on all paths (including error paths)

### Error Handling
- [ ] `KStatus` returns checked by callers
- [ ] `PUSH_ERR` used on failure paths
- [ ] `KwdbErrCode` mappings not casually modified
- [ ] No silent swallowing of `FAIL`

### Concurrency
- [ ] Shared data protected by latches/mutexes
- [ ] Lock ordering consistent
- [ ] No I/O while holding locks
- [ ] Thread pool tasks respond to cancellation

### Performance
- [ ] No redundant allocations in hot paths
- [ ] Containers pre-allocated where size is known
- [ ] Parameters use const references for large types

### API
- [ ] `override` on virtual overrides
- [ ] `explicit` on single-argument constructors
- [ ] Const-correctness applied consistently
