---
name: kwdb-refactor
description: 'Code refactoring for the KWDB database repository (Go/C++). Covers planning, complexity reduction, code smell elimination, and behavior-preserving verification.'
---

# Refactor KWDB

Improve code structure and readability without changing external behavior. Refactoring is gradual evolution, not revolution.

## Workflow Overview

```
┌─────────────────┐
│  1. PLAN        │  For multi-file/non-trivial refactors: investigate, plan, wait for confirmation
│                 │
│  2. EXECUTE     │  Apply refactoring: extract, rename, simplify, restructure
│                 │
│  3. VERIFY      │  Compile & Test → Behavior preservation checks
└─────────────────┘
```

- **Small/single-function changes** (e.g., extract one helper, rename a variable): you may skip the Plan phase and go directly to Execute → Verify.
- **Multi-file or risky changes** (e.g., restructuring a package, changing interfaces, refactoring across module boundaries): **MUST** follow all three phases.

---

## Phase 1: Plan (Multi-file / Non-trivial Refactors)

Before editing any code for multi-file or structurally significant refactors:

1. **Investigate** — Read the relevant source files, tests, and configurations. Understand the current state thoroughly. Do not edit files during investigation.
2. **Identify** — Affected files, ownership boundaries, dependencies, and hidden coupling.
3. **Sequence** — Plan changes in a safe order: contracts/types first, then implementations, then callers, then tests, then cleanup.
4. **Output the plan** using the template in `references/planning.md`.
5. **Wait for confirmation** — Stop after the plan and ask for approval before implementing. If the user already asked you to implement, still produce the plan first and wait unless they explicitly said to continue without review.

---

## Phase 2: Execute

### The Golden Rules

1. **Behavior is preserved** — Refactoring doesn't change what the code does, only how.
2. **Small steps** — Make tiny changes. One logical change at a time.
3. **Version control** — Commit before and after each safe state.
4. **Tests are essential** — Without tests, you're not refactoring, you're editing.
5. **One thing at a time** — Don't mix refactoring with feature changes.
6. **Preserve observability** — When changing retries, background work, storage paths, state transitions, or failure handling, keep logging and metrics intact.

### When NOT to Refactor

- Code that works and won't change again.
- Critical production code without adequate tests (add tests first).
- Under a tight deadline.
- "Just because" — always have a clear purpose.

### Common Refactoring Operations

| Operation | Description |
|---|---|
| Extract Method | Turn code fragment into a named function |
| Extract Class/Type | Move related behavior to a new type |
| Inline Method | Move method body back to caller |
| Rename Function/Variable | Improve clarity |
| Introduce Parameter Object | Group related parameters into a struct |
| Replace Conditional with Polymorphism | Use type dispatch instead of switch/if chains |
| Replace Magic Number/String with Constant | Named constants |
| Decompose Conditional | Break complex conditions into named functions |
| Consolidate Conditional | Combine duplicate conditions |
| Replace Nested Conditional with Guard Clauses | Early returns |
| Introduce Null Object | Eliminate nil checks |
| Replace Type Code with Enum | Strong typing via Go iota or C++ enum class |
| Replace Inheritance with Delegation | Composition over inheritance |

### Code Smells Reference

Detailed Go/C++ examples for each code smell are in `references/code-smells.md`. Read that file when you encounter:

- Long functions / god functions
- Duplicated code
- Large classes / god objects
- Long parameter lists
- Feature envy
- Primitive obsession
- Magic numbers/strings
- Nested conditionals (arrow code)
- Dead code
- Inappropriate intimacy
- Shotgun surgery
- Speculative generality

### Complexity Reduction

For reducing the cognitive complexity of a specific method, follow the detailed instructions in `references/complexity-reduction.md`.

That reference covers:
- Analyzing current complexity sources (nested conditionals, switch chains, repeated blocks, complex boolean expressions)
- Identifying extraction opportunities (validation logic, type-specific processing, common patterns)
- Extracting focused helper methods with single responsibilities
- Simplifying the main method to read as a high-level flow

### KWDB-Specific Considerations

When refactoring KWDB code, pay attention to:

- **Error handling**: Do not change error codes, error messages, or `PgCode` mappings. See `docs/agents/errors.md`.
- **Logging**: Preserve log severity, message format, and structured fields. See `docs/agents/logging.md`.
- **SQL semantics**: Ensure query results, plan selection, and type inference remain identical.
- **Time-series behavior**: Downsampling, interpolation, latest-value queries, and time-window semantics must not change.
- **Concurrency safety**: Lock ordering, atomic operations, channel semantics, and context cancellation must be preserved.
- **Go interface contracts**: Do not remove methods from interfaces; prefer adding new ones if needed.
- **C++ object lifetimes**: Be careful with ownership, raw pointers, and reference semantics.
- **Module boundaries**: Cross-module changes (`common/`, `kwbase/`, `kwdbts2/`) require extra caution and review.

---

## Phase 3: Verify

After completing the refactoring, you MUST verify:

### 1. Compilation & Tests

Run the appropriate build and test commands based on the change scope, following `docs/agents/testing-flow.md`:

- **Change scope** determines the verification level:
  - Single-function/single-file → targeted unit test (Level 1)
  - Module-internal change → module test suite (Level 2)
  - Cross-module or semantic change → broader regression/logic tests (Level 3)
- **Common entry points** (from testing-flow.md): `make build`, `make kwbase-test`, `make test`, `make test-logic`, etc.
- **MANDATORY: Verify "failed=0"** in the test output. Do not weaken tests to make a change pass.

### 2. Behavior Preservation
- Confirm that public API surfaces, error messages, and observable behavior are unchanged.
- For bug fixes, verify both the failing path and the nearest unaffected path.

### 3. Final Checks
- Dead code removed (commented-out code, unused imports, unused functions).
- New names are descriptive and follow project conventions.
- No new magic numbers or strings introduced.
- No unnecessary abstractions added.

---

## Refactoring Checklist

### Code Quality
- [ ] Functions are small and do one thing
- [ ] No duplicated code
- [ ] Descriptive names (variables, functions, types)
- [ ] No magic numbers/strings
- [ ] Dead code removed

### Structure
- [ ] Related code is together
- [ ] Clear module/package boundaries
- [ ] Dependencies flow in one direction
- [ ] No circular dependencies

### Verification
- [ ] Code compiles without errors
- [ ] Test results explicitly state "failed=0"
- [ ] Error handling and logging preserved
- [ ] SQL/time-series semantics unchanged
- [ ] Concurrency safety maintained

---

## Related References

| File | When to Read |
|---|---|
| `references/code-smells.md` | When you encounter a specific code smell and need Go/C++ examples |
| `references/complexity-reduction.md` | When tasked with reducing a specific method's cognitive complexity |
| `references/planning.md` | When planning a multi-file refactor and need the plan template |
| `docs/agents/errors.md` | When refactoring error handling code |
| `docs/agents/logging.md` | When refactoring logging code |
| `docs/agents/testing-flow.md` | When determining what tests to run and what verification level to apply |
