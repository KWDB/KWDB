---
name: kwdb-code-review
description: |
  KWDB-specific code review skill. Reviews current git changes (C++ / Go) focusing on database-specific risks:
  persistence correctness, concurrency safety, SQL/TS semantics, cross-module contracts, error code mapping, compatibility, and performance regression.
  Triggers on: code review, PR review, code quality check, reviewing changes.
  Covers: Go (kwbase/) and C++ (common/ + kwdbts2/) code review.
---

# KWDB Code Review

## Overview

Perform structured review of KWDB code changes with focus on database-specific risks: persistence correctness, concurrency safety, cross-module contracts, error code compatibility, and performance regression. Default to review-only output unless user asks to implement changes.

**Key principle**: Preserve correctness, compatibility, and operability before pursuing speed. Treat SQL semantics, time-series behavior, persistence safety, concurrency, and performance regressions as first-class risks.

## Severity Levels

| Level | Name | Description | Action |
|-------|------|-------------|--------|
| **P0** | Critical | Data loss, corruption, security vulnerability, correctness bug, broken compatibility | Must block merge |
| **P1** | High | Logic error, concurrency bug, cross-module contract violation, significant performance regression | Should fix before merge |
| **P2** | Medium | Code smell, maintainability concern, missing error handling, minor SOLID violation | Fix in this PR or create follow-up |
| **P3** | Low | Style, naming, minor suggestion | Optional improvement |

## Workflow

### Phase 1: Context Gathering

1. Run `git status -sb`, `git diff --stat`, `git diff` to scope changes.
2. Identify which modules are affected: `common/`, `kwbase/`, `kwdbts2/`, `qa/`, build system.
3. Check if changes cross module boundaries (kwbase↔kwdbts2) — these need extra scrutiny.
4. Identify entry points and critical paths: data writes, storage, SQL planning, RPC, state transitions.

**Edge cases:**
- **No changes**: If `git diff` is empty, inform user and ask about staged changes or commit range.
- **Large diff (>500 lines)**: Summarize by file first, review in batches by module/feature area.
- **Mixed concerns**: Group findings by logical feature, not file order.

### Phase 2: Architecture & Design Review

Check for module-level issues:

- **Cross-module contracts**: Does the change alter behavior crossing kwbase↔kwdbts2 boundaries? This requires explicit compatibility review.
- **Shared infrastructure**: Changes to `common/src/log/` or `common/src/error/` affect multiple subsystems — verify log shape, severity rendering, JSON error fields.
- **SQL/TS semantics**: SQL output, protocol behavior, or persistence changes need broader verification than local unit tests.
- **SOLID smells**: SRP violations, god objects, feature envy, speculative generality.
- **File organization**: Are new files in the right location per KWDB module conventions?

### Phase 3: Language-Specific Review

Read the appropriate reference file based on changed code:

| Changed module | Reference file |
|----------------|----------------|
| `kwbase/**/*.go` | [Go Review Guide](references/go-review.md) |
| `common/**` or `kwdbts2/**` | [C++ Review Guide](references/cpp-review.md) |
| Both Go and C++ | Load both |

### Phase 4: Cross-Cutting Review

Load [Cross-Cutting Review Guide](references/cross-cutting.md) for:

- KwdbErrCode → PgCode mapping changes
- Log message shape compatibility
- JSON error field contracts (`Code`, `Message`, `Action`, `Reason`)
- License header compliance
- Build system / Makefile changes
- Verification requirements per change type

### Phase 5: Summary & Output

Structure your review:

```markdown
## Code Review Summary

**Files reviewed**: X files, Y lines changed
**Modules affected**: common/ | kwbase/ | kwdbts2/ | qa/ | build
**Overall assessment**: [APPROVE / REQUEST_CHANGES / COMMENT]

---

## Findings

### P0 - Critical
(none or list)

### P1 - High
1. **[file:line]** Brief title
  - Description of issue
  - KWDB-specific context (why this matters for this codebase)
  - Suggested fix

### P2 - Medium
2. (continue numbering across sections)
  - ...

### P3 - Low
...

---

## Verification Required

- [ ] Unit tests pass for affected packages
- [ ] If cross-module change: module-level tests pass
- [ ] If SQL/TS semantics change: logic tests or regression tests
- [ ] If error code change: verify KwdbErrCode → PgCode mapping
- [ ] If log change: verify log shape compatibility
```

**Clean review**: If no issues found, explicitly state what was checked, any areas not covered, and residual risks.

## Review Principles

### Review-First

Do NOT implement any changes until the user explicitly confirms. Present findings first.

### Constructive Feedback

- Be specific and actionable: explain *why* something matters for a database system
- Reference KWDB-specific conventions and patterns
- Prioritize: distinguish blocking issues from nice-to-haves
- Balance: note well-written code too

### Communication Tips

```
❌ Bad: "This is wrong."
✅ Good: "This KStatus check is missing after PUSH_ERR — the caller won't know the operation failed. Consider returning the status to the caller."

❌ Bad: "Fix this goroutine leak."
✅ Good: "This goroutine has no exit mechanism via Stopper or context cancellation. If the server quiesces, this goroutine will leak. Consider using stopper.RunWorker()."
```

### Verification Expectations

Always include a verification checklist in the output. For each change type, use the narrowest verification that proves correctness:

| Change type | Minimum verification |
|-------------|---------------------|
| Local unit change | Targeted unit test |
| Shared infrastructure | Unit + module-level test |
| Cross-module change | Module + regression test |
| SQL/TS semantics | Logic/regression test |
| Error code / log change | Verify output shape |
| Performance | Before/after benchmark |

## Next Steps

After presenting findings, ask user how to proceed:

```markdown
---

## Next Steps

I found X issues (P0: _, P1: _, P2: _, P3: _).

**How would you like to proceed?**

1. **Fix all** — I'll implement all suggested fixes
2. **Fix P0/P1 only** — Address critical and high priority issues
3. **Fix specific items** — Tell me which issues to fix
4. **No changes** — Review complete, no implementation needed

Please choose an option or provide specific instructions.
```

## References

| File | Purpose | When to load |
|------|---------|-------------|
| `references/go-review.md` | Go code review checklist for KWDB | When reviewing `kwbase/**/*.go` |
| `references/cpp-review.md` | C++ code review checklist for KWDB | When reviewing `common/**` or `kwdbts2/**` |
| `references/cross-cutting.md` | Cross-module and project-wide concerns | Always for non-trivial changes |
