# Refactor Planning

Use this reference when planning a multi-file or non-trivial refactor.

## When to Plan

Plan **before** implementing when:
- Changes span multiple files
- The refactor touches module boundaries (`common/`, `kwbase/`, `kwdbts2/`)
- Interfaces or exported APIs are changing
- The change has concurrency, persistence, or SQL-semantics implications
- The task is ambiguous enough that you need to investigate first

For single-function, localized changes (extract one helper, rename a local variable), you may skip formal planning.

## Planning Steps

1. **Do not edit files** while preparing the plan.
2. **Search the codebase** to understand the current state. Read enough implementation, tests, configuration, and docs to make the plan specific to the repository.
3. **Identify** affected files, ownership boundaries, dependencies, and likely hidden coupling.
4. **Sequence** changes in a safe order: contracts and types first, then implementations, then callers, then tests, then cleanup.
5. **Include verification steps** between phases and a final validation command.
6. **Include rollback or recovery steps** for the riskiest phases.

## Plan Template

```markdown
## Refactor Plan: [title]

### Current State
[Brief description of how things work now]

### Target State
[Brief description of how things will work after]

### Affected Files
| File | Change Type | Dependencies |
|------|-------------|--------------|
| path | modify/create/delete | blocks X, blocked by Y |

### Execution Plan

#### Phase 1: Types and Interfaces
- [ ] Step 1.1: [action] in `file.go`
- [ ] Verify: [how to check it worked]

#### Phase 2: Implementation
- [ ] Step 2.1: [action] in `file.go`
- [ ] Verify: [how to check]

#### Phase 3: Tests
- [ ] Step 3.1: Update tests in `file_test.go`
- [ ] Verify: `go test ./...`

#### Phase 4: Cleanup
- [ ] Remove deprecated code
- [ ] Update comments/docs if needed

### Rollback Plan
If something fails:
1. [Step to undo]
2. [Step to undo]

### Risks
- [Potential issue and mitigation]
```

## After the Plan

Output the complete plan and ask for confirmation:

> "Shall I proceed with Phase 1?"

If the request is too ambiguous to plan safely, ask concise clarifying questions instead of editing files.

## Verification Between Phases

| Phase | Verification |
|---|---|
| After types/interfaces | `go build ./...` or `make` |
| After implementation | `go build ./...` and relevant unit tests |
| After tests | Full test suite: `go test ./...` or `make test` |
| After cleanup | Final build + test pass |
