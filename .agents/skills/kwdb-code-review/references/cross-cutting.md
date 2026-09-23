# Cross-Cutting Review Guide for KWDB

> Cross-module and project-level review guidelines. These apply regardless of which module the change is in.
> Focus: compatibility, contracts, observability, and verification.

## Table of Contents

- [Error Code Compatibility](#error-code-compatibility)
- [Log Compatibility](#log-compatibility)
- [License & Build](#license--build)
- [Verification by Change Type](#verification-by-change-type)
- [Security & Reliability](#security--reliability)
- [Quick Checklist](#quick-checklist-2)

---

## Error Code Compatibility

### Why This Matters

KWDB error codes (`KwdbErrCode`) are mapped to PostgreSQL error codes (`PgCode`) in `common/src/error/er_cfg.cpp`. This mapping is consumed by SQL clients, drivers, ORMs, and monitoring tools. Changing these mappings can break applications.

The error serialization path (`er_error.cpp` → `DumpToJson()`) produces `{Code, Message, Action, Reason}` JSON. Downstream tools may parse these fields.

### What to check

- [ ] Are existing `KwdbErrCode` mappings being modified? (High risk — may break clients)
- [ ] Do new `KwdbErrCode` values need a `PgCode` mapping? (Check with SQL compatibility requirements)
- [ ] Are JSON error fields (`Code`, `Message`, `Action`, `Reason`) being changed?
- [ ] Has the format of `ToString()` or `DumpToJson()` changed?
- [ ] Are error message strings changed in ways that downstream parsers might rely on?

---

## Log Compatibility

### Why This Matters

Log format is consumed by operators, log aggregators, and monitoring scripts. Severity rendering uses short format (`D`/`I`/`W`/`E`/`F`). The log infrastructure is in `common/src/log/lg_impl.cpp`.

### What to check

- [ ] Has the log message format changed (delimiters, field order, timestamps)?
- [ ] Has severity rendering changed (`D/I/W/E/F` short format)?
- [ ] Has noisy logging been added to hot paths?
- [ ] Do logs expose sensitive data (credentials, large payloads)?
- [ ] Is the log level appropriate? (DEBUG for development details, INFO for lifecycle, WARN for recoverable issues, ERROR for failures, FATAL for unrecoverable)

---

## License & Build

### License Header

All new source files must include the standard KWDB license header (Mulan PSL v2). Copy from a nearby file and update the year.

### Build System

- Makefile changes should not break existing targets
- Build configuration (`BUILD_TYPE`, `WITH_ASAN`, `WITH_TESTS`) changes affect all developers
- Go dependency changes require vendor/ directory updates
- C++ compilation flag changes (`-Wall -Wextra -Werror`) affect the entire C++ codebase
- Sanitizer configuration changes affect test and debug workflows

---

## Verification by Change Type

Use the narrowest verification that proves correctness:

| Change type | Minimum verification | Typical command |
|-------------|---------------------|-----------------|
| Local Go unit | Unit test | `go test ./pkg/<pkg>/...` |
| Local C++ unit | Unit test | `make kwdbts2-test` (targeted) |
| Shared infra (log/error) | Unit + module test | Broader test + output shape verify |
| Cross-module (kwbase↔kwdbts2) | Module + regression | `make regression-test` |
| SQL/TS semantics | Logic test | `make test-logic` |
| Error code / log | Output shape verify | Check JSON fields, log format |
| Build system | Build | `make build` + affected targets |
| Performance | Before/after benchmark | `qa/run_tsbs_test.sh` or `qa/perf_test/*` |

### General Rules

- Bug fixes: verify failing path → verify fix → verify nearest unaffected path
- Performance: record baseline command, dataset, environment; compare before/after
- SQL plan changes: preserve `EXPLAIN` or `EXPLAIN ANALYZE` evidence
- If verification is skipped due to constraints, state this explicitly

---

## Security & Reliability

### Data Integrity

- Are write operations within transactions? (Cross-step writes need atomicity)
- Are retry operations idempotent? (Same request applied twice should produce the same result)
- Is WAL written before data pages? (WAL-before-data is critical for crash recovery)

### Concurrency Safety

- Is shared state access protected?
- Are there TOCTOU patterns (check-then-act without locking)?
- Does lazy initialization have proper synchronization?

### Resource Exhaustion

- Do file handles, connections, and memory have upper bounds?
- Are there unbounded loops or recursion?
- Do external calls have timeouts?
- Do caches have size limits and eviction policies?

### Input Validation

- Is user input validated (SQL injection, path traversal)?
- Are RPC parameters validated?
- Are file paths sanitized?

---

## Quick Checklist

### Error & Log
- [ ] `KwdbErrCode` mappings not casually modified
- [ ] JSON error field format not changed
- [ ] Log shape not changed (severity, field order)
- [ ] No noisy logging added to hot paths

### License & Build
- [ ] New files have correct Mulan PSL v2 license header
- [ ] Makefile changes don't break existing targets
- [ ] Build configuration not accidentally changed

### Verification
- [ ] Narrowest verification run and passed
- [ ] Cross-module changes: regression tests run
- [ ] Error code/log changes: output shape verified
- [ ] Performance changes: before/after evidence

### Security
- [ ] Write operations have transaction protection
- [ ] Retry operations are idempotent
- [ ] Resources have upper bounds
- [ ] User input validated
