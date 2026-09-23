# Error Handling Guidance

This document defines how errors should be created, propagated, logged, and tested in this repository.

## Current Implementation Facts

- Shared error infrastructure is under `common/src/error/`.
- `common/src/error/er_cfg.cpp` maintains `KwdbErrCode` to message metadata and `PgCode` mappings.
- `common/src/error/er_error.cpp` builds the concrete error message and serializes errors into JSON fields such as `Code`, `Message`, `Action`, `Reason`, and `Internal_actions_taken`.
- `common/src/error/er_stack.cpp` stores errors in `KwdbErrorStack` and exposes `DumpToJson()` for the aggregated JSON surface.

## Core Rules

- Preserve the original error code and cause path where possible.
- Add context where recovery, diagnosis, or external reporting decisions are made.
- Do not swallow errors silently.
- Do not change existing code-to-message or code-to-`PgCode` mappings casually.
- Separate internal diagnosis detail from externally stable behavior when the interface requires compatibility.

## Compatibility Rules

- If a `KwdbErrCode` already maps to a specific message or `PgCode`, treat that mapping as externally visible until proven otherwise.
- If you add a new error code, review whether it also needs a `PgCode` mapping and stable metadata in `er_cfg.cpp`.
- If you change JSON error fields emitted by `ToString()` or `DumpToJson()`, treat it as a compatibility-sensitive change.
- Avoid brittle changes to wording that downstream tests, tooling, or clients may parse.

## When to Add Context

- IO boundaries
- RPC or protocol boundaries
- state-transition boundaries
- storage, transaction, or persistence boundaries
- user-facing SQL or API error surfaces

## When Not to Add Noise

- do not wrap the same failure at every stack layer
- do not replace a meaningful error code with a generic message
- do not log and return the same error repeatedly without new information

## Testing Expectations

- test the semantic outcome of the failure
- test error classification or code when that is the stable contract
- test JSON shape or external fields when callers depend on them
- avoid brittle full-message assertions unless the exact string is the contract

## Review Checklist

- Is the original code or cause preserved?
- Is the added context actually useful?
- Does the change preserve user-visible compatibility where required?
- Were failure paths verified, not just success paths?
