# Logging Guidance

This document defines how logging should be added or modified in this repository.

## Current Implementation Facts

- Shared logging implementation is under `common/src/log/`.
- `common/src/log/lg_impl.cpp` maps string levels `DEBUG`, `INFO`, `WARN`, `ERROR`, and `FATAL` to internal severity values.
- Severity rendering is validated in `common/src/log/test/lg_severity_test.cpp` and currently uses short forms `D`, `I`, `W`, `E`, and `F`.
- The logger fills log items with module, severity, file name, line number, thread id, timestamp, device, and formatted message buffer.

## Core Rules

- Log for diagnosis and operations, not narration.
- Preserve enough context to reconstruct failures.
- Avoid noisy logs in hot paths unless the extra signal is worth the cost.
- Do not leak secrets, credentials, or large raw payloads.
- Do not change shared log format casually; scripts and operators may depend on it.

## What to Include

- operation or phase
- key identifiers needed to correlate the event
- error code, status, or return path when applicable
- retry or attempt context when a failure may repeat

## What to Avoid

- duplicate logs at multiple layers with the same information
- per-row or per-record chatter in hot paths unless explicitly required
- unstable wording in logs that are likely to be grepped or monitored
- format changes to severity, file location, or shared writer behavior without broader review

## Severity Guidance

- `DEBUG`: development or deep diagnosis detail that is not required in normal operations
- `INFO`: important lifecycle or state progress that operators may reasonably expect to see
- `WARN`: abnormal but recoverable behavior, fallback paths, or suspicious state
- `ERROR`: user-visible or operator-visible failure in the current operation
- `FATAL`: unrecoverable condition that should terminate or indicate a process-fatal path

## Review Checklist

- Is the log actionable?
- Is the severity appropriate?
- Does it preserve enough context without leaking sensitive data?
- Does it add avoidable noise on a hot path?
- If this changes shared logging code, was a broader verification run performed?
