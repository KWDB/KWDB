# Environment Baseline Heuristics

Use this file when deciding whether a failure looks more like environment drift or repository code or test behavior. This file is intentionally host-agnostic and must not assume any personal machine, checkout path, or sync workflow.

## Stable Signals That Lean Away From Environment Drift

- the same commit builds successfully on the current machine
- peer tests in the same suite run and mostly pass
- the failure reaches a stable assertion, panic, or result mismatch at a consistent source location
- the failing binary or package is built and starts normally before the decisive failure
- the reproduction command can be narrowed to one test, package, or SQL file

## Signals That Lean Toward Environment Or Infra

- failure occurs before compile, link, package startup, or test entry
- missing tool, missing binary, missing library, or permission error
- path or worktree assumptions are violated, including GOPATH-style layout mismatches where relevant
- shell incompatibility, unsupported flags, or script interpreter mismatch
- `Killed`, OOM, disk exhaustion, port collision, or filesystem quota symptoms
- the same command is flaky across reruns without a stable assertion or stack

## Environment Questions Worth Checking

- branch and commit: is the failure tied to the exact revision under discussion
- artifacts: did the expected binary, test target, or generated output exist before the failing step
- toolchain: are required compiler, Go, CMake, protoc, or shell tools present and plausible
- path assumptions: does the invoked target rely on a specific worktree layout or environment variable setup
- machine resources: do logs show memory, disk, permission, or process-limit clues
- nearby passing signals: did a sibling target or earlier build phase succeed on the same machine

## How To Use This Baseline

Lean code or test issue when:

- runtime evidence reaches deterministic test logic
- nearby passing signals show the machine can build and run peer targets
- the decisive line points to a stable assertion, stack, result mismatch, or package-local failure

Lean environment or infra when:

- evidence stops before the intended code path executes
- missing tools, permissions, path assumptions, or machine resource limits dominate the failure
- rerunning the same command would primarily validate machine state instead of exercising a suspected logic path

If both remain plausible, report the ambiguity and name the single discriminating next command.
