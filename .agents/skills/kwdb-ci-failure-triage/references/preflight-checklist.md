# Preflight Checklist

Run this checklist before classification. The goal is to confirm that the input contains enough runtime evidence for first-pass triage and to separate missing evidence from true uncertainty.

## 1. Evidence Shape

Confirm whether the input includes at least some of these:

- failing command or target
- narrowed repro command
- failing test, package, suite, or script name
- first decisive error line
- file:line, stack frame, assertion, panic, compile error, or link error
- environment clues

If none of these are present and the input is only a source path, module guess, or vague report, stop and request the single smallest missing item.

## 2. Revision And Scope

Check whether the evidence identifies:

- branch or commit if relevant
- whether the failure came from CI, local verification, or a copied log
- whether the command is already narrow enough to reproduce one failing surface

If the current command is too broad, ask for or derive a narrower repro command in the report.

## 3. Artifact And Tooling Clues

Look for signs that the target never reached the intended code path:

- missing binary or generated artifact
- missing compiler, Go toolchain, CMake, protoc, or shell utility
- path or worktree mismatch
- permissions, disk, OOM, or port conflicts

These clues do not end triage, but they should bias classification toward `environment-or-infra`.

## 4. Nearby Passing Signals

Capture any evidence that reduces environment suspicion:

- build succeeded before the test failed
- peer tests passed
- only one test case failed
- the same source line or assertion failed consistently

If nearby passing signals are absent, avoid overstating confidence.

## 5. Gate Before Classification

Proceed to classification only when one of these is true:

- there is enough runtime evidence to place the failure into a class
- there is enough evidence to state a blocker precisely

Do not proceed by static code reading alone.
