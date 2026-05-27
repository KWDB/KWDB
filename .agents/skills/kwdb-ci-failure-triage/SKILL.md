---
name: kwdb-ci-failure-triage
description: Use when CI or local verification fails in this KWDB repository and the first goal is to classify the failure, identify the most likely module, pick the first files to inspect, and choose the smallest reproduction command before attempting a fix
---

# KWDB CI Failure Triage

## Overview

First-pass CI triage for KWDB. This skill is a repository-specific triage wrapper with a fixed report shape. Load the repository references, normalize the evidence, classify the failure, and return the completed triage report. Do not jump to code changes or speculative root causes.

## Hard Gates

- Triage only. Do not fix code, edit repository files, generate patches, or change git state while this skill is active.
- Evidence first. Do not classify, route, or speculate from source code alone when runtime or CI failure evidence is missing.
- If the available input does not contain enough failure evidence to classify safely, ask for the single smallest missing item or return a blocker report.
- If both the failing command and a decisive failure line or log excerpt are missing, the report must stay at `unknown-needs-more-logs`.
- Do not convert a missing-evidence situation into local static debugging.

## When to Use

- CI failed for this repository.
- A local verification command failed and the next question is "where should I look first?"
- The user provides any mix of failing command, test name, assertion, panic, stack trace, compile error, link error, or regression script output.

Do not use when:

- The task is already narrowed to a concrete code fix and no triage is needed.
- The request is about generic CI design outside this repository.

## Non-goals

- not root-cause proof
- not implementation planning
- not code modification
- not environment bootstrap for any specific host or machine

## Required Inputs

Start with whatever is available, then normalize into these fields:

- failing command or target
- narrowed repro if the original command is too broad
- failing test name, script name, or package name
- first decisive error line
- file:line, stack frame, or undefined symbol if present
- failure shape such as assertion, panic, stack, compile error, or link error
- environment clues such as architecture, missing tools, OOM, disk full, or path mismatch
- nearby passing signals if available, such as build succeeded, peer tests passed, or failure is isolated

If some fields are missing, continue with partial evidence and list the exact missing fields at the end.

## Workflow

1. Load `references/triage-playbook.md`.
2. Load `references/classification-checklist.md`.
3. Load `references/preflight-checklist.md`.
4. Load `references/environment-baseline.md` when the answer depends on distinguishing environment drift from code or test behavior.
5. Normalize the user-provided evidence by filling `assets/triage-input-template.md` mentally or explicitly before classification.
6. Run the preflight checklist against the normalized input.
7. If `failing command`, `failing test or script name`, and `first decisive error line` are all missing, ask for the single most useful missing item before proceeding.
8. If the available input is only source code, a hunch, or a suspected module without failure output, stop and request minimal runtime evidence instead of classifying from static inspection.
9. Use the classification checklist to choose the smallest fitting class:
   - `kwdbts2-cpp-test`
   - `kwbase-go-test`
   - `kwbase-logic-test`
   - `build-or-link`
   - `qa-regression`
   - `environment-or-infra`
   - `unknown-needs-more-logs`
10. Use the playbook to choose:
   - the highest-probability module
   - the first files or directories to inspect
   - the smallest reproduction command worth running next
11. Fill `assets/triage-report-template.md` completely. Do not invent extra sections unless the user asks.

## Output Contract

Always return the final answer by filling `assets/triage-report-template.md`.

## Guardrails

- Do not claim root cause unless the evidence directly supports it.
- Do not edit files, propose patches, or switch into implementation mode from inside this skill.
- Do not use uncommitted workspace edits, local diffs, or suspected fixes as evidence for root-cause claims.
- Do not default to `make test` when a single `ctest`, `go test`, `make testlogic`, or `make regression-test` command can narrow the issue.
- Do not ask broad follow-up questions when one concrete missing item would unblock classification.
- Do not restate all repository knowledge inline; cite the conclusion and keep the report compact.
- Do not treat a personal host, personal path, or one-off machine workflow as part of the skill contract.

## Escalation Rules

- If the failure is equally compatible with environment and code, list both and name the discriminating next command.
- If only a test name is available and it does not map cleanly to a module, ask for exactly one of:
  - the full command
  - the first failing file and line
  - the last 50 to 100 log lines around the failure
- If a build error comes from generated or vendored surfaces, point to the owning generator or third-party subtree instead of suggesting blind manual edits.

## Repository Anchors

- test entrypoints: `Makefile`, `kwbase/Makefile`
- module map: `docs/agents/architecture-index.md`
- verification policy: `docs/agents/testing-flow.md`
- preflight checklist: `references/preflight-checklist.md`
- environment baseline: `references/environment-baseline.md`
- classification checklist: `references/classification-checklist.md`
- normalized input template: `assets/triage-input-template.md`
- report template: `assets/triage-report-template.md`
- examples: `references/examples.md`
