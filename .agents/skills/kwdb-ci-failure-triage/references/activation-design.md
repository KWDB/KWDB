# Activation Design

## Trigger Prompts

- "CI failed, help triage"
- "Which module does this test failure belong to?"
- "Given this stack trace, what should I inspect first in KWDB?"
- "This `make kwdbts2-test` / `make kwbase-test` / `make test-logic` / `make regression-test` failure needs first-pass routing"
- "Classify this compile error or linker error in KWDB"

## Non-Trigger Prompts

- "Fix this bug in `kwdbts2/exec`"
- "Refactor the CI pipeline"
- generic build or test questions outside this repository

## False Positives

- Risk: a user already knows the failing module and wants an implementation, not triage
- Mitigation: if the request is already a concrete code-change task, skip this skill and work directly; do not let this skill drift into fix mode

## False Negatives

- Risk: the user provides only a single test name or assertion line without the words "CI" or "triage"
- Mitigation: treat "what module is this?" and "what command should I rerun?" as triage triggers in this repo

## First Action After Activation

- normalize the evidence with `assets/triage-input-template.md`, run `references/preflight-checklist.md`, and only then read `references/triage-playbook.md` for routing
