# AI Collaboration Model

This document defines the repository's recommended multi-person, AI-assisted
delivery model. It is intentionally artifact-driven: detailed requirement docs
stay outside the repo, while the repo keeps stable execution rules, reusable
skills, and verification guidance.

## Core Principles

- Human owners keep responsibility for goal setting, boundary decisions,
  compatibility calls, and release authorization.
- AI should produce reviewable artifacts, not just chat answers.
- Small batches win: narrow tasks, narrow diffs, narrow verification.
- Verification is a first-class stage, not cleanup after code generation.

## Artifact Model

For substantial work, track these artifacts:

| Artifact | Where it lives | Owner | Purpose |
| --- | --- | --- | --- |
| Requirement or defect record | external system | human lead | source of truth for why the work exists |
| Design or task decomposition | external system or work item | human lead with AI help | defines scope, invariants, and acceptance |
| Code + tests | repo branch | implementer agent + reviewer | executable change |
| Verification evidence | PR, CI logs, benchmark results | implementer agent | proves the claim |
| Release record | release tracker or PR | human release owner | rollout and rollback trace |

## Delivery Stages

| Stage | Required input | AI output | Gate to exit | Example metrics |
| --- | --- | --- | --- | --- |
| Intake | external issue, bug, or request | structured summary, risk tags, execution lane | goal, scope, invariants, validation plan named | intake cycle time, clarification rounds |
| Design | approved requirement + repo context | impact analysis, task breakdown, test strategy | assumptions explicit, owner agrees on boundaries | design lead time, rework rate |
| Implementation | scoped task + affected module map | code, tests, migration notes, PR draft | build and focused tests pass locally | time to first PR, AI completion rate |
| Review | PR, diff, CI output | risk summary, fix proposals, change notes | reviewer can trace why, what, and how verified | review cycle time, comment hit rate |
| Verification | branch or candidate build | targeted regression report, perf evidence | failing path and nearest unaffected path checked | verification tax, escaped defect rate |
| Release | approved change + rollout plan | release checklist, smoke plan, rollback notes | rollout and rollback path both clear | deployment frequency, change fail rate |
| Post-release | prod metrics, logs, incidents | anomaly summary, follow-up backlog | issue either closed or follow-up created | MTTR, rollback rate |

## Execution Lanes

Choose the lightest lane that preserves quality:

- `Ask mode`: small factual questions, local code lookups, or one-off commands.
- `Single-agent mode`: one bounded code change with focused verification.
- `Leader + specialist mode`: cross-module, perf-sensitive, or release-sensitive work that benefits from parallel agents for exploration, implementation, and verification.

Escalate lanes when:

- the task crosses module boundaries
- the failure is nondeterministic or perf-sensitive
- the diff becomes hard to review in one pass

## Stage Checks

### Intake

- Is the external requirement link recorded?
- Is the task bug, feature, refactor, or perf work?
- Are rollback expectations known?

### Implementation

- Did the diff stay within the promised scope?
- Did tests cover both the changed path and the nearest unaffected path?
- Did the agent record assumptions it could not verify?

### Review and Verification

- Can a reviewer trace the exact commands used?
- Are logging, error shape, compatibility, and performance risks called out?
- If CI failed, was the failure classified before rerunning?

### Release

- Is there a smoke check?
- Is there a rollback trigger?
- Are operator-visible changes documented?

## Productivity Metrics

Avoid vanity metrics such as lines of code or raw commit count. Prefer:

- `lead time`: task accepted to merged
- `time to first PR`: task start to first reviewable diff
- `AI merge rate`: AI-assisted PRs merged / AI-assisted PRs opened
- `verification tax`: review + verification time / implementation time
- `change fail rate`: releases requiring rollback, hotfix, or incident response
- `escaped defect rate`: bugs found after merge or release

Interpretation guidance:

- High throughput with high verification tax means AI is moving effort, not
  reducing it.
- High AI merge rate with rising change fail rate means gates are too weak.
- Low lead time with low escaped defect rate is the target, not raw code volume.
