# PR Guide

This document defines the minimum review quality bar for agent-generated or human-authored changes.

## Repository Context

- The repository contains both Go and C/C++ code paths.
- Many changes can affect SQL semantics, time-series behavior, shared logging, shared error handling, or regression harnesses.
- `kwbase/CONTRIBUTING.md` points contributors to the public KaiwuDB contribution process document on Gitee. Use this file as the repository-local quality supplement, not as a replacement for the upstream contribution flow.

## PR Title

- Keep the title specific and scoped to the actual change.
- Prefer a module hint when useful, for example `kwbase:`, `kwdbts2:`, `common:`, or `qa:`.
- If the change fixes a tracked defect or implements a tracked requirement, include the issue identifier in the description even if the title stays short.

## PR Description Should Include

- background or problem statement
- summary of the actual code changes
- verification performed, with concrete commands
- compatibility, rollout, or operator-facing notes when relevant
- known risks, gaps, or follow-up items

## PR Body Format

Use this file as the repository-maintained source for PR body structure.

Recommended body sections:

- `## Summary`
- `## Verification`
- `## Risks`
- `## AI / KB Usage`

The `## AI / KB Usage` section is the PR-level disclosure block consumed by CI.
Its field shape is:

- `AI-Assist: yes|no`
- `AI-Model: ...`
- `AI-Agent-Tool: ...`
- `AI-Session-Count: ...`
- `KB-Docs-Used: ...`
- `KB-Missed: yes|no`
- `KB-Missed-Note: ...`

The local helper under `.agents/skills/ai-pr-kb-usage/` reads this guide and
can prepare or refresh the PR body section automatically before PR creation or
update.

## Linkage Expectations

- link the related issue, defect record, or requirement when one exists
- link the authoritative external spec or design material for cross-module or non-trivial changes
- attach benchmark or regression evidence for performance-sensitive changes
- include log snippets, error output, or screenshots only when they materially help review

## Review Expectations

- keep the diff as small and reviewable as practical
- separate pure refactor from semantic change when feasible
- call out logging, error-code, compatibility, or benchmark-impact changes explicitly
- do not hide large behavior changes behind vague summaries

## Agent Notes

- disclose assumptions
- disclose unverified areas
- avoid overstating confidence
- if verification was skipped because of environment limits, say so clearly
