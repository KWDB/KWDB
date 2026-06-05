---
name: ai-pr-kb-usage
description: Use when a KWDB developer starts AI-assisted work on a local branch and needs to begin, maintain, or finalize branch-local PR-level AI and KB usage metrics, keep that data out of git, and render or refresh the PR section `## AI / KB Usage`
---

# AI PR / KB Usage

Use this skill when the goal is to maintain PR-level AI / KB summary fields for the current branch, without collecting session logs, prompts, doc search terms, or other sensitive local traces.

Trigger this at the start of AI-assisted implementation on a new local branch, not only when preparing the final PR body. The first action should usually be `start` so branch-local metrics exist before more AI sessions accumulate.

## Scope

- Record only PR-level aggregate fields.
- Keep all raw data in local JSON files under `.ai-metrics/`, which must stay ignored by git.
- Render or update only the `## AI / KB Usage` section.
- Read `docs/agents/pr-guide.md` as the repository-maintained PR body format source.
- Do not infer `KB-Missed` automatically. Use explicit commands.

## Primary Command

Prefer the skill-local wrapper:

```bash
.agents/skills/ai-pr-kb-usage/scripts/ai-pr <subcommand> ...
```

The canonical implementation lives here:

```bash
.agents/skills/ai-pr-kb-usage/cmd/ai-pr
```

## Core Subcommands

- `start --model <name> --tool <name>`: initialize or update the current branch record.
- `session-add [--count N]`: increment session count only.
- `doc-add <doc-id>...`: add confirmed KB doc IDs, with de-duplication.
- `doc-remove <doc-id>...`: remove doc IDs so the final list stays manually correct.
- `missed --note "<text>"`: explicitly mark KB missed.
- `missed-clear`: clear KB missed.
- `render`: print the markdown block for PR body usage.
- `sync-check`: fail when local metrics changed after the last PR body sync.
- `pr-template`: print the minimal PR body scaffold derived from `docs/agents/pr-guide.md`.
- `pr-ready --file <path>`: create or normalize a local PR body draft from `pr-guide`, then inject the current AI / KB section.
- `body-apply --file <path>`: replace or append the section inside a local markdown file.
- `validate-body --file <path>`: CI-style validation for required fields and formats.
- `status [--json]`: inspect the current local state.

## Notes

- `AI-Model` and `AI-Agent-Tool` can be passed explicitly; the script also checks common environment variables and falls back gracefully when they are absent.
- `KB-Docs-Used` is intentionally semi-manual: add candidates with `doc-add`, then trim with `doc-remove` before final PR update.
- The branch-local store defaults to `.ai-metrics/<sanitized-branch>.json`.
- Metric-changing commands set the record to `dirty`; `pr-ready` and `body-apply` clear `dirty` after syncing body content.
- `.agents/skills/ai-pr-kb-usage/scripts/ai-pr` runs the Go source via `go run ./cmd/ai-pr`; do not depend on checked-in helper binaries.
