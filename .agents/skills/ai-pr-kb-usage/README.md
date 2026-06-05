# AI / KB PR Usage Helper

This skill provides a local-only helper for recording PR-level AI / KB usage
and rendering the `## AI / KB Usage` section expected in PR bodies.

## Why this exists

- It keeps collection local to each developer machine.
- It stores only PR-level aggregate values, not prompts or session details.
- It works without a shared LLM platform or a shared Feishu proxy layer.
- It treats the PR body as the final source of truth for CI parsing.
- It reads `docs/agents/pr-guide.md` as the maintained source for PR body structure.

## Layout

- Skill root:
  - `.agents/skills/ai-pr-kb-usage/`
- Go CLI source:
  - `.agents/skills/ai-pr-kb-usage/cmd/ai-pr/`
- Skill-local runnable scripts:
  - `.agents/skills/ai-pr-kb-usage/scripts/ai-pr`
  - `.agents/skills/ai-pr-kb-usage/scripts/ci-validate-ai-pr-body`
  - `.agents/skills/ai-pr-kb-usage/scripts/demo-ai-pr`
- Local-only data:
  - `.ai-metrics/<branch>.json`

## Common commands

```bash
.agents/skills/ai-pr-kb-usage/scripts/ai-pr start --model Claude-4-Sonnet --tool Codex
.agents/skills/ai-pr-kb-usage/scripts/ai-pr session-add
.agents/skills/ai-pr-kb-usage/scripts/ai-pr doc-add DOC-102 DOC-103
.agents/skills/ai-pr-kb-usage/scripts/ai-pr doc-remove DOC-102
.agents/skills/ai-pr-kb-usage/scripts/ai-pr missed --note "WAL replay flow has no single summary doc"
.agents/skills/ai-pr-kb-usage/scripts/ai-pr missed-clear
.agents/skills/ai-pr-kb-usage/scripts/ai-pr render
.agents/skills/ai-pr-kb-usage/scripts/ai-pr status
.agents/skills/ai-pr-kb-usage/scripts/ai-pr sync-check
.agents/skills/ai-pr-kb-usage/scripts/ai-pr pr-ready --file /path/to/pr-body.md
```

## PR body update

To update only the AI / KB section inside an existing local PR body draft file:

```bash
.agents/skills/ai-pr-kb-usage/scripts/ai-pr body-apply --file /path/to/pr-body.md
```

To prepare a PR body draft from the repository PR guide and inject the latest AI / KB section:

```bash
.agents/skills/ai-pr-kb-usage/scripts/ai-pr pr-ready --file /path/to/pr-body.md
```

This command:

- reads `docs/agents/pr-guide.md`
- ensures the standard PR body headings exist
- inserts or replaces `## AI / KB Usage`
- marks the local metrics record as synced

## Dirty / sync state

Any command that changes PR-level AI or KB metrics marks the local record as `dirty`.

Check that the current branch is synced before PR submission:

```bash
.agents/skills/ai-pr-kb-usage/scripts/ai-pr sync-check
```

If this exits non-zero, run `pr-ready` or `body-apply` first.

## Runtime

The helper no longer depends on Python.

- `.agents/skills/ai-pr-kb-usage/scripts/ai-pr` runs the skill-local Go source via `go run ./cmd/ai-pr`.
- The wrapper sets `GOCACHE` automatically to a temporary directory when it is not already provided.
- The repository does not rely on checked-in helper binaries.

## CI-style validation

To validate a PR body draft file:

```bash
.agents/skills/ai-pr-kb-usage/scripts/ci-validate-ai-pr-body --file /path/to/pr-body.md
```

Validation is intentionally narrow:

- required fields must exist
- `AI-Assist` and `KB-Missed` must be `yes` or `no`
- `AI-Session-Count` must be a non-negative integer
- `KB-Missed-Note` must be non-empty when `KB-Missed: yes`

The validator does not try to infer whether the values are true.

## Environment overrides

- `AI_PR_METRICS_DIR`: override where local JSON files are stored
- `AI_PR_BRANCH_NAME`: override branch detection
- `AI_PR_MODEL_NAME`: default model when `start` or a first `session-add` omits `--model`
- `AI_PR_TOOL_NAME`: default tool when `start` or a first `session-add` omits `--tool`
- `AI_PR_GUIDE_PATH`: override the PR guide path
- `AI_PR_GOCACHE`: override the Go build cache path used by the wrapper

Explicit `start --model/--tool` values take precedence over environment defaults.
Both `start` and `session-add` append explicit `--model/--tool` values, and only
seed environment defaults when the current record is still empty for that field.
When multiple model environment variables are set, the helper uses the first
non-empty one in this priority order: `AI_PR_MODEL_NAME`, `CODEX_MODEL`,
`OPENAI_MODEL`, `ANTHROPIC_MODEL`, `OLLAMA_MODEL`, `MODEL_NAME`.

## Demo

Run:

```bash
.agents/skills/ai-pr-kb-usage/scripts/demo-ai-pr
```

The demo uses a temporary metrics directory so it does not affect real branch data.
