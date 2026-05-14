# Agent Docs Overview

This directory is the repository-local knowledge base for coding agents.
It keeps stable, reusable guidance in the repo while leaving volatile task
specs and day-to-day requirement documents in external systems.

Adoption starts with root `AGENTS.md` plus the files listed here.
Repository-local procedural skills stay optional until repeatable workflows prove
they are worth packaging under `.agents/skills/` (see `.agents/skills/README.md`).

**Initial onboarding path:** read `AGENTS.md`, then `architecture-index.md` and
`testing-flow.md`, then use `resource-index.md` as the map into topic guides such
as `logging.md`, `errors.md`, `pr-guide.md`, and `coding-style-go-cpp.md`.

Companion files use the `-zh.md` suffix (for example `README-zh.md`) and align
with selected English originals; English files remain the default source of
truth for open-source publishing.

## Directory Layout

```text
kwdb/
├── AGENTS.md
├── .agents/
│   └── skills/
│       └── README.md
└── docs/
    └── agents/
        ├── README.md
        ├── ai-collaboration-model.md
        ├── architecture-index.md
        ├── ci-release-guide.md
        ├── coding-style-go-cpp.md
        ├── components/
        │   ├── README.md
        │   └── common/
        │       └── README.md
        ├── errors.md
        ├── logging.md
        ├── pr-guide.md
        ├── resource-index.md
        └── testing-flow.md
```

## Chinese-language companions (`*-zh.md`)

These files summarize or translate selected English originals where a Chinese
edition is helpful alongside the default English text.

**Authority:** treat `docs/agents/*.md` files whose names do **not** end with
`-zh.md` as the OSS default source of truth.

| English original | Chinese edition |
| --- | --- |
| `README.md` | `README-zh.md` |
| `resource-index.md` | `resource-index-zh.md` |
| `architecture-index.md` | `architecture-index-zh.md` |
| `testing-flow.md` | `testing-flow-zh.md` |
| `ai-collaboration-model.md` | `ai-collaboration-model-zh.md` |
| `coding-style-go-cpp.md` | `coding-style-go-cpp-zh.md` |
| `pr-guide.md` | `pr-guide-zh.md` |
| `logging.md` | `logging-zh.md` |
| `errors.md` | `errors-zh.md` |
| `ci-release-guide.md` | `ci-release-guide-zh.md` |

## Layering Rules

### `AGENTS.md`

- Repository-wide rules that should apply on nearly every task.
- Keep it short, stable, and policy-oriented.
- Do not store volatile requirement details or per-task plans there.

### `docs/agents/*`

- Declarative knowledge for agents.
- Good fit: architecture, invariants, review boundaries, testing surfaces,
  logging/error compatibility rules, CI/release expectations, component maps.
- Bad fit: per-task checklists better expressed as skills.

### `.agents/skills/*`

- Procedural knowledge for repeatable workflows.
- Good fit: task intake, CI triage, test selection, perf investigation, PR
  preparation, release readiness.
- Keep skills task-oriented and bounded.
- At the current stage, keep this as a candidate list only unless the team
  agrees a workflow is stable enough to formalize.

### External requirement docs

- Detailed bug, feature, and performance specs should stay outside this repo.
- The authoritative task artifact should live in the team's issue tracker, wiki,
  or another approved system, then be linked from issues, PRs, or change
  summaries.

## Current Docs

### Core references

- `architecture-index.md`: top-level module map and task routing hints
- `coding-style-go-cpp.md`: Go and C++ style expectations for KWDB OSS trees
- `testing-flow.md`: verification levels and common entrypoints
- `logging.md`: shared logging constraints
- `errors.md`: shared error and compatibility constraints
- `pr-guide.md`: PR summary and review expectations
- `resource-index.md`: quick map to repo-local resources
- `ai-collaboration-model.md`: stage-by-stage AI-assisted delivery model
- `ci-release-guide.md`: current validation surfaces plus recommended CI/release gates

### Component area

- `components/README.md`: where component-specific notes should live
- `components/common/README.md`: shared-surface notes for common runtime behavior

## Design Posture

- Global policy stays in `AGENTS.md`.
- Stable facts stay under `docs/agents/*`.
- Repeatable procedures may later move under `.agents/skills/*`, but only after
  the team has used and reviewed them enough to stabilize.
- Volatile task specs stay outside the repository.

## Remaining Gaps

- Add component-specific notes once repeated module-level patterns stabilize.
- Add child `AGENTS.md` files only when a subtree really needs tighter local rules.
- Extend CI/release guidance once the owning platform and pipelines are explicit.
