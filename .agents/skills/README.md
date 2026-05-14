# Repository-Local Skills

Current status:

- This repository does not currently define active project-level skills.
- The first adoption step is `AGENTS.md` plus `docs/agents/*`, not a large skill catalog.
- Detailed task plans, specs, and review artifacts stay outside the repository and should be reviewed by humans before implementation.

When to add a repository-local skill later:

- the workflow is already used repeatedly in real work
- the workflow is clearly repository-specific
- the steps are easy to forget or expensive to redo incorrectly
- the team agrees the workflow is stable enough to formalize

What should not become a skill at this stage:

- general coding norms or global policy
- broad end-to-end lifecycle orchestration
- draft spec, task-plan, or review processes that still change often

If skills are introduced later, prefer a very small number of narrow,
verification-oriented skills first. Typical examples would be:

- test selection guidance for changed areas
- regression or CI failure triage
- performance verification workflows
