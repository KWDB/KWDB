# Component Notes

Use this directory for component-specific agent notes when a module repeatedly
needs local guidance that does not belong in the repository root `AGENTS.md`.

## What belongs here

- module responsibilities that are too detailed for `architecture-index.md`
- local review traps and invariants
- module-specific test entrypoints
- compatibility notes that apply to one subtree more than the whole repo

## What does not belong here

- per-task plans
- temporary debugging notes
- volatile requirement docs that should stay in external systems

Current state:

- `common/README.md` is the first component note because shared logging and error
  behavior are cross-cutting and easy to break.
