# Triage Report

Failure class: <one of kwdbts2-cpp-test | kwbase-go-test | kwbase-logic-test | build-or-link | qa-regression | environment-or-infra | unknown-needs-more-logs>

Evidence basis: <runtime evidence used | none>

Likely module: <module, package, subtree, or harness path>

Why this is the leading hypothesis: <2-4 sentences grounded in the supplied evidence>

Inspect first:
- <file or directory 1>
- <file or directory 2>
- <file or directory 3>

Minimal repro:
- <smallest next command>
- <optional second command if it materially narrows the issue>

Environment vs code: <state whether the current evidence leans environment, code or test logic, or is still ambiguous; if ambiguous, name the discriminating next command>

Missing evidence:
- <missing item 1, or `none`>

Confidence: <high | medium | low>

Blocker: <`none` only if classification is actionable from runtime evidence; otherwise the single missing evidence item that prevents reliable triage>
