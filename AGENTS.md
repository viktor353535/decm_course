# Repository Agent Guide

This repository is used for teaching. Code, configuration, and docs must optimize for student understanding, reproducibility, and low-friction setup in a VS Code + devcontainer workflow.

## Execution Environment Notes

- Work is primarily done from inside the devcontainer.
- The devcontainer has `sudo` access and mounts the host Docker socket (`/var/run/docker.sock`).
- Prefer containerized tooling and avoid host-level install assumptions beyond VS Code, Docker, and Git.
- When documenting commands, assume they are run inside the devcontainer unless explicitly stated otherwise.

## ExecPlans

When writing complex features or significant refactors, use an ExecPlan (as described in `PLANS.md`) from design to implementation.

Use an ExecPlan when work:
- touches multiple systems or services (devcontainer, Docker Compose, Superset, Airflow, dbt, ETL scripts);
- is expected to take more than one focused session;
- introduces architecture, schema, orchestration, or deployment changes;
- has unknowns that should be de-risked with prototypes.

For small and localized edits, implement directly without an ExecPlan.

## Model and Agent Compatible Code Rules

- Prefer explicit, predictable code over clever or implicit behavior.
- Keep functions small and composable; avoid hidden global state.
- Make side effects obvious and isolated (I/O, network, filesystem, database).
- Use stable, descriptive names for modules, functions, variables, env vars, and compose services.
- When a tool has an official, widely taught project layout (for example dbt), prefer that layout unless the repo has a clear teaching reason to deviate.
- If you do deviate from an official layout, document the reason near the code and in the relevant lesson docs.
- Keep scripts non-interactive by default so agents and CI can run them end-to-end.
- Use deterministic commands and document required working directory for each command.
- Make idempotent setup and bootstrap paths a priority (`up`, `init`, `seed`, `reset` should be safe to re-run).
- Fail with actionable error messages that explain how to recover.

## Instructional Quality Rules

- Assume the reader is a beginner unless explicitly stated otherwise.
- Add concise docstrings and comments that explain why a step exists, not only what it does.
- Keep comments and docs in sync with code changes.
- Whenever behavior changes, update related Markdown docs (`README.md`, lesson notes, runbooks).
- Prefer examples students can copy and run as-is.
- Avoid unnecessary abstractions in instructional code; introduce complexity only when it teaches a clear concept.
- Keep repository structure explorable. Group files by the mental model students are learning, not by clever implementation shortcuts.
- When one container includes multiple tools for teaching convenience, keep their runtimes isolated when practical and explain the tradeoff in docs.

## Documentation Minimums for Changes

For any meaningful feature or workflow change:
- update command snippets and expected outputs;
- document prerequisites and environment assumptions;
- document validation steps that students can run locally;
- explain rollback/reset path when a step can fail midway.

## Repo Intent Reminder

This project is a teaching platform for:
- modern development workflow (VS Code, Git, Docker, devcontainers);
- practical ETL and dimensional modeling;
- analytics tools (Superset, dbt, Airflow).

Favor clarity and learning value over production-grade optimization unless the lesson explicitly targets production concerns.
