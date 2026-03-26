# Lecture 4 Simple ETL Track + Advanced CLI Contrast

This ExecPlan is a living document. Update `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` as work advances.

Reference: `PLANS.md` (repository root) for standards.

## Purpose / Big Picture

Add a beginner-friendly ETL path for Lecture 4 based on a single readable Python script, then keep the current advanced CLI-driven ETL as the "what better engineering buys us next" section.

The goal is to lower initial cognitive load. Students would first see a straightforward `extract -> transform -> load` script for Airviro air-quality station `8`, with manual date window selection and a simple load-mode choice (`replace` vs `update`). After that, the lecture would pivot to the existing modular ETL package and explain what the more advanced design adds: retries, adaptive window splitting, validation, audits, long-form raw storage, and curated serving objects.

This work should be treated as a **before Lecture 4** improvement. It is directly about student onboarding for that lecture.

## Student Learning Impact

- Affects Lecture 4 directly.
- Gives students a smaller ETL example they can read top-to-bottom without switching between multiple modules immediately.
- Makes `extract`, `transform`, and `load` easier to explain step by step.
- Provides a deliberate bridge to the current advanced ETL so students can see why engineering complexity was added.
- Starts Lecture 4 with one concrete source: air-quality station `8`.
- Leaves pollen as an optional later step in the advanced ETL section instead of mixing both ideas into the first tutorial.

## Scope

In scope:
- Add a simple ETL script derived from `.instructor-private/lecture-04-simple-etl-idea/etl_template.py`.
- Use manual `--from` / `--to` window selection.
- Add a simple load mode option: `replace` (truncate/reload) or `update` (upsert).
- Make the simple ETL path pull air-quality data for station `8`.
- Rewrite `docs/lectures/lecture-04/README.md` into a step-by-step tutorial that teaches the simple ETL first.
- Keep the advanced CLI ETL in the same README as an "advanced path" and explain what it improves.
- Add explicit instructional sections about dimensional design choices, long-form ingestion, transformations, and where dimensions come from in the advanced path.
- Keep the advanced CLI examples for Lecture 4 to air-quality station `8` plus pollen station `25`.

Out of scope:
- Replacing the advanced ETL package.
- Adding Airflow or dbt dependencies to the simple ETL path.
- Reworking Lecture 5 orchestration in this change.
- Expanding Lecture 4 back to station `19`.

## Progress

- [x] Investigate current state and constraints
- [x] Implement core changes
- [x] Update docs and examples
- [x] Run validation checks
- [x] Final review and cleanup

## Surprises & Discoveries

- Discovery: the template in `.instructor-private/lecture-04-simple-etl-idea/etl_template.py` is structurally a good beginner teaching aid, but it currently points to a completely unrelated example API.
  Evidence: the template uses `restcountries.com` and a single `load()` function with truncate-only semantics.

- Discovery: the current Lecture 4 README starts with the advanced ETL path immediately.
  Evidence: `docs/lectures/lecture-04/README.md` currently leads with `make etl-bootstrap`, `make etl-backfill-2020-2025`, and the modular code walkthrough.

- Discovery: the repo does not currently contain a separate simple air-quality ETL implementation.
  Evidence: `.instructor-private/lecture-04-simple-etl-idea/` contains only the template file; no corresponding runtime code exists in `etl/`.

- Discovery: the current defaults and docs already expose station `19`, which adds multi-station and station-specific indicator complexity to Lecture 4.
  Evidence: `.env.example` defaults to `AIRVIRO_AIR_STATION_IDS=8,19`, and `v_air_quality_hourly` includes indicators documented as station-19-specific.

- Discovery: running the simple ETL as `python etl/lecture4_simple_air_quality.py ...` exposed an import-path issue when the script depended on package-style imports.
  Evidence: initial validation raised `ModuleNotFoundError: No module named 'etl'` until the script was made fully self-contained.

- Discovery: `psql` is not available in this devcontainer image, so warehouse verification is easier through Python + `psycopg2`.
  Evidence: validation attempt returned `/bin/bash: line 1: psql: command not found`.

- Discovery: plain short weekday and month labels were not enough to satisfy the intended Superset UX.
  Evidence: initial `month_short` / `weekday_short` implementation duplicated readable abbreviations instead of preserving the space-padded alphabetical-sort hack used in `superset/snippets.md`.

- Discovery: renaming a view column in place via `CREATE OR REPLACE VIEW` is not enough when inserting columns into the middle of an existing select list.
  Evidence: `make etl-bootstrap` initially failed with `psycopg2.errors.InvalidTableDefinition: cannot change name of view column "day_number" to "month_short"` until the view definition preserved column order and later used an explicit drop/create path for the `day_short` rename.

- Discovery: the advanced `run` path reapplies the Lecture 4 schema SQL on every non-dry execution.
  Evidence: `run_pipeline` in `etl/airviro/cli.py` calls `apply_schema(connection, schema_sql, settings)` before loading records, so the bootstrap file must stay non-destructive during normal reruns.

## Decision Log

- Decision: deliver a separate simple ETL script rather than simplifying the existing advanced package in place.
  Rationale: students need a small first example, but the advanced ETL still matters for Lecture 4 and Lecture 5. Keeping both paths makes the contrast explicit.
  Date: 2026-03-26

- Decision: keep the simple ETL path limited to air-quality station `8` only.
  Rationale: one source makes the first ETL walkthrough easier to grasp; pollen can be introduced later in the advanced section without overloading the opening tutorial.
  Date: 2026-03-26

- Decision: include `replace` and `update` load modes in the simple script.
  Rationale: this adds one meaningful operational choice for students without overwhelming them with advanced CLI abstractions.
  Date: 2026-03-26

- Decision: document the advanced CLI ETL as the second half of the lecture, not remove it.
  Rationale: students should see both the basic ETL pattern and the reasons for more advanced engineering design.
  Date: 2026-03-26

- Decision: recommend a separate simple target table/view rather than loading directly into the full advanced raw/audit path.
  Rationale: keeps the tutorial path easier to reason about and avoids destructive `replace` behavior interfering with the advanced ETL state.
  Date: 2026-03-26

- Decision: bake Superset-friendly sortable calendar labels into `l4_mart.dim_datetime_hour` instead of relying only on Superset calculated columns.
  Rationale: the teaching repo should let students build clean chronological weekday/month charts directly from warehouse fields, while still keeping the hack visible and discussable in the snippets reference.
  Date: 2026-03-26

- Decision: use `day_short` rather than `weekday_short`.
  Rationale: it matches the existing `day_name` / `month_name` naming pattern and keeps the dimension easier to explain.
  Date: 2026-03-26

- Decision: keep the Lecture 4 bootstrap SQL migration-free and prefer resetting `l4_raw` / `l4_mart` when the schema shape changes during prep.
  Rationale: the advanced ETL reapplies bootstrap SQL during normal runs, so the bootstrap file should stay simple, declarative, and safe for repeated use within one lecture cycle.
  Date: 2026-03-26

## Outcomes & Retrospective

Planned outcomes:
- a simple ETL script that students can read and run in one file;
- a Lecture 4 README that teaches ETL as a worked tutorial rather than only as a runbook;
- a clear comparison section showing what the advanced ETL does better;
- simple-path content simplified to air-quality station `8`, with advanced examples still allowed to add pollen.

Implemented:
- simple ETL script `etl/lecture4_simple_air_quality.py` for station `8` with `replace` and `update` load modes;
- separate simple table `l4_simple.air_quality_station_8_hourly`;
- separate advanced Lecture 4 schemas `l4_raw` and `l4_mart`;
- Lecture 4 advanced serving views labeled by station in relation names;
- `l4_mart.dim_datetime_hour` enriched with readable labels (`month_name`, `day_name`) and Superset-friendly sortable labels (`month_short`, `day_short`);
- Lecture 4 README rewritten as a step-by-step tutorial with an advanced ETL comparison section;
- `.env.example`, `Makefile`, `superset/snippets.md`, and instructor homework exports updated to match the new Lecture 4 scope.

Validation summary:
- `.venv/bin/python -m py_compile ...` passed for the changed ETL modules.
- simple ETL `replace` and `update` runs succeeded for `2025-05-01..2025-05-03`.
- advanced CLI ETL succeeded for station `8` and pollen station `25` over `2025-05-01..2025-05-03`.
- `make warehouse-status` reported `l4_raw` / `l4_mart` correctly.
- `make etl-bootstrap` succeeded after the datetime-dimension label updates and schema rename from `weekday_short` to `day_short`.
- Lecture 4 bootstrap SQL was simplified back to the current final schema shape with no migration-style `ALTER` / repair `UPDATE` logic.
- a one-time reset of `l4_raw` and `l4_mart` was performed successfully before rerunning `make etl-bootstrap`.
- direct warehouse checks confirmed padded values such as `[       May]` and `[   Thu]` in `l4_mart.dim_datetime_hour`.
- `dbt compile --select dim_datetime_hour v_air_quality_hourly` succeeded after aligning the parallel dbt models.
- after the clean reset, `warehouse-status` showed the rebuilt empty Lecture 4 schemas correctly.
- `superset_meta` remained preserved because only the `warehouse` database was used during validation.

Validation caveat:
- a fresh short advanced ETL run attempted after the clean reset failed with external Airviro HTTP 404 for `air_quality_station_8` on `2025-05-01..2025-05-03`, so the schema simplification was validated locally but not with a new successful source fetch in this final pass.

Deferred follow-up items:
- decide whether Superset exercises should start on `l4_simple.air_quality_station_8_hourly` or move immediately to `l4_mart.*`;
- decide how the later Lecture 5 warehouse separation should coexist with the new Lecture 4 `l4_*` schemas in docs and reset paths.

## Context and Orientation

Key files and directories:
- `.instructor-private/lecture-04-simple-etl-idea/etl_template.py`
- `docs/lectures/lecture-04/README.md`
- `docs/lectures/lecture-04/operations.md`
- `etl/lecture4_simple_air_quality.py`
- `etl/airviro/cli.py`
- `etl/airviro/pipeline.py`
- `etl/airviro/db.py`
- `sql/warehouse/l4_airviro_schema.sql`
- `sql/warehouse/airviro_schema.sql`
- `superset/snippets.md`
- `.env.example`
- `Makefile`

Current Lecture 4 flow:
- start Superset + Postgres;
- attach devcontainer to Compose network;
- run the advanced ETL bootstrap/backfill commands;
- inspect warehouse and build charts in Superset.

Recommended new instructional flow:
1. Simple ETL tutorial with one script and manual windows.
2. Short validation/check query or simple status step.
3. Advanced CLI ETL walkthrough explaining why the codebase is structured differently and how it extends beyond the single-station tutorial.
4. Superset analysis on the advanced marts, with explanation of where those marts and dimensions come from.

Recommended simple-script contract:
- script path: `etl/lecture4_simple_air_quality.py`;
- arguments:
  - `--from YYYY-MM-DD`
  - `--to YYYY-MM-DD`
  - `--load-mode replace|update`
- source scope: air-quality station `8`

Recommended simple-target design:
- use a separate tutorial table or small tutorial schema;
- keep the table shape easy to explain;
- avoid touching advanced audit tables and the full advanced raw-load path unless a deliberate bridge step is added later.

## Plan of Work

### Phase 1: Baseline and Design

- Review the template and map it to the Airviro API.
- Decide the simplest table design for the tutorial output.
- Decide whether the simple ETL output feeds Superset directly or remains a pedagogical stepping stone before the advanced path.
- Narrow the simple ETL tutorial to station `8`, and narrow advanced Lecture 4 examples to station `8` plus pollen.

### Phase 2: Implementation

- Create the simple ETL script with one readable file and explicit `extract()`, `transform()`, and `load()` functions.
- Implement manual window selection and `replace`/`update` load modes.
- Add any small bootstrap SQL needed for the simple ETL target table(s).
- Keep the advanced ETL package unchanged except where lecture defaults or docs need narrowing.
- Add any Make target(s) only if they improve copy-paste usability without hiding the instructional steps too early.

### Phase 3: Validation and Documentation

- Rewrite Lecture 4 README into a guided tutorial that explains each ETL stage and decision.
- Add an "advanced ETL" section that maps the simple script ideas to the existing modular package.
- Explain the advanced ingestion design patterns:
  - bounded windows;
  - retry and split-on-failure;
  - long-form raw storage;
  - idempotent upsert;
  - audit logging;
  - where dimensions and serving views come from.
- Verify that all commands are runnable from the devcontainer with copy-paste examples.

## Concrete Steps

- Run: `make up-superset`
  Expected: Postgres and Superset are available for Lecture 4 work.

- Run: `make devcontainer-join-course-network`
  Expected: ETL code can resolve `postgres` consistently from the devcontainer.

- Run: `.venv/bin/python <simple-script> --from 2025-05-01 --to 2025-05-07 --load-mode replace`
  Expected: one bounded tutorial load succeeds for station `8`.

- Run: `.venv/bin/python <simple-script> --from 2025-05-01 --to 2025-05-07 --load-mode update`
  Expected: a rerun is safe and demonstrates the difference between replace and update.

- Run: `.venv/bin/python -m etl.airviro.cli run --from 2025-05-01 --to 2025-05-31 --source-key air_quality_station_8 --source-key pollen_station_25 --verbose`
  Expected: advanced ETL path remains available as the comparison track.

## Validation and Acceptance

- A beginner can read the simple ETL script from top to bottom and understand the flow.
- The simple ETL supports manual date windows and both `replace` and `update` modes.
- Lecture 4 README becomes a guided tutorial, not only an operational overview.
- The advanced CLI ETL remains documented and explicitly contrasted with the simple path.
- The simple ETL tutorial uses only air-quality station `8`.
- The advanced Lecture 4 examples use only air-quality station `8` plus pollen station `25`.
- Docs explain where advanced dimensions come from and why the advanced design is more robust.

## Idempotence and Recovery

- `replace` mode must be safe to rerun and should fully replace the simple target table contents.
- `update` mode must be safe to rerun via deterministic keys/upsert logic.
- The simple ETL should be isolated enough that a failed tutorial run can be reset without damaging the advanced ETL state.
- Recovery instructions should include `make down`, `make up-superset`, and any tutorial-table cleanup needed.

## Artifacts and Notes

Likely artifacts:
- simple ETL script in `etl/`
- Lecture 4 advanced schema SQL in `sql/warehouse/l4_airviro_schema.sql`
- updated Lecture 4 tutorial in `docs/lectures/lecture-04/README.md`
- updated `docs/lectures/lecture-04/operations.md`
- updated `superset/snippets.md`

Questions to resolve during implementation:
- should the simple ETL land in a separate schema/table or a simplified parallel raw table in `raw`?
- should Lecture 4 default environment values change globally, or should docs override broader defaults for lecture delivery?
- should the simple ETL include a tiny warehouse-status helper, or should validation remain SQL/manual?

## Interfaces and Dependencies

- Depends on the existing devcontainer, Postgres, and Superset workflow from the repo root.
- Should use copy-paste-friendly commands that run inside the devcontainer.
- Must not require Airflow or dbt for the simple ETL path.
- Must coexist with the advanced `etl.airviro` package so Lecture 4 can show both the simple script and the advanced pipeline.
