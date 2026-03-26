# Lecture 5 Star Schema Refactor

This ExecPlan is a living document. Update `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` as work advances.

Reference: `PLANS.md` (repository root) for standards.

## Purpose / Big Picture

Refactor the current dbt transformation layer from a small set of staging models and Superset-oriented views into a clearer dimensional model for Lecture 5.

Today the repository already demonstrates ETL, orchestration, and dbt execution, but the marts are still closer to "presentation views over staging" than to a teachable star schema. This change would make Lecture 5 a better modeling lesson, reduce the chance of misleading Superset charts, and introduce a more explicit boundary between raw ETL, dbt modeling, and BI-facing datasets.

This plan is intentionally separate from the Lecture 4 simplification work. Recommendation: do **not** implement this refactor before Lecture 4 delivery. Revisit it immediately after Lecture 4 and before Lecture 5.

## Student Learning Impact

- Affects Lecture 5 directly.
- Makes fact grain, conformed dimensions, and dbt model layering easier to teach.
- Reduces Superset mistakes caused by students ignoring `station_id` in mixed-station charts.
- Introduces classic dimensional concepts in a way that builds naturally on Lecture 4 ETL.

## Scope

In scope:
- Refactor dbt marts into clearer `dimensions`, `facts`, and `presentation` layers.
- Add `dim_station`.
- Replace or supersede `dim_datetime_hour` with `dim_date` and `dim_time_hour`.
- Introduce fact models for hourly air quality and daily pollen.
- Preserve or rebuild learner-friendly presentation views for Superset.
- Update Lecture 5 and dbt documentation.

Out of scope:
- Rewriting the Python ETL raw-load contract.
- Introducing a full semantic layer or dbt metrics layer.
- Pulling live station metadata automatically from an external API.
- Changing Lecture 4 delivery before that lecture is taught.

## Progress

- [x] Investigate current state and constraints
- [ ] Implement core changes
- [ ] Update docs and examples
- [ ] Run validation checks
- [ ] Final review and cleanup

## Surprises & Discoveries

- Discovery: the current dbt layer is still mostly `staging + marts`, without a clear fact layer.
  Evidence: `dbt/models/` currently contains `staging/stg_airviro_measurement.sql` plus marts like `v_air_quality_hourly.sql` and `v_pollen_daily.sql`, but no fact models.

- Discovery: `station_id` is present in current marts, but students can still make misleading charts if they do not group or filter by station.
  Evidence: `dbt/models/marts/v_air_quality_hourly.sql` includes `station_id`, and indicators such as `hum`, `rain`, `press`, and `rad` are documented as station-19-specific in `dbt/models/marts/marts.yml`.

- Discovery: the current `dim_datetime_hour` is helpful operationally but less explicit than separate date and time dimensions for teaching a star schema.
  Evidence: `dbt/models/marts/dim_datetime_hour.sql` combines calendar and hour-of-day attributes into one table keyed by `observed_at`.

## Decision Log

- Decision: schedule this refactor **after Lecture 4** and before Lecture 5, not before Lecture 4.
  Rationale: it is directly relevant to Lecture 5, while implementing it early would create unnecessary churn in Lecture 4 materials and student workflows.
  Date: 2026-03-26

- Decision: introduce `dim_station` first-class rather than relying on `station_id` as a naked fact column.
  Rationale: mixed-station facts are easy to visualize incorrectly in Superset, especially when some indicators exist at only one station.
  Date: 2026-03-26

- Decision: keep compatibility presentation views on top of the refactored facts/dims.
  Rationale: preserves a low-friction Superset experience and gives instructors an easier migration path.
  Date: 2026-03-26

- Decision: use seed- or repo-defined station metadata first, not a live metadata API dependency.
  Rationale: keeps the lecture reproducible and avoids adding fragile external dependencies to the dimensional-model lesson.
  Date: 2026-03-26

## Outcomes & Retrospective

Planned outcomes:
- a clear dimensional dbt layer for Lecture 5;
- safer BI-facing models with explicit station context;
- docs that explain fact grain and dimension origin clearly.

Deferred until implementation:
- exact surrogate-key strategy;
- whether old mart names remain as compatibility views or are retired fully;
- whether to introduce station-availability helper tables for indicator coverage.

## Context and Orientation

Key files and directories:
- `dbt/models/staging/stg_airviro_measurement.sql`
- `dbt/models/marts/dim_datetime_hour.sql`
- `dbt/models/marts/dim_indicator.sql`
- `dbt/models/marts/dim_wind_direction.sql`
- `dbt/models/marts/v_air_quality_hourly.sql`
- `dbt/models/marts/v_pollen_daily.sql`
- `dbt/models/marts/v_airviro_measurements_long.sql`
- `dbt/models/marts/marts.yml`
- `dbt/README.md`
- `docs/lectures/lecture-05/README.md`
- `docs/lectures/lecture-05/operations.md`
- `airflow/dags/airviro_incremental.py`
- `airflow/dags/airviro_backfill.py`

Current model state:
- raw facts land in `raw.airviro_measurement`;
- dbt staging is a direct canonical view over raw;
- marts are a mixture of lookup dimensions and wide serving views.

Recommended target model shape:
- `models/staging/`
  - `stg_airviro_measurement`
- `models/intermediate/`
  - `int_air_quality_hourly_long`
  - `int_air_quality_hourly_wide`
  - `int_pollen_daily`
- `models/marts/dimensions/`
  - `dim_date`
  - `dim_time_hour`
  - `dim_station`
  - `dim_indicator`
  - `dim_wind_direction`
- `models/marts/facts/`
  - `fct_air_quality_hourly`
  - `fct_pollen_daily`
- `models/marts/presentation/`
  - `v_air_quality_hourly_enriched`
  - `v_pollen_daily_enriched`
  - optional backward-compatible views matching current names

Assumptions:
- Air quality fact grain: one row per `station x observed_at hour`.
- Pollen fact grain: one row per `station x observed_date x indicator`.
- `dim_station` may start from a seed/manual metadata file.

## Plan of Work

### Phase 1: Baseline and Design

- Inventory all current dbt models, tests, and references from docs and Airflow.
- Define exact fact grains and dimension contracts.
- Decide naming and compatibility strategy for current view names.
- Define the minimal station metadata required for `dim_station`.

### Phase 2: Implementation

- Create new dbt directory layout for `intermediate`, `dimensions`, `facts`, and `presentation`.
- Add `dim_date`, `dim_time_hour`, and `dim_station`.
- Refactor `dim_indicator` and `dim_wind_direction` into the new dimensional layer.
- Build `fct_air_quality_hourly` and `fct_pollen_daily`.
- Rebuild Superset-friendly views on top of facts and dimensions.
- Expand dbt tests to cover keys, relationships, and expected not-null boundaries.

### Phase 3: Validation and Documentation

- Run `dbt build` and confirm all models/tests pass.
- Validate example queries that require explicit station grouping/filtering.
- Update Lecture 5 docs and `dbt/README.md` with modeling rationale and model map.
- Add notes that explain why station context is mandatory for trustworthy plots.

## Concrete Steps

- Run: `make dbt-build`
  Expected: current baseline passes before refactor work starts.

- Run: `find dbt/models -maxdepth 3 -type f | sort`
  Expected: current model inventory is captured for migration planning.

- Run: `rg -n "station_id|hum|rain|press|rad" dbt/models docs`
  Expected: all current station-sensitive modeling and doc touchpoints are identified.

- Run: `make airflow-trigger-incremental`
  Expected: after the refactor, Airflow-triggered dbt still works against the new model layout.

## Validation and Acceptance

- dbt models form a clear star-schema-oriented layout.
- `dim_station` exists and is used consistently by facts and presentation views.
- `dim_date` and `dim_time_hour` replace the single combined datetime dimension, or the old datetime dimension is clearly downgraded to a compatibility layer.
- Superset-facing models expose station context in a way that discourages accidental cross-station aggregation.
- Lecture 5 docs explain fact grain, dimension origin, and why the new structure exists.

## Idempotence and Recovery

- Keep current view names available as compatibility views during transition.
- Use dbt views/tables in a way that allows repeated `dbt build` without manual cleanup.
- If the refactor breaks lesson flow, roll back by restoring current mart definitions and rerunning `make dbt-build`.
- If state becomes confusing locally, use `make reset-volumes`, reload ETL data, then rerun dbt.

## Artifacts and Notes

To prepare before implementation:
- capture current dbt lineage;
- capture example Superset chart patterns that currently require manual station filtering;
- decide whether `dim_station` metadata will live in a seed CSV or SQL model.

## Interfaces and Dependencies

- Upstream contract: `raw.airviro_measurement` remains the raw source table.
- dbt continues to run inside `airflow-scheduler` via existing Make targets.
- Airflow DAGs should continue calling `dbt seed`, `dbt run`, and `dbt test` without changing the orchestration contract.
- Presentation-layer relations must remain easy to connect in Superset.
