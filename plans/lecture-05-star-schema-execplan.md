# Lecture 5 Star Schema Refactor

This ExecPlan is a living document. Update `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` as work advances.

Reference: `PLANS.md` (repository root) for standards.

## Purpose / Big Picture

Refactor the current dbt transformation layer from a small set of staging models and Superset-oriented views into a clearer dimensional model for Lecture 5, while evaluating whether the shared warehouse service can move from plain Postgres to `pg_duckdb` without breaking the teaching stack.

Today the repository already demonstrates ETL, orchestration, and dbt execution, but the marts are still closer to "presentation views over staging" than to a teachable star schema. This change would make Lecture 5 a better modeling lesson, reduce the chance of misleading Superset charts, and introduce a more explicit boundary between raw ETL, dbt modeling, and BI-facing datasets.

This plan is intentionally separate from the Lecture 4 simplification work. Recommendation: do **not** implement this refactor before Lecture 4 delivery. Revisit it immediately after Lecture 4 and before Lecture 5.

New direction after the packaging experiments:
- keep Lecture 4 intact and build Lecture 5 into separate `l5_*` schemas;
- reuse the lessons learned from the `ohuseire.ee` API work and keep the raw contract long-form;
- explore `pg_duckdb` as the shared database image only if the swap is low-friction and preserves the current local workflow;
- if `pg_duckdb` replacement is unsafe or too disruptive, keep plain Postgres and continue treating DuckDB as an optional analytics capability instead of the base engine.

## Student Learning Impact

- Affects Lecture 5 directly.
- Makes fact grain, conformed dimensions, and dbt model layering easier to teach.
- Reduces Superset mistakes caused by students ignoring `station_id` in mixed-station charts.
- Introduces classic dimensional concepts in a way that builds naturally on Lecture 4 ETL.
- Creates a concrete example of choosing a storage/query engine tradeoff without hiding operational risk from students.
- Gives a better place to explain DST, time dimensions, and why surrogate keys can be useful even when a timestamp already exists.

## Scope

In scope:
- Evaluate whether `pg_duckdb` can replace the current shared Postgres image with acceptable compatibility for Airflow metadata, Superset metadata, dbt, and warehouse workloads.
- Keep Lecture 5 work isolated in separate schemas such as `l5_raw` and `l5_mart`, following the Lecture 4 pattern.
- Refactor dbt marts into clearer `dimensions`, `facts`, and `presentation` layers.
- Add `dim_station`.
- Replace or supersede `dim_datetime_hour` with `dim_date` and `dim_time_hour`.
- Add a DST-safe hour surrogate strategy so repeated or skipped timestamps do not break daily ordering.
- Introduce fact models for hourly air quality and daily pollen.
- Preserve the long-form raw/staging contract so window stitching can work on `(timestamp, indicator, value, station)` without overlap assumptions across indicator arrays.
- Preserve or rebuild learner-friendly presentation views for Superset.
- Update Lecture 5 and dbt documentation.

Out of scope:
- Rewriting the Python ETL raw-load contract.
- Introducing a full semantic layer or dbt metrics layer.
- Pulling live station metadata automatically from a new metadata API other than the current `ohuseire.ee` extraction work.
- Changing Lecture 4 delivery before that lecture is taught.
- Forcing a destructive warehouse migration if `pg_duckdb` cannot safely reuse the current local setup.

## Progress

- [x] Investigate current state and constraints
- [x] Implement core changes
- [x] Update docs and examples
- [x] Run validation checks
- [x] Final review and cleanup

## Surprises & Discoveries

- Discovery: the original dbt layer was mostly `staging + marts`, without a clear fact layer.
  Evidence: the pre-refactor dbt project centered on a single staging measurement model plus presentation marts like `v_air_quality_hourly.sql` and `v_pollen_daily.sql`, but no fact models.

- Discovery: `station_id` is present in current marts, but students can still make misleading charts if they do not group or filter by station.
  Evidence: `dbt/models/marts/v_air_quality_hourly.sql` includes `station_id`, and indicators such as `hum`, `rain`, `press`, and `rad` are documented as station-19-specific in `dbt/models/marts/marts.yml`.

- Discovery: the current `dim_datetime_hour` is helpful operationally but less explicit than separate date and time dimensions for teaching a star schema.
  Evidence: `dbt/models/marts/dim_datetime_hour.sql` combines calendar and hour-of-day attributes into one table keyed by `observed_at`.

- Discovery: the repository already has the beginnings of lecture-specific schema isolation for ETL, but dbt still targets the shared `mart` schema by default.
  Evidence: `etl/airviro/config.py` defaults `AIRVIRO_RAW_SCHEMA` and `AIRVIRO_MART_SCHEMA` to `l4_raw` and `l4_mart`, while `dbt/profiles.yml` still points dbt to schema `mart`.

- Discovery: the current raw grain is already long-form enough to support timestamp-based stitching, but boundary-safe historical loads still need overlapping fetch windows.
  Evidence: `dbt/models/staging/ohuseire/stg_ohuseire_measurement.sql` and `dbt/models/marts/presentation/v_ohuseire_measurements_long.sql` expose `source_type`, `station_id`, `observed_at`, `indicator_code`, and `value_numeric` as one row per measurement, while later ETL review showed per-window trimming alone can still miss the leading edge of the next request.

- Discovery: replacing the shared `postgres` service is a cross-cutting infrastructure change, not just a warehouse tweak.
  Evidence: `docker-compose.yml` uses the same `postgres` service for Superset metadata, Airflow metadata, and the warehouse database, and `postgres/init/01-create-app-databases.sh` bootstraps all three databases and roles there.

- Discovery: the official `pg_duckdb` project now ships a Docker image and documents PostgreSQL 14-18 support, but its docs also highlight filesystem and execution constraints that matter for a teaching stack.
  Evidence: the `pg_duckdb` README documents `docker run ... pgduckdb/pgduckdb:18-v1.1.1`, lists PostgreSQL 14-18 as supported, and the settings docs note that local filesystem access is disabled for non-superusers unless they have `pg_read_server_files` and `pg_write_server_files`; `duckdb.unsafe_allow_execution_inside_functions` is disabled by default.

- Discovery: the next ETL iteration should assume individual indicator timestamps are trustworthy while treating cross-indicator positional alignment as untrustworthy.
  Evidence: the current long-form measurement table keeps `observed_at` per measurement, and the planned experiment direction is to stitch windows on `(timestamp, indicator, station)` while accepting incomplete leading/trailing edges per window.

- Discovery: DST handling needs to move from a pure timestamp key to a teaching-friendly surrogate strategy that preserves both ordering and original timestamps.
  Evidence: the current `dim_datetime_hour` keys rows by `observed_at`; the new requirement is to keep correct measurements even when timestamps repeat or skip, while still giving each complete daily indicator series a stable 24-step order when the source delivers one.

- Discovery: a single shared Python environment for Airflow and dbt remains brittle even on Airflow 3.1.8 because of dependency conflicts, but a single teaching image with separate runtimes is still practical.
  Evidence: the packaging experiment reproduced shared-environment conflicts, while the local course image now successfully runs Airflow from `/opt/airflow-venv` and dbt from `/opt/dbt-venv`.

- Discovery: `pg_duckdb` works well as the shared database image for this repo, but existing Postgres volumes may require a preload-setting bootstrap and restart cycle.
  Evidence: fresh containers worked directly, while reused data directories needed `shared_preload_libraries=pg_duckdb`, a restart, extension creation, and a final restart for `duckdb.postgres_role`.

- Discovery: migrating an existing local data directory onto the current `pgduckdb` image can surface collation-version warnings without breaking the course workflows.
  Evidence: the reused warehouse volume emitted collation mismatch warnings after the image swap, while Airflow metadata, dbt builds, and DuckDB file queries still worked.

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

- Decision: keep Lecture 5 isolated in new schemas such as `l5_raw` and `l5_mart` instead of reusing or rewriting the Lecture 4 schemas.
  Rationale: this preserves the delivered Lecture 4 workflow, reduces migration risk, and keeps the dimensional-model lesson easy to compare against the earlier design.
  Date: 2026-03-27

- Decision: keep the Airviro raw contract long-form and perform window stitching by measurement keys, not by indicator-array index position.
  Rationale: this is more robust to partial windows, indicator-specific gaps, and index shifts between retrievals.
  Date: 2026-03-27

- Decision: preserve the original API timestamp in facts, but add a separate DST-safe hour surrogate/ordering concept for Lecture 5.
  Rationale: students need both the real timestamp and a stable analytic hour key when local time repeats or skips around DST.
  Date: 2026-03-27

- Decision: attempt `pg_duckdb` only behind a compatibility-first lens; if it cannot keep the current local database workflow reasonably intact, fall back to plain Postgres.
  Rationale: the course benefits from DuckDB capability, but not enough to justify breaking the stack used by Airflow, dbt, and Superset.
  Date: 2026-03-27

- Decision: make the leaner local `uv`-built Airflow and Superset images the default teaching runtime, while keeping dbt isolated in its own virtual environment inside the Airflow image.
  Rationale: the local images materially shrink build outputs and stay instructional as long as the Dockerfiles and lockfiles remain simple, pinned, and well documented.
  Date: 2026-03-27

- Decision: make `pgduckdb/pgduckdb:16-v1.1.1` the default shared database image for the local stack, with an explicit recovery path for reused volumes.
  Rationale: it preserves PostgreSQL compatibility for Airflow and Superset, adds DuckDB-backed file access, and keeps the operational story teachable as long as the preload bootstrap is documented.
  Date: 2026-03-27

- Decision: keep Airflow and dbt in one teaching image, but isolate dbt in `/opt/dbt-venv`.
  Rationale: one image keeps the student workflow simple, while the separate virtual environment avoids the dependency conflict found in the packaging experiment.
  Date: 2026-03-27

- Decision: keep Lecture 4 raw tables on the legacy `airviro_*` names, but give Lecture 5 explicit `ohuseire_*` raw table names behind simple ETL configuration.
  Rationale: this finishes the Lecture 5 naming cleanup without breaking Lecture 4 CLI and warehouse flows.
  Date: 2026-03-28

## Outcomes & Retrospective

Delivered outcomes:
- a clear layered dbt project for Lecture 5 with `staging`, `intermediate`, `marts/dimensions`, `marts/facts`, and `marts/presentation`;
- isolated Lecture 5 schemas `l5_raw` and `l5_mart`, leaving Lecture 4 intact in `l4_*`;
- a seeded `dim_station` and a split `dim_date` / `dim_time_hour` design;
- long-form facts with Superset-friendly presentation views on top;
- ETL behavior that keeps original timestamps, fetches each logical window with one day of overlap on both sides, and trims back to the requested window instead of shifting timestamps by indicator index;
- a documented `pg_duckdb` default database path with shared file access and a recoverable bootstrap sequence for reused volumes;
- Lecture 5 raw tables renamed to `l5_raw.ohuseire_measurement` and `l5_raw.ohuseire_ingestion_audit`, while Lecture 4 stayed on `airviro_*`;
- a targeted `make reset-l5` recovery path that rebuilds only the Lecture 5 schemas.

Validation snapshot:
- direct ETL load to `l5_raw` succeeded for course stations;
- `make dbt-build` completed successfully with 15 models built and 73 tests passing;
- `pg_duckdb` file access worked via `read_csv('/warehouse-files/sample.csv')` as the `warehouse` role after bootstrap;
- the Airflow runtime is now a local `uv`-built image on `debian:bookworm-slim`.

Residual caveats:
- reused Postgres volumes may show collation-version warnings after the image swap;
- the local `uv` image adds lockfile maintenance work, so version bumps should stay deliberate and documented;
- station metadata is intentionally seed-driven for reproducibility, not automatically synced from a live metadata API.

## Context and Orientation

Key files and directories:
- `dbt/models/staging/ohuseire/stg_ohuseire_measurement.sql`
- `dbt/models/intermediate/ohuseire/int_air_quality_measurement.sql`
- `dbt/models/intermediate/ohuseire/int_air_quality_hourly_wide.sql`
- `dbt/models/intermediate/ohuseire/int_pollen_daily.sql`
- `dbt/models/marts/dimensions/dim_station.sql`
- `dbt/models/marts/dimensions/dim_date.sql`
- `dbt/models/marts/dimensions/dim_time_hour.sql`
- `dbt/models/marts/facts/fct_air_quality_hourly.sql`
- `dbt/models/marts/facts/fct_pollen_daily.sql`
- `dbt/models/marts/presentation/v_air_quality_hourly.sql`
- `dbt/models/marts/presentation/v_pollen_daily.sql`
- `dbt/models/marts/presentation/v_ohuseire_measurements_long.sql`
- `dbt/README.md`
- `docs/lectures/lecture-05/README.md`
- `docs/lectures/lecture-05/operations.md`
- `airflow/dags/ohuseire_incremental.py`
- `airflow/dags/ohuseire_backfill.py`
- `sql/warehouse/l5_ohuseire_schema.sql`
- `postgres/init/02-enable-pgduckdb.sh`

Current model state:
- raw facts land in `l5_raw.ohuseire_measurement`;
- dbt staging is a direct canonical view over the Lecture 5 raw schema;
- intermediate models prepare long-form measurement data for facts and presentation views;
- facts live in `l5_mart.fct_air_quality_hourly` and `l5_mart.fct_pollen_daily`;
- presentation views remain available for Superset and beginner-friendly querying.

Implemented model shape:
- `models/staging/ohuseire/`
  - `stg_ohuseire_measurement`
- `models/intermediate/ohuseire/`
  - `int_air_quality_measurement`
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
  - `v_air_quality_hourly`
  - `v_pollen_daily`
  - `v_ohuseire_measurements_long`
  - `v_airviro_measurements_long` (compatibility alias)

Assumptions:
- Air quality fact grain: one row per `station x observed_date x hour_key x indicator`.
- Pollen fact grain: one row per `station x observed_date x indicator`.
- `dim_station` may start from a seed/manual metadata file.
- Lecture 5 schemas should be independent from Lecture 4 schemas.
- The `ohuseire.ee` API remains the source system, but the ETL should trust per-measurement timestamps more than cross-indicator list positions.
- If DuckDB-backed file access is enabled later, shared storage should be explicit and teachable, likely via a bind-mounted project directory rather than hidden container-local paths.

## Plan of Work

### Phase 1: Baseline and Design

- Inventory all current dbt models, tests, and references from docs and Airflow.
- Evaluate `pg_duckdb` replacement risk for the current shared database service and identify the lowest-risk adoption path.
- Define exact fact grains and dimension contracts.
- Define the Lecture 5 schema names and how ETL/dbt select them.
- Decide naming and compatibility strategy for current view names.
- Define the minimal station metadata required for `dim_station`.
- Define the long-form window-stitching contract for Ohuseire retrievals, including fetch overlap and trim rules.
- Define DST handling rules for the new hour dimension surrogate and fact ordering.

### Phase 2: Implementation

- Parameterize ETL/dbt for Lecture 5 schemas without disturbing Lecture 4 defaults.
- Create new dbt directory layout for `intermediate`, `dimensions`, `facts`, and `presentation`.
- Add `dim_date`, `dim_time_hour`, and `dim_station`.
- Refactor `dim_indicator` and `dim_wind_direction` into the new dimensional layer.
- Build `fct_air_quality_hourly` and `fct_pollen_daily`.
- Add the long-format intermediate models needed for window stitching and DST-safe ordering.
- Rebuild Superset-friendly views on top of facts and dimensions.
- Expand dbt tests to cover keys, relationships, and expected not-null boundaries.
- If feasible, integrate `pg_duckdb` into the Compose stack while preserving current databases and validating local file-access behavior deliberately.

### Phase 3: Validation and Documentation

- Run `dbt build` and confirm all models/tests pass.
- Validate example queries that require explicit station grouping/filtering.
- Validate daily air-quality sequences for ordering, expected counts, and DST edge cases.
- Validate any `pg_duckdb` change against Airflow metadata, Superset metadata, warehouse connectivity, and existing local data survival expectations.
- Update Lecture 5 docs and `dbt/README.md` with modeling rationale and model map.
- Add notes that explain why station context is mandatory for trustworthy plots.
- Document when DuckDB capability is part of the teaching story and when plain Postgres behavior still matters.

## Concrete Steps

- Run: `make dbt-build`
  Expected: current baseline passes before refactor work starts.

- Run: `find dbt/models -maxdepth 3 -type f | sort`
  Expected: current model inventory is captured for migration planning.

- Run: `rg -n "station_id|hum|rain|press|rad" dbt/models docs`
  Expected: all current station-sensitive modeling and doc touchpoints are identified.

- Run: `sed -n '1,260p' docker-compose.yml`
  Expected: current database coupling and migration blast radius are explicit before any `pg_duckdb` change.

- Run: `sed -n '1,220p' etl/airviro/config.py`
  Expected: Lecture 5 schema parameterization can be planned from the existing ETL config hooks.

- Run: `make airflow-trigger-incremental`
  Expected: after the refactor, Airflow-triggered dbt still works against the new model layout.

## Validation and Acceptance

- dbt models form a clear star-schema-oriented layout.
- `dim_station` exists and is used consistently by facts and presentation views.
- `dim_date` and `dim_time_hour` replace the single combined datetime dimension, or the old datetime dimension is clearly downgraded to a compatibility layer.
- Lecture 5 data lands in its own schemas without breaking Lecture 4.
- Long-form stitching does not rely on indicator index alignment and uses overlapping fetch windows so adjacent historical windows can recover boundary rows before deduplication.
- The chosen hour surrogate strategy preserves measurement ordering across DST edge cases while keeping original timestamps available.
- Superset-facing models expose station context in a way that discourages accidental cross-station aggregation.
- Lecture 5 docs explain fact grain, dimension origin, and why the new structure exists.
- If `pg_duckdb` is adopted, the stack still boots cleanly and the current local database workflow remains understandable and recoverable.

## Idempotence and Recovery

- Keep current view names available as compatibility views during transition.
- Use dbt views/tables in a way that allows repeated `dbt build` without manual cleanup.
- If the refactor breaks lesson flow, roll back by restoring current mart definitions and rerunning `make dbt-build`.
- If state becomes confusing locally, prefer `make reset-l5`, reload Lecture 5 data, then rerun dbt.
- If `pg_duckdb` replacement proves unsafe, revert the database image change before continuing with the Lecture 5 model refactor.

## Artifacts and Notes

To prepare before implementation:
- capture current dbt lineage;
- capture example Superset chart patterns that currently require manual station filtering;
- decide whether `dim_station` metadata will live in a seed CSV or SQL model.

## Interfaces and Dependencies

- Upstream contract: Lecture 4 keeps the legacy raw tables, while Lecture 5 uses `l5_raw.ohuseire_measurement` as its raw source table.
- Preferred Lecture 5 contract: raw data remains long-form and schema-selectable (`l5_raw` / `l5_mart`).
- dbt continues to run inside `airflow-scheduler` via existing Make targets.
- Airflow DAGs should continue calling `dbt seed`, `dbt run`, and `dbt test` without changing the orchestration contract.
- Presentation-layer relations must remain easy to connect in Superset.
- Database engine direction under evaluation: replace `postgres:16` with a compatible `pg_duckdb` image only if the shared metadata + warehouse responsibilities remain stable enough for the course.
