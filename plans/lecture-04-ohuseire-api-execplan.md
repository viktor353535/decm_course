# Lecture 4 Ohuseire API Migration

This ExecPlan is a living document. Update `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` as work advances.

Reference: `PLANS.md` (repository root) for standards.

## Purpose / Big Picture

Lecture 4 currently teaches against the old `airviro.klab.ee/station/csv` endpoint, but that route now redirects and ends in `404 Not Found`.

The new public site at `www.ohuseire.ee` still exposes machine-consumable data, but through JSON endpoints such as:
- `/api/station/{locale}`
- `/api/indicator/{locale}`
- `/api/monitoring/{locale}`

This change updates the Lecture 4 teaching path end to end so both the simple ETL script and the advanced CLI ETL use the live JSON API and continue to load:
- Tartu air-quality data
- Tartu pollen data

The lecture should also become slightly more API-literate: students should see that modern APIs often provide metadata endpoints, structured JSON payloads, and discoverable identifiers instead of one hard-coded CSV download URL.

## Student Learning Impact

- Affects Lecture 4 directly.
- Keeps the lecture runnable despite the old CSV endpoint being gone.
- Adds a realistic example of API discovery through metadata endpoints.
- Lets students compare a simple one-file ETL and an advanced CLI ETL against the same live API.
- Introduces a few gentle API best-practice ideas without turning Lecture 4 into an API-design lecture.

## Scope

In scope:
- Switch Lecture 4 ETL examples from the dead CSV route to the live `ohuseire.ee` JSON API.
- Keep Lecture 4 source scope to:
  - Tartu air quality (`station_id=8`)
  - Tartu pollen (`station_id=25`)
- Use station and indicator metadata endpoints so those sources are discoverable from the API.
- Update the simple ETL script to call the new API.
- Update the advanced Lecture 4 ETL pipeline to call the new API.
- Handle the historical timestamp-staggering quirk observed in older `monitoring` responses.
- Update Lecture 4 docs, examples, and `.env.example`.
- Keep Lecture 5 out of scope for now.

Out of scope:
- Repairing Lecture 5 dbt / Airflow behavior against the new API.
- Refactoring the ETL package name away from `airviro`.
- Introducing full API client abstractions or external SDK dependencies.

## Progress

- [x] Investigate current state and constraints
- [x] Implement core changes
- [x] Update docs and examples
- [x] Run validation checks
- [x] Final review and cleanup

## Surprises & Discoveries

- Discovery: the old CSV endpoint is no longer a usable source.
  Evidence: `https://airviro.klab.ee/station/csv?...` redirects to `https://www.ohuseire.ee/station/csv?...`, which returns `404`.

- Discovery: the new site still exposes structured data endpoints.
  Evidence: the frontend bundle calls `/api/station/{locale}`, `/api/indicator/{locale}`, `/api/timeseries/{locale}`, and `/api/monitoring/{locale}`.

- Discovery: the new browser CSV download is generated client-side from monitoring JSON rather than fetched from a dedicated CSV export endpoint.
  Evidence: the frontend bundle builds CSV rows from `getMonitorings()` after calling `loadMonitorings`.

- Discovery: older historical `monitoring` responses can return staggered timestamps by indicator order instead of fully aligned timestamps.
  Evidence: Tartu air-quality and Tartu pollen requests for `2025-05-01..2025-05-03` returned rows where the first indicator appeared at `01:00`, the next at `02:00`, and so on, while recent March 2026 windows returned properly aligned timestamps.

- Discovery: the station metadata endpoint is sufficient to discover the current Tartu lecture sources.
  Evidence:
  - station `8` -> `name=Tartu`, `type=URBAN`, `indicators=[21,23,4,3,1,6,37,41,66]`
  - station `25` -> `name=Tartu`, `type=POLLEN`, `indicators=[48,51,59,49,57,47,62,44]`

- Discovery: source-key filtering must happen before source-config expansion when the local `.env` still contains Lecture 5 stations.
  Evidence: a dry run failed with `Air-quality station 19 was not found in station API` until `get_source_configs()` was changed to build only the explicitly requested Lecture 4 source keys.

## Decision Log

- Decision: keep Lecture 4 focused on Tartu air-quality station `8` and Tartu pollen station `25`.
  Rationale: this matches the current lecture scope and keeps station modeling complexity out of Lecture 4.
  Date: 2026-03-26

- Decision: use the JSON metadata endpoints as part of the ETL implementation, not only as documentation references.
  Rationale: this makes the lecture more honest and more robust because indicator IDs and station descriptions come from the source API itself.
  Date: 2026-03-26

- Decision: normalize the historical staggered timestamp responses during ETL rather than changing Lecture 4 to recent-only data windows.
  Rationale: the lecture should remain able to work with both recent and older windows when the API provides data, and the normalization can be explained as data-quality handling.
  Date: 2026-03-26

- Decision: keep the current Lecture 4 warehouse schemas (`l4_simple`, `l4_raw`, `l4_mart`) and view names.
  Rationale: this migration should fix the source layer without forcing a separate warehouse redesign right before teaching.
  Date: 2026-03-26

## Outcomes & Retrospective

Planned outcomes:
- Lecture 4 ETL paths work again against the live source API.
- Tartu air-quality and pollen data are discoverable from API metadata.
- The simple ETL remains beginner-friendly.
- The advanced ETL remains the robust comparison path.
- Lecture 4 docs clearly explain the new API shape and a few practical API-handling ideas.

Implemented:
- `etl/lecture4_simple_air_quality.py` was rewritten to:
  - discover station `8` and its indicators from the live API;
  - fetch monitoring JSON instead of CSV;
  - normalize older staggered historical timestamps;
  - pivot long-form API rows into the tutorial table `l4_simple.air_quality_station_8_hourly`.

- `etl/airviro/config.py` now defaults to:
  - `AIRVIRO_BASE_URL=https://www.ohuseire.ee/api`
  - `AIRVIRO_API_LOCALE=en`

- `etl/airviro/pipeline.py` was updated to:
  - fetch station and indicator metadata from the API;
  - build Lecture 4 source configs from live metadata;
  - fetch monitoring JSON windows;
  - normalize older staggered historical timestamps before loading raw measurements.

- `etl/airviro/cli.py` now applies `--source-key` filtering before source-config expansion so leftover Lecture 5 station IDs in `.env` do not break Lecture 4 runs.

- Lecture 4 docs were updated in:
  - `docs/lectures/lecture-04/README.md`
  - `docs/lectures/lecture-04/operations.md`
  - `README.md`
  - `.env.example`
  - `Makefile`

- The homework draft at `docs/lectures/lecture-04/homework/homework_3.md` was updated to use the validated March 2026 demo window.

Validation summary:
- `py_compile` passed for:
  - `etl/airviro/config.py`
  - `etl/airviro/pipeline.py`
  - `etl/airviro/cli.py`
  - `etl/lecture4_simple_air_quality.py`

- Simple ETL live runs succeeded:
  - `2026-03-10..2026-03-12` with `replace`
  - `2025-05-01..2025-05-03` with `update`

- Advanced ETL dry runs succeeded:
  - `2026-03-10..2026-03-12`
  - `2025-05-01..2025-05-03`

- Advanced ETL live run succeeded:
  - `2026-03-10..2026-03-12` for `air_quality_station_8` and `pollen_station_25`
  - `2025-05-01..2025-05-03` for `air_quality_station_8` and `pollen_station_25`

- Clean warehouse validation succeeded:
  - dropped `l4_raw` and `l4_mart`
  - reran `make etl-bootstrap`
  - reran live advanced ETL loads successfully

- Warehouse checks after validation confirmed:
  - `l4_simple.air_quality_station_8_hourly` = 72 rows for `2026-03-10..2026-03-12`
  - `l4_mart.v_air_quality_hourly_station_8` = 72 rows for `2026-03-10..2026-03-12`
  - `l4_mart.v_pollen_daily_station_25` = 24 rows for `2026-03-10..2026-03-12`
  - `l4_mart.v_airviro_measurements_long` = 672 rows for the final docs-aligned March 2026 warehouse state
  - historical May 2025 ranges now load with clean hourly/daily bounds after timestamp normalization

Deferred follow-up items:
- Lecture 5 dbt / Airflow migration to the new API.
- Any wider station expansion or `dim_station` introduction.

## Context and Orientation

Key files and directories:
- `etl/lecture4_simple_air_quality.py`
- `etl/airviro/config.py`
- `etl/airviro/pipeline.py`
- `etl/airviro/cli.py`
- `etl/airviro/db.py`
- `docs/lectures/lecture-04/README.md`
- `docs/lectures/lecture-04/operations.md`
- `.env.example`
- `Makefile`

Current source behavior:
- old CSV route: broken
- new monitoring route: live JSON
- metadata routes: live JSON

## Plan of Work

### Phase 1: Baseline and Design

- Document the new API endpoints and the lecture scope.
- Decide how station and indicator metadata should flow into the ETL.
- Define a safe normalization rule for staggered historical timestamps.

### Phase 2: Implementation

- Update the simple ETL to use JSON monitoring data for Tartu air quality.
- Update the advanced ETL package to use station/indicator/monitoring JSON endpoints.
- Keep warehouse structures stable while changing only extraction and transformation logic.

### Phase 3: Validation and Documentation

- Update Lecture 4 tutorial text and commands.
- Add short API best-practices notes for students.
- Validate both simple and advanced ETL paths with live Tartu data.

## Concrete Steps

- Run a simple ETL window for Tartu air quality.
- Run an advanced ETL window for Tartu air quality and Tartu pollen.
- Confirm the resulting warehouse relations populate correctly.
- Update examples and notes to match the tested date windows and endpoints.

## Validation and Acceptance

- Simple ETL loads `l4_simple.air_quality_station_8_hourly` from the new API.
- Advanced ETL loads `l4_raw` / `l4_mart` from the new API.
- Tartu air-quality and pollen source IDs are traceable to API metadata.
- Lecture 4 docs no longer point students at the dead CSV route.
- Lecture 4 examples use date windows that are validated against the live API.

## Idempotence and Recovery

- The simple ETL must keep `replace` and `update` semantics.
- The advanced ETL must remain safe to rerun with upserts.
- If the Lecture 4 schema shape changes during prep, `l4_raw` and `l4_mart` can still be reset and bootstrapped again.

## Artifacts and Notes

Likely artifacts:
- updated Lecture 4 simple ETL script
- updated advanced ETL extraction logic
- refreshed lecture docs and examples
