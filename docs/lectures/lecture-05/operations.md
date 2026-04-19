# Lecture 5 Operations Guide

Start with the lecture overview first:
- [Lecture 5 Overview](./README.md)

This page is the follow-up guide for commands, DAG behavior, data contracts, and recovery steps.

## Terms Used On This Page

- ETL:
  Extract, Transform, Load. In this lecture, Python handles the source extraction and raw load steps.
- DAG:
  Directed Acyclic Graph. In Airflow, a DAG is one workflow.
- Watermark:
  The stored progress marker for a source.
- Incremental load:
  A run that loads only the next needed slice of data instead of all history.
- Backfill:
  An intentional load of older history.
- Schema:
  A named area inside the database.
- Warehouse:
  The database area where we keep both raw data and analysis-ready data.
- Mart:
  The cleaned, analysis-ready part of the warehouse.
- Seed:
  A small comma-separated values (CSV) file that dbt loads into the warehouse.
- Model:
  In dbt, a SQL file that builds a table or a view.
- Daylight saving time (DST):
  The clock change in spring and autumn that can create skipped or repeated local hours.

## System Components

- Source API:
  [Ohuseire API Reference](../../reference/ohuseire-api.md)
- Python ETL:
  `etl/airviro/`
- Airflow DAGs:
  `airflow/dags/`
- dbt project:
  `dbt/`
- Lecture 5 raw schema:
  `l5_raw`
- Lecture 5 mart schema:
  `l5_mart`

## Quick Start Commands

Start the Lecture 5 stack:

```bash
make up-airflow
```

Bootstrap the raw layer and inspect warehouse status:

```bash
make etl-bootstrap-l5
make warehouse-status-l5
```

Run dbt manually:

```bash
make dbt-debug
make dbt-build
```

List DAGs and DAG runs:

```bash
make airflow-list-dags
make airflow-list-runs DAG_ID=ohuseire_incremental
```

Trigger incremental:

```bash
make airflow-trigger-incremental
```

Trigger backfill:

```bash
make airflow-trigger-backfill BACKFILL_START=2020-01-01 BACKFILL_END=2025-12-31 BACKFILL_CHUNK_DAYS=31 BACKFILL_SOURCE_KEYS=air_quality_station_4,air_quality_station_8,pollen_station_25
```

Trigger backfill for one source only:

```bash
make airflow-trigger-backfill BACKFILL_START=2020-01-01 BACKFILL_SOURCE_KEYS=air_quality_station_4
```

## DAG Workflows

### Incremental DAG

DAG id:
- `ohuseire_incremental`

Purpose:
- keep Lecture 5 data moving forward automatically

Flow:

1. ensure raw schema and watermark table exist
2. read per-source watermark state
3. plan the next date window for each source
4. run ETL for the planned windows
5. run `dbt seed`, `dbt run`, `dbt test`
6. advance per-source watermarks only after success

Important idea:
- the watermark means "last fully closed day loaded successfully"

### Backfill DAG

DAG id:
- `ohuseire_backfill`

Purpose:
- load historical data on demand

Flow:

1. read backfill parameters
2. split the requested date range into chunks
3. run ETL chunk by chunk
4. run `dbt seed`, `dbt run`, `dbt test`
5. optionally advance incremental watermarks

## Data Contracts

Raw source tables:

- `l5_raw.ohuseire_measurement`
- `l5_raw.ohuseire_ingestion_audit`
- `l5_raw.pipeline_watermark`

dbt layers:

- staging:
  - `l5_mart.stg_ohuseire_measurement`
- intermediate:
  - `l5_mart.int_air_quality_measurement`
  - `l5_mart.int_air_quality_hourly_wide`
  - `l5_mart.int_pollen_daily`
- dimensions:
  - `l5_mart.dim_station`
  - `l5_mart.dim_date`
  - `l5_mart.dim_time_hour`
  - `l5_mart.dim_indicator`
  - `l5_mart.dim_wind_direction`
- facts:
  - `l5_mart.fct_air_quality_hourly`
  - `l5_mart.fct_pollen_daily`
- presentation:
  - `l5_mart.v_ohuseire_measurements_long`
  - `l5_mart.v_air_quality_hourly`
  - `l5_mart.v_pollen_daily`

## Modeling Notes

- `stg_ohuseire_measurement` is the stable staging view over `l5_raw.ohuseire_measurement`.
- `fct_air_quality_hourly` stays long-form: one row per station, date, local clock hour, repeated-hour occurrence, and indicator.
- `v_air_quality_hourly` is the wide reporting view built on top of that fact.
- `dim_station` comes from a seed, which means dbt loads a small CSV file from the repository so the lesson stays reproducible.
- `dim_time_hour` is a simple 24-row local clock-hour dimension keyed by `hour_key`.
- The ETL fetches each logical window with one day of overlap on both sides before trimming back to the requested dates. That helps recover boundary rows without index-based timestamp shifts.

## Daylight Saving Time (DST) Note

The warehouse keeps both:

- the original local `observed_at` timestamp from the source
- a local `hour_key` that matches the wall-clock hour `0..23`
- `hour_occurrence_in_day` to distinguish repeated autumn hours when the source returns them

This lets us discuss repeated or skipped local clock hours without renumbering later observations into misleading hour labels.

Useful fact columns for DST discussion:

- `hour_occurrence_in_day`
- `is_expected_dst_transition_day`
- `is_complete_day_series`
- `has_repeated_clock_hour`
- `has_missing_clock_hour`
- `has_repeated_or_skipped_clock_hour`
- `has_unexpected_clock_pattern`
- `measurements_in_day`
- `distinct_clock_hours_in_day`

## Common Recovery Steps

If the raw layer is missing:

```bash
make etl-bootstrap-l5
```

If the mart looks stale after ETL work:

```bash
make dbt-build
```

If the DAGs are paused:

```bash
make airflow-unpause-dags
```

If the local stack becomes confusing and you are happy to reset local Lecture 5 data:

```bash
make reset-l5
```

## Operational Notes

- Incremental catch-up is bounded per run by `AIRFLOW_OHUSEIRE_INCREMENTAL_MAX_DAYS`, which sets the maximum number of days one run will try to catch up.
- If the stack is down for a period, later scheduled runs continue from watermark.
- Once watermark reaches yesterday, incremental keeps refreshing today's date each hour.
- The DAG uses `catchup=False`, which means Airflow does not create one scheduled run for every missed historical interval. Watermark state handles backlog instead.
- Watermarks are tracked per source, for example `ohuseire_incremental:air_quality_station_4`.

## Platform Notes

These details matter mostly when debugging the local environment:

- Airflow and dbt share one local image, and dbt lives in its own Python virtual environment at `/opt/dbt-venv`.
- The local Superset image is also custom-built and small.
- The shared database defaults to `pgduckdb`, a PostgreSQL-compatible service with optional DuckDB file-query features.
- `warehouse/files/` is mounted into the database and Airflow containers as a shared folder for file-based experiments.

Optional validation example from inside the devcontainer:

```bash
make devcontainer-join-course-network
psql postgresql://warehouse:warehouse@postgres:5432/warehouse -c "select * from read_csv('/warehouse-files/sample.csv') limit 5;"
```
