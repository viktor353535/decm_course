# Lecture 5 Runbook

Start with the lecture overview first:
- [Lecture 5 Overview](./README.md)

This page is the follow-up runbook for commands, DAG behavior, data contracts, and recovery steps.

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
  - also available as:
    `l5_mart.v_airviro_measurements_long`

## Modeling Notes

- `stg_ohuseire_measurement` is the source-conformed contract over `l5_raw.ohuseire_measurement`.
- `fct_air_quality_hourly` stays long-form: one row per station, date, hour slot, and indicator.
- `v_air_quality_hourly` is the wide reporting view built on top of that fact.
- `dim_station` comes from a seed so the lesson stays reproducible.
- `dim_time_hour` is a simple 24-row hour-slot dimension keyed by `hour_key`.
- The ETL fetches each logical window with one day of overlap on both sides before trimming back to the requested dates. That helps recover boundary rows without index-based timestamp shifts.

## Daylight Saving Time Note

The warehouse keeps both:

- the original `observed_at` timestamp from the source
- an analytic `hour_key` that numbers measurements `0..23` within each station, indicator, and day

This lets us discuss repeated or skipped local clock hours without losing the original source timestamp.

Useful fact columns for DST discussion:

- `is_complete_day_series`
- `has_repeated_clock_hour`
- `has_repeated_or_skipped_clock_hour`
- `measurements_in_day`
- `distinct_clock_hours_in_day`

## Common Recovery Steps

If the raw layer is missing:

```bash
make etl-bootstrap-l5
```

If your local database still contains `l5_raw.airviro_measurement`, rebuild only the Lecture 5 schemas:

```bash
make reset-l5
```

Lecture 4 schemas can stay in place.

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

- Incremental catch-up is bounded per run by `AIRFLOW_OHUSEIRE_INCREMENTAL_MAX_DAYS`.
- If the stack is down for a period, later scheduled runs continue from watermark.
- Once watermark reaches yesterday, incremental keeps refreshing today's date each hour.
- The DAG uses `catchup=False`; watermark state handles backlog instead of scheduler-created historical runs.
- Watermarks are tracked per source, for example `ohuseire_incremental:air_quality_station_4`.

## Platform Notes

These details matter mostly when debugging the local environment:

- Airflow and dbt share one local image, and dbt lives in its own `/opt/dbt-venv`.
- The local Superset image is also custom-built and small.
- The shared database defaults to `pgduckdb`, which keeps normal PostgreSQL behavior and also allows optional DuckDB file queries.
- `warehouse/files/` is bind-mounted into the database and Airflow containers for those file-based experiments.

### pg_duckdb Volume Migration Note

Fresh volumes:

- `pgduckdb` works as a normal PostgreSQL service immediately.

Existing volumes migrated from plain Postgres may need one extra bootstrap/restart cycle:

```bash
make pgduckdb-bootstrap
make down
make up-airflow
make pgduckdb-bootstrap
make down
make up-airflow
```

Optional validation example from inside the devcontainer:

```bash
make devcontainer-join-course-network
psql postgresql://warehouse:warehouse@postgres:5432/warehouse -c "select * from read_csv('/warehouse-files/sample.csv') limit 5;"
```

If you see collation-version warnings after switching an existing volume to the `pgduckdb` image, treat them as a local migration caveat. A clean reset is still the simplest fix:

```bash
make reset-volumes
make up-airflow
```
