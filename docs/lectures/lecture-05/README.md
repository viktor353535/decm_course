# Lecture 5: Airflow + dbt Orchestration

## Audience and Goal

This lecture builds on Lecture 4. Instead of running ETL by hand, we now work through a complete pipeline flow:

1. read source metadata and measurements from the Ohuseire API
2. load raw data into a warehouse
3. transform that raw data with dbt
4. orchestrate the full flow with Airflow

Goal: understand how API-driven ETL, dimensional modeling, dbt, and Airflow fit together in one clear local stack.

## ETL Or ELT?

In common data-engineering literature, this Lecture 5 pipeline is best described as an **ELT-style** flow:

- Python extracts data from the Ohuseire API.
- Python loads raw, source-shaped rows into `l5_raw`.
- dbt transforms that already-loaded raw data into analytical models in `l5_mart`.
- Airflow orchestrates the whole workflow.

You may notice that dbt also materializes tables and views. In strict step-by-step language, that can feel like a second load step. In most references, though, this is still grouped under **ELT** because the important boundary is that transformation happens after raw data has already been loaded into the warehouse.

Useful references:

- [IBM: ELT vs. ETL](https://www.ibm.com/think/topics/elt-vs-etl)
- [dbt Labs: Data transformation is the "T" in ETL/ELT](https://www.getdbt.com/blog/data-transformation-best-practices)
- [Apache Airflow: Use Airflow for ETL/ELT pipelines](https://airflow.apache.org/use-cases/etl_analytics/)

## Learning Outcomes

After this lecture, we should be able to:

1. Explain the difference between `extract`, `load`, `transform`, and `orchestrate`.
2. Run and monitor the `ohuseire_incremental` and `ohuseire_backfill` DAGs.
3. Explain why this repository keeps Lecture 5 raw data in `l5_raw` and dbt models in `l5_mart`.
4. Describe the dbt layers `staging -> intermediate -> marts`.
5. Explain why the Lecture 5 ETL is idempotent and why that matters.
6. Explain watermark-based incremental loading and backfill.
7. Explain why the warehouse keeps both the source timestamp and an analytic `hour_key`.

## System Overview

Lecture 5 introduces one end-to-end analytics flow:

1. Source API
   - station metadata
   - indicator metadata
   - monitoring measurements
2. Python ETL
   - discover source metadata
   - fetch bounded windows
   - normalize and upsert raw rows
3. Raw warehouse layer
   - `l5_raw.ohuseire_measurement`
   - `l5_raw.ohuseire_ingestion_audit`
   - `l5_raw.pipeline_watermark`
4. dbt transformation layer
   - `staging -> intermediate -> marts`
5. Airflow orchestration
   - schedule incremental runs
   - trigger backfills
   - run dbt after ETL succeeds

That means the lecture is not only "about Airflow". It is about how Airflow coordinates the rest of the data pipeline.

## Architecture At A Glance

Lecture 4 and Lecture 5 use different schemas:

- Lecture 4: `l4_*`
- Lecture 5: `l5_raw` and `l5_mart`

That separation helps us compare:

- manual ETL versus orchestrated ETL
- serving views built directly from ETL versus a dbt-modeled warehouse
- a simpler operational setup versus a more structured pipeline

## ETL Stages In This Repository

### 1. Discover

The ETL starts by reading metadata from the source API:

- stations
- station indicator lists
- indicator names, formulas, and units

Shared source reference:
- [Ohuseire API Reference](../../reference/ohuseire-api.md)

### 2. Extract

The ETL requests measurements in bounded date windows.

Important behaviors:

- explicit request windows instead of unbounded fetches
- retries for transient failures
- split-on-failure logic for large windows
- one-day overlap on both sides of each logical window

That overlap is important because the source API can behave strangely around historical window edges.

### 3. Load Raw Data

The ETL writes normalized long-form rows into `l5_raw`.

Raw grain:
- one row per `source_type + station_id + observed_at + indicator_code`

The load is idempotent:
- rerunning the same logical window should not create duplicate raw measurements
- audit rows describe what was fetched and loaded

### 4. Transform With dbt

dbt then turns raw rows into a small dimensional warehouse:

- staging models standardize raw contracts
- intermediate models prepare reusable logic
- marts build dimensions, facts, and presentation views

### 5. Orchestrate With Airflow

Airflow ties the steps together:

- bootstrap and prerequisite checks
- incremental planning from watermarks
- ETL execution
- `dbt seed`, `dbt run`, `dbt test`
- watermark advancement only after success

## Good Design Patterns To Notice

- Idempotency:
  ETL windows can be rerun safely because raw rows are keyed and upserted instead of blindly appended.
- Separate raw and mart schemas:
  `l5_raw` preserves source-shaped data; `l5_mart` contains curated analytical models.
- Long-form raw storage:
  adding new indicators is easier when raw data stays long-form.
- Watermark-based incremental design:
  Airflow tracks per-source progress without depending on scheduler catchup.
- Overlap and trim window stitching:
  the ETL trusts each measurement timestamp and uses overlap to recover boundary rows.
- dbt layering:
  small models are easier to reason about than one large transformation query.
- Tests close to transformations:
  `dbt test` makes the modeling layer observable, not just "built".
- Lightweight DAG files:
  top-level DAG code stays small while shared logic lives in helper modules.

## Read The Code In This Order

These files give a good top-to-bottom tour of the Lecture 5 system:

1. Source API shape:
   [ohuseire-api.md](../../reference/ohuseire-api.md)
2. Warehouse bootstrap objects:
   [l5_ohuseire_schema.sql](../../../sql/warehouse/l5_ohuseire_schema.sql)
3. ETL command entry point:
   [cli.py](../../../etl/airviro/cli.py)
4. ETL extraction and normalization logic:
   [pipeline.py](../../../etl/airviro/pipeline.py)
5. ETL database writes:
   [db.py](../../../etl/airviro/db.py)
6. Shared Airflow DAG helpers:
   [ohuseire_dag_utils.py](../../../airflow/dags/ohuseire_dag_utils.py)
7. Incremental DAG:
   [ohuseire_incremental.py](../../../airflow/dags/ohuseire_incremental.py)
8. Backfill DAG:
   [ohuseire_backfill.py](../../../airflow/dags/ohuseire_backfill.py)
9. dbt project guide:
   [dbt README](../../../dbt/README.md)
10. dbt model folders:
    - [staging](../../../dbt/models/staging/ohuseire/)
    - [intermediate](../../../dbt/models/intermediate/ohuseire/)
    - [dimensions](../../../dbt/models/marts/dimensions/)
    - [facts](../../../dbt/models/marts/facts/)
    - [presentation](../../../dbt/models/marts/presentation/)

## Hands-On Flow

Run from repo root inside the devcontainer.

Start the stack and bootstrap the warehouse:

```bash
make up-airflow
make etl-bootstrap-l5
make warehouse-status-l5
```

Then run the end-to-end Airflow flow:

```bash
make airflow-unpause-dags
make airflow-trigger-incremental
make airflow-list-runs DAG_ID=ohuseire_incremental
```

If you want to rerun only the dbt modeling layer after raw data is loaded:

```bash
make dbt-build
```

If you want to browse the dbt model graph and generated documentation locally:

```bash
make dbt-docs-serve
```

Backfill example:

```bash
make airflow-trigger-backfill BACKFILL_START=2020-01-01 BACKFILL_END=2020-12-31 BACKFILL_CHUNK_DAYS=31 BACKFILL_SOURCE_KEYS=air_quality_station_4,air_quality_station_8,pollen_station_25
```

Open Airflow UI:
- <http://localhost:8080>
- user: `airflow`
- pass: `airflow`

Open dbt docs:
- <http://localhost:8081>

## What To Observe

After the core flow runs, check these things:

- `l5_raw.ohuseire_measurement`
  raw long-form measurements
- `l5_raw.pipeline_watermark`
  Airflow incremental progress state
- `l5_mart.fct_air_quality_hourly`
  long-form fact table
- `l5_mart.fct_pollen_daily`
  daily pollen fact table
- `l5_mart.v_air_quality_hourly`
  wide reporting view
- `l5_mart.v_ohuseire_measurements_long`
  simple exploratory long-form view

Inside Airflow, focus on:

- which tasks belong to ETL
- where dbt starts
- when watermarks advance
- how incremental and backfill differ

## How This Local Stack Differs From Larger Deployments

This local stack stays small:

- local Docker Compose instead of a production deployment platform
- Airflow and dbt inside one local image
- LocalExecutor instead of Celery or Kubernetes
- one shared PostgreSQL-compatible service instead of multiple platform components

This keeps the lecture focused on data flow and design patterns instead of platform complexity. Other deployments may split these services further.

## Need More Detail?

Use these follow-up materials:

- operational behavior and commands:
  [operations.md](./operations.md)
- dbt structure and model notes:
  [dbt README](../../../dbt/README.md)

If you reused an older local database volume and the stack behaves strangely after switching database images, use the recovery notes in [operations.md](./operations.md).
