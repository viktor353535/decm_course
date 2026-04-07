# Ohuseire dbt Project

This project contains the SQL-first transformation layer for the Lecture 5 warehouse.

Lecture 4 and Lecture 5 use different warehouse schemas:

- Lecture 4 focuses on ETL foundations in `l4_*`
- Lecture 5 focuses on orchestration and dimensional modeling in `l5_raw` and `l5_mart`

This dbt project is the Lecture 5 transformation layer.

## How dbt Fits Into Lecture 5

In Lecture 5, dbt is the step between "raw data loaded successfully" and "we can analyze trustworthy warehouse tables".

Flow:

1. Python ETL loads source-shaped rows into `l5_raw`
2. dbt standardizes those rows in staging models
3. dbt prepares reusable logic in intermediate models
4. dbt builds dimensions, facts, and presentation views in `l5_mart`
5. dbt tests help confirm that the modeled warehouse still matches the intended grain and relationships

## Run from this repository

Use the Make targets from the repository root:

```bash
make dbt-debug
make dbt-build
make dbt-docs
make dbt-docs-serve
```

`make dbt-build` runs:

1. `dbt seed`
2. `dbt run`
3. `dbt test`

All commands run inside the `airflow-scheduler` container using the same dependencies as Airflow DAG tasks.

`make dbt-docs` runs `dbt docs generate` and writes documentation artifacts into `dbt/target/`.
Those generated files are intentionally ignored by git.

`make dbt-docs-serve` regenerates the docs and starts a small optional `dbt-docs` Compose service that runs `dbt docs serve` on `http://127.0.0.1:8081`.
That service reuses the same Airflow+dbt image, but it stays separate from the Airflow API server so each container keeps one clear job.

Do not open `dbt/target/index.html` directly with `file://`.
The generated site loads `manifest.json` and `catalog.json` next to the HTML file, and browsers often block that local-file fetch flow.
Serve the folder over HTTP instead.

## Profile and Schemas

The profile lives in `dbt/profiles.yml`.

Important environment variables:

- `DBT_SOURCE_RAW_SCHEMA` defaults to `l5_raw`
- `DBT_TARGET_SCHEMA` defaults to `l5_mart`

That means:

- sources point at the Lecture 5 raw schema
- models build into the Lecture 5 mart schema
- Lecture 4 schemas can stay untouched while we work through Lecture 5

## Model Layers

This project follows the layered layout recommended in the official dbt structure guides, adapted to the course repository.

- `models/staging/ohuseire/`
  Source-conformed views over `l5_raw.ohuseire_measurement`.
- `models/intermediate/ohuseire/`
  Small transformation steps that prepare data for facts and presentation models.
- `models/marts/dimensions/`
  Conformed dimensions used across facts and views.
- `models/marts/facts/`
  Long-form fact tables with explicit grains.
- `models/marts/presentation/`
  Analysis-friendly and Superset-friendly reporting views built on the facts and dimensions.

## Read The Models In This Order

If we are reading the dbt project for the first time, this order works well:

1. `models/staging/ohuseire/stg_ohuseire_measurement.sql`
   See how raw rows are normalized into one stable source contract.
2. `models/intermediate/ohuseire/int_air_quality_measurement.sql`
   See how hourly measurement logic and `hour_key` assignment are prepared.
3. `models/intermediate/ohuseire/int_pollen_daily.sql`
   Compare the daily pollen path with the hourly air-quality path.
4. `models/marts/dimensions/`
   Read the warehouse dimensions used by the facts.
5. `models/marts/facts/`
   See the business grains of the final fact tables.
6. `models/marts/presentation/`
   See the analytical views built on top of the facts.

## Key Models

- `stg_ohuseire_measurement`
  Canonical staging view over the raw table.
- `int_air_quality_measurement`
  Adds a surrogate `hour_key` for each station, indicator, and day.
- `fct_air_quality_hourly`
  Long-form hourly fact at the grain `station x date x hour slot x indicator`.
- `fct_pollen_daily`
  Long-form daily fact at the grain `station x date x indicator`.
- `v_air_quality_hourly`
  Wide reporting view built from the long-form hourly fact.
- `v_pollen_daily`
  Enriched daily pollen presentation view.
- `v_ohuseire_measurements_long`
  Simple exploratory long-form presentation view.

## Seeds

- `seeds/dim_station_seed.csv`
  Stable station metadata kept in the repository for reproducible lessons.
- `seeds/dim_wind_direction_seed.csv`
  8-sector wind direction mapping.

## Modeling Notes

- Raw ingestion stays long-form so ETL window stitching can deduplicate overlapping fetch windows instead of depending on indicator-array index alignment.
- Each top-level ETL window is fetched with a small date overlap and trimmed back to the requested window so historical backfills do not lose boundary rows.
- Presentation views sit on top of facts instead of replacing them.
- The hourly fact keeps the original `observed_at` timestamp and also stores an analytic `hour_key` so we can discuss daylight saving time edge cases without losing the source timestamp.
- The project is split into small models so we can read and test one step at a time instead of reverse-engineering one large SQL file.

## Validation

Run these from the repository root:

```bash
make dbt-debug
make dbt-build
```

Expected outcome:

- seeds load successfully
- all models build into `l5_mart`
- data tests pass

Small SQL style note:

- Some dbt tests use shorthand like `group by 1, 2, 3, 4`.
- In SQL, that means "group by the 1st, 2nd, 3rd, and 4th selected expressions".
- It is valid PostgreSQL syntax, but it depends on the order of the `select` list, so read it as positional shorthand rather than as literal column names.

## Recovery

- If raw tables are missing, run `make etl-bootstrap-l5`.
- If the mart looks stale after ETL work, rerun `make dbt-build`.
- If the Lecture 5 warehouse state becomes confusing, use `make reset-l5` and reload the lesson flow from the lecture instructions.
