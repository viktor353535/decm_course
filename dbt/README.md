# Airviro dbt Project

This project contains the SQL-first transformation layer for course lectures.

Current repo note:
- Lecture 4 ETL now writes to Lecture 4-specific schemas (`l4_simple`, `l4_raw`, `l4_mart`).
- The Lecture 5 dbt/Airflow warehouse refactor is planned separately and is not implemented in this change.

## Run from this repository

Use the Make targets (recommended):

```bash
make dbt-debug
make dbt-build
```

`make dbt-build` runs:
1. `dbt seed`
2. `dbt run`
3. `dbt test`

All commands run inside the `airflow-scheduler` container using the same dependencies as Airflow DAG tasks.

## Model Layers

- `models/staging/stg_airviro_measurement.sql`:
  canonical staging view over `raw.airviro_measurement`.
- `models/marts/`:
  dimensions and mart views used by Superset.
- `seeds/dim_wind_direction_seed.csv`:
  8-sector wind direction mapping.
