# Updating From Lecture 4

This note is for the case where we already have a working Lecture 4 setup and want to move forward to Lecture 5 without losing more local data than necessary.

## Short Answer

Lecture 5 does not intentionally break the Lecture 4 ETL flow.

Lecture 4 still keeps its own defaults:

- raw schema: `l4_raw`
- mart schema: `l4_mart`
- raw measurement table: `airviro_measurement`
- raw ingestion audit table: `airviro_ingestion_audit`

Lecture 5 adds a second warehouse area:

- raw schema: `l5_raw`
- mart schema: `l5_mart`

That means Lecture 4 and Lecture 5 can live side by side in the same local database.

## What Changed In The Local Stack

The main local platform changes are:

- the database container now uses `pgduckdb`
- Airflow now uses the local course image
- Superset now uses the local course image
- the Airflow containers are wired for the Lecture 5 `ohuseire_*` flow

Those are local infrastructure changes, not a rewrite of the Lecture 4 warehouse layout.

## Safe Upgrade Path

Run these steps from the repository root inside the devcontainer:

```bash
git pull
make down
make up-superset
make warehouse-status
```

If Lecture 4 still looks healthy, then start the Lecture 5 stack:

```bash
make up-airflow
make etl-bootstrap-l5
make warehouse-status-l5
```

At that point:

- Lecture 4 data should still be in `l4_*`
- Lecture 5 structures should now exist in `l5_*`

## What We Usually Keep

In most cases, keep the existing `.env` file if it already works.

If something looks off, compare it with:

- [`.env.example`](/workspaces/course/.env.example)

The current example file still keeps the Lecture 4 ETL defaults for:

- `OHUSEIRE_AIR_STATION_IDS=8`
- `OHUSEIRE_POLLEN_STATION_IDS=25`

## Recovery Steps

If the new database container has trouble with an older local volume, try:

```bash
make pgduckdb-bootstrap
```

If Lecture 5 data becomes confusing and we want to rebuild only Lecture 5:

```bash
make reset-l5
```

That command drops only:

- `l5_raw`
- `l5_mart`

It does not remove Lecture 4 data.

## Destructive Reset Warning

These commands remove Lecture 4 data too:

- `make reset-volumes`
- `make reset-all`

Use them only when we are happy to rebuild the whole local stack from scratch.

## What Still Works For Lecture 4

These Lecture 4 commands are still valid after updating:

- `make up-superset`
- `make etl-bootstrap`
- `make etl-dry-run`
- `make etl-backfill-2020-2025`
- `make etl-backfill-2020-today`
- `make warehouse-status`

Lecture 5 adds new commands. It does not replace the Lecture 4 workflow.
