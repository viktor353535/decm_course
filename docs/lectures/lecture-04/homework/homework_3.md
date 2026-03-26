# HW3 (due 24.04): ETL Processes, Superset, and Data Visualization

Objective: start the Lecture 4 environment, run the ETL workflow, connect Superset to the warehouse, and create visualizations based on the course datasets.

## Task 1: Lecture 4 Environment and Superset Startup Review

Start the Lecture 4 environment from the repository root inside the devcontainer:

```bash
make init
make up-superset
make devcontainer-join-course-network
make ps
```

In your own words, explain what each command does and why it is needed.

Your explanation should cover:
- what each command does;
- which services are expected to exist for Lecture 4 (`postgres`, `superset`, `superset-redis`, and the one-time `superset-init`);
- why `make devcontainer-join-course-network` is needed before running ETL from the devcontainer;
- how `make` and Docker Compose work together in this repository.

Also explain how you would start working again in each of these cases:
- the Superset stack already exists but is stopped;
- the containers have been removed;
- you changed `.env` values or reopened the repository from a different host folder.

## Task 2: Data Analysis and Visualization in Superset

Make sure the current Lecture 4 datasets are available in your Superset instance.

Run the ETL and validation flow from the repository root inside the devcontainer:

```bash
make etl-bootstrap
.venv/bin/python -m etl.airviro.cli run --from 2026-03-10 --to 2026-03-12 --source-key air_quality_station_8 --source-key pollen_station_25 --verbose
make warehouse-status
```

Then connect Superset to the warehouse and use the current serving views:
- `l4_mart.v_air_quality_hourly_station_8`
- `l4_mart.v_pollen_daily_station_25`
- `l4_mart.v_airviro_measurements_long` (optional helper dataset)

Warehouse connection values:
- host: `postgres`
- port: `5432`
- database: `warehouse`
- username: `warehouse`
- password: `warehouse`

Create a chart in Superset that shows dust particle concentration (`PM10`) by:
- rows: weekday;
- columns: hour of day (`0-23`);
- metric: average `pm10`.

Recommended dataset:
- `l4_mart.v_air_quality_hourly_station_8`

Recommended fields:
- weekday: `day_name`
- hour: `hour_number`
- metric: `AVG(pm10)`

If weekday labels sort alphabetically, try `day_short` together with `day_of_week_number`.

## Task 3: Dashboard Creation in Superset

Create a new dashboard in Superset that includes:
- the chart you created in Task 2;
- two more charts of your choice;
- at least one chart based on `l4_mart.v_pollen_daily_station_25`;
- at least one chart based on air-quality indicators from `l4_mart.v_air_quality_hourly_station_8` such as `temp`, `ws10`, `pm2_5`, `pm10`, `o3`, `no2`, or `so2`;
- a Markdown element containing a title, a brief explanation of the dashboard, and your name or student code.

Try to keep the visuals different from each other, for example a pivot table, line chart, and box plot or bar chart.

## Submission

Submit:
- one Markdown document for Task 1;
- one screenshot of the Task 2 chart;
- one screenshot or exported image of the final dashboard from Task 3.
