"""Shared helpers for Ohuseire Airflow DAGs.

These helpers keep DAG files small and make command/database side-effects explicit.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path
import shlex
import subprocess
import re
from typing import Iterable

import psycopg2

REPO_ROOT = Path("/opt/airflow")
DBT_DIR = REPO_ROOT / "dbt"
AIRFLOW_PYTHON = Path("/opt/airflow-venv/bin/python")
PIPELINE_NAME_INCREMENTAL = "ohuseire_incremental"
LEGACY_PIPELINE_NAME_INCREMENTAL = "airviro_incremental"


def _env(names: tuple[str, ...], default: str) -> str:
    for name in names:
        raw = os.getenv(name)
        if raw is not None and raw.strip():
            return raw.strip()
    return default


def _sql_identifier_env(names: tuple[str, ...], default: str) -> str:
    """Read one schema-like env var and reject unsafe values."""

    value = _env(names, default)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(
            f"{names[0]} must be a simple SQL identifier (letters, digits, underscore)"
        )
    return value


def _raw_schema() -> str:
    """Return the raw schema used by the current Airflow runtime."""

    return _sql_identifier_env(("OHUSEIRE_RAW_SCHEMA", "AIRVIRO_RAW_SCHEMA"), "l5_raw")


def utc_today() -> date:
    """Return today's date in UTC."""

    return datetime.now(timezone.utc).date()


def parse_iso_date(value: str) -> date:
    """Parse YYYY-MM-DD date strings."""

    return datetime.strptime(value, "%Y-%m-%d").date()


def split_date_range(start_date: date, end_date: date, chunk_days: int) -> list[tuple[date, date]]:
    """Split an inclusive date range into fixed-size chunks."""

    if chunk_days < 1:
        raise ValueError("chunk_days must be >= 1")

    windows: list[tuple[date, date]] = []
    current = start_date
    while current <= end_date:
        window_end = min(current + timedelta(days=chunk_days - 1), end_date)
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)
    return windows


def _warehouse_connect():
    """Connect to the warehouse database using shared env vars."""

    return psycopg2.connect(
        host=_env(("WAREHOUSE_DB_HOST",), "postgres"),
        port=int(_env(("WAREHOUSE_DB_PORT",), "5432")),
        dbname=_env(("WAREHOUSE_DB_NAME",), "warehouse"),
        user=_env(("WAREHOUSE_DB_USER",), "warehouse"),
        password=_env(("WAREHOUSE_DB_PASSWORD",), "warehouse"),
        connect_timeout=10,
    )


def ensure_watermark_table() -> None:
    """Create watermark state table if it does not exist."""

    raw_schema = _raw_schema()
    create_sql = """
    CREATE TABLE IF NOT EXISTS {raw_schema}.pipeline_watermark (
      pipeline_name text PRIMARY KEY,
      watermark_date date NOT NULL,
      updated_at timestamp with time zone NOT NULL DEFAULT now()
    );
    """.format(raw_schema=raw_schema)
    with _warehouse_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
        conn.commit()


def get_watermark(pipeline_name: str) -> date | None:
    """Read watermark date for one pipeline."""

    raw_schema = _raw_schema()
    query = """
    SELECT watermark_date
    FROM {raw_schema}.pipeline_watermark
    WHERE pipeline_name = %s
    """.format(raw_schema=raw_schema)
    with _warehouse_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (pipeline_name,))
            row = cursor.fetchone()
    if row is None:
        return None
    return row[0]


def set_watermark(pipeline_name: str, watermark_date: date) -> None:
    """Upsert watermark date."""

    raw_schema = _raw_schema()
    query = """
    INSERT INTO {raw_schema}.pipeline_watermark (pipeline_name, watermark_date)
    VALUES (%s, %s)
    ON CONFLICT (pipeline_name)
    DO UPDATE SET
      watermark_date = EXCLUDED.watermark_date,
      updated_at = now()
    """.format(raw_schema=raw_schema)
    with _warehouse_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (pipeline_name, watermark_date))
        conn.commit()


def set_watermark_greatest(pipeline_name: str, candidate_date: date) -> None:
    """Upsert watermark using the greater of existing/candidate values."""

    raw_schema = _raw_schema()
    query = """
    INSERT INTO {raw_schema}.pipeline_watermark (pipeline_name, watermark_date)
    VALUES (%s, %s)
    ON CONFLICT (pipeline_name)
    DO UPDATE SET
      watermark_date = GREATEST({raw_schema}.pipeline_watermark.watermark_date, EXCLUDED.watermark_date),
      updated_at = now()
    """.format(raw_schema=raw_schema)
    with _warehouse_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (pipeline_name, candidate_date))
        conn.commit()


def _parse_station_ids(
    *,
    csv_env_names: tuple[str, ...],
    single_env_names: tuple[str, ...],
    default_station_id: int,
) -> list[int]:
    """Parse station-id configuration with backward-compatible env names."""

    raw_csv = _env(csv_env_names, "")
    if raw_csv:
        station_ids: list[int] = []
        seen: set[int] = set()
        for part in raw_csv.split(","):
            token = part.strip()
            if not token:
                continue
            station_id = int(token)
            if station_id in seen:
                continue
            seen.add(station_id)
            station_ids.append(station_id)
        if station_ids:
            return station_ids

    raw_single = _env(single_env_names, "")
    if raw_single:
        return [int(raw_single)]

    return [default_station_id]


def get_configured_sources() -> list[dict[str, object]]:
    """Return source config metadata from env settings."""

    air_station_ids = _parse_station_ids(
        csv_env_names=("OHUSEIRE_AIR_STATION_IDS", "AIRVIRO_AIR_STATION_IDS"),
        single_env_names=("OHUSEIRE_AIR_STATION_ID", "AIRVIRO_AIR_STATION_ID"),
        default_station_id=8,
    )
    pollen_station_ids = _parse_station_ids(
        csv_env_names=("OHUSEIRE_POLLEN_STATION_IDS", "AIRVIRO_POLLEN_STATION_IDS"),
        single_env_names=("OHUSEIRE_POLLEN_STATION_ID", "AIRVIRO_POLLEN_STATION_ID"),
        default_station_id=25,
    )

    sources: list[dict[str, object]] = []
    for station_id in air_station_ids:
        sources.append(
            {
                "source_key": f"air_quality_station_{station_id}",
                "source_type": "air_quality",
                "station_id": station_id,
            }
        )
    for station_id in pollen_station_ids:
        sources.append(
            {
                "source_key": f"pollen_station_{station_id}",
                "source_type": "pollen",
                "station_id": station_id,
            }
        )
    return sources


def incremental_source_watermark_key(
    source_key: str,
    *,
    pipeline_name: str = PIPELINE_NAME_INCREMENTAL,
) -> str:
    """Build per-source watermark key for incremental orchestration."""

    return f"{pipeline_name}:{source_key}"


def get_watermark_with_fallback(primary_name: str, legacy_name: str | None = None) -> date | None:
    """Read the preferred watermark row first, then fall back to one legacy name."""

    watermark = get_watermark(primary_name)
    if watermark is not None or legacy_name is None:
        return watermark
    return get_watermark(legacy_name)


def _run_command(command: list[str], *, cwd: Path) -> None:
    """Run one shell command with explicit logging and strict failure behavior."""

    printable = " ".join(shlex.quote(part) for part in command)
    print(f"[ohuseire] running: {printable} (cwd={cwd})")
    completed = subprocess.run(command, cwd=cwd, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed ({completed.returncode}): {printable}")


def run_etl_range(
    start_date: date,
    end_date: date,
    *,
    verbose: bool,
    source_key: str | None = None,
) -> None:
    """Run ETL for one inclusive date range."""

    command = [
        str(AIRFLOW_PYTHON),
        "-m",
        "etl.airviro.cli",
        "run",
        "--from",
        start_date.isoformat(),
        "--to",
        end_date.isoformat(),
    ]
    if source_key:
        command.extend(["--source-key", source_key])
    if verbose:
        command.append("--verbose")
    _run_command(command, cwd=REPO_ROOT)


def run_dbt_build() -> None:
    """Run dbt seed/run/test in the mounted dbt project."""

    command_batches: Iterable[list[str]] = (
        ["dbt", "seed", "--project-dir", ".", "--profiles-dir", "."],
        ["dbt", "run", "--project-dir", ".", "--profiles-dir", "."],
        ["dbt", "test", "--project-dir", ".", "--profiles-dir", "."],
    )
    for command in command_batches:
        _run_command(command, cwd=DBT_DIR)


def ensure_etl_schema() -> None:
    """Run ETL bootstrap command to ensure required schemas/tables/views exist."""

    command = [str(AIRFLOW_PYTHON), "-m", "etl.airviro.cli", "bootstrap-db"]
    _run_command(command, cwd=REPO_ROOT)
