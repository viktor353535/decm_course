"""Database helpers for Airviro ETL loading."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

try:
    import psycopg2
    from psycopg2 import extras
    from psycopg2.extensions import connection as PgConnection
except ImportError as exc:  # pragma: no cover - runtime environment concern
    raise RuntimeError(
        "psycopg2 is required. Run inside the project virtualenv: .venv/bin/python ..."
    ) from exc

from .config import Settings
from .pipeline import MeasurementRow


def _replace_schema_tokens(sql_text: str, settings: Settings) -> str:
    """Replace schema placeholders in SQL bootstrap files."""

    return (
        sql_text.replace("__RAW_SCHEMA__", settings.airviro_raw_schema).replace(
            "__MART_SCHEMA__", settings.airviro_mart_schema
        )
    )


def connect_warehouse(settings: Settings) -> tuple[PgConnection, str]:
    """Connect to warehouse DB using candidate hosts.

    Returns:
      (connection, selected_host)
    """

    last_error: Exception | None = None
    for host in settings.candidate_db_hosts():
        try:
            conn = psycopg2.connect(
                host=host,
                port=settings.warehouse_db_port,
                dbname=settings.warehouse_db_name,
                user=settings.warehouse_db_user,
                password=settings.warehouse_db_password,
                connect_timeout=5,
            )
            conn.autocommit = False
            return conn, host
        except Exception as exc:  # pragma: no cover - connection failure path
            last_error = exc

    raise RuntimeError(
        f"Unable to connect to warehouse using hosts {settings.candidate_db_hosts()}: {last_error}"
    )


def apply_schema(connection: PgConnection, sql_path: Path, settings: Settings) -> None:
    """Apply schema bootstrap SQL."""

    sql_text = _replace_schema_tokens(sql_path.read_text(encoding="utf-8"), settings)
    with connection.cursor() as cursor:
        cursor.execute(sql_text)
    connection.commit()


def collect_warehouse_status(
    connection: PgConnection,
    settings: Settings,
    *,
    indicator_limit: int = 500,
    audit_limit: int = 10,
) -> dict[str, Any]:
    """Collect warehouse-health and data-completeness metrics.

    Args:
      connection: Open warehouse connection.
      indicator_limit: Maximum number of indicator-level rows to return.
      audit_limit: Maximum number of most-recent audit rows to return.
    """

    if indicator_limit < 1:
        raise ValueError("indicator_limit must be >= 1")
    if audit_limit < 1:
        raise ValueError("audit_limit must be >= 1")

    status: dict[str, Any] = {}
    measurement_table = settings.measurement_table
    ingestion_audit_table = settings.ingestion_audit_table
    watermark_table = settings.pipeline_watermark_table
    with connection.cursor(cursor_factory=extras.RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT
              current_database() AS database_name,
              current_user AS database_user,
              now() AT TIME ZONE 'UTC' AS collected_at_utc
            """
        )
        status["database"] = dict(cursor.fetchone())

        cursor.execute(
            """
            SELECT
              to_regclass(%s) IS NOT NULL AS has_measurement_table,
              to_regclass(%s) IS NOT NULL AS has_ingestion_audit_table,
              to_regclass(%s) IS NOT NULL AS has_pipeline_watermark_table
            """
            ,
            (measurement_table, ingestion_audit_table, watermark_table),
        )
        table_status = dict(cursor.fetchone())
        status["table_status"] = table_status
        status["raw_schema"] = settings.airviro_raw_schema
        status["mart_schema"] = settings.airviro_mart_schema
        status["measurement_table_name"] = settings.measurement_table_name
        status["ingestion_audit_table_name"] = settings.ingestion_audit_table_name

        if not table_status["has_measurement_table"]:
            status["warning"] = (
                f"{measurement_table} does not exist yet. "
                "Run bootstrap first: make etl-bootstrap or make etl-bootstrap-l5"
            )
            return status

        cursor.execute(
            f"""
            SELECT
              COUNT(*)::bigint AS measurement_rows,
              COUNT(DISTINCT source_type)::int AS source_type_count,
              COUNT(DISTINCT station_id)::int AS station_count,
              COUNT(DISTINCT indicator_code)::int AS indicator_count,
              MIN(observed_at) AS first_observed_at,
              MAX(observed_at) AS last_observed_at,
              COUNT(*) FILTER (WHERE value_numeric IS NULL)::bigint AS null_value_rows
            FROM {measurement_table}
            """
        )
        status["measurement_totals"] = dict(cursor.fetchone())

        cursor.execute(
            f"""
            SELECT
              source_type,
              station_id,
              COUNT(*)::bigint AS row_count,
              COUNT(DISTINCT indicator_code)::int AS indicator_count,
              COUNT(*) FILTER (WHERE value_numeric IS NULL)::bigint AS null_value_rows,
              MIN(observed_at) AS first_observed_at,
              MAX(observed_at) AS last_observed_at
            FROM {measurement_table}
            GROUP BY source_type, station_id
            ORDER BY source_type, station_id
            """
        )
        status["coverage_by_source"] = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            f"""
            WITH indicator_span AS (
              SELECT
                source_type,
                station_id,
                indicator_code,
                MIN(observed_at) AS first_observed_at,
                MAX(observed_at) AS last_observed_at,
                COUNT(*)::bigint AS row_count,
                COUNT(*) FILTER (WHERE value_numeric IS NULL)::bigint AS null_value_rows
              FROM {measurement_table}
              GROUP BY source_type, station_id, indicator_code
            ),
            indicator_completeness AS (
              SELECT
                source_type,
                station_id,
                indicator_code,
                row_count,
                null_value_rows,
                first_observed_at,
                last_observed_at,
                CASE
                  WHEN source_type = 'pollen' THEN 'daily'
                  ELSE 'hourly'
                END AS expected_grain,
                CASE
                  WHEN first_observed_at IS NULL OR last_observed_at IS NULL THEN 0::bigint
                  WHEN source_type = 'pollen' THEN ((EXTRACT(EPOCH FROM (last_observed_at - first_observed_at)) / 86400)::bigint + 1)
                  ELSE ((EXTRACT(EPOCH FROM (last_observed_at - first_observed_at)) / 3600)::bigint + 1)
                END AS expected_rows
              FROM indicator_span
            )
            SELECT
              source_type,
              station_id,
              indicator_code,
              row_count,
              expected_grain,
              expected_rows,
              GREATEST(expected_rows - row_count, 0)::bigint AS missing_rows,
              ROUND(
                (GREATEST(expected_rows - row_count, 0)::numeric / NULLIF(expected_rows, 0)::numeric) * 100,
                2
              ) AS missing_pct,
              null_value_rows,
              ROUND((null_value_rows::numeric / NULLIF(row_count, 0)::numeric) * 100, 2) AS null_value_pct,
              first_observed_at,
              last_observed_at
            FROM indicator_completeness
            ORDER BY source_type, station_id, indicator_code
            LIMIT %s
            """,
            (indicator_limit,),
        )
        status["indicator_completeness"] = [dict(row) for row in cursor.fetchall()]

        if table_status["has_pipeline_watermark_table"]:
            cursor.execute(
                f"""
                SELECT
                  pipeline_name,
                  watermark_date,
                  updated_at
                FROM {watermark_table}
                ORDER BY pipeline_name
                """
            )
            status["watermarks"] = [dict(row) for row in cursor.fetchall()]
        else:
            status["watermarks"] = []

        if table_status["has_ingestion_audit_table"]:
            cursor.execute(
                f"""
                SELECT
                  created_at,
                  batch_id,
                  source_key,
                  source_type,
                  station_id,
                  window_start,
                  window_end,
                  rows_read,
                  records_upserted,
                  duplicate_records,
                  split_events,
                  status
                FROM {ingestion_audit_table}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (audit_limit,),
            )
            status["recent_ingestion_runs"] = [dict(row) for row in cursor.fetchall()]
        else:
            status["recent_ingestion_runs"] = []

    return status


def upsert_measurements(
    connection: PgConnection,
    rows: Iterable[MeasurementRow],
    settings: Settings,
) -> int:
    """Insert or update normalized measurements."""

    payload = [
        (
            row.source_type,
            row.station_id,
            row.observed_at,
            row.local_hour_occurrence,
            row.indicator_code,
            row.indicator_name,
            row.value_numeric,
            row.source_row_hash,
        )
        for row in rows
    ]
    if not payload:
        return 0

    query = f"""
    INSERT INTO {settings.measurement_table} (
      source_type,
      station_id,
      observed_at,
      local_hour_occurrence,
      indicator_code,
      indicator_name,
      value_numeric,
      source_row_hash
    )
    VALUES %s
    ON CONFLICT (source_type, station_id, observed_at, indicator_code, local_hour_occurrence)
    DO UPDATE SET
      indicator_name = EXCLUDED.indicator_name,
      value_numeric = EXCLUDED.value_numeric,
      source_row_hash = EXCLUDED.source_row_hash,
      extracted_at = now();
    """

    with connection.cursor() as cursor:
        extras.execute_values(cursor, query, payload, page_size=5000)
    connection.commit()
    return len(payload)


def refresh_dimensions(connection: PgConnection, settings: Settings) -> None:
    """Refresh dimension tables from loaded raw facts."""

    refresh_sql = f"""
    INSERT INTO {settings.airviro_mart_schema}.dim_indicator (source_type, indicator_code, indicator_name)
    SELECT DISTINCT source_type, indicator_code, indicator_name
    FROM {settings.measurement_table}
    ON CONFLICT DO NOTHING;

    UPDATE {settings.airviro_mart_schema}.dim_indicator AS target
    SET indicator_name = source.indicator_name
    FROM (
      SELECT DISTINCT source_type, indicator_code, indicator_name
      FROM {settings.measurement_table}
    ) AS source
    WHERE target.source_type = source.source_type
      AND target.indicator_code = source.indicator_code
      AND target.indicator_name IS DISTINCT FROM source.indicator_name;

    INSERT INTO {settings.airviro_mart_schema}.dim_datetime_hour (
      observed_at,
      date_value,
      year_number,
      quarter_number,
      month_number,
      month_name,
      month_short,
      day_number,
      hour_number,
      iso_week_number,
      day_of_week_number,
      day_name,
      day_short
    )
    SELECT
      source.observed_at,
      source.observed_at::date,
      EXTRACT(YEAR FROM source.observed_at)::int,
      EXTRACT(QUARTER FROM source.observed_at)::int,
      EXTRACT(MONTH FROM source.observed_at)::int,
      TRIM(TO_CHAR(source.observed_at, 'Month')),
      CASE EXTRACT(MONTH FROM source.observed_at)::int
        WHEN 1 THEN '           Jan'
        WHEN 2 THEN '          Feb'
        WHEN 3 THEN '         Mar'
        WHEN 4 THEN '        Apr'
        WHEN 5 THEN '       May'
        WHEN 6 THEN '      Jun'
        WHEN 7 THEN '     Jul'
        WHEN 8 THEN '    Aug'
        WHEN 9 THEN '   Sep'
        WHEN 10 THEN '  Oct'
        WHEN 11 THEN ' Nov'
        ELSE 'Dec'
      END,
      EXTRACT(DAY FROM source.observed_at)::int,
      EXTRACT(HOUR FROM source.observed_at)::int,
      EXTRACT(WEEK FROM source.observed_at)::int,
      EXTRACT(ISODOW FROM source.observed_at)::int,
      TRIM(TO_CHAR(source.observed_at, 'Dy')),
      CASE EXTRACT(ISODOW FROM source.observed_at)::int
        WHEN 1 THEN '      Mon'
        WHEN 2 THEN '     Tue'
        WHEN 3 THEN '    Wed'
        WHEN 4 THEN '   Thu'
        WHEN 5 THEN '  Fri'
        WHEN 6 THEN ' Sat'
        ELSE 'Sun'
      END
    FROM (
      SELECT DISTINCT observed_at
      FROM {settings.measurement_table}
    ) AS source
    WHERE NOT EXISTS (
      SELECT 1
      FROM {settings.airviro_mart_schema}.dim_datetime_hour AS target
      WHERE target.observed_at = source.observed_at
    );

    WITH source_datetime AS (
      SELECT DISTINCT
        observed_at,
        observed_at::date AS date_value,
        EXTRACT(YEAR FROM observed_at)::int AS year_number,
        EXTRACT(QUARTER FROM observed_at)::int AS quarter_number,
        EXTRACT(MONTH FROM observed_at)::int AS month_number,
        TRIM(TO_CHAR(observed_at, 'Month')) AS month_name,
        CASE EXTRACT(MONTH FROM observed_at)::int
          WHEN 1 THEN '           Jan'
          WHEN 2 THEN '          Feb'
          WHEN 3 THEN '         Mar'
          WHEN 4 THEN '        Apr'
          WHEN 5 THEN '       May'
          WHEN 6 THEN '      Jun'
          WHEN 7 THEN '     Jul'
          WHEN 8 THEN '    Aug'
          WHEN 9 THEN '   Sep'
          WHEN 10 THEN '  Oct'
          WHEN 11 THEN ' Nov'
          ELSE 'Dec'
        END AS month_short,
        EXTRACT(DAY FROM observed_at)::int AS day_number,
        EXTRACT(HOUR FROM observed_at)::int AS hour_number,
        EXTRACT(WEEK FROM observed_at)::int AS iso_week_number,
        EXTRACT(ISODOW FROM observed_at)::int AS day_of_week_number,
        TRIM(TO_CHAR(observed_at, 'Dy')) AS day_name,
        CASE EXTRACT(ISODOW FROM observed_at)::int
          WHEN 1 THEN '      Mon'
          WHEN 2 THEN '     Tue'
          WHEN 3 THEN '    Wed'
          WHEN 4 THEN '   Thu'
          WHEN 5 THEN '  Fri'
          WHEN 6 THEN ' Sat'
          ELSE 'Sun'
        END AS day_short
      FROM {settings.measurement_table}
    )
    UPDATE {settings.airviro_mart_schema}.dim_datetime_hour AS target
    SET
      date_value = source.date_value,
      year_number = source.year_number,
      quarter_number = source.quarter_number,
      month_number = source.month_number,
      month_name = source.month_name,
      month_short = source.month_short,
      day_number = source.day_number,
      hour_number = source.hour_number,
      iso_week_number = source.iso_week_number,
      day_of_week_number = source.day_of_week_number,
      day_name = source.day_name,
      day_short = source.day_short
    FROM source_datetime AS source
    WHERE target.observed_at = source.observed_at
      AND (
        target.date_value IS DISTINCT FROM source.date_value
        OR target.year_number IS DISTINCT FROM source.year_number
        OR target.quarter_number IS DISTINCT FROM source.quarter_number
        OR target.month_number IS DISTINCT FROM source.month_number
        OR target.month_name IS DISTINCT FROM source.month_name
        OR target.month_short IS DISTINCT FROM source.month_short
        OR target.day_number IS DISTINCT FROM source.day_number
        OR target.hour_number IS DISTINCT FROM source.hour_number
        OR target.iso_week_number IS DISTINCT FROM source.iso_week_number
        OR target.day_of_week_number IS DISTINCT FROM source.day_of_week_number
        OR target.day_name IS DISTINCT FROM source.day_name
        OR target.day_short IS DISTINCT FROM source.day_short
      );
    """

    with connection.cursor() as cursor:
        cursor.execute(refresh_sql)
    connection.commit()


def log_ingestion_audit(
    connection: PgConnection,
    settings: Settings,
    *,
    batch_id: str,
    source_key: str,
    source_type: str,
    station_id: int,
    window_start: datetime,
    window_end: datetime,
    rows_read: int,
    records_upserted: int,
    duplicate_records: int,
    split_events: int,
    status: str,
    message: str | None = None,
) -> None:
    """Insert one ingestion-audit record."""

    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            INSERT INTO {settings.ingestion_audit_table} (
              batch_id,
              source_key,
              source_type,
              station_id,
              window_start,
              window_end,
              rows_read,
              records_upserted,
              duplicate_records,
              split_events,
              status,
              message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                batch_id,
                source_key,
                source_type,
                station_id,
                window_start,
                window_end,
                rows_read,
                records_upserted,
                duplicate_records,
                split_events,
                status,
                message,
            ),
        )
    connection.commit()
