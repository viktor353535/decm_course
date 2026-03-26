"""Simple Lecture 4 ETL script for Tartu air-quality data.

This module intentionally keeps the ETL flow in one file so students can read
`extract`, `transform`, and `load` top-to-bottom before moving to the more
advanced modular ETL package.
"""

from __future__ import annotations

from argparse import ArgumentParser
from datetime import datetime, timedelta
import html
import json
import os
from pathlib import Path
import re
from typing import Iterable
import unicodedata
from urllib import parse, request

try:
    import psycopg2
except ImportError as exc:  # pragma: no cover - runtime environment concern
    raise RuntimeError(
        "psycopg2 is required. Run inside the project virtualenv: .venv/bin/python ..."
    ) from exc


DEFAULT_API_BASE_URL = "https://www.ohuseire.ee/api"
DEFAULT_API_LOCALE = "en"
STATION_ID = 8
STATION_TYPE = "INDICATOR"
TARGET_SCHEMA = "l4_simple"
TARGET_TABLE = "air_quality_station_8_hourly"
TARGET_RELATION = f"{TARGET_SCHEMA}.{TARGET_TABLE}"
KNOWN_INDICATOR_CODE_ALIASES = {
    "temp10": "temp",
    "temp_10": "temp",
    "temperature_at_10_m": "temp",
}

TARGET_COLUMNS = (
    "so2",
    "no2",
    "co",
    "o3",
    "pm10",
    "pm2_5",
    "temp",
    "wd10",
    "ws10",
)


def load_env_file(path: Path) -> None:
    """Load KEY=VALUE pairs from the local .env file if present."""

    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip()


def parse_args() -> ArgumentParser:
    parser = ArgumentParser(
        description="Simple Lecture 4 ETL for Tartu air-quality data from the Ohuseire API"
    )
    parser.add_argument("--from", dest="from_date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--load-mode",
        required=True,
        choices=("replace", "update"),
        help="replace = truncate/reload, update = upsert by station_id + observed_at",
    )
    return parser


def format_api_date(value: datetime) -> str:
    """Format Python datetime to the Ohuseire date parameter style."""

    return value.strftime("%d.%m.%Y")


def source_api_base_url() -> str:
    """Return the source API base URL, honoring local `.env` overrides."""

    return os.getenv("AIRVIRO_BASE_URL", DEFAULT_API_BASE_URL).strip()


def source_api_locale() -> str:
    """Return the source API locale, honoring local `.env` overrides."""

    return os.getenv("AIRVIRO_API_LOCALE", DEFAULT_API_LOCALE).strip() or DEFAULT_API_LOCALE


def build_api_url(endpoint: str, params: dict[str, str] | None = None) -> str:
    """Build one Ohuseire API URL."""

    url = f"{source_api_base_url().rstrip('/')}/{endpoint.lstrip('/')}"
    if params:
        url = f"{url}?{parse.urlencode(params)}"
    return url


def fetch_json(endpoint: str, params: dict[str, str] | None = None) -> object:
    """Fetch one JSON payload from the Ohuseire API."""

    url = build_api_url(endpoint, params)
    with request.urlopen(url, timeout=45) as response:
        return json.loads(response.read().decode("utf-8-sig"))


def strip_html_tags(raw_value: str | None) -> str:
    """Convert HTML-rich API fields into readable plain text."""

    if not raw_value:
        return ""

    without_tags = re.sub(r"<[^>]*>", "", raw_value)
    cleaned = without_tags.replace("<", "").replace(">", "")
    return html.unescape(cleaned).strip()


def normalize_indicator_code(raw_name: str) -> str:
    """Create stable warehouse-friendly indicator codes."""

    normalized = unicodedata.normalize("NFKD", raw_name)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_name = ascii_name.replace("%", "pct").replace(".", "_")
    ascii_name = re.sub(r"[^a-zA-Z0-9]+", "_", ascii_name).strip("_").lower()
    if not ascii_name:
        return "unknown_indicator"
    if ascii_name[0].isdigit():
        return f"i_{ascii_name}"
    return KNOWN_INDICATOR_CODE_ALIASES.get(ascii_name, ascii_name)


def parse_localized_numeric(raw_value: str) -> float | None:
    """Parse numeric values like `0,5`, `3 061`, or empty strings."""

    value = raw_value.strip().strip('"')
    if value in {"", "-", "NA", "N/A", "null", "NULL"}:
        return None

    compact = value.replace("\u00a0", "").replace("\u202f", "").replace(" ", "")
    compact = compact.replace(",", ".")
    return float(compact)


def discover_station_metadata() -> dict[str, object]:
    """Discover station 8 from the station metadata endpoint."""

    payload = fetch_json(f"station/{source_api_locale()}")
    if not isinstance(payload, dict) or not isinstance(payload.get("features"), list):
        raise ValueError("Station API returned an unexpected payload")

    for feature in payload["features"]:
        if not isinstance(feature, dict) or int(feature.get("id", -1)) != STATION_ID:
            continue
        properties = feature.get("properties") or {}
        if not isinstance(properties, dict):
            continue
        return {
            "station_id": STATION_ID,
            "station_name": str(properties.get("name") or f"station_{STATION_ID}"),
            "station_type": str(properties.get("type") or ""),
            "airviro_code": str(properties.get("airviro_code") or ""),
            "indicator_ids": tuple(int(value) for value in properties.get("indicators") or []),
        }

    raise ValueError(f"Station {STATION_ID} was not found in the station API")


def discover_indicator_metadata() -> dict[int, dict[str, str]]:
    """Discover indicator definitions for air-quality measurements."""

    payload = fetch_json(f"indicator/{source_api_locale()}", {"type": STATION_TYPE})
    if not isinstance(payload, list):
        raise ValueError("Indicator API returned an unexpected payload")

    metadata: dict[int, dict[str, str]] = {}
    for item in payload:
        if not isinstance(item, dict) or "id" not in item:
            continue

        formula_text = strip_html_tags(str(item.get("formula") or ""))
        name_text = strip_html_tags(str(item.get("name") or ""))
        code_seed = formula_text or name_text or f"indicator_{item['id']}"
        metadata[int(item["id"])] = {
            "code": normalize_indicator_code(code_seed),
            "name": name_text or formula_text or code_seed,
        }
    return metadata


def should_normalize_staggered_rows(
    measurements: list[dict[str, object]],
    indicator_ids: tuple[int, ...],
    from_date: str,
    to_date: str,
) -> bool:
    """Detect older API payloads whose timestamps are shifted by indicator order."""

    if not measurements or not indicator_ids:
        return False

    start = datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.strptime(to_date, "%Y-%m-%d")
    expected_slots = ((end - start).days + 1) * 24
    distinct_timestamps = {row["measured_at"] for row in measurements}
    return len(distinct_timestamps) > expected_slots


def normalize_staggered_rows(
    measurements: list[dict[str, object]],
    indicator_ids: tuple[int, ...],
) -> list[dict[str, object]]:
    """Shift older staggered timestamps back to clean hourly timestamps."""

    indicator_offsets = {
        indicator_id: index + 1 for index, indicator_id in enumerate(indicator_ids)
    }
    normalized: list[dict[str, object]] = []
    for row in measurements:
        offset_hours = indicator_offsets.get(int(row["indicator_id"]))
        if offset_hours is None:
            return measurements

        normalized.append(
            {
                **row,
                "measured_at": row["measured_at"] - timedelta(hours=offset_hours),
            }
        )
    return normalized


def extract(from_date: str, to_date: str) -> dict[str, object]:
    """Extract Tartu air-quality monitoring rows plus API metadata."""

    start = datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.strptime(to_date, "%Y-%m-%d")
    station = discover_station_metadata()
    indicators = discover_indicator_metadata()
    params = {
        "stations": str(STATION_ID),
        "type": STATION_TYPE,
        "range": ",".join([format_api_date(start), format_api_date(end)]),
        "indicators": ",".join(str(value) for value in station["indicator_ids"]),
    }
    payload = fetch_json(f"monitoring/{source_api_locale()}", params)
    if not isinstance(payload, list):
        raise ValueError("Monitoring API returned an unexpected payload")

    return {
        "station": station,
        "indicators": indicators,
        "measurements": payload,
    }


def transform(extracted: dict[str, object], from_date: str, to_date: str) -> list[dict[str, object]]:
    """Transform API monitoring rows into wide hourly warehouse rows."""

    station = extracted["station"]
    indicators = extracted["indicators"]
    raw_measurements = extracted["measurements"]

    if not isinstance(station, dict) or not isinstance(indicators, dict):
        raise ValueError("Extracted payload is missing station or indicator metadata")
    if not isinstance(raw_measurements, list):
        raise ValueError("Extracted payload is missing monitoring rows")

    parsed_measurements: list[dict[str, object]] = []
    for index, item in enumerate(raw_measurements, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Monitoring row {index} is not an object")

        measured_at = datetime.strptime(str(item.get("measured") or ""), "%Y-%m-%d %H:%M:%S")
        indicator_id = int(item.get("indicator"))
        value_raw = "" if item.get("value") is None else str(item.get("value"))
        value_numeric = parse_localized_numeric(value_raw)

        parsed_measurements.append(
            {
                "station_id": int(item.get("station")),
                "measured_at": measured_at,
                "indicator_id": indicator_id,
                "value_numeric": value_numeric,
            }
        )

    if should_normalize_staggered_rows(
        parsed_measurements,
        station["indicator_ids"],
        from_date,
        to_date,
    ):
        parsed_measurements = normalize_staggered_rows(
            parsed_measurements,
            station["indicator_ids"],
        )

    rows_by_timestamp: dict[datetime, dict[str, object]] = {}
    for measurement in parsed_measurements:
        if measurement["station_id"] != STATION_ID:
            raise ValueError(
                f"API returned station {measurement['station_id']} instead of station {STATION_ID}"
            )

        metadata = indicators.get(int(measurement["indicator_id"]))
        if metadata is None:
            raise ValueError(
                f"Indicator {measurement['indicator_id']} was not found in the indicator API"
            )

        indicator_code = metadata["code"]
        if indicator_code not in TARGET_COLUMNS:
            continue

        observed_at = measurement["measured_at"]
        row = rows_by_timestamp.setdefault(
            observed_at,
            {
                "station_id": STATION_ID,
                "observed_at": observed_at,
                **{column: None for column in TARGET_COLUMNS},
            },
        )
        row[indicator_code] = measurement["value_numeric"]

    rows = list(rows_by_timestamp.values())
    rows.sort(key=lambda item: item["observed_at"])
    return rows


def build_db_config() -> dict[str, object]:
    """Read warehouse DB connection settings from the local .env."""

    return {
        "host": os.getenv("WAREHOUSE_DB_HOST", "postgres"),
        "port": int(os.getenv("WAREHOUSE_DB_PORT", "5432")),
        "dbname": os.getenv("WAREHOUSE_DB_NAME", "warehouse"),
        "user": os.getenv("WAREHOUSE_DB_USER", "warehouse"),
        "password": os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse"),
    }


def ensure_target_table(cursor: psycopg2.extensions.cursor) -> None:
    """Create the lecture 4 simple ETL schema/table if needed."""

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_RELATION} (
          station_id integer NOT NULL,
          observed_at timestamp without time zone NOT NULL,
          so2 double precision,
          no2 double precision,
          co double precision,
          o3 double precision,
          pm10 double precision,
          pm2_5 double precision,
          temp double precision,
          wd10 double precision,
          ws10 double precision,
          loaded_at timestamp with time zone NOT NULL DEFAULT now(),
          CONSTRAINT air_quality_station_8_hourly_pk
            PRIMARY KEY (station_id, observed_at)
        )
        """
    )


def load(rows: Iterable[dict[str, object]], load_mode: str) -> int:
    """Load transformed rows into the lecture 4 simple ETL table."""

    payload = [
        (
            row["station_id"],
            row["observed_at"],
            row["so2"],
            row["no2"],
            row["co"],
            row["o3"],
            row["pm10"],
            row["pm2_5"],
            row["temp"],
            row["wd10"],
            row["ws10"],
        )
        for row in rows
    ]

    conn = psycopg2.connect(**build_db_config())
    try:
        with conn.cursor() as cursor:
            ensure_target_table(cursor)

            if load_mode == "replace":
                cursor.execute(f"TRUNCATE TABLE {TARGET_RELATION}")

            cursor.executemany(
                f"""
                INSERT INTO {TARGET_RELATION} (
                  station_id,
                  observed_at,
                  so2,
                  no2,
                  co,
                  o3,
                  pm10,
                  pm2_5,
                  temp,
                  wd10,
                  ws10
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (station_id, observed_at)
                DO UPDATE SET
                  so2 = EXCLUDED.so2,
                  no2 = EXCLUDED.no2,
                  co = EXCLUDED.co,
                  o3 = EXCLUDED.o3,
                  pm10 = EXCLUDED.pm10,
                  pm2_5 = EXCLUDED.pm2_5,
                  temp = EXCLUDED.temp,
                  wd10 = EXCLUDED.wd10,
                  ws10 = EXCLUDED.ws10,
                  loaded_at = now()
                """,
                payload,
            )
        conn.commit()
    finally:
        conn.close()

    return len(payload)


def main() -> int:
    load_env_file(Path(".env"))
    parser = parse_args()
    args = parser.parse_args()

    print("=== Lecture 4 Simple ETL ===")
    print(f"Source API: {source_api_base_url()}")
    print("Discovered source: Tartu air-quality station 8")
    print(f"Target table: {TARGET_RELATION}")
    print(f"Window: {args.from_date}..{args.to_date}")
    print(f"Load mode: {args.load_mode}")

    extracted = extract(args.from_date, args.to_date)
    rows = transform(extracted, args.from_date, args.to_date)
    loaded = load(rows, args.load_mode)

    print(f"Extracted monitoring rows: {len(extracted['measurements'])}")
    print(f"Transformed hourly rows: {len(rows)}")
    print(f"Loaded rows: {loaded}")
    if rows:
        print(f"First observed_at: {rows[0]['observed_at']}")
        print(f"Last observed_at: {rows[-1]['observed_at']}")
    print("=== Simple ETL Complete ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
