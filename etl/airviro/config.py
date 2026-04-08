"""Configuration and environment helpers for the Ohuseire ETL pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import re
import socket
import subprocess


def load_env_file(path: Path) -> None:
    """Load KEY=VALUE pairs from an .env file if present.

    Existing environment variables are kept as-is to allow runtime overrides.
    """

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


def _env_first(names: tuple[str, ...], default: str) -> str:
    """Return the first non-empty environment value from a preferred name list."""

    for name in names:
        raw = os.getenv(name)
        if raw is not None and raw.strip():
            return raw.strip()
    return default


def _as_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    return int(raw)


def _as_int_first(names: tuple[str, ...], default: int) -> int:
    return int(_env_first(names, str(default)))


def _as_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, str(default)).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _as_bool_first(names: tuple[str, ...], default: bool) -> bool:
    raw = _env_first(names, str(default)).lower()
    return raw in {"1", "true", "yes", "on"}


def _as_int_tuple(name: str, default: tuple[int, ...]) -> tuple[int, ...]:
    """Parse comma-separated integer env values into a stable tuple."""

    raw = os.getenv(name, "").strip()
    if not raw:
        return default

    values: list[int] = []
    seen: set[int] = set()
    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        value = int(item)
        if value in seen:
            continue
        seen.add(value)
        values.append(value)

    if not values:
        return default
    return tuple(values)


def _as_int_tuple_first(names: tuple[str, ...], default: tuple[int, ...]) -> tuple[int, ...]:
    """Parse the first configured comma-separated integer env value into a stable tuple."""

    raw = _env_first(names, "").strip()
    if not raw:
        return default

    values: list[int] = []
    seen: set[int] = set()
    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        value = int(item)
        if value in seen:
            continue
        seen.add(value)
        values.append(value)

    if not values:
        return default
    return tuple(values)


def _as_identifier(name: str, default: str) -> str:
    """Parse a SQL identifier-like env value and reject unsafe values."""

    value = os.getenv(name, default).strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(
            f"{name} must be a simple SQL identifier (letters, digits, underscore)"
        )
    return value


def _as_identifier_first(names: tuple[str, ...], default: str) -> str:
    """Parse the first configured SQL identifier-like env value and reject unsafe values."""

    value = _env_first(names, default)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(
            f"{names[0]} must be a simple SQL identifier (letters, digits, underscore)"
        )
    return value


@dataclass(frozen=True)
class Settings:
    """Runtime configuration for extraction and loading."""

    airviro_base_url: str
    airviro_locale: str
    air_station_ids: tuple[int, ...]
    pollen_station_ids: tuple[int, ...]
    request_timeout_seconds: int
    request_retries: int
    minimum_split_window_days: int
    air_quality_window_days: int
    pollen_window_days: int
    warehouse_db_name: str
    warehouse_db_user: str
    warehouse_db_password: str
    warehouse_db_port: int
    warehouse_db_host: str
    airviro_raw_schema: str
    airviro_mart_schema: str
    measurement_table_name: str
    ingestion_audit_table_name: str
    airviro_refresh_mart_dimensions: bool

    @classmethod
    def from_env(cls) -> "Settings":
        air_station_ids = _as_int_tuple_first(
            ("OHUSEIRE_AIR_STATION_IDS", "AIRVIRO_AIR_STATION_IDS"),
            (8,),
        )
        if not air_station_ids:
            air_station_ids = (
                _as_int_first(("OHUSEIRE_AIR_STATION_ID", "AIRVIRO_AIR_STATION_ID"), 8),
            )

        pollen_station_ids = _as_int_tuple_first(
            ("OHUSEIRE_POLLEN_STATION_IDS", "AIRVIRO_POLLEN_STATION_IDS"),
            (25,),
        )
        if not pollen_station_ids:
            pollen_station_ids = (
                _as_int_first(
                    ("OHUSEIRE_POLLEN_STATION_ID", "AIRVIRO_POLLEN_STATION_ID"),
                    25,
                ),
            )

        return cls(
            airviro_base_url=_env_first(
                ("OHUSEIRE_BASE_URL", "AIRVIRO_BASE_URL"),
                "https://www.ohuseire.ee/api",
            ),
            airviro_locale=_env_first(
                ("OHUSEIRE_API_LOCALE", "AIRVIRO_API_LOCALE"),
                "en",
            ),
            air_station_ids=air_station_ids,
            pollen_station_ids=pollen_station_ids,
            request_timeout_seconds=_as_int_first(
                ("OHUSEIRE_TIMEOUT_SECONDS", "AIRVIRO_TIMEOUT_SECONDS"),
                45,
            ),
            request_retries=_as_int_first(
                ("OHUSEIRE_REQUEST_RETRIES", "AIRVIRO_REQUEST_RETRIES"),
                3,
            ),
            minimum_split_window_days=_as_int_first(
                ("OHUSEIRE_MIN_SPLIT_DAYS", "AIRVIRO_MIN_SPLIT_DAYS"),
                7,
            ),
            air_quality_window_days=_as_int_first(
                ("OHUSEIRE_AIR_WINDOW_DAYS", "AIRVIRO_AIR_WINDOW_DAYS"),
                300,
            ),
            pollen_window_days=_as_int_first(
                ("OHUSEIRE_POLLEN_WINDOW_DAYS", "AIRVIRO_POLLEN_WINDOW_DAYS"),
                365,
            ),
            warehouse_db_name=os.getenv("WAREHOUSE_DB_NAME", "warehouse").strip(),
            warehouse_db_user=os.getenv("WAREHOUSE_DB_USER", "warehouse").strip(),
            warehouse_db_password=os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse").strip(),
            warehouse_db_port=_as_int("WAREHOUSE_DB_PORT", 5432),
            warehouse_db_host=os.getenv("WAREHOUSE_DB_HOST", "postgres").strip(),
            airviro_raw_schema=_as_identifier_first(
                ("OHUSEIRE_RAW_SCHEMA", "AIRVIRO_RAW_SCHEMA"),
                "l4_raw",
            ),
            airviro_mart_schema=_as_identifier_first(
                ("OHUSEIRE_MART_SCHEMA", "AIRVIRO_MART_SCHEMA"),
                "l4_mart",
            ),
            measurement_table_name=_as_identifier_first(
                ("OHUSEIRE_MEASUREMENT_TABLE", "AIRVIRO_MEASUREMENT_TABLE"),
                "airviro_measurement",
            ),
            ingestion_audit_table_name=_as_identifier_first(
                ("OHUSEIRE_INGESTION_AUDIT_TABLE", "AIRVIRO_INGESTION_AUDIT_TABLE"),
                "airviro_ingestion_audit",
            ),
            airviro_refresh_mart_dimensions=_as_bool_first(
                ("OHUSEIRE_REFRESH_MART_DIMENSIONS", "AIRVIRO_REFRESH_MART_DIMENSIONS"),
                True,
            ),
        )

    @property
    def air_station_id(self) -> int:
        """Backward-compatible first air station id accessor."""

        return self.air_station_ids[0]

    @property
    def pollen_station_id(self) -> int:
        """Backward-compatible first pollen station id accessor."""

        return self.pollen_station_ids[0]

    @property
    def measurement_table(self) -> str:
        """Fully qualified raw measurement table name for the current runtime."""

        return f"{self.airviro_raw_schema}.{self.measurement_table_name}"

    @property
    def ingestion_audit_table(self) -> str:
        """Fully qualified ingestion-audit table name for the current runtime."""

        return f"{self.airviro_raw_schema}.{self.ingestion_audit_table_name}"

    @property
    def pipeline_watermark_table(self) -> str:
        """Fully qualified pipeline-watermark table name for the current runtime."""

        return f"{self.airviro_raw_schema}.pipeline_watermark"

    def candidate_db_hosts(self) -> list[str]:
        """Candidate hostnames to support both devcontainer and compose contexts."""

        candidates: list[str] = []

        for host in self.warehouse_db_host.split(","):
            normalized = host.strip()
            if normalized and normalized not in candidates:
                candidates.append(normalized)

        gateway = _default_gateway_ip()
        fallback_hosts = [
            "postgres",
            "host.docker.internal",
            "localhost",
            "127.0.0.1",
        ]
        if gateway:
            fallback_hosts.insert(2, gateway)

        for fallback in fallback_hosts:
            if fallback not in candidates:
                candidates.append(fallback)

        # If the devcontainer is attached to compose network, use service DNS first.
        candidates = _promote_if_resolves(candidates, "postgres")

        return candidates


def _default_gateway_ip() -> str | None:
    """Read default gateway IP from container routing table if available."""

    try:
        output = subprocess.check_output(
            ["ip", "-4", "route", "show", "default"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:  # pragma: no cover - environment dependent
        return None

    parts = output.split()
    if "via" not in parts:
        return None
    via_index = parts.index("via")
    if via_index + 1 >= len(parts):
        return None
    return parts[via_index + 1].strip()


def _promote_if_resolves(hosts: list[str], preferred_host: str) -> list[str]:
    """Move host to the front if DNS resolution works in current runtime."""

    try:
        socket.getaddrinfo(preferred_host, None)
    except OSError:
        return hosts

    return [preferred_host] + [host for host in hosts if host != preferred_host]
