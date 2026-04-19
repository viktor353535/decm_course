"""Minimal Superset configuration for the teaching stack.

The local stack keeps Superset metadata in PostgreSQL by default, but callers
can still override the URI explicitly when needed.
"""

from __future__ import annotations

import os
from pathlib import Path


SUPERSET_HOME = os.getenv("SUPERSET_HOME", "/app/superset_home")
Path(SUPERSET_HOME).mkdir(parents=True, exist_ok=True)

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "not-for-production-change-me")

default_database_uri = (
    "postgresql+psycopg2://"
    f"{os.getenv('SUPERSET_DB_USER', 'superset')}:"
    f"{os.getenv('SUPERSET_DB_PASSWORD', 'superset')}@"
    f"{os.getenv('SUPERSET_DB_HOST', os.getenv('POSTGRES_HOST', 'postgres'))}:"
    f"{os.getenv('SUPERSET_DB_PORT', os.getenv('POSTGRES_PORT', '5432'))}/"
    f"{os.getenv('SUPERSET_DB_NAME', 'superset_meta')}"
)

SQLALCHEMY_DATABASE_URI = os.getenv(
    "SUPERSET_SQLALCHEMY_DATABASE_URI",
    default_database_uri,
)

# Keep the local stack predictable and lightweight.
ROW_LIMIT = int(os.getenv("SUPERSET_ROW_LIMIT", "5000"))
