#!/usr/bin/env bash
set -euo pipefail

extension_available="$(
  psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" -Atqc \
    "select 1 from pg_available_extensions where name = 'pg_duckdb' limit 1"
)"

if [ "${extension_available:-0}" != "1" ]; then
  echo "pg_duckdb extension is not available in this image; skipping DuckDB bootstrap."
  exit 0
fi

current_preload="$(
  psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" -Atqc \
    "show shared_preload_libraries"
)"

if [[ ",${current_preload}," != *",pg_duckdb,"* ]]; then
  next_preload="pg_duckdb"
  if [ -n "${current_preload}" ]; then
    next_preload="${current_preload},pg_duckdb"
  fi

  psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<EOSQL
ALTER SYSTEM SET shared_preload_libraries = '${next_preload}';
EOSQL

  echo "Configured shared_preload_libraries='${next_preload}'. Restart Postgres, then rerun this script."
  exit 0
fi

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<EOSQL
ALTER SYSTEM SET duckdb.postgres_role = '${WAREHOUSE_DB_USER}';
EOSQL

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${WAREHOUSE_DB_NAME}" <<EOSQL
CREATE EXTENSION IF NOT EXISTS pg_duckdb;
GRANT pg_read_server_files TO ${WAREHOUSE_DB_USER};
GRANT pg_write_server_files TO ${WAREHOUSE_DB_USER};
EOSQL

echo "pg_duckdb bootstrap complete for database '${WAREHOUSE_DB_NAME}'. Restart Postgres to activate duckdb.postgres_role."
