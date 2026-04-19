#!/usr/bin/env bash
set -euo pipefail

SUPERSET_COMMAND="${1:-}"
SUPERSET_HOME="${SUPERSET_HOME:-/app/superset_home}"
SUPERSET_PORT="${SUPERSET_PORT:-8088}"
export SUPERSET_HOME

mkdir -p "${SUPERSET_HOME}"
if [[ "$(id -u)" == "0" ]]; then
    chown -R "${SUPERSET_UID:-1000}:0" "${SUPERSET_HOME}" /app /home/superset
fi
umask 0002

bootstrap_superset() {
    local admin_password="${SUPERSET_ADMIN_PASSWORD:-${ADMIN_PASSWORD:-}}"

    superset db upgrade

    if [[ -n "${admin_password}" ]]; then
        superset fab create-admin \
            --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
            --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Superset}" \
            --lastname "${SUPERSET_ADMIN_LASTNAME:-Admin}" \
            --email "${SUPERSET_ADMIN_EMAIL:-admin@example.com}" \
            --password "${admin_password}" || true
    fi

    superset init
}

if [[ "${SUPERSET_COMMAND}" == "bash" ]]; then
    shift
    exec /bin/bash "$@"
fi

if [[ "${SUPERSET_COMMAND}" == "python" ]]; then
    shift
    exec python "$@"
fi

if [[ "${SUPERSET_COMMAND}" == "superset" ]]; then
    shift
    exec superset "$@"
fi

if [[ "${SUPERSET_COMMAND}" == "bootstrap" ]]; then
    shift
    bootstrap_superset "$@"
    exit 0
fi

if [[ "${SUPERSET_COMMAND}" == "run-server" ]]; then
    shift
    exec superset run -h 0.0.0.0 -p "${SUPERSET_PORT}" --with-threads "$@"
fi

exec superset "$@"
