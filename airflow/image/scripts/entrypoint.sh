#!/usr/bin/env bash
set -euo pipefail

AIRFLOW_COMMAND="${1:-}"
AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow}"
CONNECTION_CHECK_MAX_COUNT="${CONNECTION_CHECK_MAX_COUNT:-20}"
CONNECTION_CHECK_SLEEP_TIME="${CONNECTION_CHECK_SLEEP_TIME:-3}"
export AIRFLOW_HOME

mkdir -p "${AIRFLOW_HOME}/dags" "${AIRFLOW_HOME}/logs" "${AIRFLOW_HOME}/plugins" "${AIRFLOW_HOME}/config"
umask 0002

wait_for_airflow_db() {
    local countdown="${CONNECTION_CHECK_MAX_COUNT}"
    while true; do
        if airflow db check >/dev/null 2>&1; then
            break
        fi
        countdown=$((countdown - 1))
        if [[ "${countdown}" -le 0 ]]; then
            echo "ERROR: airflow db check did not succeed before timeout." >&2
            return 1
        fi
        sleep "${CONNECTION_CHECK_SLEEP_TIME}"
    done
}

create_www_user() {
    local password="${_AIRFLOW_WWW_USER_PASSWORD:-}"
    if [[ -z "${password}" ]]; then
        echo "ERROR: _AIRFLOW_WWW_USER_PASSWORD must be set when _AIRFLOW_WWW_USER_CREATE is enabled." >&2
        return 1
    fi

    if airflow config get-value core auth_manager | grep -q "FabAuthManager"; then
        airflow users create \
            --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
            --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME:-Airflow}" \
            --lastname "${_AIRFLOW_WWW_USER_LASTNAME:-Admin}" \
            --email "${_AIRFLOW_WWW_USER_EMAIL:-airflowadmin@example.com}" \
            --role "${_AIRFLOW_WWW_USER_ROLE:-Admin}" \
            --password "${password}" || true
    fi
}

if [[ "${AIRFLOW_COMMAND}" == "bash" ]]; then
    shift
    exec /bin/bash "$@"
fi

if [[ "${AIRFLOW_COMMAND}" == "python" ]]; then
    shift
    exec python "$@"
fi

if [[ "${AIRFLOW_COMMAND}" == "dbt" ]]; then
    shift
    exec dbt "$@"
fi

if [[ "${AIRFLOW_COMMAND}" == "airflow" ]]; then
    shift
    AIRFLOW_COMMAND="${1:-}"
fi

if [[ "${CONNECTION_CHECK_MAX_COUNT}" -gt 0 ]] && [[ "${AIRFLOW_COMMAND}" != "" ]] && [[ "${AIRFLOW_COMMAND}" != "version" ]]; then
    wait_for_airflow_db
fi

if [[ -n "${_AIRFLOW_DB_MIGRATE:-}" ]]; then
    airflow db migrate || true
fi

if [[ -n "${_AIRFLOW_WWW_USER_CREATE:-}" ]]; then
    create_www_user
fi

if [[ "$#" -eq 0 ]] && [[ "${_AIRFLOW_DB_MIGRATE:-}" == "true" ]]; then
    exit 0
fi

exec airflow "$@"
