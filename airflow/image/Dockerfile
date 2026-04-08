FROM debian:bookworm-slim AS builder

ARG DEBIAN_FRONTEND=noninteractive
ARG UV_VERSION=0.11.2
ENV AIRFLOW_VIRTUAL_ENV=/opt/airflow-venv
ENV DBT_VIRTUAL_ENV=/opt/dbt-venv
ENV VIRTUAL_ENV=/opt/airflow-venv
ENV PATH="${DBT_VIRTUAL_ENV}/bin:${AIRFLOW_VIRTUAL_ENV}/bin:/usr/local/bin:/usr/bin:/bin"
ENV UV_NO_CACHE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=0

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        python3 \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN arch="$(dpkg --print-architecture)" \
    && case "${arch}" in \
        amd64) uv_arch="x86_64-unknown-linux-gnu" ;; \
        arm64) uv_arch="aarch64-unknown-linux-gnu" ;; \
        *) echo "Unsupported architecture: ${arch}" >&2; exit 1 ;; \
    esac \
    && curl -fsSL -o /tmp/uv.tar.gz "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-${uv_arch}.tar.gz" \
    && tar -xzf /tmp/uv.tar.gz -C /tmp \
    && install -m 0755 /tmp/uv-*/uv /usr/local/bin/uv \
    && install -m 0755 /tmp/uv-*/uvx /usr/local/bin/uvx \
    && rm -rf /tmp/uv.tar.gz /tmp/uv-*

WORKDIR /build
COPY airflow-requirements.lock dbt-requirements.lock /build/

RUN python3 -m venv "${AIRFLOW_VIRTUAL_ENV}" \
    && uv pip sync --python "${AIRFLOW_VIRTUAL_ENV}/bin/python" /build/airflow-requirements.lock \
    && python3 -m venv "${DBT_VIRTUAL_ENV}" \
    && uv pip sync --python "${DBT_VIRTUAL_ENV}/bin/python" /build/dbt-requirements.lock

FROM debian:bookworm-slim AS runtime

ARG AIRFLOW_UID=50000
ARG AIRFLOW_VERSION=3.1.8
ARG DBT_VERSION=1.9.1
ARG DEBIAN_FRONTEND=noninteractive
ENV AIRFLOW_HOME=/opt/airflow
ENV HOME=/home/airflow
ENV AIRFLOW_VIRTUAL_ENV=/opt/airflow-venv
ENV DBT_VIRTUAL_ENV=/opt/dbt-venv
ENV VIRTUAL_ENV=/opt/airflow-venv
ENV PATH="${DBT_VIRTUAL_ENV}/bin:${AIRFLOW_VIRTUAL_ENV}/bin:/usr/local/bin:/usr/bin:/bin"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        git \
        libpq5 \
        python3 \
    && rm -rf /var/lib/apt/lists/* \
    && echo "airflow:x:${AIRFLOW_UID}:0:Airflow User:${HOME}:/bin/bash" >> /etc/passwd \
    && mkdir -p /home/airflow /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins /opt/airflow/config \
    && chown -R "${AIRFLOW_UID}:0" /opt/airflow /home/airflow \
    && chmod -R g=u /opt/airflow /home/airflow

COPY --from=builder /opt/airflow-venv /opt/airflow-venv
COPY --from=builder /opt/dbt-venv /opt/dbt-venv
COPY scripts/entrypoint.sh /entrypoint

RUN ln -sf /opt/airflow-venv/bin/airflow /usr/local/bin/airflow \
    && ln -sf /opt/airflow-venv/bin/python /usr/local/bin/python \
    && ln -sf /opt/dbt-venv/bin/dbt /usr/local/bin/dbt \
    && chmod 0755 /entrypoint

LABEL org.opencontainers.image.title="course-airflow-local" \
      org.opencontainers.image.description="Teaching-focused local Airflow image built with uv-managed virtual environments" \
      org.opencontainers.image.version="${AIRFLOW_VERSION}-dbt${DBT_VERSION}"

WORKDIR /opt/airflow
USER ${AIRFLOW_UID}:0
ENTRYPOINT ["/entrypoint"]
CMD ["airflow", "version"]
