FROM debian:bookworm-slim AS builder

ARG DEBIAN_FRONTEND=noninteractive
ARG UV_VERSION=0.11.2
ARG SUPERSET_VERSION=6.0.0
ENV VIRTUAL_ENV=/opt/superset-venv
ENV PATH="${VIRTUAL_ENV}/bin:/usr/local/bin:/usr/bin:/bin"
ENV UV_NO_CACHE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=0

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        python3 \
        python3-dev \
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
COPY requirements.lock /build/requirements.lock

RUN python3 -m venv "${VIRTUAL_ENV}" \
    && uv pip sync --python "${VIRTUAL_ENV}/bin/python" /build/requirements.lock

FROM debian:bookworm-slim AS runtime

ARG DEBIAN_FRONTEND=noninteractive
ARG SUPERSET_UID=1000
ARG SUPERSET_VERSION=6.0.0
ENV SUPERSET_HOME=/app/superset_home
ENV SUPERSET_PORT=8088
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
ENV FLASK_APP=superset
ENV HOME=/home/superset
ENV VIRTUAL_ENV=/opt/superset-venv
ENV PATH="${VIRTUAL_ENV}/bin:/usr/local/bin:/usr/bin:/bin"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        libpq5 \
        python3 \
    && rm -rf /var/lib/apt/lists/* \
    && echo "superset:x:${SUPERSET_UID}:0:Superset User:${HOME}:/bin/bash" >> /etc/passwd \
    && mkdir -p /app/pythonpath /app/superset_home /home/superset \
    && chown -R "${SUPERSET_UID}:0" /app /home/superset \
    && chmod -R g=u /app /home/superset

COPY --from=builder /opt/superset-venv /opt/superset-venv
COPY config/superset_config.py /app/pythonpath/superset_config.py
COPY scripts/entrypoint.sh /entrypoint

RUN ln -sf /opt/superset-venv/bin/superset /usr/local/bin/superset \
    && ln -sf /opt/superset-venv/bin/python /usr/local/bin/python \
    && chmod 0755 /entrypoint

LABEL org.opencontainers.image.title="course-superset-local" \
      org.opencontainers.image.description="Teaching-focused local Superset image built with uv and DuckDB support" \
      org.opencontainers.image.version="${SUPERSET_VERSION}"

WORKDIR /app
USER ${SUPERSET_UID}:0
ENTRYPOINT ["/entrypoint"]
CMD ["superset", "version"]
