# DECM Course Workspace (Lectures 3-5) test

This repository is the hands-on workspace for lectures 3, 4, and 5.

Read and execute in order:
1. Lecture 3: VS Code, Docker, Git, devcontainers
2. Lecture 4: Python ETL + Superset basics
3. Lecture 5: Airflow + dbt orchestration

## 1) Start Here (Lecture 3)

Primary guide:
- `docs/lectures/lecture-03/README.md`

Topic guides:
- `docs/lectures/lecture-03/vscode.md`
- `docs/lectures/lecture-03/docker.md`
- `docs/lectures/lecture-03/git.md`
- `docs/lectures/lecture-03/git-troubleshooting.md`
- `docs/lectures/lecture-03/devcontainers.md`
- `docs/lectures/lecture-03/troubleshooting.md`

Class bootstrap (inside devcontainer):

```bash
make init
make up-superset
```

Open Superset:
- URL: <http://localhost:8088>
- username: `admin`
- password: `admin`

## 2) Continue With Lecture 4 (ETL + Superset)

Primary guide:
- `docs/lectures/lecture-04/README.md`

Operational notes:
- `docs/lectures/lecture-04/operations.md`

Recommended setup:

```bash
make up-superset
make devcontainer-join-course-network
```

Note: keep Airflow stopped during Lecture 4 to avoid background orchestration side effects.
Airflow is started in Lecture 5.

Core ETL flow:

```bash
make etl-bootstrap
make etl-backfill-2020-2025
make warehouse-status
```

Optional verbose run:

```bash
make etl-backfill-2020-2025 VERBOSE=1
```

Superset SQL helper snippets:
- `superset/snippets.md`

## 3) Continue With Lecture 5 (Airflow + dbt)

Primary guide:
- `docs/lectures/lecture-05/README.md`

Operational notes:
- `docs/lectures/lecture-05/operations.md`

Core commands:

```bash
make up-airflow
make dbt-build
make airflow-unpause-dags
make airflow-trigger-incremental
```

Open Airflow:
- URL: <http://localhost:8080>
- username: `airflow`
- password: `airflow`

## Stack Lifecycle (Quick Reference)

Start services:

```bash
make up-superset
make up-airflow
make up-all
```

Inspect services:

```bash
make ps
make logs SERVICE=superset
make logs SERVICE=airflow-scheduler
make logs SERVICE=postgres
```

Stop and reset:

```bash
make down
make reset-volumes
make reset-all
```

## Environment Notes

- Work from inside the devcontainer.
- Host requirements: VS Code, Docker, Git.
- `.env` is local-only and ignored by git.
- `.env.example` is the template used by `make init`.
- Docker runs through the mounted host socket, so bind mount paths must be valid on the host.

## Lecture Index

- `docs/lectures/index.md`
