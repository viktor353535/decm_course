# Docker Guide (Lecture 3)

## What Docker Is

Docker is a way to package and run applications in containers with predictable dependencies.

Core terms:

1. Image: immutable template (for example `postgres:16`)
2. Container: running instance of an image
3. Volume: Docker-managed persistent storage
4. Bind mount: host path mounted into a container
5. Network: communication layer between containers
6. Compose: tool to run multi-container apps from one YAML file

## What Docker Is Not

1. Not a full virtual machine replacement.
2. Not automatic persistence unless you use volumes/mounts.
3. Not a security boundary by default.
4. Not a replacement for backups.

Reference:

- Docker container fundamentals: <https://docs.docker.com/get-started/docker-concepts/the-basics/what-is-a-container/>

## Core Commands

Pull and run a demo container:

```bash
docker pull hello-world
docker run --rm hello-world
```

Run a background web container:

```bash
docker run -d --name nginx-demo -p 8081:80 nginx:stable
docker ps
docker logs nginx-demo
docker stop nginx-demo
docker rm nginx-demo
```

## Dockerfile Basics

Minimal example:

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY . /app
RUN useradd -m appuser
USER appuser
CMD ["python", "--version"]
```

Build and run:

```bash
docker build -t demo-python .
docker run --rm demo-python
```

## Docker Compose Basics

Compose lets you define multiple services, networks, and volumes in one file.

In this repository you already use:

```bash
make up-superset
make up-airflow
make up-all
```

Direct equivalent:

```bash
docker compose --profile superset --profile airflow up -d
```

Inside this repository, prefer the `make up-*` targets from the devcontainer.
They resolve the workspace bind-mount path for the host Docker daemon, which matters on Windows-hosted setups.
For debugging, run `make print-host-workspace` to inspect the final path passed to Compose.

References:

- Compose overview: <https://docs.docker.com/compose/>
- Compose quickstart: <https://docs.docker.com/compose/gettingstarted/>
- Legacy standalone compose note: <https://docs.docker.com/compose/install/standalone/>

## Bind Mounts vs Volumes

Use bind mounts when:

1. You want live code editing from host/editor.
2. You need to inspect generated files directly in the project folder.

Use named volumes when:

1. You need persistent application/database state.
2. You do not need direct host-side editing.

References:

- Bind mounts: <https://docs.docker.com/engine/storage/bind-mounts/>
- `docker run` storage examples: <https://docs.docker.com/engine/containers/run/>

## Better Practices (Beginner-Friendly)

1. Prefer `docker compose` over legacy `docker-compose`.
2. Run application processes as non-root user inside containers when practical.
3. Keep secrets in environment files, not hard-coded in images.
4. Keep containers disposable; persist only what matters (volumes/databases).

Security note:

- On Linux, membership in `docker` group effectively grants root-level capabilities on the host.
  Reference: <https://docs.docker.com/engine/install/linux-postinstall/>
