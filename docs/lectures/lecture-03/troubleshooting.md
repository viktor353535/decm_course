# Lecture 3 Troubleshooting

Use this checklist before asking for help.

## 1) `docker` commands fail with daemon/connection error

Symptom:

- `Cannot connect to the Docker daemon`

Fix:

1. Start Docker Desktop.
2. Wait until it shows healthy/running.
3. Retry command.

## 2) VS Code cannot reopen in container

Symptom:

- `Dev Containers: Reopen in Container` fails or is missing.

Fix:

1. Install Dev Containers extension (`ms-vscode-remote.remote-containers`).
2. Reload VS Code window.
3. Retry reopen command.

## 3) Windows students get path or mount issues

Symptom:

- Bind mounts not resolving as expected.

Fix:

1. Run the stack with `make up-superset`, `make up-airflow`, or `make up-all` from inside the devcontainer.
2. Run `make print-host-workspace` and confirm the resolved path looks like a real host path, not `/workspaces/...`.
3. Confirm Docker Desktop uses WSL2 backend.
4. Reopen folder from local filesystem path (not temporary archive path).
5. Rebuild/reopen devcontainer.

## 4) Permission denied on mounted files

Symptom:

- Cannot edit or write to files from inside container.

Fix:

1. Ensure project is opened with normal user permissions.
2. Rebuild container so user/group mappings apply cleanly.
3. Avoid running editor as administrator unless required.

## 5) Port already in use (`8088`, `8080`, `5432`)

Symptom:

- Compose up fails due to port conflict.

Fix:

1. Stop conflicting local service.
2. Or stop previous course containers:

```bash
make down
```

3. Retry:

```bash
make up-all
```

## 6) Git push fails due to authentication

Symptom:

- Push rejected or auth prompt loops.

Fix:

1. Sign in to GitHub in VS Code.
2. Confirm remote URL points to your own fork.
3. Retry `Sync Changes`.

If this does not resolve quickly, use the dedicated guide:

- [Git Troubleshooting](./git-troubleshooting.md)

## 7) `make` command not found in devcontainer

Symptom:

- `make: command not found`

Fix:

1. Rebuild/reopen devcontainer.
2. Check `.devcontainer/post-create.sh` completed.
3. Re-run setup commands.
