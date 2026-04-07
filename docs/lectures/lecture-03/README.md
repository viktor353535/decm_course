# Lecture 3: VS Code, Docker, Git, Devcontainers

## Audience and Goal

This lecture assumes no IT background.
By the end of the session, we should be able to run this project in a devcontainer and make one git commit on our own branch.

## Learning Outcomes

After this lecture, we should be able to:

1. Navigate core VS Code views and use the integrated terminal.
2. Explain Docker basics (image, container, volume, network) and run basic commands.
3. Create commits using the VS Code Source Control UI and follow basic GitHub Flow.
4. Reopen a project inside a devcontainer and run project bootstrap commands.

## Preflight Checklist

Check these before we begin:

1. VS Code installed.
2. Docker Desktop installed and running.
3. Git installed.
4. On Windows: WSL2 enabled (Docker Desktop Linux container backend).
5. GitHub account available for push/branch/PR flow.

References:

- VS Code docs: <https://code.visualstudio.com/docs/getstarted/userinterface>
- Docker Desktop install docs: <https://docs.docker.com/desktop/setup/install/windows-install/>
- Git install docs: <https://git-scm.com/downloads>

## Practical Flow (Core Path)

Use this sequence:

1. Clone repository and open in VS Code.
2. Install required extensions (Dev Containers and Docker).
3. Reopen folder in devcontainer.
4. In VS Code terminal run:

```bash
make init
make up-superset
```

5. Open Superset at <http://localhost:8088>.
6. Create a new git branch in VS Code.
7. Edit one file, commit via VS Code Source Control UI, and push.

## Topic Guides

- [VS Code Guide](./vscode.md)
- [Docker Guide](./docker.md)
- [Git Guide (UI-first)](./git.md)
- [Git Troubleshooting (VS Code auth/account/config)](./git-troubleshooting.md)
- [Devcontainer Guide](./devcontainers.md)
- [Troubleshooting](./troubleshooting.md)

## Companion Repositories

- Git conflict + pull request playground:
  <https://github.com/ELIXIREstonia/2024-11-06-git-playground>
- Devcontainer badge demo:
  <https://github.com/adlerpriit/dev_test>

## Optional Deep-Dive

If time allows:

1. Build a tiny custom Docker image from a Dockerfile.
2. Run a two-service Compose example.
3. Compare git commit flow in VS Code UI vs CLI.
