# Devcontainers Guide (Lecture 3)

## What a Devcontainer Is

A devcontainer is a reproducible development environment configuration for VS Code.
It lets us share the same toolchain without installing most dependencies on the host OS.

Reference:

- Dev Containers docs: <https://code.visualstudio.com/docs/devcontainers/create-dev-container>
- Devcontainer badge example repository:
  <https://github.com/adlerpriit/dev_test>

## Why We Use It in This Course

1. Lower setup friction across Windows/macOS.
2. Consistent tool versions.
3. Easy onboarding when command-line experience is limited.

## This Repository's Current Setup

Key files:

1. `.devcontainer/devcontainer.json`
2. `.devcontainer/post-create.sh`

Current approach:

1. Starts from a prebuilt base image (`mcr.microsoft.com/devcontainers/base:trixie`)
2. Mounts host Docker socket into the devcontainer
3. Runs `post-create.sh` to install tools and Python packages

## Three Common Patterns

### Pattern A: Prebuilt image (fastest to start)

`devcontainer.json` uses `image`.

Best when:

1. You want simple setup.
2. Base environment is enough with minimal customization.

### Pattern B: Dockerfile-based

`devcontainer.json` uses `build.dockerfile`.

Best when:

1. You need pinned dependencies in your own image.
2. You want full control of packages/users.

### Pattern C: Docker Compose-based

`devcontainer.json` references `dockerComposeFile` and a `service`.

Best when:

1. You already have multi-service architecture.
2. Dev environment must attach to one service in a larger stack.

## Customization Fields to Teach

Useful `devcontainer.json` fields for this course:

1. `extensions`: install required VS Code extensions
2. `mounts`: add bind mounts (for example Docker socket)
3. `containerEnv`: environment variables inside devcontainer
4. `postCreateCommand`: bootstrap dependencies once container is created
5. `remoteUser`: run as non-root user

## Non-Root Best Practice

Prefer a non-root runtime user whenever possible.

Why:

1. Fewer accidental permission issues on mounted files.
2. Safer defaults.
3. Closer to modern team workflows.

Related references:

- Docker rootless mode: <https://docs.docker.com/engine/security/rootless/>
- Dockerfile `USER` instruction reference: <https://docs.docker.com/reference/dockerfile/>

## Core Runbook

1. Open repository in VS Code.
2. Run `Dev Containers: Reopen in Container`.
3. Wait until `postCreateCommand` finishes.
4. In terminal run:

```bash
make init
make up-superset
```

5. Verify Superset opens at <http://localhost:8088>.

## Optional Demo Repository

For a devcontainer badge example on GitHub, use:

- <https://github.com/adlerpriit/dev_test>
