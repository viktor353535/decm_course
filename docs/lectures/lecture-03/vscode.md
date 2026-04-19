# VS Code Guide (Lecture 3)

## Core Path

This guide focuses on the VS Code features we will use in this course every week.

## Required Extensions for This Course

Install these extensions from the VS Code Extensions view (`Ctrl+Shift+X` / `Cmd+Shift+X`):

1. Dev Containers (`ms-vscode-remote.remote-containers`)
2. Docker (`ms-azuretools.vscode-docker`)

Optional:

1. LLM/assistant extension of your choice (not required for course completion)

References:

- Extension marketplace docs: <https://code.visualstudio.com/docs/getstarted/extensions>
- Dev Containers docs: <https://code.visualstudio.com/docs/devcontainers/create-dev-container>

## VS Code Views You Need

Use the Activity Bar on the left:

1. Explorer: file and folder navigation
2. Search: find across files
3. Source Control: git changes, staging, commit, sync
4. Run and Debug: not central in lecture 3, but useful later
5. Extensions: install/update extensions

Bottom panel:

1. Terminal: run `make`, `git`, and `docker` commands
2. Problems/Output: inspect errors and logs

Reference:

- UI docs: <https://code.visualstudio.com/docs/getstarted/userinterface>

## Core Workflow in This Repo

1. `File -> Open Folder...` and open the project folder.
2. Trust the workspace when prompted.
3. Open Command Palette (`Ctrl+Shift+P` / `Cmd+Shift+P`).
4. Run `Dev Containers: Reopen in Container`.
5. Open terminal (`Ctrl+`` / `Cmd+``) and run:

```bash
make init
make up-superset
```

## Good Habits for Beginners

1. Read terminal output fully before retrying.
2. Keep one terminal tab for setup commands and one for logs.
3. Use Source Control view frequently instead of guessing what changed.
4. Commit small, meaningful changes.

## Optional Deep-Dive

1. Learn keyboard shortcuts:
   - Command Palette: `Ctrl+Shift+P` / `Cmd+Shift+P`
   - Toggle terminal: ``Ctrl+` `` / ``Cmd+` ``
2. Explore settings sync and profile export for lab machines.
3. Add optional AI extension, but keep all core steps tool-agnostic.
