# Git Troubleshooting (VS Code + GitHub)

This guide targets common Git issues in VS Code, especially when older local setups or multiple GitHub accounts get mixed together.

## Fast Diagnostic Checklist

Run these in VS Code terminal first:

```bash
git --version
git status
git remote -v
git config --get user.name
git config --get user.email
git config --global --get user.name
git config --global --get user.email
```

Interpret quickly:

1. `git --version` fails: Git is not installed or not on PATH.
2. `remote -v` points to wrong repository/account: fix `origin`.
3. `user.name` / `user.email` unexpected: set repo-local identity.

## 1) VS Code Source Control does not work

Symptoms:

1. Source Control view is empty when repo has changes.
2. Commit/sync actions are unavailable.

Fix:

1. Confirm folder opened is the repository root.
2. Confirm this is a git repo:

```bash
git rev-parse --is-inside-work-tree
```

3. Reload VS Code window (`Developer: Reload Window`).
4. Ensure built-in Git extension is enabled in VS Code extensions.

## 2) Wrong GitHub account is used

Symptoms:

1. Push goes to unexpected account/repo.
2. VS Code auth popup shows old account.

Fix:

1. In VS Code Accounts menu, sign out of GitHub.
2. Remove stale GitHub credentials from OS credential store.
3. Sign in again with the correct account.
4. Verify remote:

```bash
git remote -v
```

5. If needed, reset `origin`:

```bash
git remote set-url origin <your-fork-url>
```

## 3) Authentication fails when pushing

Symptoms:

1. `Authentication failed`.
2. Repeated login prompts.
3. `Permission denied (publickey)` for SSH remotes.

Fix:

1. Prefer HTTPS remote for beginner setup (simpler account flow).
2. Reauthenticate in VS Code and retry.
3. If using SSH, verify key registration in GitHub.
4. Check remote protocol:

```bash
git remote -v
```

## 4) Commit author is wrong

Symptoms:

1. Commits show previous course account name/email.

Fix (repo-local is the safest default here):

```bash
git config user.name "Your Name"
git config user.email "your-email@example.com"
```

Verify:

```bash
git config --get user.name
git config --get user.email
```

## 5) "Dubious ownership" or safe directory warning

Symptom:

1. `fatal: detected dubious ownership in repository`

Fix:

```bash
git config --global --add safe.directory /workspaces/course
```

## 6) Push rejected because branch is behind

Symptoms:

1. `non-fast-forward` error.

Fix:

```bash
git pull --rebase
git push
```

If conflicts appear, resolve in VS Code Source Control, then continue.

## 7) Last-resort cleanup for broken local auth state

Use only if regular sign-out/sign-in fails:

1. Sign out from VS Code GitHub account.
2. Remove GitHub credentials from OS credential manager/keychain.
3. Restart VS Code.
4. Sign in again.

## 8) If Git UI still blocks progress

Fallback path to keep work moving:

1. Pair with a nearby partner for one commit flow.
2. Use CLI fallback for commit + push:

```bash
git add .
git commit -m "lecture1: first commit"
git push -u origin <branch-name>
```

3. Return to the VS Code UI flow after the first successful push.
