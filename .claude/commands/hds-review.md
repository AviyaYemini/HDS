---
description: Review the current changes against the HDS constitution and the original spec
allowed-tools: Bash(git diff:*), Bash(git status:*), Bash(git log:*)
---

Review the uncommitted changes and the commits on this branch (`git diff HEAD`, `git status`, `git log main..HEAD` if a main branch exists) against `.specify/memory/constitution.md` and `docs/hds-spec.md`.

Check each of the ten constitution principles and report only violations, each with file, line and the fix. Also check:
- every endpoint touched enforces the role rules (Employee, Manager, Admin, Client) and has an allowed and a forbidden test;
- no technology leaked into `specs/*/spec.md`;
- the change matches the active feature's `tasks.md` and nothing outside it;
- every user-facing string is externalised.

Finish with a verdict: ready to commit, or the list of blockers. Do not edit any files.
