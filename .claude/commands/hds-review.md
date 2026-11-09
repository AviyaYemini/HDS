---
description: Review the current changes against the HDS constitution and the original spec, and record the findings in the feature's review ledger
allowed-tools: Bash(git diff:*), Bash(git status:*), Bash(git log:*), Read, Edit(specs/**/review-ledger.md), Write(specs/**/review-ledger.md)
---

Run once per pull request, when the branch is ready to push; a group's close has no review (the owner's working rules of 2026-11-08).

Review the uncommitted changes and the commits on this branch (`git diff HEAD`, `git status`, and, if a main branch exists, `git log main..HEAD` and `git diff main...HEAD`, the committed changes themselves) against `.specify/memory/constitution.md` and `docs/hds-spec.md`.

Check each of the ten constitution principles and report only violations, each with file, line and the fix. Also check:
- every endpoint touched enforces the role rules (Employee, Manager, Admin, Client) and has an allowed and a forbidden test;
- no technology leaked into `specs/*/spec.md`;
- the change matches the active feature's `tasks.md` and nothing outside it;
- every user-facing string is externalised.

Then record the findings in the active feature's review ledger, `review-ledger.md` in the feature's folder (named in `.specify/feature.json`). Create it from `.claude/hds/review-ledger-template.md` if it does not exist. Add one row per new finding: the next free `R<n>` id, source `/hds-review`, the finding with its file and line, and status `open`. A ledger started before 2026-11-07 has a fifth column, the fix commit: leave that cell empty. Do not repeat a finding already in the ledger. Edit nothing else: neither the code nor any other file.

Finish with a verdict: ready to commit, or the list of blockers, and the ledger's open count.
