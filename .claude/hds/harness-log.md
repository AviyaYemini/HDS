# Harness log

The harness-improvements PR (`chore/claude-harness`), after 006 PR 1 and before PR 2. A reviewer
session reviews; this session implements; nothing is pushed without the owner. One commit per item.

## Item 4: the test-database guard

- **Which databases**: `tests/run_database.allowed_database` refuses, by name, any database a run may
  not use. Outside CI: only `hds_p3_test`, or a run's own (`hds_run_<pid>_test`, and the migration
  tests' `hds_run_<pid>_<name>_test`). Those were already the design (each run gets its own unless
  `TEST_DATABASE_URL` names one), so they stay allowed. Never `hds_test` and never the development
  database `hds`. In CI (`CI` set), also `hds_test`, which the integration job sets. `hds_check`
  and `hds_scratch` are CI's too, but no pytest run uses them (the migration check and the contract
  job run Alembic and the server directly), and the existing `*_test` rule refuses them anyway.
- **One run at a time**: `tests/run_lock.RunLock` takes `pg_try_advisory_lock` on a key derived from
  the database name, on a connection of its own, before the session touches the database, and holds
  it to the end. A second run on the same database stops at once: "another test run is using the
  database 'hds_p3_test'". The connection lives in an event loop on a thread of its own, since the
  session fixture is synchronous and each `asyncio.run` ends its loop.
- **A template clone per run**: not worth it now. Creating and migrating a run's database takes a
  few seconds; the suite takes about 25 minutes. Worth revisiting if migrations grow slow.
- **Tests**: 11 new unit tests (`test_run_database.py`: allowed and refused names, in and out of CI,
  the lock key) failed first on the missing functions, then passed. Two integration tests
  (`test_run_lock.py`) run with the database suite at the end of this PR.

## Item 2: the git hooks

- `make hooks` installs the pre-commit and commit-msg hooks with pre-commit 4.7.0 through `uvx`.
- The config was stale and partly broken, so installing it as it stood would have failed or fought
  CI: ruff 0.4.4 and mypy 1.10 (src only, without the project's packages) against the project's
  ruff 0.17.3 and mypy 2.4. And the commitlint entry pointed at commitlint's own repository, which
  publishes no pre-commit hook, so it could never install. Now ruff, ruff format and mypy are local
  hooks running the project's versions (`uv run --extra dev`, from the lockfile), and commitlint
  comes from `alessandrojcm/commitlint-pre-commit-hook` v9.27.0 with config-conventional 21.2.4, on
  a Node pre-commit downloads (this Mac has none). gitleaks stays as it was.
- **Proof**, in a separate clone (its own `.git/hooks`; a worktree would share this repository's):
  `make hooks` installed both hooks; "Bad Subject." was refused (subject-empty, type-empty,
  subject-full-stop); a Python file with an unused import was refused by ruff (F401) and the format
  check; clean commits passed every hook. The first try showed `uv run ruff` cannot start ruff in a
  fresh clone, because ruff is in the `dev` extra; hence `--extra dev`. This repository's own
  `.git/hooks` were never touched.
