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
