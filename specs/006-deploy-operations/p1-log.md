# 006 decision log

Standing rules: tests on the private database `hds_p3_test` only, one run at a time, after checking
`pg_stat_activity`; no push and no pull request until PR 1 (Groups A to C) is ready; no edit to a
committed migration; no test deleted or weakened, and every test change explained here. Each group:
failing tests first, then code, mutation checks, the gates, /hds-review and /code-review high, this
log and one commit. A reviewer session ("HDS helper") reviews each group before the next starts.

## Stages

- **Specify**: from the stored prompt; sections 9 and 10 are in `docs/hds-api.md`, not
  `docs/hds-spec.md` (which ends at section 7).
- **Clarify**: five answers (a cluster on the operator's Mac that installs published releases; at most
  a day of data lost; an off-machine backup copy that the restore test uses; a message per failure; a
  code-only rollback).
- **Plan**: the owner chose an SMTP sender, Flux, and Google Drive now with S3 later. Traces came back
  into scope as FR-008a (the original §9 and the plan prompt both ask for them).
- **Tasks**: T001 to T067 in Groups A to M.
- **Analyze and two reviewer rounds**: readiness fails only when the database is behind; worker
  liveness counts a held job; Vault's snapshot is age-encrypted to a public key; the client address
  lost on the Mac is an accepted limit; the manifest is compared before migrate; the compatibility test
  applies the head's migrations; five pull requests (A–C, D–F, G–H, I–L, M).

## Group A: pins and the secret checks (T001 to T006)

- **T001 pins** (`infra/VERSIONS.md`): every tool is a release at least two weeks old on 2026-11-06,
  so newer ones were passed over: Trivy v0.76.1 (not v0.76.2, one day old), k3s v1.37.1+k3s1 (not
  v1.37.2, three days), Flux v2.9.6 (not v2.10.0, eleven days), uv 0.13.5, kube-prometheus-stack
  92.11.0, OpenTelemetry 1.45.1/0.66b1 (not 1.45.2, eight days). Helm stays on 3 (v3.22.1), as the
  plan names Helm 3, although Helm 4 exists. Images are pinned by tag and index digest. The PostgreSQL
  image in compose and CI moved from the floating `16-alpine` to `16.15-alpine` by digest.
  - The Python packages are recorded in `VERSIONS.md` but not yet added: each joins `pyproject.toml`
    and the lock in the group that first imports it (C, D, E), so the image never carries unused
    code (code review). T001's text says so now.
- **T002 secret scan**: gitleaks over the whole history first found 11 hits, all four made-up values
  in unit tests (`test_deployment_files`, `test_logging_redaction`, `test_mail_outbox`,
  `test_throttle_keys`). `.gitleaks.toml` allows exactly those four values, anchored, and nothing else;
  the history then scans clean. The failing proof: a throwaway worktree and branch with an AWS-format
  key pair in `tmp/leak.txt`; the scan of that one commit exited 1 naming `tmp/leak.txt` line 2. The
  branch and worktree were removed. The CI job scans only the commits a run adds (a pull request's,
  or a push's), as the task says; it runs the image as the checkout's owner so git accepts the mount.
  The pre-commit hook uses gitleaks' `gitleaks` id, pinned at v8.30.1; its `gitleaks-docker` id runs
  an unpinned image, so it was not used.
- **T003/T004 published values**: 17 tests. They failed first on the missing
  `KNOWN_DEVELOPMENT_VALUES`. The published-values test reads `.env.example`, the compose file, every
  workflow and `tests/conftest.py`; its parser first misread compose's `${VAR:-default}` and
  `${VAR:?message}` and missed `setdefault("NAME", "value")`, and was fixed to read both. It finds
  `change-me`, `ci-only-not-a-real-secret-0123456789`, `hds_check`, `hds_dev`, `hds_e2e`,
  `hds_scratch`, `hds_test` and `test-only-jwt-secret-0123456789-abcdef`; all are on the list.
  - **Changed tests, with the reason**: 24 existing unit tests in `test_settings_environment.py`,
    `test_startup_and_cors.py` and `test_throttle_keys.py` started a production configuration on the
    default `DATABASE_URL`, whose password `hds_dev` is now refused. A new fixture,
    `distinct_database_password` in `tests/unit/conftest.py`, gives them a password the repository
    does not publish, as `real_mail_backend` gets them past the mail check; each file applies it with
    one `pytestmark` line. No assertion changed. No integration or concurrency test builds a
    production configuration.
- **T005 image**: `infra/tests/check_api_image.sh` failed first ("the image runs as root (uid 0)"),
  then passed on the hardened `Dockerfile.api` (uid 10001, `HDS_VERSION` and the OCI version label,
  `uv` copied from its pinned image instead of an unpinned `pip install uv`). The image still imports
  `hds.main` and `hds.worker`. Only `/app/.dev-mail` is writable by the process. CI runs the script in
  the `lint` job.
- **T006 mutation checks**, each caught: dropping `hds_e2e` from the list (3 tests failed); comparing
  without trimming and lower-casing (2 failed); running the check in development too (the suite failed
  at import, since the test settings use a published key).
- **Gates**: ruff, format, `mypy --strict src/ tests/` (339 files), interrogate 100%, the import graph
  and pip-audit (no known vulnerabilities) all clean. `alembic check` on `hds_p3_test` reported an
  index `idx_special_days_project` that no migration or model defines: a leftover in that long-lived
  database, not from this change. On a fresh throwaway PostgreSQL container `alembic check` is clean,
  which is what CI runs.
- **/code-review high** found nine issues; all were checked and fixed:
  1. The published-values list missed `e2e-only-not-a-secret-0123456789-abcdefgh`
     (`apps/client/playwright.config.ts`), `hds` (`apps/api/scripts/contract_cli.sh`) and
     `unused-by-down` (`Makefile`), because the test read a fixed list of files. It now reads every
     tracked configuration file (YAML, shell, ini, toml, Makefiles, Dockerfiles, tool configs, the env
     example and the test settings): 26 files. Test bodies, docs and source stay out, since they hold
     made-up values that tests use as strong secrets.
  2. An unreadable `DATABASE_URL` raised SQLAlchemy's `ArgumentError`, not a validation error; it is
     now refused naming `DATABASE_URL`.
  3. The `HDS_VERSION` build argument came before `uv sync`, so every new version reinstalled every
     dependency; it now comes last.
  4. The gitleaks range broke on a push with an all-zero or vanished `before`; it then scans the
     pushed commit alone (`-1 <sha>`, checked locally).
  5. The database password was read only from the URL's user part; the `password` query option and
     `PGPASSWORD` are now checked too.
  6. The separate placeholder check could no longer fire (the placeholder is on the list); removed.
  7. CI's `setup-uv` installed whatever uv was newest; all eleven uses now pin 0.13.5.
  8. Five runtime packages were added before any code used them; deferred (above).
  9. The user had no home directory; it now has `/home/hds`, set as `HOME`.

  Four new tests (URL option, `PGPASSWORD`, unreadable URL, the three new values) failed first, then
  passed. Mutation checks, each caught: no query option, no `PGPASSWORD`, catching the wrong
  exception, dropping the e2e key.
- **Note**: `uv run`, after the lock was reverted, re-synced the environment (two packages:
  `opentelemetry-api` and the project's own build) while the full suite ran in the background. No test
  imports either.
- **Full suite** on `hds_p3_test` (25 min): 2462 passed, 1 error. The error was in the setup of
  `tests/concurrency/test_accept_locks.py::test_accepts_staff_lock_does_not_deadlock_with_a_keyed_assignment`:
  seeding `employee@hds.test` hit `uq_users_email_ci`, rows left in the shared database. The file
  alone passed, and the whole concurrency folder (60 tests) then passed; nothing in Group A touches
  that seeding. Recorded as a one-off, not fixed here. The run started before the review fixes; those
  touch only the production start-up check, and the unit suite after them passed (1022, then 21 in the
  file with the compared-form test the reviewer suggested).
- **Separate commit** `5fc1326` (not part of Group A): CI's unit job had failed on every push to
  `main` since 002, because six hook tests ran the real hook in the checkout and the hook refuses
  commits on `main`. Reproduced from a worktree on `main` (6 failed), fixed in the test only, and
  green there and here (32 passed).

## Group C: health, version and metrics (T013 to T019)

Done before Group B, by agreement with the reviewer: B's quickstart and the compose healthcheck need
`/readyz`, and tasks.md allows this order ("Do T007 to T011 after T014").

- **Layers**: the "behind" rule and the worker's pulse are in `core/health.py`; the two reads
  (`SELECT 1`, `alembic_version`) in `repositories/health.py`; the 2-second check in
  `services/health.py`; `wiring.health` builds it on `get_session_factory`, so the tests' database
  override reaches it; `routers/health.py` serves `/livez`, `/readyz` and `/health`. The inline
  `/health` in `main.py` moved to the router, with the same answer plus the version.
- **Tests first**: `test_health.py` (10), `test_health_endpoints.py` (5), `test_metrics.py` (7),
  `test_metrics_port.py` (2), `test_worker_ops.py` (4), `test_worker_metrics.py` (1) each failed
  first on the missing modules, then passed.
- **The 2-second limit** is tested with the limit shrunk to 0.05 s rather than a 2-second wait; the
  integration test proves a refused connection answers 503 within 2.5 s.
- **Metrics**: a pure ASGI middleware, outermost, labels by `scope["route"].path_format` (set by
  Starlette while routing) or `unmatched`. Each app has its own registry. The lifespan serves it on
  `METRICS_PORT` with `prometheus_client`'s server; the tests set `METRICS_PORT=0` in
  `tests/conftest.py`, and `test_metrics_port.py` starts a server on a free port through the real
  lifespan. `create_app` takes `metrics_port` for that test.
- **Worker**: `Claim` now carries `lease_until`; `run_once` holds it on the pulse while the job runs
  and records the duration and outcome; the solve and store moved, unchanged, into `_run`, which
  returns the outcome. `serve()` runs the job loop and the operations app (uvicorn, Starlette) in one
  event loop. Smoke-tested against `hds_p3_test` on port 19101: `/livez` 200, `/readyz` 200,
  `/metrics` with all four states and the version.
- **Dependency**: `prometheus-client` 0.26.0 added now, as VERSIONS.md says.
- **Mutation checks**, each caught: label by path (3 failed); no time limit (1); ignore migrations
  (4); an unknown revision counted as behind (1); the heartbeat never ages (2); the held job ignored
  (1); no metrics server (1). A first attempt went wrong: the backups were keyed by file name, and
  `core/health.py` and `services/health.py` share one, so the restore copied the service module
  over the core one (and zsh did not split the test list, so that run tested nothing). The core
  module was rewritten from this session's text, its tests passed again, and the checks were rerun
  with a bash script that keys backups by path and verifies every restore.
- **/code-review high** (10 findings) and the reviewer's Group C review (4) overlapped; fixed:
  1. `/livez` and `/health` went through the full health service, so the migration history loaded
     lazily on the first probe and any fault there made liveness 500. They now answer from the
     version alone; the history is read in the API's lifespan and in `worker.serve()`, so a missing
     or two-headed history stops the process at boot (a new test).
  2. `METRICS_PORT` defaulted to 9100, so a second process on one host (a local uvicorn beside
     compose, the Playwright web server, `contract_cli.sh`) could not start. The default is now 0
     (off); compose and the chart set 9100. A taken port stops start-up with an error naming
     `METRICS_PORT` (a new test).
  3. The ready check shared the request pool, so a busy pool would take every pod out together.
     It now has its own NullPool engine (`get_health_session_factory`); the tests override it beside
     `get_session_factory`. A new test holds a one-connection request pool and `/readyz` still
     answers 200.
  4. A result thrown away because another worker reclaimed the job counted as `succeeded`, and a
     failure whose storing raised was not counted. Now `superseded` is not counted and the default
     outcome is `failed` (three unit tests, `test_worker_outcomes.py`).
  5. The worker's `/readyz` also checked migrations, against the contract; it now checks the
     database only (`check_migrations=False`).
  6. The 2-second limit covered closing the session, which can hang on a dead socket. The reads now
     run in a task that is abandoned at the limit (a new test with a close that hangs 30 s answers
     within 1 s; before the fix it took 30 s).
  7. Every `ProgrammingError` read as "no version table"; only SQLSTATE 42P01 does now, so a
     permission fault reports `database` (checked against the real driver first).
  8. The worker's stuck answer used a status outside the contract; it is now `not_ready` with
     `failing: ["worker_loop"]`.
  9. Alembic's own `CommandError` for two heads is now converted to the documented `RuntimeError`.
  10. `counts_by_state` repeated the state list; it now returns the table's counts and
      `JobMetrics.set_counts` fills absent states with 0.
  11. The health service is now built once per factory (`lru_cache`).

  Declined: serving the API's metrics with uvicorn as the worker does. `prometheus_client`'s own
  server in a thread is the library's documented way, needs no second ASGI server in the API
  process, and is shut down in the lifespan. Deferred to Group F, as the reviewer asked: uvicorn's
  signal capture in the worker's ops server (now written into T031).
  Mutation checks on these, each caught: waiting for the cleanup (2 failed, and the run took 65 s);
  every `ProgrammingError` read as missing (1); migrations always checked (1); the heads error not
  converted (1); superseded counted (survived at first: the test looked only at `succeeded` and
  `failed`; strengthened to "no observation at all", then caught); an unstored failure uncounted
  (1); the old worker body (1).
- **`float` is banned in `src`** (`test_time_money_invariants.py` rejects the name anywhere, not
  only on money). Durations are now `timedelta` (the ready limit, the stale window, `observe()`)
  and clocks `perf_counter_ns` and `monotonic_ns`; the tests keep `float` (the scan covers `src`
  only). `.env.example` gains `HDS_VERSION` (`test_deployment_files.py` wants every setting there).
- **Test runs**: the full suite started before the review fixes was stopped at 23% (no failure so
  far), as the reviewer proposed: it tested the old code, and its subprocess-based tests would pick
  up the edited files. Instead, on the final code: unit and property suites 1062 passed (the two
  redaction timing cases that ran slow beside the full suite passed alone, 93 in their file); the
  Group C database tests (health, metrics port, worker metrics, worker, schedule jobs) 36 passed.
  One full suite on the final PR 1 tree, after Group B, is the gate before the push.
- **Gates**: ruff, format, `mypy --strict src/ tests/` (351 files), interrogate 100%, the import
  graph and pip-audit clean. The worker smoke test passed again on the final code.

## Group B: the one-command local environment (T007 to T012)

- **Where the check runs**: `make wipe` deletes `infra/volumes/postgres`, which in this checkout holds
  the developer's own database and `hds_p3_test`, and their stack runs from it. So the quickstart
  check never ran here. A harness in the scratchpad copied the tree as a clean checkout has it
  (`git ls-files -co --exclude-standard`: tracked and new files, nothing git ignores) and ran it there
  under `COMPOSE_PROJECT_NAME=hds-quickstart` on other ports. The script also refuses to run without
  `QUICKSTART_THROWAWAY=1`. The developer's `infra-*` containers were untouched throughout.
- **To make that possible**, the compose host ports are variables with the old defaults
  (`POSTGRES_PORT`, `API_PORT`, `WEB_PORT`, `API_METRICS_PORT`, `WORKER_METRICS_PORT`), and image
  names carry the compose project (`infra-api:local`), so two stacks never share one.
- **Failing first**: the check stopped at step 1 (`make env` did not exist).
- **What the runs found, and the fixes**:
  1. Right after `make down`, Docker Desktop still held the stack's ports for a moment, so the port
     check refused `make up` (step 7). It now gives a taken port up to 5 s to free itself.
  2. A second `make up` recreated the API (step 8): `up --build` rebuilt the API image, and its ID
     changes on every build although every layer is identical (only the config differs). `make up`
     now builds only missing images; `make build` rebuilds. The code and migrations are mounted, so
     a code change needs no build.
  3. The check's own port holder could not bind while Docker still held the port; it now retries and
     signals when it listens.
  4. A harness bug, not the change: a failed run left the throwaway stack up, and the harness's
     `compose down` failed silently (compose refuses any command without `JWT_SECRET_KEY`), so the
     next run deleted the folder under a running database. The harness now removes the project's
     containers by their compose label first.

  Then all 12 steps passed: cold `make up` 34 s, warm 30 s (with Docker's build cache).
- **Step 7** creates a qualification, not a shift as T007 says: it proves the same thing (data
  survives a restart) with one call.
- **Mutation check**: with the port check disabled (in the throwaway copy only), step 11 fails
  ("make up started with port 18000 taken").
- **Hook blocks**, each fixed at the cause, not worked around: an rsync line that excluded the
  settings file by name (replaced by `git ls-files`, which needs no exclusion list); a README edit
  that quotes a copy of the settings file (made with the file tools, since it reads no file); this
  log entry, for the same reason; and a broad `rm -rf` of the scratchpad copy (left in place).
- **A mistake of mine**: a `gitleaks dir` run meant for four paths scanned the whole folder, without
  `--redact`, and printed the developer's local signing key from their git-ignored settings file into
  the session. It is a development key, never committed. The owner has been told to rotate it. From
  now on gitleaks runs only in `git` mode, on commits, with `--redact`.
- **The published-values test** first read the Makefile's `sed` pattern (`.*/JWT_SECRET_KEY=`) as a
  value. After the code review it skips only sed, regex and format patterns (`.*`, `[...]`, `{6`,
  `%s`), not every value starting with punctuation; a new test pins both sides.
- **/code-review high** (10 findings), all fixed:
  1. The quickstart script publishes the seed's password `Dev1234!` as an assignment, which the
     published-values test would catch once committed; `dev1234!` is now on the refused list.
  2. and 3. Without `--build`, `make up` kept stale images after a pull (the client bundle, the API's
     dependencies, a changed `API_PORT` baked into the bundle). The cause of the earlier recreate was
     BuildKit's provenance record, which carries a new build reference each time: with provenance on,
     two builds of the same tree gave two ids; with `--provenance=false`, the same id
     (`SOURCE_DATE_EPOCH` alone did not help). Both builds now set `provenance: false`, and `make up`
     builds again, recreating nothing when nothing changed (step 8 checks this).
  4. Without `nc` every port looked free; `ports_free.sh` now refuses, saying it needs netcat.
  5. The web container took 8081, the Expo dev server's port, and CORS allowed only it. It is now on
     8090, and CORS keeps the dev origins 8081 and 3000 too.
  6. The parser fix above.
  7. A failed `make up` printed only the migrate log; it now prints every service's state and recent
     logs.
  8. Only migrate mounted `alembic/`; the API and worker, which read the history at start-up, now
     mount it too, as the README says.
  9. Step 8 and step 12 hard-coded the database user and name; they follow `POSTGRES_USER` and
     `POSTGRES_DB`.
  10. The API image was built once per service; only `api` builds it now (`pull_policy: never` on the
      others), and a cold run shows one build and no pull attempts.

  The quickstart then passed all 12 steps again: cold 21 s, warm 20 s.
- **Full suite on the PR 1 tree** (A, B and C, before B's review fixes): 2512 passed in 28 min. B's
  review fixes touched one Python line (a list entry) and one test; the unit and property suites
  after them: 1065 passed.
- **Gates**: ShellCheck (pinned image) clean on all four scripts; ruff, format, `mypy --strict` (351
  files), interrogate 100%; the compose and Makefile changes covered by the quickstart run.
- **The reviewer's Group B review**: a build without any cache (`build --no-cache --pull`, as CI
  starts) took 42 s on this Mac, well inside step 3's 600 s. Two fixes: `make env` writes the
  settings file with umask 077 (mode 0600), which step 1 now checks; and the dockerignore excludes
  settings files at any depth (`**/`), so a nested one never reaches the web build, whose bundle
  would carry its `EXPO_PUBLIC_` values. ShellCheck clean; the quickstart passed all 12 steps again.
  To watch in CI: the runner's Compose version (`--wait` with a one-shot service, `build.provenance`)
  and that `nc` is present.

## PR 8's CI

- **Run 42200877912, step 7**: "permission denied" on `infra/volumes/postgres` while loading the
  build context. The `!**/` exception added in the review fix made the builder walk every excluded
  folder, and on Linux the database's data folder is 0700 and belongs to its own user. Fixed in
  `6b1ee04`: the dockerignore has no exceptions at all (no image needs the example file). macOS does
  not enforce that folder's ownership the same way, so the harness could not reproduce it.
- **Run 42201726100, step 8**: "make up restarted the running API". The api and web manifests and
  configs were identical on every build, but the runner's Compose builds through bake and attached a
  provenance attestation anyway, so the manifest list, which is the image id under the containerd
  store, changed each time. The fix: `export BUILDX_NO_DEFAULT_ATTESTATIONS := 1` in the Makefile;
  `provenance: false` stays in the compose file, and the CI job prints `docker version` and
  `docker compose version` before the check. Local proof in a throwaway copy with both
  `provenance: false` lines removed: two `make build` runs printed no attestation and gave the same
  api and web ids. Control: the same copy built directly with compose, without the switch,
  printed attestations and gave two different api ids. The harness then passed all 12 steps on the
  real tree (cold 28 s, warm 26 s); ShellCheck clean.

## Group D: traces (T020 to T024)

Branch `006-deploy-operations-d-f`, from main at `d2e9eda` (PR 9 merged). First, `0fca5af` adds T032a,
the postgres healthcheck over TCP, a follow-up from PR 9's CodeRabbit review.

- **Dependencies**: `opentelemetry-sdk` and `opentelemetry-exporter-otlp-proto-http` 1.45.1, and
  `opentelemetry-instrumentation-fastapi` and `-sqlalchemy` 0.66b1, exactly as `infra/VERSIONS.md`
  pinned them in Group A. `opentelemetry-api` was already in the lock at 1.45.2, from a dev
  dependency, and is now 1.45.1 to match the SDK. pip-audit runs in the gates.
- **Failing first**: `test_tracing.py` failed on the missing `hds.core.tracing`. `test_migration_024`
  ran only at the group's end, as the database rule says.
- **T021**: migration 024 adds `schedule_jobs.trace_context VARCHAR(55)`, nullable, an expand step.
  `alembic check` on a fresh throwaway PostgreSQL: no new operations; downgrade to 023 and up again:
  fine. The long-lived `hds_p3_test` reports a stray `idx_special_days_project` that no migration
  defines, a leftover from an earlier run, which ci_local_db.sh's comment warns about. Left alone.
- **T023**: `core/tracing.py`:
  - `configure` sets the provider once per process, instruments the app's engine, and instruments
    each FastAPI app it is given; OTLP only when the endpoint is set (`span_processors`).
  - The queue stores `current_traceparent()`. The worker runs each job in a `schedule_job.run`
    span (consumer) from `continue_from(row)`; an empty context for null or malformed, so it never
    joins an unrelated current span.
  - The log filter adds `trace_id` and `span_id` inside a valid span. The worker now logs through
    `configure_logging`, the API's JSON lines.
- **Review R1 (the reading of "malformed")**: T022 said a malformed header stores null. Under the
  instrumentation every request has a trace, continued or new, so the job stores the API's own
  trace, and a malformed header is never stored. That follows FR-008a, so jobs queued by the app,
  which sends no header, are traced too. The HDS helper agreed; T022, T024 and data-model.md were
  amended in `db0cb6f`. Null: jobs before 024, or queued outside any trace.
- **Review R3 and D1**: the probes are not traced (named in T023 now). The instrumentation
  searches its patterns anywhere in the full URL, so they are anchored to the whole path:
  `/api/v1/health-checks` was untraced before the fix. A test now checks three such routes.
- **R2**: `apps/api/README.md` lists `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_SERVICE_NAME` and has a
  Traces section.
- **Mutation checks (T024)**, each run on `test_tracing.py` and restored:
  - a new trace in the worker (`Context()` in place of `continue_from`): 2 failed;
  - the traceparent check accepts any value: 9 failed;
  - trace ids missing from logs: 3 failed;
  - the queue drops the trace: 4 failed;
  - a malformed row joins the current trace (`get_current()`): survived at first, as no test ran a
    job inside a span. A test was added, and it then failed.
- **`/code-review high` (R5 to R14)**: ten findings, unverified as that mode reports them. Eight
  fixed, each with a test, and each test was seen to fail on a mutant of its fix:
  - R5: a sampler drops a root client span, so SQL outside a request or a job (the poll every
    5 s, the metrics' counts) is no one-span trace. Under `ParentBased`, statements inside stay.
  - R6: the job span carries `hds.schedule_job.outcome`, and its status is error when failed.
  - R7: `worker.main` shuts the provider down on the way out; Group F's SIGTERM handling makes
    that path reachable.
  - R8: `RedactingExporter` wraps the OTLP exporter. It strips query strings (seen in a probe:
    `http.url` held `?search=Dana+Levi`) and redacts text attributes, events and the status.
  - R9: the trace-context helpers moved to `hds.core.trace_context` (OpenTelemetry API only), so
    the service no longer loads FastAPI, the instrumentation or the exporter.
  - R10: `current_traceparent` uses the propagator's `inject`, and the log filter uses
    `format_trace_id` and `format_span_id`.
  - R11: `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` turns export on too. `OTEL_SDK_DISABLED` is not in
    contracts/ops.md and was not added.
  - R14: the filter always sets both fields, `None` outside a span, so a hand-passed `extra`
    never writes one.
  - Won't-fix: R12, `tracestate` (the column is the traceparent alone, by data-model.md); R13,
    the provider set on import (process-global by OpenTelemetry's design, nothing exported without
    an endpoint).
  - The ASGI send and receive spans are left out (`exclude_spans`), which halves a request's spans.

## Group E: SMTP mail and the first Admin (T025 to T029)

- **Dependency**: `aiosmtpd` 1.4.6 (dev only), as `infra/VERSIONS.md` pinned it in Group A; it
  brings `atpublic` 9.0.0. pip-audit clean. The STARTTLS certificate for the tests is made by
  `openssl` in pytest's temporary folder and trusted through the test's own TLS context, so no key
  is stored and the certificate checks stay on; no `cryptography` dependency was added.
- **Failing first**: `test_smtp_mail.py` and `test_settings_smtp.py` failed on the missing
  `MailDeliveryError`; `test_create_admin.py` ran at the group's end, on `hds_p3_test`.
- **T026**: `SmtpMailSender` uses `smtplib` in `asyncio.to_thread`, a 10 s timeout per step,
  STARTTLS with the relay's certificate checked, and `MailDeliveryError` raised without its cause.
  `smtp` is in `REAL_MAIL_BACKENDS`, with `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`,
  `SMTP_PASSWORD` (`SecretStr`, also checked against the published values), `MAIL_FROM` and
  `SMTP_STARTTLS`. `get_mail_sender` picks it, as for the outbox (R16; T026 amended).
- **Existing guards that caught gaps**: the settings must be in `.env.example` and the README;
  no `float` annotation (the timeout is an `int`); every hash follows `release_connection`; every
  error code has an `en` and a `he` message.
- **T028**: `FirstAdminService` (R15; T028 amended in `d84d58c`): it validates, then hashes with
  the connection released, then takes `pg_advisory_xact_lock` and checks `any_exists`, then creates.
  The script runs it in `unit_of_work` (a plain `session.begin()` conflicts with
  `release_connection`'s commit). `--name` is optional (R17).
- **Review R19 to R28 (`/code-review high`)**, each with a test seen failing first, except R23:
  - R19: the name is checked first (1 to 255); a database error prints its type only.
  - R20: `MAIL_FROM` must hold an address. R21: user name and password, both or neither.
  - R22: `--help` exits 0. R24: the CA store is read on first delivery, in the thread.
  - R26: `SMTP_STARTTLS=false` keeps the connection plain. R27: the README says the timeout is
    per step and that port 465 is not supported. R28: `exists().select_from(User)`.
  - R23: `main` disposes the engine, as every script does. No test: the run without it showed no
    warning either, so the assertion I added checked nothing and was removed (its mutant lived).
- **Mutation checks (T029 and the review)**, on the final code: STARTTLS skipped when not
  offered, never used, the password in the error, the certificate unchecked, create_admin allowed
  twice, STARTTLS off in production, and the R19 to R26 mutants were all killed. Equivalent: the
  cause kept (the test relay's refusal quotes no secret); `from None` stays as a safeguard.
- **Database tests** on `hds_p3_test` (free first): `test_create_admin`, `test_mail_after_commit`,
  `test_delivery_after_response`: 23 passed.
