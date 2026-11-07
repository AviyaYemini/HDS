# Tasks: Deployment and operations

**Input**: `specs/006-deploy-operations/`:
- **spec.md**: US1 to US6, FR-001 to FR-027 with FR-008a and FR-010a, SC-001 to SC-008
- **plan.md**: Groups A to M
- **research.md**: R1 to R14
- **data-model.md**, **contracts/ops.md** and **quickstart.md**, scenarios 1 to 6

**Tests**: required (Constitution IV). Every rule gets a test, and each test is run and seen to fail
for the right reason before the code that satisfies it exists. Infrastructure has tests too:
- a CI job, or a script run by one, that fails before the change and passes after it;
- `helm template` assertions for the chart.

Tests run on the private database `hds_p3_test`, one run at a time.

**Each group ends with**:
1. mutation checks, where code was written;
2. the gates: ruff, format, `mypy --strict src/ tests/`, interrogate, the import graph,
   `alembic check`, plus ShellCheck and `helm lint` where they apply;
3. `/hds-review` and `/code-review high`;
4. the log (`p1-log.md`);
5. a commit.

**Organization**: one group per plan phase, in plan order. Each group ends green and is committed.

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Group A, pins and the secret checks (setup; US6)

**Goal**: every tool pinned; FR-025 and FR-026. **Independent test**: quickstart scenario 6, first
three bullets.

- [X] T001 Pin versions in `infra/VERSIONS.md` (one table: tool or image, version, digest where an image, why) and use them everywhere they appear:
  - **images**: `python:3.12-slim`, `postgres:16`, `nginxinc/nginx-unprivileged`, `rclone/rclone`
  - **cluster**: age, k3d, k3s, Helm, Flux, Vault chart, Vault Secrets Operator chart, kube-prometheus-stack chart
  - **CI tools**: Trivy, gitleaks, ShellCheck, kubeconform
  - **Python packages**: `prometheus-client`, the OpenTelemetry packages and `aiosmtpd`, recorded here and each added to `apps/api/pyproject.toml` and `uv.lock` in the group that first imports it (C, D and E), so no image carries an unused dependency (§10 Dependencies)
- [X] T002 [P] [US6] Failing test, a CI job `secrets` in `.github/workflows/ci.yml` that runs gitleaks over the pull request's commits. Prove it fails: run it locally on a throwaway branch carrying an AWS-format sample key in `tmp/leak.txt`, and check that the output names the file and the line. Then add the gitleaks hook to `.pre-commit-config.yaml`, plus `.gitleaks.toml` allowing only the published development placeholders, each listed by exact value with a comment (FR-025)
- [X] T003 [P] [US6] Failing tests `apps/api/tests/unit/test_settings_known_values.py`. With `ENVIRONMENT=production`, start-up is refused for:
  - every password or key value published in `.env.example`, `infra/docker-compose.yml`, `.github/workflows/*.yml` and `apps/api/tests/conftest.py`, such as `hds_dev`, `hds_scratch`, `hds_e2e`, `change-me` and `ci-only-not-a-real-secret-0123456789`, whether it appears as `JWT_SECRET_KEY`, as `THROTTLE_SECRET_KEY` or as the password inside `DATABASE_URL`;
  - a value that only differs from one of those by case or surrounding space;
  - in every case the error names the setting and never the value.

  With `ENVIRONMENT=development` the same values start. A second test reads those files and fails if a value published there is missing from the refused list, so a new example value cannot slip through (FR-026)
- [X] T004 [US6] Add `KNOWN_DEVELOPMENT_VALUES` and the check to `apps/api/src/hds/core/config.py`, and parse the password from `DATABASE_URL` with SQLAlchemy's `make_url`. Update the module docstring and `apps/api/README.md` (Configuration)
- [X] T005 [P] Failing check, a CI step in the `lint` job: `docker build -f infra/Dockerfile.api` and then `docker run --rm --entrypoint id <image> -u` print a non-zero uid, and `docker inspect` shows the label `org.opencontainers.image.version` and the env `HDS_VERSION` equal to the build arg. Then harden `infra/Dockerfile.api`: a pinned base by digest, `ARG HDS_VERSION=dev`, a non-root user `hds` (uid 10001), `uv` installed from a pinned version, no `pip install uv` at an unpinned version, and `CMD` unchanged
- [X] T006 Mutation checks: drop one value from the list; compare without trimming; let the check run in development. Then the gates, both reviews, the log, and a commit `feat(api): refuse published development secrets outside development (006 Group A)`

## Phase 2: Group B, the one-command local environment (US1, P1)

**Goal**: FR-001 to FR-005, SC-001. **Independent test**: quickstart scenario 1.

- [X] T007 [US1] Failing test, a CI job `quickstart` in `.github/workflows/ci.yml` and the script `scripts/ops/quickstart_check.sh`, which works from a clean checkout and fails at the first unmet step:
  1. `make env` writes `.env` with a 64-hex-character `JWT_SECRET_KEY`.
  2. A second `make env` exits non-zero and leaves the file unchanged.
  3. `make up` (with `timeout 600`, under SC-001's 15 minutes) returns. Log how long it took.
  4. `curl localhost:8000/readyz` reaches 200 within 120 s.
  5. `curl localhost:8090/` returns HTML containing the app's root element.
  6. Each sample account from `apps/api/src/hds/scripts/seed_dev.py` signs in through `POST /auth/login`.
  7. A shift created through the API, then `make down` and `make up` (with `timeout 120`; log the time), is still there.
  8. `make up` again, on the running stack, exits 0, creates no second organisation and restarts nothing (the port check skips this project's own running services).
  9. With `JWT_SECRET_KEY=` emptied, `make up` exits non-zero with output naming `JWT_SECRET_KEY`.
  10. `make wipe` with `yes` piped in deletes the data, and the next `make up` has only the seed.
  11. `make down`; then, with port 8000 held by another process, `make up` exits non-zero naming `8000`, and `docker compose ps` shows nothing running (no half-started stack).
  12. `make wipe`; then start only the database (`docker compose ... up -d postgres`) and create one of migration 001's tables by hand so 001 fails (the port check skips the running `postgres` service): `make up` exits non-zero, the API is not started, and the output shows the migration error. No repository file changes for this step.

  It needs `/readyz`, so it runs after T013; until then it is expected to fail at step 4. Record that failure in the log (FR-001 to FR-005)
- [X] T008 [P] [US1] `infra/Dockerfile.web`: a multi-stage build that runs `pnpm install --frozen-lockfile` and `pnpm --filter hds-client export:web` with `EXPO_PUBLIC_API_URL=""`, then copies `apps/client/dist` into the pinned `nginx-unprivileged`, listening on 8080 with an SPA fallback to `index.html`, configured in `infra/web/nginx.conf`. Locally the API is on another origin, so the compose build passes `EXPO_PUBLIC_API_URL=http://localhost:8000` as a build arg; the cluster image keeps it empty (research R2)
- [X] T009 [US1] `infra/docker-compose.yml`:
  - **`migrate`**: a one-shot service on the API image, running `alembic upgrade head && python -m hds.scripts.seed_dev`.
  - **API and worker**: `api` and `worker` depend on `migrate: service_completed_successfully`. `api` has a healthcheck on `/readyz` and maps `9100:9100`; `worker` maps `9101:9100`.
  - **`web`**: from `Dockerfile.web` at `8090:8080` (not 8081, the Expo dev server's port), depending on the API being healthy.
  - **Comments**: remove the note that migrations are not run on start.
  - **No new defaults** for secrets.
- [X] T010 [US1] `Makefile`, with each target's comment saying what it keeps and what it deletes:
  - **`env`**: writes `.env` from `.env.example` with a fresh `openssl rand -hex 32`; refuses if `.env` exists.
  - **`up`**: first checks that ports 5432, 8000, 8090, 9100 and 9101 are free and, if one is taken, names it and starts nothing. A port whose service is already running in this compose project (`docker compose ps --status running --services`) is skipped, so `make up` on a running stack still works; then starts everything and waits for the API to be healthy.
  - **`down`**: unchanged.
  - **`wipe`**: asks `Delete the local database? [y/N]`, then runs `down -v` and removes `infra/volumes/postgres`.
- [X] T011 [US1] Docs: the root `README.md` quickstart becomes the prerequisites (Docker, make), then `make env`, then `make up`, then the sample accounts and their roles. Rewrite the local section of `infra/README.md` for the new services and targets, and keep the proxy note
- [X] T012 Gates, ShellCheck on `scripts/ops/quickstart_check.sh`, both reviews, the log, and a commit `feat(infra): start the whole system with one command (006 Group B)`

## Phase 3: Group C, health, version and metrics (US2, P1)

**Goal**: FR-006 to FR-010. **Independent test**: quickstart scenario 2.

- [X] T013 [P] [US2] Failing tests `apps/api/tests/unit/test_health.py`, using a fake health repository:
  - **ready**: `ready()` is ok when the ping succeeds and the revision is the code's head.
  - **newer**: a revision the code's script history does not know counts as ready, because it is newer and FR-016 keeps the code working on it (this is what lets old pods serve during an upgrade and after a rollback).
  - **database**: it reports `failing: ["database"]` when the ping raises or takes over 2 s; time is controlled, with no real waiting.
  - **migrations**: it reports `failing: ["migrations"]` only when the database is behind: `alembic_version` empty or missing, or a revision in the code's own history that is not its head.
  - **live**: `live()` never touches the repository.
  - **bodies**: a body never contains the exception's text.

  Failing integration tests `apps/api/tests/integration/test_health_endpoints.py`:
  - `GET /livez`, `/readyz` and `/health` answer 200 with `{"status":"ok","version":...}`.
  - With the database stopped (an engine pointed at a closed port), `/readyz` answers 503 `{"status":"not_ready","failing":["database"],...}` within 2.5 s, while `/livez` answers 200.
  - None of the three is in `/api/v1/openapi.json`, and none needs authentication (FR-006, FR-009, FR-010).
- [X] T014 [US2] Add `apps/api/src/hds/repositories/health.py` (`ping()` and `revision()`, which reads `alembic_version`) and `apps/api/src/hds/core/health.py` (`live()` and `ready(repo, history, limit=2.0)` using `asyncio.timeout`; `history` is the code's known revisions and its head, read from Alembic's `ScriptDirectory` once at start-up, and "behind" means a known revision that is not the head, or none). Add the routes in `apps/api/src/hds/main.py`, with `/health` answering as `/livez`. Add `HDS_VERSION` to `apps/api/src/hds/core/config.py`
- [X] T015 [P] [US2] Failing tests `apps/api/tests/unit/test_metrics.py`:
  - **labels**: after requests to `/api/v1/shifts/<uuid>`, a 404 path, `/auth/login` with a body and a query string containing an e-mail address, the registry holds `hds_http_requests_total` only with `route` values from the app's route templates or `unmatched`;
  - **status**: the `status` label is the code;
  - **duration**: the duration histogram has a sample per request;
  - **no personal data**: no label value contains a UUID, an `@` or a `?`;
  - **build info**: `hds_build_info{version}` is 1.

  Failing integration test `apps/api/tests/integration/test_metrics_port.py`: the metrics answer on `METRICS_PORT` and `GET /metrics` on the public port is 404 (FR-007, FR-009)
- [X] T016 [US2] Add `apps/api/src/hds/core/metrics.py`, a pure ASGI middleware that reads the matched route's `path_format` after routing, with its own `CollectorRegistry`. Start the metrics server on `METRICS_PORT` (default 9100) in the app's lifespan, but not under the test client's lifespan unless asked. Add `METRICS_PORT` to config
- [X] T017 [P] [US2] Failing tests `apps/api/tests/unit/test_worker_ops.py`:
  - **live**: the worker's ops app answers `/livez` 200 while the loop's heartbeat is under 30 s old, or while the worker holds a job still inside its lease; 503 when neither holds.
  - **long solve**: `/livez` stays 200 through a 60 s solve (time controlled), and turns 503 once the held job passes its lease.
  - **ready**: `/readyz` adds the database ping.
  - **metrics**: `/metrics` has `hds_schedule_jobs{state}` for all four states, counted from the table through a fake repository, plus `hds_schedule_job_duration_seconds{outcome}` after a run.

  Failing integration test `apps/api/tests/integration/test_worker_metrics.py`: with two queued jobs and one failed job, the gauge reads 2 and 1 (FR-006, FR-008)
- [X] T018 [US2] Worker: in `apps/api/src/hds/worker.py`, serve a Starlette ops app with uvicorn on `METRICS_PORT` in the worker's event loop, and add a heartbeat set on each loop turn, plus the held job's `lease_until`, which liveness also accepts (research R6). Add `count_by_state()` to `apps/api/src/hds/repositories/schedule_job.py`, and observe the job duration in `run_once`
- [X] T019 [US2] Mutation checks: label by path; skip the 2 s limit; ready ignores migrations; ready fails on an unknown (newer) revision; heartbeat never ages; liveness ignores the held job; metrics on the public port. Then the gates, both reviews, the log, and a commit `feat(api): live and ready checks, version and RED metrics (006 Group C)`

## Phase 4: Group D, traces (US2, P1)

**Goal**: FR-008a. **Independent test**: queue a schedule job through the API, and the worker's log
lines for it carry the API request's `trace_id`.

- [X] T020 [P] [US2] Failing tests `apps/api/tests/integration/test_migration_024.py`: upgrade from 023 and downgrade back. `schedule_jobs.trace_context` is `VARCHAR(55)`, nullable, and null on existing rows. Rule, quoted from data-model: "written once when the job is queued and never changed; null for jobs queued before the migration and for requests that carry no trace"
- [X] T021 [US2] Add the migration `apps/api/alembic/versions/024_schedule_job_trace_context.py`, which states what and why: one trace per job (§9), an expand step the previous release ignores (FR-016), and a downgrade that drops the column. Add the column to `apps/api/src/hds/models/schedule_job.py`, and keep `alembic check` clean
- [X] T022 [P] [US2] Failing tests `apps/api/tests/unit/test_tracing.py`, with an in-memory span exporter:
  - **queuing**: a request carrying `traceparent: 00-<32 hex>-<16 hex>-01` that queues a job stores that trace in `trace_context`;
  - **malformed**: a malformed header is never stored: the job stores the new trace the API started for that request (review R1);
  - **no trace**: a job queued outside any trace stores null;
  - **the worker**: running the job makes spans whose trace id equals the stored one, and a null starts a new trace;
  - **logs**: every JSON log line inside a request or a job has `trace_id` and `span_id`, while lines outside have neither;
  - **export**: with `OTEL_EXPORTER_OTLP_ENDPOINT` unset, no exporter is installed and nothing is sent (FR-008a)
- [X] T023 [US2] Add `apps/api/src/hds/core/tracing.py`: `configure(service_name)` sets up the provider, FastAPI and SQLAlchemy instrumentation, and OTLP over HTTP only when the endpoint is set. Add `current_traceparent()` and `continue_from(traceparent)`, which validates the format. The health probes (`/livez`, `/readyz`, `/health`) are not traced; they answer every few seconds (review R3). Wire them in:
  - **the queue**: `apps/api/src/hds/services/schedule_job.py` stores `current_traceparent()` when queuing;
  - **the worker**: `apps/api/src/hds/worker.py` wraps each job in a span continued from the row;
  - **logs**: `apps/api/src/hds/core/logging.py` adds `trace_id` and `span_id`.
- [X] T024 [US2] Mutation checks: a new trace in the worker; the traceparent check accepts any value; trace ids missing from logs. Then the gates, both reviews, the log, and a commit `feat(api): one trace per schedule job across the API and the worker (006 Group D)`

## Phase 5: Group E, SMTP mail and the first Admin (foundational for US3 and US4 messages, and for a production start)

**Goal**: research R9 and R13. **Independent test**: with `ENVIRONMENT=production` and
`MAIL_BACKEND=smtp` against a local `aiosmtpd`, the API starts and a password reset arrives.

- [X] T025 [P] Failing tests `apps/api/tests/unit/test_smtp_mail.py`, against an `aiosmtpd` controller on a free port:
  - **delivery**: `SmtpMailSender.send` delivers one message with `From: MAIL_FROM` and the right recipient, subject and body, and uses STARTTLS. The test server offers STARTTLS with a throwaway certificate created at test time and never stored.
  - **refusals**: a server without STARTTLS is refused, and nothing is sent.
  - **errors**: connection and authentication errors raise a typed `MailDeliveryError`, carrying neither the password nor the token.
  - **logs**: no log line contains the token or the address in full.

  Failing settings tests in `apps/api/tests/unit/test_settings_smtp.py`:
  - production with `MAIL_BACKEND=smtp` starts when `SMTP_HOST` and `MAIL_FROM` are set;
  - it is refused naming the setting without `SMTP_HOST` or `MAIL_FROM`, or with `SMTP_STARTTLS=false`.
- [X] T026 Add `SmtpMailSender` to `apps/api/src/hds/core/mail.py`, using `smtplib.SMTP` in `asyncio.to_thread` with `starttls()` required and a 10 s timeout. Register `smtp` in `REAL_MAIL_BACKENDS` in `apps/api/src/hds/core/config.py` and add the `SMTP_*` and `MAIL_FROM` settings, with the password as `SecretStr`. `get_mail_sender` in `core/mail.py` picks it, as it does the outbox, and `DeferredMailer` stays around it (review R16). Update the docstrings that say no real backend exists, `infra/README.md` and `apps/api/README.md`
- [X] T027 [P] Failing tests `apps/api/tests/integration/test_create_admin.py`, all using services and repositories:
  - `python -m hds.scripts.create_admin --email a@example.com --organisation "Acme" --timezone Asia/Jerusalem`, with an optional `--name` (default "Administrator", since a user needs a name; review R17) and the password on stdin, creates the organisation and one Admin;
  - it exits non-zero if any user exists, if the password fails the password rules, or if stdin is empty;
  - the password never appears in its output.
- [X] T028 Add `apps/api/src/hds/scripts/create_admin.py`, which uses `FirstAdminService` in `apps/api/src/hds/services/first_admin.py`. No organisation service exists, so that service reuses the employee rule `validate_initial_password` and the site rule `validate_timezone`, with `OrganisationRepository.create`, `UserRepository.any_exists` and `UserRepository.lock_first_admin` (review R15)
- [X] T029 Mutation checks: STARTTLS skipped; the password in the error; create_admin allowed twice. Then the gates, both reviews, the log, and a commit `feat(api): SMTP mail backend and a first-Admin command (006 Group E)`

## Phase 6: Group F, the worker drains on shutdown (US5, P2)

**Goal**: FR-020. **Independent test**: send SIGTERM to a worker in the middle of a job; the job ends
once, and the worker exits within the grace period.

- [X] T030 [P] [US5] Failing tests `apps/api/tests/integration/test_worker_drain.py`, with real processes on the test database:
  - **during a solve**: SIGTERM during a solve (the solver stubbed to take 3 s) lets it finish and store, and the process exits 0 with no new claim;
  - **before the solve**: SIGTERM after claiming but before solving hands the job back: `queued`, the attempt given back (0), `lease_until` and `started_at` null, announced on the `schedule_jobs` channel; a second worker claims it at once and stores it once, as attempt 1 (review R35, R41; data-model.md);
  - **lease expiry**: SIGKILL in the middle of a solve leaves the job to be reclaimed after its lease. The test then sets the job's `lease_until` in the past on the test database and starts a second worker, which reclaims it (the claim already takes running jobs past their lease) and stores it once; `attempts` is 2 (FR-020). No lease setting is added
- [X] T031 [US5] In `apps/api/src/hds/worker.py`, handle SIGTERM by stopping the claim loop and finishing the current job. The operations server (Group C) runs uvicorn in the same event loop, and uvicorn's `Server.serve()` installs its own SIGINT and SIGTERM handlers and re-raises the signal on exit, which today cancels the job loop mid-solve. Run it without that signal capture (a `Server` subclass with a no-op `capture_signals`), and do not cancel the job loop when the ops server ends: the loop decides. Add a T030 case: SIGTERM during a solve still lets it store, with the ops server running. Add `release(id, attempt)` to `apps/api/src/hds/repositories/schedule_job.py` for a job claimed but not yet solved, checked before the build and again before the solve; a release that fails leaves the lease to expire and never fails the job. SIGINT drains like SIGTERM, and a second signal takes the default action (a forced exit). If the operations server ends on its own, the worker drains and then exits non-zero. A handed-back run is not observed in the job-duration metric, as a superseded one is not (review R30, R31, R34, R37, R39, R40). Add a comment saying the chart's `terminationGracePeriodSeconds: 90` must exceed the longest solve (60 s) plus storing
- [X] T032a [US1] Follow-up from PR 9's review (CodeRabbit): the `postgres` healthcheck in `infra/docker-compose.yml`, and the four postgres service healthchecks in `.github/workflows/ci.yml` (review R33), run `pg_isready` over the socket, which the image's first-start temporary server answers before it takes TCP, so `migrate` can start too early on the first `make up`. Probe over TCP (`pg_isready -h 127.0.0.1 -U ...`, and `--health-cmd "pg_isready -h 127.0.0.1"` in CI), as `scripts/ops/ci_local_db.sh` does. The compose worker gets `stop_grace_period: 90s`, the chart's grace (review R36). CI's quickstart job exercises it (FR-001)
- [X] T032 [US5] Mutation checks: no release; exit before storing; claim after SIGTERM. Then the gates, both reviews, the log, and a commit `feat(worker): finish or hand back the current job on shutdown (006 Group F)`
- [X] T032b [US5] Follow-up from PR 10's review (CodeRabbit): in `apps/api/src/hds/worker.py`, when the operations port is taken, uvicorn's start-up calls `sys.exit(1)`; the `SystemExit` escapes the event loop through the ops task, and `asyncio.run` then cancels the job loop mid-job, which T031 forbids. Run `serve()` in a wrapper that turns `SystemExit` into an ordinary end, so the "ops server ended on its own" path drains the job and exits non-zero (R30, R40). Tests: a unit test with the ops server's `serve()` raising `SystemExit` during a job (the job stores, then the error), and a `test_worker_drain` case with the ops port held
- [X] T032c Follow-up from PR 10's review (CodeRabbit): `apps/api/tests/unit/test_smtp_mail.py`'s `test_no_log_line_holds_the_token_or_the_full_address` captures every logger, so "a delivery is logged" passes on aiosmtpd's or asyncio's records alone. Require a record from `hds.mail`, and check the token, recipient and password over every `hds*` record. Mutant: drop the sender's `logger.info`; the test must fail

## Phase 7: Group G, the Helm chart (foundational for US3, US4, US5)

**Goal**: FR-016 to FR-019, and the base for Groups H to L. **Independent test**: the CI `chart` job.

- [X] T033 Failing test, a CI job `chart` with the script `infra/helm/tests/assert.sh`. It runs `helm lint` and kubeconform on `helm template` for `values-local.yaml` and `values-ci.yaml`, then asserts with `yq` on the rendered manifests:
  - **API Deployment**: `replicas: 2`, `maxUnavailable: 0`, `maxSurge: 1`; readiness on `/readyz`, liveness on `/livez` and startup as in `contracts/ops.md` §1; `preStop` sleep 5; `terminationGracePeriodSeconds: 30`; runs as non-root with a read-only root filesystem; images referred to by digest when `image.digest` is set.
  - **Worker Deployment**: `replicas: 1`, probes on port 9100, `terminationGracePeriodSeconds: 90`.
  - **PodDisruptionBudget**: `minAvailable: 1` for the API.
  - **Migration Job**: a `pre-install,pre-upgrade` hook running `alembic upgrade head` on the API image.
  - **Ingress**: sends `/api`, `/auth` and `/me` to the API and `/` to the web, and routes neither `/metrics` nor port 9100.
  - **NetworkPolicy**: lets only the `monitoring` namespace reach port 9100.
  - **Settings**: `TRUSTED_PROXY_COUNT: "1"` and `ENVIRONMENT: production` for local; no Secret holds a literal value in either values file.
  - **History**: `maxHistory` is documented as 10 for Flux.
  - `make ci-local` runs the same script, and ShellCheck covers it, locally and in CI (review R51).
  - **`hds-postgres`** (research R3, amended): the same lint and kubeconform, and: no Secret holds a literal value; the StatefulSet runs as non-root with `pg_isready` probes over TCP; its NetworkPolicy lets in only the hds pods; the password comes from `hds-api`'s `DATABASE_PASSWORD` by name. The hds chart's `DATABASE_URL` names the Service from its values.
- [X] T034 [P] Failing test `apps/api/tests/unit/test_ingress_prefixes.py`: every path in the OpenAPI document of `hds.main.app` (FastAPI nests included routers in a private type, so `app.routes` is not walked; review R50) starts with a prefix listed in `infra/helm/hds/values.yaml` under `ingress.apiPrefixes`, or is one of the three health paths, which the ingress does not route (research R2)
- [X] T035 Add `infra/helm/hds/`: `Chart.yaml`, `values.yaml`, `values-local.yaml`, `values-ci.yaml`, and the templates for the API, worker, web, migration Job, Ingress, PodDisruptionBudget, NetworkPolicy, ServiceAccounts and `_helpers.tpl`; its values name the database Service. Add `infra/helm/hds-postgres/` (research R3, amended): `Chart.yaml`, `values.yaml`, and the templates for the PostgreSQL StatefulSet, its Service and a NetworkPolicy that lets in only the hds pods. Secrets are referred to by name (`hds-api`, `hds-smtp`, `hds-backup`, `hds-registry`) and never defined in values. The PostgreSQL password comes from `hds-api`'s `DATABASE_PASSWORD`
- [X] T036 [P] Failing test `apps/api/tests/integration/test_migrate_lock.py`: two `python -m hds.scripts.migrate` processes started together on one database both exit 0, the upgrade runs once, and the second waits on the advisory lock (FR-016). A database at a revision the code does not know (newer, for example the older image after a rollback) is left untouched and the command exits 0, rather than `alembic upgrade head` stopping on an unknown revision. Then add `apps/api/src/hds/scripts/migrate.py`, which takes `pg_advisory_lock` and runs `alembic upgrade head`, and is the migration Job's command
- [X] T037 Gates, `helm lint`, both reviews, the log, and a commit `feat(infra): a Helm chart with rolling updates, probes and a locked migration hook (006 Group G)`

## Phase 8: Group H, backups and the restore test (US3, P1)

**Goal**: FR-011 to FR-014, SC-005, SC-006. **Independent test**: the CI `backup-restore` job, and
quickstart scenario 3 in the cluster (Group M).

- [X] T038 [US3] Failing test, a CI job `backup-restore` with the script `infra/backup/tests/run.sh`, using compose with the backup image. The two rclone remotes are crypt over local directories, standing in for the machine and for Drive, with throwaway passwords. The script fails at the first unmet step:
  1. It seeds and creates known shifts, then runs `backup.sh`, which writes a `.dump` and a `.manifest.json` to both remotes. The manifest has every field in data-model.md, and its `sha256` matches the dump.
  2. The underlying directories hold no readable table name or e-mail address, which `grep` checks.
  3. `restore-test.sh` restores the offsite copy into a fresh `postgres:16` container and passes.
  4. A changed row count in the manifest makes `restore-test.sh` fail, naming the table; a changed `alembic_revision` in the manifest makes it fail, naming both revisions. This comparison runs before `migrate`.
  5. A corrupted offsite dump makes `restore-test.sh` fail on the checksum.
  6. An unreachable offsite remote makes `backup.sh` exit non-zero, and the local copy exists.
  7. `prune.sh`, run with a fake date 31 days on, deletes the expired backups but keeps the newest valid one, even when it is expired. With ten `vault-*.snap.age` files on each side, it keeps the newest 8 and never deletes the newest one.
  8. `restore.sh` into a database with data refuses without `CONFIRM=replace`, and with it, restores and the shifts match.

  (FR-011 to FR-014)
- [X] T039 [P] [US3] Add `infra/Dockerfile.backup`, built from the pinned `postgres:16` with the pinned rclone binary copied from `rclone/rclone`, `jq`, `curl` and the pinned `age`, running as non-root. Add `infra/backup/rclone.conf.tmpl`, which defines the plain remotes `local` (a path on the backup volume) and `offsite` (type `drive` now; `s3` later, which changes only this remote), and `local-crypt` and `offsite-crypt` as crypt over them, with names encrypted and is filled from environment variables at run time. No secret goes in the template (research R11)
- [X] T040 [US3] Add `infra/backup/backup.sh`. It runs `pg_dump --format=custom`, computes the row counts, the revision and the SHA-256, and writes the dump and then the manifest to `local-crypt:` and `offsite-crypt:`. If either fails it exits non-zero. Add `infra/backup/prune.sh`, which deletes past `expires_on` and never deletes the newest backup whose checksum verifies. It also keeps the newest 8 `vault-*.snap.age` files on `local:` and `offsite:` (they have no manifest) and never deletes the newest one. Both scripts are POSIX `sh` with `set -eu` and log no secret. They share `infra/backup/lib.sh`, which renders the rclone config from the environment; `hds-rclone.sh` runs rclone with it (the runbook and the test), and `vault-snapshot.sh` is T043's snapshot job (review R74)
- [X] T041 [P] [US3] Failing tests `apps/api/tests/integration/test_check_restore.py`: `python -m hds.scripts.check_restore`, run after `migrate`, against a restored database exits 0. It takes no manifest: the revision and row-count comparison happens before `migrate`, in `restore-test.sh` (T038 step 4), because a migration may change both. A backup from an older revision restores, migrates and then passes; a backup at a revision the code does not know passes without migrating. It runs the `ready()` checks and loads the organisation, its users and the last month's shifts through the repositories, it writes nothing (the test checks no row changes), and its output holds no personal data. `OrganisationRepository.all()` lists the organisations for it (review R74)
- [X] T042 [US3] Add `apps/api/src/hds/scripts/check_restore.py`, and `infra/backup/restore-test.sh`, which downloads the newest offsite backup, verifies it, runs `pg_restore` into the sidecar database, compares the revision and row counts with the manifest; `python -m hds.scripts.migrate` and then `check_restore` follow on the copy in the API image (T043's `hds-api` container; `run.sh` runs them the same way), since the backup image has no Python (research R11, review R72). Add `infra/backup/restore.sh`, which refuses a database with data unless `CONFIRM=replace` is set. It only restores: `make ops-restore` (T049b) scales the API and the worker to 0 before it, runs `migrate` after it, and only then scales them back, from the operator's machine, so an older backup leaves no pods that never become ready (review R76, R78)
- [X] T043 [US3] Chart templates `infra/helm/hds/templates/backup-cronjob.yaml` and `restore-test-cronjob.yaml`:
  - **backup**: 03:30, `timeZone` set from values (02:30 is skipped on the spring-forward day, review R83).
  - **restore test**: Sundays 04:00, with an empty `postgres:16` as a native sidecar (an init container with `restartPolicy: Always`), listed **before** the `hds-backup` init container and with a `startupProbe` on `pg_isready`, because Kubernetes waits only for a native sidecar's startup probe before starting the next init container. The download, checksum, `pg_restore` and manifest comparison run in an `hds-backup` init container; `migrate` and `check_restore` then run in an `hds-api` container on the same image as the release, so the check uses the running code.
  - **both**: `startingDeadlineSeconds: 86400`, `concurrencyPolicy: Forbid`, history limits of 3. The backup mounts a PVC `hds-backups` for `local-crypt` (kept when the release is uninstalled); the restore test does not, since it reads only the offsite copy (review R73). The backup reaches the database through the Service the hds values name, which the `hds-postgres` release provides (research R3, amended).

  Add `vault-snapshot-cronjob.yaml`, which runs weekly and takes a raft snapshot through Vault's HTTP API with `curl` (the image needs no vault binary), encrypts it with `age` to the **public** key in the chart values (`vault.snapshotRecipient`, not a secret), and writes `vault-<UTC time>.snap.age` through the plain `offsite:` remote and the plain local volume, **not** through `offsite-crypt:`, whose password lives in Vault (research R11). It runs under its own ServiceAccount (T052). Extend `infra/helm/tests/assert.sh` with these values, including that the postgres sidecar is the first init container and has a `pg_isready` startup probe
- [X] T044 [US3] Add `docs/runbooks/restore.md`:
  - restoring a named backup in the running cluster;
  - restoring after losing the machine: a new cluster, then Vault from its snapshot (downloaded from Drive in a browser, decrypted with the age private key, restored with the unseal keys), then the database from Drive. The recovery kit kept outside the machine is the unseal keys, the age private key and the Google account;
  - the Drive OAuth set-up, including the 7-day expiry warning for apps left in testing (research R11);
  - moving the offsite copy to S3 by changing the `offsite` remote only.

  The steps are numbered with expected output, so the procedure takes under an hour (SC-005)
- [X] T045b [US3] `scripts/ops/backup_jobs.sh`, called by `make ops-backup`, `make ops-restore-test` and `make ops-backups` (Proposed, contracts/ops.md §5). `run` makes a Job from the CronJob, waits for it, prints its logs and exits non-zero on failure, and refuses while a Job of that CronJob is still active (`kubectl create job --from` skips `concurrencyPolicy: Forbid`); `list` shows the backup names on Drive from a one-off pod on the backup image. `infra/tests/check_backup_jobs.sh` checks it against a fake `kubectl` (review R76)
- [X] T045 [US3] Mutation checks on the scripts: the manifest written before the dump; prune deleting the newest; the restore test using the local copy; no checksum check. Then ShellCheck, the gates, both reviews, the log, and a commit `feat(infra): encrypted daily backups with a weekly restore test (006 Group H)`

## Phase 9: Group I, release and Flux (US4, P2)

**Goal**: FR-015, FR-018, FR-021 to FR-023, SC-002, SC-008. **Independent test**: quickstart scenario 4
(Group M); in CI, the release workflow's dry run.

- [X] T046 [US4] Failing test, a workflow `.github/workflows/release.yml` with a `workflow_dispatch` dry-run input that builds and scans but does not push. Prove both gates fail on purpose:
  - a branch that pins a base image with a known critical CVE makes the Trivy step exit non-zero;
  - a sample key placed in the image makes the Trivy secret step exit non-zero.

  Record both runs in the log. GitHub dispatches only a workflow on the default branch, so the workflow's own dry run cannot run before the merge (review R99): `infra/tests/release_gates.sh` proves both gates locally with the workflow's scripts and flags (`infra/release/scan.sh`, the pinned Trivy image), on `infra/Dockerfile.backup` over an old `postgres` base pinned by digest and on the real image with a private key generated at run time; and proves that the pushed images are the scanned layouts, against a throwaway local registry. `dry_run=false` fails at once. **After the merge, the user**: `gh workflow run release.yml -f dry_run=true`
- [X] T047 [US4] Fill in `.github/workflows/release.yml`:
  - **Trigger**: `on: workflow_run` for `CI`, only when `conclusion == 'success'` and the branch is `main`, with `concurrency: { group: release, cancel-in-progress: false }`.
  - **Build**: build `hds-api`, `hds-web` and `hds-backup` for `linux/amd64,linux/arm64` with `HDS_VERSION` set to the SHA.
  - **Scan**: run Trivy for vulnerabilities (HIGH and CRITICAL fail) and for secrets.
  - **Publish**: push to `ghcr.io/${{ github.repository_owner }}` tagged with the SHA. Then `helm package` with `--app-version <sha> --version 0.1.<run_number>` and the image digests written into values, and `helm push` to `oci://ghcr.io/<owner>/charts`.
  - **Permissions**: `packages: write` and `contents: read` only.
  - **Settled in Group I** (reviews R98 to R101, research R4):
    - **Trust**: the job also requires CI's run to be for a `push` from this repository (a fork's pull request from a branch named `main` passes `branches: [main]`), and `infra/release/trust.sh` checks the event again before any of the tested commit's code runs; the release checks out `head_sha`. Every action is pinned by commit.
    - **Build once**: each image is built for both platforms into an OCI image layout, scanned per platform, and pushed as it is by `skopeo copy --all --preserve-digests` (`infra/release/push_images.sh`), which checks each pushed digest against the layout's.
    - **Both charts** (`infra/release/publish_charts.sh`): `hds` at `0.1.<run number>` with the digests in its values, rendered first to check every image is by digest; `hds-postgres` at its own `Chart.yaml` version, pushed only when the registry answers "not found". The `chart` job, on pull requests, fails a change to `infra/helm/hds-postgres` that does not raise its version (`infra/helm/tests/chart_version.sh`, full history).
    - **Scan**: `--ignore-unfixed` (the owner's reading of FR-015, Proposed). Fixed findings are raised, not ignored: the web image runs `apk upgrade` over its base; the backup image drops `gosu` and builds rclone v1.75.2 and age v1.3.2 from source with Go 1.27.3.
    - **actionlint** checks both workflows in the CI `lint` job and `make ci-local` (review R110), since this one can write packages.
  - **Follow-ups**:
    - **On or after 2026-11-10**: set `RCLONE_RAISE=golang.org/x/net@v0.62.0` in `infra/Dockerfile.backup` (the modules it raises with it are all older than two weeks; the go.mod diff is in the log), rescan, and record it. Until then the backup image fails the gate on CVE-2026-78661, as it should.
    - **At the first real release**: if GHCR answers "denied" rather than "not found" for the `hds-postgres` package before it exists, the release fails with nothing pushed (review R117); then decide with the reviewer how to publish its first version.
    - **On or after 2026-11-20**: when rclone 1.76.0 is two weeks old, scan its official binary; if it is clean, go back to copying it from `rclone/rclone` by digest and drop the source build.
- [X] T047c [US4] Sign every release, and have Flux install only signed charts (FR-028, **Proposed**: the owner's request on 2026-11-07; research R4; review R128). In `.github/workflows/release.yml`, `id-token: write` on the release job only. `infra/release/sign.sh` signs keyless with cosign (pinned by digest), by digest only, never by tag; `push_images.sh` signs each image index straight after its push and digest check, and `publish_charts.sh` each chart by the digest read back from the registry with skopeo, hds-postgres again each release. Its key mode and every stand-in (`HDS_SIGN_KEY`, `HDS_COSIGN`, `HDS_SIGN`, `HDS_HELM`, `HDS_SKOPEO`) are refused when `GITHUB_ACTIONS=true`. Both OCIRepositories carry `verify` (`provider: cosign`, `matchOIDCIdentity` with issuer `^https://token\.actions\.githubusercontent\.com$` and subject `^https://github\.com/AviyaYemini/HDS/\.github/workflows/release\.yml@refs/heads/main$`). Proofs: `assert.sh` checks the identity on both; `check_release.sh` checks the job's permissions, that every signature is by digest and after its push, and that the stand-ins are refused in Actions; `release_gates.sh` step (f) shows a key-signed push verifies and that another key, an unsigned image and a moved tag are refused. Flux's own refusal is checked in T059's k3d cluster; the identity's exact form, with the first real release (T064)
- [X] T048 [P] [US4] Add `infra/flux/clusters/local/`: Leave helm-controller's drift detection off for `hds` (`driftDetection.mode` unset or `disabled`): runbook §2 step 7 suspends the `hds-backup` CronJob with `kubectl patch`, which drift correction would undo on the next reconcile (review R97)
  - `kustomization.yaml`;
  - `oci-repository.yaml` and `oci-repository-postgres.yaml`, one for each chart (review R98), with `semver: ">=0.1.0"` and `interval: 5m`;
  - `helm-release-postgres.yaml`, the `hds-postgres` release, installed first (research R3, amended);
  - `helm-release.yaml`, with `dependsOn` the `hds-postgres` release, `install.remediation.retries: 0`, `upgrade.remediation.retries: 0`, `upgrade.remediation.strategy: rollback`, `timeout: 10m`, `maxHistory: 10`, and `values-local` from a ConfigMap;
  - `alertmanager-provider.yaml` and `alert.yaml`, which forward `HelmRelease` errors to Alertmanager (FR-010a, FR-018).

  A CI step validates them with kubeconform and Flux's CRD schemas (the `crd-schemas.tar.gz` of Flux v2.9.6, which T063 installs, by SHA-256). The HelmReleases live in `flux-system` with `releaseName`, `targetNamespace` and `storageNamespace` set, so `helm -n hds` finds the releases; `upgrade.remediation.remediateLastFailure: true`, without which `retries: 0` never rolls back (review R102); `values-local` comes from the ConfigMap `hds-values` in `flux-system`, which T063 creates from `infra/helm/hds/values-local.yaml`
- [X] T049 [US4] `scripts/ops/rollback.sh` and `scripts/ops/resume.sh`, called by `make ops-rollback` and `make ops-resume`:
  - **rollback**: `flux suspend helmrelease hds`, then `helm rollback hds` to the previous revision with `--wait`, then print the version now running. It never runs a migration downgrade (FR-021).
  - **resume**: `flux resume`.

  Add them to `Makefile`, and add `docs/runbooks/operations.md` §Deploys
- [X] T049b [US3] `scripts/ops/restore.sh`, called by `make ops-restore BACKUP=<name> [CONFIRM=replace]`, from the operator's machine with kubectl (review R76, R77): suspend the `hds` HelmRelease; save each Deployment's replica count; scale the API and the worker to 0 and wait until no pod is left, counting pods still in their grace period (`.status.replicas` already leaves those out, review R80); run `infra/backup/restore.sh` in a Job on the backup image; run `migrate` in a Job on the API image the Deployment runs (the release's own migrate Job, from `helm get hooks hds`); scale back to the saved counts; resume. Flux stays suspended for the whole run, so a release applied mid-restore cannot run its pre-upgrade migrate hook against a half-restored database or reset the replica counts. Settle the expected output in `docs/runbooks/restore.md` §1. Settled in Group I: it runs `infra/backup/restore.sh --check` first, while the API still serves, so a refusal stops nothing; it suspends the `hds-backup` CronJob too and sets its `suspend` back to the value it found (runbook §2 keeps it suspended until its step 10); `restore.sh` exits 3 when it failed having changed nothing, and only then are the API and the worker started again; any other failure leaves them at 0 with Flux suspended
- [X] T050 [US4] Mutation checks: release on a failed CI; no concurrency group; rollback without suspend. Then the gates, ShellCheck, both reviews, the log, and a commit `feat(infra): publish scanned releases to GHCR for Flux to install (006 Group I)`

## Phase 10: Group J, Vault and the Vault Secrets Operator (US6, P1)

**Goal**: FR-024, FR-027. **Independent test**: quickstart scenario 6, last bullet (Group M); in CI, the
chart assertions.

- [X] T051 [US6] Failing assertions in `infra/helm/tests/assert.sh`:
  - with `vault.enabled=true` the chart renders a `VaultConnection`, a `VaultAuth` (Kubernetes auth, role `hds`), and one `VaultStaticSecret` per path in data-model.md (`hds/api`, `hds/smtp`, `hds/backup`, `hds/registry`), each with `refreshAfter: 60s` and `rolloutRestartTargets` naming the Deployments that read it;
  - with `vault.enabled=false` (CI) none renders, and the chart expects the Secrets to exist.

  Settled in Group J (research R3 amended; review R135): the Vault Secrets Operator's objects are a third chart, `infra/helm/hds-secrets`, installed first, since in `hds` the first install deadlocks (`hds-postgres` reads `hds-api` before it exists, and the pre-install migration hook runs before any of `hds`'s resources). The `hds` chart renders none in any values, and CI makes the Secrets by hand. The VaultStaticSecrets are kept on uninstall; `hds-api` and `hds-smtp` restart `hds-api` and `hds-worker`, `hds-backup` nothing (only Jobs read it), never the `hds-postgres` StatefulSet; `hds-registry` renders only with `registry.enabled`, as a `dockerconfigjson`. kubeconform checks them against the pinned operator chart's CRDs, strict (review R140), and `assert.sh` renders Vault's and the operator's charts with `infra/cluster`'s values
- [X] T052 [US6] Add `infra/helm/hds/templates/vault-secrets.yaml` (as the chart `infra/helm/hds-secrets`, T051), and add `infra/cluster/vault-values.yaml`, which is one node with integrated storage (raft) on a PVC, the injector off and the UI off. Add `infra/cluster/vault-policy.hcl`, which gives read-only access to `hds/*` for the `hds` role, and `infra/cluster/vault-snapshot-policy.hcl` with only `path "sys/storage/raft/snapshot" { capabilities = ["read"] }`, bound to a Kubernetes-auth role `vault-snapshot` for the snapshot job's own ServiceAccount (`hds-vault-snapshot`; the chart's `vault.role`), with a short token TTL. Write only `kubernetes_host` to `auth/kubernetes/config`, never `kubernetes_ca_cert` or `token_reviewer_jwt`, so Vault reads both from its own pod and a snapshot restored onto a new cluster still lets VSO and the snapshot job sign in (docs/runbooks/restore.md §2 step 7, review R95). Add `infra/cluster/vso-values.yaml`. Settled in Group J (reviews R132 to R138): the policy reads `hds/data/*` (KV v2); the roles bind exactly `hds-vault` and `hds-vault-snapshot` in `hds`, for the audience `vault`, with 10- and 5-minute tokens (the snapshot pod gets a projected token for that audience and no API-server token); `scripts/ops/vault_configure.sh` (`make ops-vault-configure`, Proposed) writes the mount, the auth config, the policies and the roles, reading the token from the environment only, and `infra/tests/check_vault_policy.sh` runs it against a dev Vault of the pinned version and checks what each token can and cannot do; Vault's NetworkPolicy (Proposed) lets in only the operator's namespace and the snapshot pods
- [X] T053 [US6] `scripts/ops/unseal.sh` (`make ops-unseal`) reads the unseal key from the terminal without echo and runs `vault operator unseal`. Add `docs/runbooks/operations.md` §Vault: run init once, keep the unseal key and root token in a password manager, put them in no file, revoke the root token after set-up, generate the age key pair once (the public key into the values, the private key into the password manager with the unseal keys), and write each path's keys with the commands listed. Note that while Vault is sealed after a restart the snapshot job fails and `HdsVaultSnapshotFailed` fires, as a reminder to unseal. Settled in Group J: the key reaches Vault on standard input with `vault write sys/unseal key=-` (`vault operator unseal -` takes "-" as the key; review R141), from a silent prompt or a pipe; the paths are written as one JSON object on stdin (`vault kv put -mount=hds PATH -`) and a single key changed with `vault kv patch ... KEY=-`, never `kv put` (review R136); the database password changes in Vault first, then with `\password hds` (review R133); the root token stays in the shell's memory (`read -rs`), never `~/.vault-token`; never uninstall `hds-secrets`
- [X] T054 [US6] Gates, `helm lint`, both reviews, the log, and a commit `feat(infra): secrets from Vault through the Vault Secrets Operator (006 Group J)`

## Phase 11: Group K, monitoring and failure messages (US2, US3, US4)

**Goal**: FR-007 to FR-010a. **Independent test**: the `promtool` rule tests in CI; in the cluster, a
broken backup sends an e-mail (Group M).

- [X] T055 [P] [US3] Failing rule tests `infra/cluster/alerts/tests.yaml`, run with `promtool test rules` in the `chart` job. Each alert in `contracts/ops.md` §3 fires on its series:
  - a failed `hds-backup` Job;
  - `kube_cronjob_status_last_successful_time` older than 26 h for the backup and 8 d for the restore test;
  - a failed `hds-restore-test` or `hds-vault-snapshot` Job.

  The names are the real CronJobs' (`hds-backup`, `hds-restore-test`, `hds-vault-snapshot`; their Jobs' names derive from them), not shortened ones

  `HdsBackupMissing` and `HdsRestoreTestMissing` carry `for: 1h`: a case where the series turns stale and recovers within the hour (the Mac waking after a day off and the catch-up run finishing) sends nothing. Each one stays quiet on healthy series, and no rule uses `hds_http_*` (Clarifications: no rules on error rates). Settled in Group K (reviews R159, R162): the rules are one raw file, `infra/helm/hds/files/alert-rules.yaml`, which the PrometheusRule reads as it is and promtool tests (`infra/tests/check_alerts.sh`, the cluster's Prometheus version). A Job is a CronJob's by its owner (`kube_job_owner`), and only each CronJob's newest Job counts, through two recording rules: a failed `make ops-backup` alerts, a refused `make ops-restore` (no owner) never does, an old failure stops alerting after a later success. The "missing" alerts also fire when a CronJob has never succeeded, 26 h or 8 d after it was made
- [X] T056 [US3] (kube-prometheus-stack goes in the namespace `values.monitoring.namespace`, `monitoring`, which the NetworkPolicy and Flux's Alertmanager provider rely on) Add `infra/helm/hds/templates/prometheus-rules.yaml` (the five job alerts, the two "missing" ones with `for: 1h`) and `servicemonitors.yaml` (the API and the worker on port 9100). Add `infra/cluster/monitoring-values.yaml` for kube-prometheus-stack: Grafana off; Alertmanager with one e-mail receiver using `smarthost`, `auth_username` and `auth_password` from the `hds-smtp` Secret, `require_tls: true`, and `to` from `OPERATOR_EMAILS`; message templates naming the alert, the object, the time and the summary only (FR-010a). Settled in Group K (reviews R159 to R165): Alertmanager in `monitoring` cannot read `hds-smtp`, and `OPERATOR_EMAILS` are personal, so its whole `alertmanager.yaml` is rendered by a VaultStaticSecret in `monitoring`, in hds-secrets, with its own sign-in (Vault role `alertmanager`, which reads `hds/data/smtp` only); every value goes in as `toJson`, safe against any character (`infra/tests/check_alertmanager.sh` fills it with hostile values and runs `amtool check-config`). The message templates replace Alertmanager's `email.default.subject` and `email.default.html` from a ConfigMap; one receiver and no sub-routes, so Flux's alerts are e-mailed too. Prometheus selects every ServiceMonitor and PrometheusRule, whatever their labels; the stack's default rules are off (its Watchdog would e-mail constantly); kube-state-metrics filters no family; the stack's kinds render only with `monitoring.enabled` (off in CI). The API's and the worker's Services already expose the ops port
- [X] T057 [US2] `scripts/ops/status.sh` (`make ops-status`) prints:
  - per Deployment, ready over desired and the image version;
  - the last successful and the last failed backup and restore test;
  - the last three `helm history` rows;
  - the firing alerts from Alertmanager's API, reached through `kubectl port-forward`;
  - Vault: sealed or not, and `vault snapshot: not configured (vault.snapshotRecipient is empty)`, marked `FAIL` in production, until T064 sets the public key (review R138).

  Each failing item is marked `FAIL`. Test it against a k3d cluster in the `rolling-update` job (T059). Settled in Group K (review R161): it also fails when Prometheus or Alertmanager is not ready or Alertmanager's configuration Secret is missing, since nothing else says alerting is down; it reads the alerts through the API server's service proxy (`kubectl get --raw`), not a port-forward; `infra/tests/check_status.sh` checks every FAIL against a fake kubectl and helm
- [X] T058 Gates, ShellCheck, both reviews, the log, and a commit `feat(infra): scrape with Prometheus and e-mail failure alerts (006 Group K)`

## Phase 12: Group L, the rolling-update proof (US5, P2)

**Goal**: FR-016, FR-017, FR-019, SC-003. **Independent test**: the CI `rolling-update` and
`compatibility` jobs.

- [ ] T059 [US5] Failing test, a CI job `rolling-update` with the script `infra/tests/rolling_update.sh`:
  1. Create a k3d cluster from `infra/cluster/k3d.yaml`.
  2. Build the images at the pull request's base commit and the head, and import them with `k3d image import`. The head's API image gets one extra throwaway additive migration (a nullable column on a scratch table) on top, built only in this job, so every run upgrades across a migration even when the pull request has none.
  3. Create the `hds-*` Secrets from throwaway values, and install both charts at the base, `hds-postgres` first, then `hds` with `values-ci.yaml` (`ENVIRONMENT=production`, SMTP pointed at an in-cluster fake server).
  4. Create the first Admin.
  5. Start `infra/tests/load.py`, which runs for the whole upgrade: 20 requests a second, signed-in reads and writes, plus a refresh every 30 s.
  6. Queue a schedule job, then run `helm upgrade --wait` to the head.
  7. Stop the load.

  The job fails on:
  - any response other than the expected one, and any connection error;
  - the signed-in session no longer refreshing;
  - the schedule job not ending `succeeded` with exactly one result;
  - `make ops-status` reporting `FAIL`.

  It also checks Flux's signature check (T047c): with source-controller and an OCIRepository whose `verify` uses a key made in the job, an unsigned chart in a registry in the cluster leaves it not Ready with a verification error, and the same chart signed with that key makes it Ready.

  It also runs the Playwright smoke journeys against the upgraded cluster's ingress, and checks the hop we control (ADR 007, research R14): two pods inside the cluster, with different pod addresses, call Traefik's Service and fail sign-in, and their failures count separately; a third request with a forged `X-Forwarded-For` is still counted under its own pod's address
- [ ] T060 [US5] Add `infra/tests/load.py` (httpx, using only API dependencies already present, which prints totals and the failures) and `infra/cluster/k3d.yaml` (one server, ports 8080:80 and 8443:443, local-path)
- [ ] T061 [P] [US5] Failing test, a CI job `compatibility` with the script `infra/tests/compat.sh`: check out the base commit into a temporary worktree, copy the head's `apps/api/alembic/versions/` over the base's (keeping the base's `env.py`), and run the base's integration tests with the base's code, skipping the base's own `test_migration_*` tests, which pin the old head. After the run, assert that `alembic_version` holds the head's revision. Files are copied rather than a conftest switch added, because the base has no such switch until this feature merges. A change that drops or renames a column the base uses fails here (FR-016). Prove it on a throwaway branch with a migration that renames `shifts.headcount`, and record the failing run in the log. That branch, like every mutant, lives only in a local throwaway branch or worktree, is deleted after, and is never pushed
- [ ] T062 [US5] Gates, ShellCheck, both reviews, the log, and a commit `test(infra): prove zero-downtime updates and old-new compatibility (006 Group L)`

## Phase 13: Group M, the shared environment end to end (US1 to US6; run with the owner)

**Goal**: quickstart scenarios 3, 4 and 6 on the real machine, and SC-002 to SC-008.

- [ ] T063 Add `scripts/ops/cluster.sh` (`make ops-cluster`), which runs once per machine and is safe to run again:
  1. It checks for k3d, kubectl, helm, flux, vault and rclone, naming any that is missing.
  2. It creates the k3d cluster.
  3. It installs Flux (only the source, helm and notification controllers), Vault (in `vault`), the Vault Secrets Operator (in `vault-secrets-operator`, the namespace Vault's NetworkPolicy lets in) and kube-prometheus-stack (in `monitoring`) from the pinned charts, with the values in `infra/cluster/`.
  4. It creates the `hds` namespace, after Flux and before the apply: both releases install into it, Helm creates no namespace without `install.createNamespace`, and the Vault Secrets Operator writes the `hds-*` Secrets there before `hds` installs (review R125). It creates the ConfigMap `hds-values` in `flux-system` from `infra/helm/hds/values-local.yaml` (key `values.yaml`).
  5. It does **not** apply `infra/flux/clusters/local/`: `scripts/ops/flux.sh` (`make ops-flux`) does, as its own step, once Vault is unsealed and its paths are written (operations.md, section Vault, step 8; review R135). `hds-secrets` turns Ready as soon as Helm applies its objects, before the operator has synced, so `hds-postgres` applied earlier would start without its Secret and fail an install that is not retried. `make ops-flux` (**Proposed**) needs no Vault token (review R149): it applies the sources and only the `hds-secrets` HelmRelease, waits until the operator has synced `hds-api`, `hds-smtp` and `hds-backup` into `hds` (a missing path or a sealed Vault stops it there, with nothing else installed), and only then applies `hds-postgres` and `hds`.
  6. It prints the next steps: init and unseal Vault, configure it, write the secrets, `make ops-flux`, create the first Admin. Alertmanager is not Ready until `make ops-flux` has synced its configuration (`alertmanager-hds`, from hds-secrets): expected, not a fault (review R165); `make ops-status` fails on it after that.
- [ ] T064 With the owner, on their machine:
  1. `make ops-cluster`, then Vault init and unseal, `make ops-vault-configure`, then write the `hds/*` paths (operations.md, section Vault): the SMTP relay, the Drive token from `rclone authorize drive` with scope `drive.file`, and the crypt passwords. Make the age key pair and set `vault.snapshotRecipient`. Then `make ops-flux`.
  2. Decide whether the GHCR packages are public or private. If private, add `hds/registry` (with `registry.enabled` in hds-secrets and `imagePullSecrets: [hds-registry]` in hds), and make the pull secret `ghcr-flux` in `flux-system` by hand for the OCIRepositories' `secretRef` (review R150).
  3. Merge to `main` and watch the first deploy arrive within 30 minutes (SC-002). Both OCIRepositories must be Ready with the signature verified (T047c): `flux -n flux-system get sources oci` and the source's `SourceVerified` condition. A verification error here means the identity in `infra/flux/clusters/local/oci-repository*.yaml` does not match the certificate's subject; compare it with `cosign verify` output for the chart before changing it.
  4. `make ops-create-admin`.
  5. `make ops-backup` and `make ops-restore-test`, then check that the Drive folder holds only encrypted names. Then `make ops-restore BACKUP=<name>`, which refuses, and with `CONFIRM=replace`, which restores (quickstart scenario 3, review R79).
  6. Break the Drive token and see the `HdsBackupFailed` e-mail, then restore the token.
  7. `make ops-rollback`, then `make ops-resume`, timed (SC-008).
  8. Rotate `JWT_SECRET_KEY` and stay signed in.

  Record each outcome, with times, in the log
- [ ] T065 [P] Add ADR 013 `docs/adr/013-deployment-and-operations.md`: the cluster, the pull-based deploys (with Group I's choices, research R4: the trust check on `workflow_run`, one build per release scanned and pushed as it is, `--ignore-unfixed` as Proposed, `hds-postgres` versioned by hand; review R111; keyless signing and Flux's verification of the charts, Proposed, with their limit that whoever can push to `main` can sign, T047c), Vault with the operator (with Group J's choices: the third chart `hds-secrets` and the first-install deadlock it avoids, Flux given the releases only after Vault is written, the restart targets, the database password's rotation order, the stdin-only handling of keys and tokens; review R147), the private metrics port, the alerts (with Group K's choices: Alertmanager's configuration rendered from Vault in `monitoring`, owner-based selection of the backup Jobs, no default rules and so `make ops-status` checking alerting itself; review R172), traces through the job row, the SMTP relay, backups through rclone, Vault's snapshot through age, and the accepted limit that the client's address is lost before Traefik on a Mac (research R14; revisiting it with the cloud move is Proposed), with the rejected alternatives from research.md. Add the same limit to the proxy note in `infra/README.md`. Add it to the index in `docs/`
- [ ] T066 [P] Update `docs/hds-api.md` §9 with an operations note linking the runbooks, and §12 decision 8 as settled (a local k3d cluster). Also add `apps/api/README.md` for the new settings, `infra/README.md` for the shared environment, the `docs/` index, and `.env.example` for the new optional settings, leaving secrets empty
- [ ] T067 Final gates on the whole branch, the Markdown link check, `/hds-review`, `/code-review high`, the log, and a commit `docs(spec): close 006`

## Dependencies

- **A** comes first: it pins every tool and image the other groups use.
- **B** (US1) needs `/readyz` from **C**. Do T007 to T011 after T014, or accept the expected failure at step 4 until then.
- **C**, then **D** (US2), each need only A.
- **E** needs only A. It must land before **G**'s `values-ci` (production needs SMTP) and before **K**.
- **F** (US5) needs only A.
- **G** needs C (the probes), E and F (the grace period). It blocks H's CronJobs, J, K and L.
- **H** (US3): its scripts and the `backup-restore` job need only A and B. The CronJob templates need G.
- **I** (US4) needs G.
- **J** (US6) needs G.
- **K** needs G, H and I.
- **L** (US5) needs G, F and K's `status.sh`.
- **M** needs all of the above, and the owner.

## Parallel opportunities

- **In A**: T002, T003 and T005 touch different files.
- **In C**: T013, T015 and T017 are three test files; T014 and T016 then run in sequence, because both edit `main.py`.
- **Across groups**: E (T025 to T028) and F (T030 to T031) can run beside C and D, since they touch other files.
- **In H**: T039 and T041 run beside T038's script.
- **In I**: T048 (Flux files) runs beside T047.
- **In M**: T065 and T066 can be written while the owner sets up accounts.

## Implementation strategy

- **MVP**: Groups A to C. With them a developer starts everything with one command, and the parts
  report their health, version and metrics. This is US1 and the core of US2.
- **Next**: D, E and F make the API production-ready, adding traces, real mail and a clean shutdown.
- **Then**: G to L build and prove the shared environment in CI, without the owner's machine.
- **Finally**: M turns it on for real.
- Each group is committed on its own and leaves `main` deployable.
- **Pull requests** (Constitution IX): five, each squash-merged and started from the updated `main`:
  A–C (kept together: B's quickstart needs C's `/readyz`), D–F, G–H, I–L and M.
