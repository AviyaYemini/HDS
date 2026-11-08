# HDS API

The FastAPI backend of HDS. It serves a JSON REST API; the OpenAPI document is the contract
that the generated TypeScript client is built from.

## Layout

```text
src/hds/
  main.py          builds the app and mounts the routers
  routers/         HTTP: validation, dependencies, schemas. No business rules
  services/        business rules and role checks
  repositories/    the only code that queries the database
  models/          SQLAlchemy tables and constraints
  scheduling/      pure package: roster rules and the two solvers (004, ADR 011)
  payroll/         pure package: pricing a shift with its premiums (005, ADR 012)
  wiring.py        builds services for routers and the worker
  core/            settings, security, errors, i18n, logging, dependencies
  locales/         translation files keyed by error code
  scripts/         seed_dev, export_openapi, dev_issue_link, purge_expired_records, create_admin,
                   migrate, check_restore
  tools/           check_imports (the layering checker)
alembic/           database migrations
tests/             unit, integration, contract and performance tests
scripts/           contract_cli.sh and contract_hooks.py (Schemathesis against a running server)
```

Dependencies point one way: routers, then services, then repositories, then models. The rule
is Constitution II and `hds.tools.check_imports` enforces it. Each module's docstring states its
layer and what it must not import.

## Configuration

Settings are read from the environment (and a `.env` file in development) by
`hds.core.config`.

Outside development (`ENVIRONMENT` other than `development` or `test`), start-up is also refused when
`JWT_SECRET_KEY`, `THROTTLE_SECRET_KEY`, the database password (in `DATABASE_URL`, its `password`
option, or `PGPASSWORD`) is a value this repository publishes in a configuration file: `.env.example`,
compose, the workflows, scripts, Makefiles, tool configs and the test settings
(`KNOWN_DEVELOPMENT_VALUES`, compared ignoring case and surrounding space). An unreadable
`DATABASE_URL` is refused too. The error names
the setting, never the value. Add any new example value to that list in the same change; a test
fails until you do.

| Variable | Meaning | Default |
| --- | --- | --- |
| `ENVIRONMENT` | **Required, no default.** `development` and `test` are development environments; any other value (`production`, `staging`, ...) turns on the strict secret, email and mail-backend checks | none |
| `JWT_SECRET_KEY` | **Required, no default.** Signs access tokens. Outside development it must be at least 32 bytes and not the published placeholder; it is never empty | none |
| `JWT_ALGORITHM` | Signing algorithm for access tokens; the verifier accepts only this one | `HS256` |
| `MAIL_BACKEND` | How messages are delivered. `outbox` (development only) writes a redacted record; `smtp` delivers through a relay over STARTTLS and is the one real backend, which outside development is required | `outbox` in development |
| `MAIL_OUTBOX_PATH` | File the `outbox` backend appends to; each line holds the masked recipient, subject, purpose, expiry and the body with the token replaced by `[REDACTED]` | `apps/api/.dev-mail/outbox.jsonl` |
| `SMTP_HOST`, `SMTP_PORT` | The relay for `MAIL_BACKEND=smtp`; the host is required with it. Submission with STARTTLS, usually port 587; port 465 (TLS from the first byte) is not supported. Each message is sent in a thread, with a 10 s timeout on each step of the conversation | none, `587` |
| `SMTP_USERNAME`, `SMTP_PASSWORD` | The relay sign-in, both or neither (neither: the relay needs none). The password is a secret, from the environment or Vault: never logged, never in an error, and refused outside development if the repository publishes it | none |
| `MAIL_FROM` | The sender of every message, as `Name <address>` or an address; required with `MAIL_BACKEND=smtp`, and refused if it holds no address | none |
| `SMTP_STARTTLS` | Upgrade to TLS before signing in or sending, checking the relay's certificate. A relay that does not offer it is refused and nothing is sent. Must be `true` outside development; `false`, for a local relay, keeps the connection plain | `true` |
| `DATABASE_URL` | Async SQLAlchemy URL. Outside development its password must not be a published one | local development database |
| `CORS_ORIGINS` | JSON list of allowed origins; `*` is rejected | `localhost:3000`, `localhost:8081` |
| `ACCESS_TOKEN_TTL_SECONDS` | Access-token lifetime | `900` |
| `REFRESH_TOKEN_TTL_DAYS` | Refresh-token lifetime (web sessions) | `7` |
| `REFRESH_TOKEN_TTL_DAYS_NATIVE` | Refresh-token lifetime of phone sessions, renewed on every refresh (ADR 016) | `30` |
| `LOGIN_MAX_FAILURES`, `LOGIN_LOCKOUT_MINUTES` | Account lockout: the account locks on the failure that reaches the limit, and the count restarts when the lock ends | `5`, `15` |
| `REFRESH_REUSE_GRACE_SECONDS` | A refresh token used a second time within this many seconds is refused and nothing else happens; later, every session of the user ends. Must be greater than 0 | `10` |
| `LOGIN_RATE_LIMIT_PER_NETWORK` | Failed sign-ins allowed from one network address in one window of `LOGIN_LOCKOUT_MINUTES`; a success does not clear the count; `0` turns the limit off | `30` |
| `THROTTLE_SECRET_KEY` | Optional key for the HMAC of addresses in the throttle table, at least 32 bytes outside development. Unset, a key is derived from `JWT_SECRET_KEY`. Changing either restarts every count | derived |
| `LOGIN_THROTTLE_RETENTION_HOURS` | Age at which an unlocked throttle row may be removed. Normal requests remove them, so no scheduled job is needed | `24` |
| `RESET_REQUEST_LIMIT_PER_ADDRESS` | Password-reset requests for one address in one 60-minute window. Above it the answer stays 202 and nothing is issued or sent; `0` turns the limit off | `5` |
| `RESET_REQUEST_LIMIT_PER_NETWORK` | The same for one network address, whatever the addresses submitted. Above it the answer stays 202, nothing is issued or sent, and one warning per window is logged; `0` turns the limit off | `30` |
| `RESET_JOB_CONCURRENCY`, `RESET_JOB_QUEUE_LIMIT` | Reset jobs that may run at once in one worker, and that may wait. A job runs after its response; past that it is dropped and logged without the address, and that one email is lost | `2`, `100` |
| `RESET_JOB_TIMEOUT_SECONDS` | How long one reset job may run before it is stopped, so a stalled mail provider cannot hold every place. Its link stays stored, and its email is lost | `30` |
| `TRUSTED_PROXY_COUNT` | Reverse proxies in front of the API. At `0` the forwarded header is ignored. Behind a proxy it **must** be set, or the network limits set to `0`: otherwise every user shares the proxy's address, so one bad window locks everyone out of sign-in and silently stops reset mail for everyone, with only a warning in the log. A wrong value cannot be detected at start-up | `0` |
| `HASH_CONCURRENCY`, `HASH_QUEUE_LIMIT` | Password hashes that may run at once in one worker, and that may wait. Past that a request is refused with 503 `service.busy` and `Retry-After` | `2`, `8` |
| `SCHEDULE_TIME_LIMIT_SECONDS` | How long the schedule worker's solver may search before it returns its best roster, 1 to 60 (004) | `20` |
| `HDS_VERSION` | The version the process reports in `/livez`, `/readyz` and the `hds_build_info` metric; the image build sets it to the commit | `dev` |
| `METRICS_PORT` | The operations port: the API's `/metrics`, and the worker's `/livez`, `/readyz` and `/metrics`. Never the public port. `0` serves none; compose and the chart set `9100`, and give each process on one host its own. A taken port stops start-up naming this setting | `0` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Where spans go, over OTLP (HTTP), for example `http://collector:4318`; `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` also works. Unset, spans are made, so the logs can name them, and dropped; nothing is sent | unset |
| `OTEL_SERVICE_NAME` | The service name on the spans; the chart sets it | `hds-api` / `hds-worker` |
| `TEST_DATABASE_URL` | Tests only; a database to use and keep, whose name must end in `_test`. Unset, each run creates its own `hds_run_<process id>_test` and drops it at the end | a database per run |

## Health and metrics

These are for probes and operators, not the client, so none is in the OpenAPI document and none
needs a sign-in (specs/006-deploy-operations, contracts/ops.md):

| Where | Path | Answers |
| --- | --- | --- |
| API, public port | `GET /livez` | 200 while the process runs; it touches nothing else |
| API, public port | `GET /readyz` | 200 when the database answers within 2 s and is not behind the code's migrations; else 503 with `failing` (`database` or `migrations`) |
| API, public port | `GET /health` | the same as `/livez`, kept for existing probes |
| API, `METRICS_PORT` | `GET /metrics` | `hds_http_requests_total`, `hds_http_request_duration_seconds` (by route template), `hds_build_info` |
| Worker, `METRICS_PORT` | `/livez`, `/readyz`, `/metrics` | alive while its loop turns or a held job is inside its lease; `hds_schedule_jobs{state}`, `hds_schedule_job_duration_seconds` |

"Behind" means `alembic_version` is empty or holds an older revision this code knows. A revision the
code does not know is newer and counts as ready, so old pods keep serving during an upgrade and
after a rollback. The ready check opens connections of its own, never the request pool, so a busy
pool cannot take every pod out of service together. The worker's readiness is the database only.
The migration history is read at start-up: a missing or two-headed history stops the process.

## Worker shutdown

On SIGTERM or SIGINT the schedule worker drains (specs/006 FR-020): it claims nothing more, and the
job it holds is finished and stored, or, if its solve has not started, handed back to the queue
with its attempt given back, so the next worker takes it at once. It then exits 0. A second signal
forces the exit; that job is reclaimed after its lease. If the operations server stops on its own,
the worker drains the same way and exits non-zero. The chart's `terminationGracePeriodSeconds: 90`
(and compose's `stop_grace_period: 90s`) must exceed the longest solve, 60 s, plus storing.

## Traces

Each schedule job is one trace, from the request that queued it through the worker that ran it
(specs/006 FR-008a, research R8). A request continues the caller's `traceparent` when it is valid
and starts a new trace otherwise; the job stores that trace (`schedule_jobs.trace_context`), and the
worker runs the job in a span continued from it, or in a new trace when the job has none. The
health probes are not traced.

Every log line inside a request or a job carries `trace_id` and `span_id` next to `request_id`, so
a trace can be followed through the logs; lines outside both carry neither. The worker writes the
same JSON lines as the API. No trace store is deployed: without `OTEL_EXPORTER_OTLP_ENDPOINT`
(or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`) nothing is exported. What is exported is redacted first,
as the logs are: URLs lose their query strings, and text attributes, exception events and error
messages go through the log redaction. A SQL statement outside a request or a job (the worker's
poll) starts no trace, and a failed job's span is marked as an error.

## Common tasks

```bash
uv sync --extra dev                              # install
uv run alembic upgrade head                      # apply migrations
uv run alembic check                             # models and migrations must agree
uv run python -m hds.scripts.seed_dev            # development data
uv run python -m hds.scripts.export_openapi      # print the OpenAPI document
uv run python -m hds.scripts.dev_issue_link EMAIL invite   # development only: show a token once
uv run python -m hds.scripts.purge_expired_records         # bulk clean; nothing depends on scheduling it
uv run python -m hds.scripts.create_admin --email EMAIL --organisation NAME --timezone ZONE  # once
uv run python -m hds.scripts.migrate             # the cluster's migration Job: locked, once
uv run python -m hds.scripts.check_restore       # the restore test's last step, on a restored copy
```

`migrate` is the Helm chart's migration hook (specs/006 FR-016). It holds a PostgreSQL advisory
lock while it runs `alembic upgrade head`, so two runs at once (a retried hook, two releases) never
migrate together: the second waits, then finds the database at head and does nothing. A database at
a revision this code does not know is newer, as after a rollback to the previous image; it is left
as it is and the command exits 0. Exit status is otherwise Alembic's, or 1 when the database cannot
be reached.

`check_restore` is the weekly restore test's last step (specs/006 FR-014), run on the restored copy
after `migrate`. It runs the ready checks, then loads every organisation, its users and its last
month's shifts through the repositories, in read-only transactions. It prints counts only, never a
name or an address, and exits 1 when the copy is not ready or holds no organisation.

`create_admin` makes the organisation and its first Admin in an empty database, for an
environment that has no seed (specs/006 research R13); it refuses once any user exists. It reads
the password from standard input, at a hidden prompt in a terminal. To pipe it in without leaving
it in the shell's history:

```bash
read -rs PW && printf '%s\n' "$PW" | uv run python -m hds.scripts.create_admin \
  --email admin@example.org --organisation "Acme" --timezone Asia/Jerusalem [--name "Dana Levi"]
unset PW
```

Exit status 0 is created, 1 refused (users exist, or a bad password, address, name or zone), 2 bad
usage. The password is never printed, and the address is shown masked.

`ENVIRONMENT` and `JWT_SECRET_KEY` must be set for every command that starts the application or
reads its settings (`uvicorn`, `seed_dev`, `export_openapi`, `dev_issue_link`, the tests); a
missing one stops the command with a validation error that names it. `alembic` does not need them.
`dev_issue_link` issues a fresh invitation token for a pending account, or a reset token for an active one, prints it once
and stores it nowhere; the mail outbox never holds a usable token.

## Tests

| Suite | Command | Needs |
| --- | --- | --- |
| Unit | `uv run pytest tests/unit` | nothing |
| Integration and contract | `uv run pytest tests/integration tests/contract` | PostgreSQL |
| Performance smoke | `uv run pytest tests/performance` | PostgreSQL |
| Sign-in timing (opt-in, skipped by default) | `uv run pytest -m timing tests/integration/test_login_timing.py -s` | PostgreSQL, about a minute |

Start PostgreSQL with `docker compose -f ../../infra/docker-compose.yml up -d postgres`. The
tests create their own database, `hds_run_<process id>_test`, apply the migrations and drop it at
the end, so several runs can go at once, and migration tests' scratch databases carry the same id.
One left by a killed run is dropped by the next run. Set
`TEST_DATABASE_URL` to use a named database instead, as CI does. `scripts/two_runs_at_once.sh`
proves two runs can share a server. Every test module's docstring lists the requirement ids it
covers.

The timing test (SC-207) sends 500 wrong-password sign-ins each for an unknown and a known address
and passes when the medians differ by less than 15%; it prints both medians and the p-value. Run
it by hand before a release and record the result in the pull request. The gating check for the
same requirement counts the password checks and runs in every test run.

`scripts/contract_cli.sh` runs Schemathesis against a running, seeded server. Start that server
with `LOGIN_RATE_LIMIT_PER_NETWORK=0`: the run fails sign-ins from one address on purpose, and
otherwise a second run inside the window could not sign in. The script does not start the server. The operations on which a valid request is correctly refused (a date order, a time zone, the sign-in lockout) skip only "valid data must be accepted", and are listed once, in `tests/contract/unprovable_operations.json`, which the script and the pytest contract suite both read. `scripts/contract_hooks.py` keeps the run's seeded accounts alive (the fuzzer otherwise deactivates its own admin, after which later operations only ever see 401/403) and gives the operations that need related records something real to act on. It writes data, so
it refuses any database whose name does not end in `_test` or `_scratch`.

## Quality gates

CI runs these, and so should you before opening a pull request:

```bash
uv run ruff check src tests alembic  # lint, including docstring rules (Google convention)
uv run ruff format --check src tests alembic
uv run interrogate src               # docstring coverage must be 100%
uv run mypy --strict src/ tests/
uv run python -m hds.tools.check_imports
uv run alembic check                 # needs DATABASE_URL pointing at a migrated database
```

## Known limitations

- **Unknown addresses cannot be locked.** The lockout count lives on the account, so an address
  with no account has nothing to count against. After the limit, a real account answers 423
  `auth.account_locked` where an unknown address keeps answering 401 `auth.invalid_credentials`.
  The locked-account notice is the one accepted difference in sign-in answers (feature 001
  FR-005; feature 002 Story 7). Closing it would need a table of failures per address.
- **A 409 for an address held elsewhere reveals that the address exists** somewhere in the
  deployment (not in which organisation), because addresses are unique across all organisations.
- **The sign-in timing is equal to within about 5%, not exactly equal.** A known account's wrong
  password also writes and commits its failure count (about 3 ms against about 55 ms for the
  Argon2 check), which an unknown address cannot do.

## API reference

- Generated: `/docs` and `/api/v1/openapi.json` on a running server.
- Contract the implementation is checked against:
  [`openapi.yaml`](../../specs/001-foundation-auth-org/contracts/openapi.yaml). CI fails when the
  two disagree on operations, status codes (errors included) or query parameters.
- Written overview: [`docs/hds-api.md`](../../docs/hds-api.md).

Errors are `application/problem+json` with a stable `code`; the codes and their Hebrew and
English texts are in `src/hds/locales`.

### Added for the redesign (feature 007, Proposed)

Each is in the contract and in [`docs/hds-api.md`](../../docs/hds-api.md):

- `GET /me` answers `capabilities` and `managed_project_ids` (ADR 015); sign-in takes
  `client_kind` (ADR 016); `/me/sessions` and `/auth/logout-others`.
- Availability submissions (`/me/availability-submissions`, migrations 026 and 030; ADR 017).
- Roster editing by a project's managers, assignments' own times (migration 031; ADR 018),
  `If-Match` on shift and assignment changes, and assignments named by their person.
- Roster tools: overrides, the publish review, copy week, week templates, bulk edit, the
  submission tracker (migration 032).
- Schedule runs scoped to a Manager's projects, with `elapsed_seconds` for the progress bar.
- Saved views (`/me/saved-views`, migration 033) and the work log's key figures.
- The organisation's contact card (`/organisation/contact`).

The e2e runs start the schedule worker beside the API with `SCHEDULE_TIME_LIMIT_SECONDS=5`, so a
queued run is solved within a test (`apps/client/playwright.config.ts`, `scripts/ops/e2e_ux.sh`).
