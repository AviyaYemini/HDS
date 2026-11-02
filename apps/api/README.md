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
  core/            settings, security, errors, i18n, logging, dependencies
  locales/         translation files keyed by error code
  scripts/         seed_dev, export_openapi, dev_issue_link
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

| Variable | Meaning | Default |
| --- | --- | --- |
| `ENVIRONMENT` | **Required, no default.** `development` and `test` are development environments; any other value (`production`, `staging`, ...) turns on the strict secret, email and mail-backend checks | none |
| `JWT_SECRET_KEY` | **Required, no default.** Signs access tokens. Outside development it must be at least 32 bytes and not the published placeholder; it is never empty | none |
| `JWT_ALGORITHM` | Signing algorithm for access tokens; the verifier accepts only this one | `HS256` |
| `MAIL_BACKEND` | How messages are delivered. `outbox` (development only) writes a redacted record; outside development a real backend is required and none exists yet | `outbox` in development |
| `MAIL_OUTBOX_PATH` | File the `outbox` backend appends to; each line holds the masked recipient, subject, purpose, expiry and the body with the token replaced by `[REDACTED]` | `apps/api/.dev-mail/outbox.jsonl` |
| `DATABASE_URL` | Async SQLAlchemy URL | local development database |
| `CORS_ORIGINS` | JSON list of allowed origins; `*` is rejected | `localhost:3000`, `localhost:8081` |
| `ACCESS_TOKEN_TTL_SECONDS` | Access-token lifetime | `900` |
| `REFRESH_TOKEN_TTL_DAYS` | Refresh-token lifetime | `7` |
| `LOGIN_MAX_FAILURES`, `LOGIN_LOCKOUT_MINUTES` | Account lockout: the account locks on the failure that reaches the limit, and the count restarts when the lock ends | `5`, `15` |
| `REFRESH_REUSE_GRACE_SECONDS` | A refresh token used a second time within this many seconds is refused and nothing else happens; later, every session of the user ends. Must be greater than 0 | `10` |
| `LOGIN_RATE_LIMIT_PER_NETWORK` | Failed sign-ins allowed from one network address in one window of `LOGIN_LOCKOUT_MINUTES`; a success does not clear the count; `0` turns the limit off | `30` |
| `THROTTLE_SECRET_KEY` | Optional key for the HMAC of addresses in the throttle table, at least 32 bytes outside development. Unset, a key is derived from `JWT_SECRET_KEY`. Changing either restarts every count | derived |
| `LOGIN_THROTTLE_RETENTION_HOURS` | Age at which an unlocked throttle row may be removed. Normal requests remove them, so no scheduled job is needed | `24` |
| `TRUSTED_PROXY_COUNT` | Reverse proxies in front of the API. At `0` the forwarded header is ignored. Behind a proxy it **must** be set, or the network limit set to `0`: otherwise every user shares the proxy's address and one bad window locks everyone out. A wrong value cannot be detected at start-up | `0` |
| `HASH_CONCURRENCY`, `HASH_QUEUE_LIMIT` | Password hashes that may run at once in one worker, and that may wait. Past that a request is refused with 503 `service.busy` and `Retry-After` | `2`, `8` |
| `TEST_DATABASE_URL` | Tests only; the database name must end in `_test` | local `hds_test` |

## Common tasks

```bash
uv sync --extra dev                              # install
uv run alembic upgrade head                      # apply migrations
uv run alembic check                             # models and migrations must agree
uv run python -m hds.scripts.seed_dev            # development data
uv run python -m hds.scripts.export_openapi      # print the OpenAPI document
uv run python -m hds.scripts.dev_issue_link EMAIL invite   # development only: show a token once
```

`ENVIRONMENT` and `JWT_SECRET_KEY` must be set for every command that starts the application or
reads its settings (`uvicorn`, `seed_dev`, `export_openapi`, `dev_issue_link`, the tests); a
missing one stops the command with a validation error that names it. `alembic` does not need them.
`dev_issue_link` issues a fresh invitation or reset token for an existing account, prints it once
and stores it nowhere; the mail outbox never holds a usable token.

## Tests

| Suite | Command | Needs |
| --- | --- | --- |
| Unit | `uv run pytest tests/unit` | nothing |
| Integration and contract | `uv run pytest tests/integration tests/contract` | PostgreSQL |
| Performance smoke | `uv run pytest tests/performance` | PostgreSQL |
| Sign-in timing (opt-in, skipped by default) | `uv run pytest -m timing tests/integration/test_login_timing.py -s` | PostgreSQL, about a minute |

Start PostgreSQL with `docker compose -f ../../infra/docker-compose.yml up -d postgres`. The
tests create the `*_test` database and apply the migrations themselves. Every test module's
docstring lists the requirement ids it covers.

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
  two disagree on operations, success codes or query parameters.
- Written overview: [`docs/hds-api.md`](../../docs/hds-api.md).

Errors are `application/problem+json` with a stable `code`; the codes and their Hebrew and
English texts are in `src/hds/locales`.
