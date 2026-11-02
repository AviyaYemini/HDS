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
  scripts/         seed_dev, export_openapi
  tools/           check_imports (the layering checker)
alembic/           database migrations
tests/             unit, integration, contract and performance tests
scripts/           contract_cli.sh (Schemathesis against a running server)
```

Dependencies point one way: routers, then services, then repositories, then models. The rule
is Constitution II and `hds.tools.check_imports` enforces it. Each module's docstring states its
layer and what it must not import.

## Configuration

Settings are read from the environment (and a `.env` file in development) by
`hds.core.config`.

| Variable | Meaning | Default |
| --- | --- | --- |
| `DATABASE_URL` | Async SQLAlchemy URL | local development database |
| `JWT_SECRET_KEY` | Signs access tokens | a placeholder, refused in production |
| `ENVIRONMENT` | `production` turns on the strict secret and email checks | `development` |
| `CORS_ORIGINS` | JSON list of allowed origins; `*` is rejected | `localhost:3000`, `localhost:8081` |
| `ACCESS_TOKEN_TTL_SECONDS` | Access-token lifetime | `900` |
| `REFRESH_TOKEN_TTL_DAYS` | Refresh-token lifetime | `7` |
| `LOGIN_MAX_FAILURES`, `LOGIN_LOCKOUT_MINUTES` | Account lockout | `5`, `15` |
| `TEST_DATABASE_URL` | Tests only; the database name must end in `_test` | local `hds_test` |

## Common tasks

```bash
uv sync --extra dev                              # install
uv run alembic upgrade head                      # apply migrations
uv run alembic check                             # models and migrations must agree
uv run python -m hds.scripts.seed_dev            # development data
uv run python -m hds.scripts.export_openapi      # print the OpenAPI document
```

## Tests

| Suite | Command | Needs |
| --- | --- | --- |
| Unit | `uv run pytest tests/unit` | nothing |
| Integration and contract | `uv run pytest tests/integration tests/contract` | PostgreSQL |
| Performance smoke | `uv run pytest tests/performance` | PostgreSQL |

Start PostgreSQL with `docker compose -f ../../infra/docker-compose.yml up -d postgres`. The
tests create the `*_test` database and apply the migrations themselves. Every test module's
docstring lists the requirement ids it covers.

`scripts/contract_cli.sh` runs Schemathesis against a running, seeded server. It writes data, so
it refuses any database whose name does not end in `_test` or `_scratch`.

## Quality gates

CI runs these, and so should you before opening a pull request:

```bash
uv run ruff check src tests          # lint, including docstring rules (Google convention)
uv run ruff format --check src tests
uv run interrogate src               # docstring coverage must be 100%
uv run mypy --strict src/
uv run python -m hds.tools.check_imports
```

## API reference

- Generated: `/docs` and `/api/v1/openapi.json` on a running server.
- Contract the implementation is checked against:
  [`openapi.yaml`](../../specs/001-foundation-auth-org/contracts/openapi.yaml). CI fails when the
  two disagree on operations, success codes or query parameters.
- Written overview: [`docs/hds-api.md`](../../docs/hds-api.md).

Errors are `application/problem+json` with a stable `code`; the codes and their Hebrew and
English texts are in `src/hds/locales`.
