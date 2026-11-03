# HDS 2.0

HDS is a workforce, client and project administration system for an organisation that
staffs sites with employees. Managers and Admins manage employees, groups, qualifications,
clients, sites and projects; Employees maintain their own profile; Clients see only their own
projects. Scheduling is planned and will live in a separate pure package.

This repository is a monorepo:

| Path | What it holds |
| --- | --- |
| [`apps/api`](apps/api/README.md) | The Python (FastAPI) backend: the only part with working features today |
| `apps/client` | Web client scaffold; waits for the client feature |
| `packages/api-client` | TypeScript API client, generated from the OpenAPI document |
| [`infra`](infra/README.md) | Docker Compose and the API image |
| [`docs`](docs/README.md) | Product spec, API reference, architecture guide and ADRs |
| [`specs`](specs/001-foundation-auth-org/spec.md) | One folder per feature: spec, plan, tasks, contract |

## Quick start

You need Docker, Docker Compose, `make` and [uv](https://docs.astral.sh/uv/). Run every command from
the repository root unless a step says otherwise.

```bash
# 1. Create the .env file in the repository root. ENVIRONMENT and JWT_SECRET_KEY are required and
#    have no defaults: the API and docker compose both refuse to start without them. The file is
#    git-ignored; never commit a real secret. (.env.example lists every variable, optional ones too.)
printf 'JWT_SECRET_KEY=%s\nENVIRONMENT=development\n' "$(openssl rand -hex 32)" > .env

# 2. Build and start PostgreSQL and the API in containers.
make up        # docker compose -f infra/docker-compose.yml --env-file .env up -d --build
make ps        # state of the containers
make logs      # follow the logs; `make logs S=api` for one service
make down      # stop and remove the containers (database files in infra/volumes are kept)

# 3. Create the schema and load development data (the stack from step 2 must be up).
docker compose -f infra/docker-compose.yml --env-file .env exec api alembic upgrade head
docker compose -f infra/docker-compose.yml --env-file .env exec api python -m hds.scripts.seed_dev
```

The API is then at <http://localhost:8000>; the interactive docs are at `/docs` and the OpenAPI
document at `/api/v1/openapi.json`. `seed_dev` lists the accounts it creates in its module
docstring. They are for local development only.

**Why `--env-file .env`:** Compose reads its `.env` from the folder of the compose file (`infra/`),
not from where you run it, so the commands above pass the root file explicitly. Do not swap this for
`--project-directory .`: the compose file's relative paths (build context, volumes) assume `infra/`.
The `make` targets already do it. `make down` needs no real secret: it supplies a dummy
`JWT_SECRET_KEY` when none is set, while `make up` refuses to start without a real one.

**Running the API from source instead** (PostgreSQL still in its container: `make up`, or
`docker compose -f infra/docker-compose.yml --env-file .env up -d postgres`). The API reads a `.env`
from the folder it is started in, so copy yours there once:

```bash
cp .env apps/api/.env
cd apps/api
uv sync --extra dev
uv run alembic upgrade head
uv run python -m hds.scripts.seed_dev
uv run uvicorn hds.main:app --reload
```

The settings, required and optional, are listed in [`.env.example`](.env.example) and described in
[`apps/api/README.md`](apps/api/README.md#configuration). Code baked into the image (a migration,
for example) is only picked up when the image is rebuilt, so run `make up` again after changing it.

Invitation and password-reset emails are not delivered in development: the development mail
backend appends a redacted record to `apps/api/.dev-mail/outbox.jsonl` and never stores a usable
token. To get a working token for a manual test, run
`uv run python -m hds.scripts.dev_issue_link <email> invite` (or `reset`) from `apps/api`; it
prints the token once and stores it nowhere.

## Running the tests

From `apps/api`:

```bash
uv run pytest tests/unit                       # fast, no database
uv run pytest tests/integration tests/contract # needs PostgreSQL; see apps/api/README.md
uv run pytest                                  # everything
```

The sign-in timing comparison (SC-207) is slow and noisy, so it is skipped by default. Run it by
hand before a release with `uv run pytest -m timing tests/integration/test_login_timing.py -s`
(about a minute; it needs PostgreSQL and prints the two medians). Its pass mark is a median ratio
below 1.15; the p-value is printed, not asserted.

Each test run creates its own database and drops it at the end, so several runs can go at once. A
named `TEST_DATABASE_URL` must end in `_test`, and the tests refuse any other.
The quality gates that CI also runs are listed in [`apps/api/README.md`](apps/api/README.md).

## How the project is run

- **Rules:** [`.specify/memory/constitution.md`](.specify/memory/constitution.md) is binding.
  It sets the layering, security, money and time rules, the quality gates and the
  documentation standard.
- **Process:** each feature goes through Spec Kit (specify, clarify, plan, tasks, analyze,
  implement). Feature folders are in [`specs`](specs/001-foundation-auth-org/spec.md).
- **Scope:** [`docs/hds-spec.md`](docs/hds-spec.md) is the source of truth; anything beyond it
  is marked *Proposed*.
- **Contributing:** use Conventional Commits, keep pull requests small, and fill in the
  [pull request template](.github/pull_request_template.md), including the docs checkbox.
