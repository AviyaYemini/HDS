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

You need Docker, Docker Compose and [uv](https://docs.astral.sh/uv/).

```bash
# 1. Copy the example settings and replace the placeholders. Never commit real secrets.
cp .env.example .env

# 2. Start PostgreSQL (the compose file lives in infra/).
docker compose -f infra/docker-compose.yml up -d postgres

# 3. Install the API's dependencies, create the schema and load development data.
cd apps/api
uv sync --extra dev
uv run alembic upgrade head
uv run python -m hds.scripts.seed_dev

# 4. Run the API with auto-reload.
uv run uvicorn hds.main:app --reload
```

The API is then at <http://localhost:8000>; the interactive docs are at `/docs` and the
OpenAPI document at `/api/v1/openapi.json`. `seed_dev` lists the accounts it creates in its
module docstring. They are for local development only.

To run the whole stack in containers instead, use
`docker compose -f infra/docker-compose.yml up --build`, then apply the migrations with
`docker compose -f infra/docker-compose.yml exec api alembic upgrade head`.

## Running the tests

From `apps/api`:

```bash
uv run pytest tests/unit                       # fast, no database
uv run pytest tests/integration tests/contract # needs PostgreSQL; see apps/api/README.md
uv run pytest                                  # everything
```

The integration tests use a separate `*_test` database and refuse to run against any other.
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
