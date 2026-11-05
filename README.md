# HDS 2.0

HDS is a workforce, client and project administration system for an organisation that
staffs sites with employees. Managers and Admins manage employees, groups, qualifications,
clients, sites and projects; Employees maintain their own profile; Clients see only their own
projects. Employees submit their availability and see their published shifts; Managers generate a
roster across every active project and accept it as drafts, and Admins adjust it by hand with rule
warnings, all judged by the pure `hds.scheduling` package.

This repository is a monorepo:

| Path | What it holds |
| --- | --- |
| [`apps/api`](apps/api/README.md) | The Python (FastAPI) backend: the only part with working features today |
| `apps/client` | The Expo app (iOS, Android, web): sign-in, My availability, My shifts |
| `packages/api-client` | TypeScript API client, generated from the OpenAPI document; never edited by hand |
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

**The schedule worker.** Roster generation runs outside the API, in a worker that claims the jobs the
API queues ([ADR 011](docs/adr/011-solvers-and-job-queue.md)). `make up` starts one (the `worker`
service). From source, run it beside the API, on the same database:

```bash
cd apps/api
uv run python -m hds.worker
```

Without a worker, jobs stay queued and nothing else is affected. `SCHEDULE_TIME_LIMIT_SECONDS` (1 to
60, default 20) caps each solve.

The settings, required and optional, are listed in [`.env.example`](.env.example) and described in
[`apps/api/README.md`](apps/api/README.md#configuration). Code baked into the image (a migration,
for example) is only picked up when the image is rebuilt, so run `make up` again after changing it.

Invitation and password-reset emails are not delivered in development: the development mail
backend appends a redacted record to `apps/api/.dev-mail/outbox.jsonl` and never stores a usable
token. To get a working token for a manual test, run
`uv run python -m hds.scripts.dev_issue_link <email> invite` (or `reset`) from `apps/api`; it
prints the token once and stores it nowhere.

## The client app

You need Node.js 22 and pnpm (the version is pinned in the root `package.json`; `corepack enable`
picks it up). From the repository root:

```bash
pnpm install
pnpm --filter hds-client start   # Expo dev server; press w for the web, or open it on a phone
```

The app talks to `EXPO_PUBLIC_API_URL`, `http://localhost:8000` by default, so start the API first.
On a phone the session is kept in the secure store; on the web it is kept in memory only, so a reload
signs in again ([ADR 010](docs/adr/010-client-stack.md)).

| Command (repository root) | What it does |
| --- | --- |
| `pnpm client:lint` | ESLint, including the rule that refuses literal text in JSX |
| `pnpm client:typecheck` | `tsc --noEmit`, strict |
| `pnpm client:test` | Jest with coverage (70% of lines at least) |
| `pnpm client:e2e` | Builds the web app and runs Playwright against a real API (below) |
| `pnpm api-client:generate` | Regenerates `packages/api-client` from the API's OpenAPI document |
| `pnpm api-client:check` | Fails when the committed client differs from the API; CI runs it |

`pnpm client:e2e` starts its own API on port 8100 and serves the web build on port 3000. It uses
`E2E_DATABASE_URL`, which must name a database ending in `_test`, migrates it and runs `seed_dev`;
the specs set up the rest through the API. Install the browser once with
`pnpm --filter hds-client exec playwright install chromium`.

## Running the tests

From `apps/api`:

```bash
uv run pytest tests/unit                       # fast, no database
uv run pytest tests/integration tests/contract # needs PostgreSQL; see apps/api/README.md
uv run pytest                                  # everything
```

The solver benchmark compares the greedy and CP-SAT solvers at 10, 30 and 100 employees and checks
that 30 employees get a week within 30 seconds (004 SC-001). It takes about a minute and is skipped
by default: run `uv run pytest -m benchmark`, or `uv run python -m benchmarks.compare_solvers` for the
table.

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
