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
| [`infra`](infra/README.md) | Docker Compose, the API and web images, pinned versions |
| [`docs`](docs/README.md) | Product spec, API reference, architecture guide and ADRs |
| [`specs`](specs/001-foundation-auth-org/spec.md) | One folder per feature: spec, plan, tasks, contract |

## Quick start

You need Docker (with Compose) and `make`; nothing else is installed on your machine. Run every
command from the repository root.

```bash
make env    # writes .env with a fresh random JWT_SECRET_KEY; it refuses to overwrite an existing one
make up     # starts everything and waits until it is ready
```

`make up` starts PostgreSQL, brings the database to the current version and loads the development
data (once: running it again adds nothing), then the API, the schedule worker and the web client.
It refuses before starting anything when a port it needs is taken, and stops with the migration's
log when a migration fails. Then:

| What | Where |
| --- | --- |
| The web client | <http://localhost:8090> |
| The API, its interactive docs, the OpenAPI document | <http://localhost:8000>, `/docs`, `/api/v1/openapi.json` |
| Health checks | <http://localhost:8000/livez>, `/readyz` |
| Metrics (API, worker) | <http://localhost:9100/metrics>, <http://localhost:9101/metrics> |

Sign in with any of the development accounts, all with the password `Dev1234!` (for local use only):

| Account | Role |
| --- | --- |
| `admin@hds.test` | Admin |
| `manager@hds.test` | Manager |
| `employee@hds.test`, `extra@hds.test` | Employee |
| `client@hds.test` | Client |

The other targets:

```bash
make down   # stop everything; the database is kept
make wipe   # stop everything and DELETE the local database (it asks first)
make build  # only rebuild the images (make up also does)
make ps     # state of the containers
make logs   # follow the logs; `make logs S=api` for one service
make hooks  # once per clone: install the git hooks (needs uv)
make ci-local       # CI's checks that need no database, for what the branch changed (uv, Docker)
make ci-local-full  # all of them, once, when the pull request is ready to push
make ci-local-db    # about 25 minutes, at PR-ready if the API changed: the database suites
```

`make ci-local-full` runs what CI runs without a database: ruff, format, interrogate,
`mypy --strict`, the import graph, pip-audit, the unit and property tests, the API image check,
the ops targets and release scripts against fakes, ShellCheck, actionlint, the Helm charts, Vault's
policies, the alert rules, the Markdown links, gitleaks and commitlint over `origin/main..HEAD`
(`CI_LOCAL_BASE` moves the start), and the client; the client's checks run in the pinned Node
image, so no Node is needed. `make ci-local` runs only the steps for the paths changed since the
merge base with `origin/main`: the API's for `apps/api/` or the 001 contract, the client's for
`apps/client/`, `packages/` and the workspace files (an API change alone runs the generated
client's drift check), and each infra check for its own files; the Markdown links, gitleaks and
commitlint always run. It ends with what ran and what was skipped, and why; a docs-only change
should take about a minute (an estimate from the steps' times). `make ci-local-db` adds the migration check on a throwaway database and the
database suites on `hds_p3_test`. The full suite runs in CI.

`make hooks` installs pre-commit 4.7.0 as a uv tool (`uv tool install`, so it outlives
`uv cache clean`), then the git hooks from `.pre-commit-config.yaml`. Each commit then runs ruff, ruff format and `mypy --strict` on `apps/api` (the project's own
versions) and the gitleaks scan of the staged change; each message runs commitlint, with the rules
CI uses. pre-commit downloads the Node and Go they need. CI stays the authority, and
`git commit --no-verify` skips the hooks when you mean to.

`make up` rebuilds the images each time, so a pull that changes the client or a dependency is picked
up; when nothing changed, the build yields the same image and a running stack is left as it is. The
API's code and migrations are also mounted, so a change to them needs only `make down && make up`.
The port check needs `nc` (netcat), which macOS and most Linux systems have. Ports default to
5432, 8000, 8090, 9100 and 9101; set `POSTGRES_PORT`, `API_PORT`, `WEB_PORT`, `API_METRICS_PORT` or
`WORKER_METRICS_PORT` to move one. `scripts/ops/quickstart_check.sh` checks all of this from a clean
checkout in CI ([infra/README.md](infra/README.md)).

**Why `--env-file .env`:** Compose reads its `.env` from the folder of the compose file (`infra/`),
not from where you run it, so the `make` targets pass the root file explicitly. Do not swap this for
`--project-directory .`: the compose file's relative paths (build context, volumes) assume `infra/`.
`make down` needs no real secret: it supplies a stand-in `JWT_SECRET_KEY` when none is set, while
`make up` refuses to start without a real one.

**Running the API from source instead** (PostgreSQL still in its container:
`docker compose -f infra/docker-compose.yml --env-file .env up -d postgres`). You need
[uv](https://docs.astral.sh/uv/). The API reads a `.env` from the folder it is started in, so copy
yours there once:

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
[`apps/api/README.md`](apps/api/README.md#configuration).

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
