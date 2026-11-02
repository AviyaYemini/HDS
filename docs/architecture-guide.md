# HDS 2.0 architecture guide: full-stack basics through your own code

This guide explains what every folder in the repo is for, how a request moves through the code, why the pieces are in this order, and why we took this route. Every example is a real file in this repo.

---

## 1. What "full stack" means

A full-stack app is several layers stacked on top of each other. Each layer has one job and talks only to its neighbours.

```
 PEOPLE         employees, managers, admins, clients
   |
 CLIENT         web + phone app (Expo / TypeScript)         apps/client        (not built yet)
   |  HTTPS + JSON, described by the OpenAPI contract
 API            FastAPI app: receives requests, replies     apps/api/src/hds
   |
 DATA ACCESS    SQL queries, nothing else                   repositories/
   |
 DATABASE       PostgreSQL: the only place data lives       infra/ (Docker)
```

Two more things sit beside the stack and hold it up:

- **Infrastructure** (`infra/`, `.github/`): how the whole thing runs on a laptop, in CI, later on a server.
- **Quality guardrails** (tests, linters, CI, migrations): how you stay sure it still works after every change.

The backend is built first because the client is only a way to *use* the backend. The rules (who may do what, what a valid project is) live on the server. A client can be hacked or replaced, so it can never be the place that enforces rules.

---

## 2. One request, end to end

Take "a manager creates a project": `POST /api/v1/projects`. Follow it through the files.

1. **`main.py`** creates the app and registers all routers. It also adds CORS, a request ID header, error handlers and `/health`.
2. **`routers/projects.py`** matches the URL. FastAPI first checks the body against `ProjectCreateRequest` (types from `core/types.py`: `Money`, `Name`, `StrictDate`). Bad shapes are rejected here with a 422 before any of your code runs.
3. **`core/deps.py`**, through `Depends(require_manager)`, reads the token, loads the user, and refuses with 401 or 403 if the role is wrong.
4. The router builds a **`ProjectService`** and calls `create_project`. It does nothing else.
5. **`services/project.py`** applies the business rules: the actor must be a manager (`core/authz.py`), billing rate above zero, end date not before start date, and the client and site must exist *in the caller's organisation*.
6. **`repositories/project.py`** runs the SQL: `Project(...)`, add, commit. It is the only layer allowed to talk to the database.
7. **`models/project.py`** is the Python description of the `projects` table that the repository uses.
8. **PostgreSQL** stores the row, enforcing its own rules again (a CHECK that `billing_rate > 0`, foreign keys).
9. The result goes back up. FastAPI turns it into JSON using `ProjectRead`. If anything raised an exception, **`core/error_handler.py`** turns it into a standard error response (`application/problem+json`) with a stable `code`.

The rule worth remembering: **calls go down, results come back up, and no layer skips another.** The router never writes SQL, and the repository never knows what HTTP is.

---

## 3. Folder by folder

### `apps/api/src/hds/` (the backend)

| Folder / file | Job | Depends on | Real example |
| --- | --- | --- | --- |
| `routers/` | The HTTP surface. URLs, request/response shapes, which role may call. No business logic. | services, core | `projects.py` |
| `services/` | Business rules and decisions. Know nothing about HTTP or SQL. | repositories, core, models | `project.py`, `auth.py` |
| `repositories/` | All database reads and writes. Every query is scoped to an organisation. | models | `base.py` has `OrgScopedRepository` |
| `models/` | Python classes that mirror database tables. | nothing | `project.py`, `user.py` |
| `core/` | Shared plumbing used by all layers (see below). | models (for roles) | `deps.py`, `security.py` |
| `locales/` | Translation files (`en.json`, `he.json`). Keys are error codes. | nothing | `en.json` |
| `scripts/` | Command-line helpers you run by hand. | everything | `seed_dev.py`, `export_openapi.py` |
| `tools/` | Developer tools that check the code itself. | nothing | `check_imports.py` |
| `main.py` | Wires everything into one app. | all routers | |

**Dependency direction is one-way:** `routers -> services -> repositories -> models`. `tools/check_imports.py` reads every file and fails the build if a lower layer imports a higher one, or if a router or service contains SQL. That rule is written in the constitution and enforced by code, not by good intentions.

### `core/` (what's inside)

| File | What it does |
| --- | --- |
| `config.py` | Reads settings (database URL, JWT secret, token lifetimes) from environment variables. |
| `database.py` | Creates the database connection pool and the `get_db` session each request receives. |
| `security.py` | Argon2id password hashing, access-token (JWT) creation and decoding, refresh-token generation. Only a hash of the refresh token is stored. |
| `deps.py` | FastAPI dependencies: `get_current_user`, `require_manager`, `require_admin`. This is the front door check on every protected route. |
| `authz.py` | `ensure_role(...)`: the same role check repeated inside services. Two locks on the door, so a forgotten router check can't expose a service. |
| `exceptions.py` | Your own error types: `NotFoundError`, `ForbiddenError`, `ConflictError`, ... |
| `error_handler.py` | Converts those errors into uniform HTTP responses with a machine-readable `code`. |
| `i18n.py` | Looks up the human text for an error code in English or Hebrew. |
| `types.py` | Reusable validated field types (`Money` as a decimal string, `Name` with a length limit). Their limits match the database column sizes. |
| `openapi.py` | Adds the real error responses to the generated API description. |
| `logging.py`, `mail.py` | Logging setup, and sending invitation and reset emails. (I haven't opened these two.) |

### `apps/api/` (around the code)

| Item | Job |
| --- | --- |
| `alembic/versions/` | Database migrations: numbered files that build the schema step by step. |
| `tests/unit` | Fast tests of services and logic with fake repositories. |
| `tests/integration` | Tests that call the real API against a real PostgreSQL. |
| `tests/contract` | Checks the running app still matches the published API contract. |
| `pyproject.toml`, `uv.lock` | Dependencies, and the exact versions that were tested. Tool settings for ruff, mypy and pytest. |

### Outside the API

| Folder | Job |
| --- | --- |
| `apps/client/` | The future phone and web app. Today it holds only translation files and Playwright test scaffolds that are skipped until the UI exists. |
| `packages/api-client/` | Will hold the TypeScript client *generated* from the API contract. It's a stub. |
| `infra/` | `docker-compose.yml` (PostgreSQL + API) and `Dockerfile.api`. `infra/volumes/` holds the database's files on your disk and is git-ignored. |
| `.github/workflows/ci.yml` | The automated checks that run on every pull request. |
| `docs/` | Your spec and API reference, plus `adr/` (architecture decision records: why we chose what we chose). |
| `specs/` | Spec Kit's output per feature: `spec.md` (what), `plan.md` (how), `tasks.md` (steps). |
| `.specify/`, `.claude/` | Spec Kit's templates and constitution, and the Claude Code harness (hooks and commands). |
| `.pre-commit-config.yaml` | Runs ruff, mypy and commit-message checks on your machine before a commit is accepted. |

---

## 4. Why this order

There are two different orders, and they're easy to mix up.

**A. The order of the layers (top to bottom).** Dependencies must point one way. If services could import routers, you'd get a knot: you couldn't test a service without starting a web server, and changing a URL could break a business rule. With one-way dependencies you can test each layer alone: unit tests replace the repository with a fake, integration tests exercise everything together.

**B. The order we *built* things (bottom to top).** Look at the migrations:

```
001 organisations -> 002 clients -> 003 sites -> 004 users -> 005 refresh_tokens
-> 006 groups -> 007 qualifications -> 008/009 memberships -> 010 projects -> ...
```

That is dependency order. A project points at a client and a site, so those tables must exist first. A user points at an organisation. You build what others depend on first. The same logic applies upward: database, then models, repositories, services, routers, and finally the client. Each layer can be tested before the next exists.

**C. The order of the process (Spec Kit).** Constitution (rules for the project) -> specify (what and why, no tech) -> clarify -> plan (how) -> tasks -> analyze -> implement. Writing code is the last step, because an unclear requirement costs minutes to fix in a spec and days to fix in code. The analyze step already caught real ordering bugs in the task list, such as seeding projects before the projects table existed.

**D. Why Foundation (auth + organisation) came first.** Every other feature (shifts, scheduling, pay) needs to know *who is asking* and *which organisation's data*. Building those after the fact means retrofitting security through every endpoint. That is why the organisation-scoping rule had to be added across all repositories before more features stacked on top.

---

## 5. How the main concerns are handled

**Security**
- Passwords are hashed with Argon2id. Plain passwords are never stored.
- Sign-in returns a short-lived access token (15 minutes) plus a refresh token that is stored only as a hash and rotated on every use. Reuse of an old refresh token revokes all of that user's tokens (ADR 003).
- Roles (employee, manager, admin, client) are checked twice: in the router and in the service. Role is re-read from the database on every request, so a demotion takes effect immediately.
- **Multi-tenancy:** every table carries `organization_id`, and `OrgScopedRepository` makes every lookup filter by it. A row from another organisation looks exactly like a missing row (404). `test_org_scoping.py` proves it.

**Correctness**
- Money is `NUMERIC(12,4)` in the database and `Decimal` in Python, never float. A CHECK constraint makes a non-positive rate impossible at the database level, even if the code has a bug.
- Times are stored in UTC with time zones.
- Deletes are "soft" (`is_deleted`), so history survives (ADR 002).

**Errors and languages**
- Every failure has a stable `code` (like `users.last_admin`). The client picks the wording from `locales/en.json` or `he.json`, so adding Hebrew later means filling in text, not changing logic.

**The contract**
- FastAPI generates `openapi.json` from the code. `scripts/export_openapi.py --check` and the contract tests fail if the code and the committed contract drift apart. That contract will generate the TypeScript client, so the phone app can never call an endpoint that doesn't exist.

---

## 6. The tests: why three kinds

| Kind | Speed | What it proves | Catches |
| --- | --- | --- | --- |
| Unit | milliseconds | One service's rules, with fakes for the database | Wrong business logic |
| Integration | seconds | Real HTTP + real PostgreSQL together | Wiring bugs, migrations, SQL, permissions |
| Contract | seconds | The API still matches its published description | Accidental breaking changes |

The first real PostgreSQL run found five bugs the fake setup had hidden, including two migrations that would have failed on any fresh database. That is the reason integration tests use the real database.

Security tests are written to be *able to fail*: Claude Code removed the token-revocation code and confirmed nine tests broke. A test that cannot fail proves nothing.

---

## 7. The guardrails, in layers

1. **Your editor / Claude hooks** block bad edits as they happen (no code before `tasks.md`, no float money).
2. **Pre-commit** runs ruff, mypy and a commit-message check before a commit is accepted.
3. **CI** (`ci.yml`) re-runs everything on a clean machine for each pull request: lint, strict types, import-graph, dependency audit, unit and integration tests, contract check.
4. **Constitution + ADRs** record the rules and the reasons, so a decision isn't re-argued every month.

The same idea at every level: catch a mistake as early and as cheaply as possible.

---

## 8. Why we took this route

| Choice | Why | Cost / alternative |
| --- | --- | --- |
| Clean slate instead of patching HDS 1.0 | The old code mixed concerns and had no tests; its value was the *requirements* | Slower start, much stronger foundation |
| Spec first (Spec Kit) | Cheap to fix ideas, expensive to fix code; AI tools drift without a written target | More up-front reading |
| Python + FastAPI | Typed, async, auto-generates the API contract; Python is also where the scheduling solver (OR-Tools) lives | Slower than Go or Rust, irrelevant at this scale |
| PostgreSQL | Exact decimals, time zones, constraints, real transactions | Heavier than SQLite, needs Docker |
| Layered backend | Each part testable and replaceable alone | More files than a single script |
| API-first with a generated client | One source of truth for web and phone | Needs discipline: change the contract, regenerate |
| Docker Compose | Same database for everyone, one command | Docker Desktop on the Mac |
| Backend before client | Rules and data are the product; the UI is a view | You see no UI yet |

---

## 9. Things I noticed in the code (worth fixing)

- `main.py` uses `allow_origins=["*"]` together with `allow_credentials=True`. Fine for local development; before deploying, restrict it to your real web origin.
- `ProjectRepository.update` and `soft_delete` write by `id` only. They are safe today because the service checks the organisation first, but adding the organisation to the `WHERE` makes the repository safe by itself (defence in depth).
- The default JWT secret is a placeholder. The app should refuse to start in production with the default.
- The request ID middleware makes an ID and returns it but doesn't put it in the logs, so you can't yet trace one request through the log lines.

---

## 10. Glossary

- **API**: the set of URLs a program can call.
- **Endpoint**: one URL plus method, for example `POST /api/v1/projects`.
- **ORM** (SQLAlchemy): maps Python classes to database tables.
- **Migration** (Alembic): a versioned change to the database structure.
- **JWT**: a signed token proving who you are, so the server needn't remember sessions.
- **Multi-tenant**: one app serving several organisations whose data must never mix.
- **Soft delete**: mark as deleted instead of removing the row.
- **OpenAPI**: a machine-readable description of the whole API.
- **CI**: automated checks run on every change.
- **ADR**: a short record of one architectural decision and why.
- **Contract test**: checks that the live API matches its description.
