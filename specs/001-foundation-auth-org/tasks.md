# Tasks: Foundation — Authentication and Organisation

**Input**: Design documents from `specs/001-foundation-auth-org/`

**Prerequisites**: plan.md ✓ spec.md ✓ research.md ✓ data-model.md ✓ contracts/openapi.yaml ✓

**Tests**: Included per Constitution Principle IV (Test-First). Unit tests MUST be written
and confirmed to FAIL before the corresponding service implementation is created.

**Organisation**: Phases 3–8 map to the six user stories in spec.md. Each phase is an
independently testable increment.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: Parallelisable — different files, no incomplete dependency
- **[USN]**: User Story N (maps to spec.md priorities P1–P6)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Monorepo skeleton, tooling, Docker, CI scaffolding. No business logic yet.

- [X] T001 Create monorepo top-level directories: `apps/api/`, `apps/client/` (stub), `packages/api-client/` (stub), `infra/`, `docs/adr/`
- [X] T002 Initialise `apps/api` Python project with uv: `pyproject.toml` (name=`hds-api`, python=`3.12`); runtime deps: `fastapi`, `pydantic[email]>=2`, `sqlalchemy[asyncio]>=2`, `alembic`, `argon2-cffi`, `python-jose[cryptography]`, `asyncpg`, `aiofiles`; `uv.lock` committed
- [X] T003 [P] Add dev/test deps to `apps/api/pyproject.toml`: `pytest`, `pytest-asyncio`, `hypothesis`, `schemathesis`, `httpx`, `aiosqlite`; configure `[tool.ruff]` (select `["E","F","I","N","UP"]`, format quote-style double); configure `[tool.mypy]` (strict=true, python_version="3.12")
- [X] T004 [P] Create `infra/docker-compose.yml`: `postgres` service (image `postgres:16-alpine`, env `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` from `.env`, port 5432); `api` service (build `infra/Dockerfile.api`, `depends_on: postgres`, `DATABASE_URL` env, volume mount `apps/api/src`)
- [X] T005 [P] Create `infra/Dockerfile.api`: `FROM python:3.12-slim`; copy `apps/api`; `RUN pip install uv && uv sync --frozen`; `CMD ["uvicorn", "hds.main:app", "--host", "0.0.0.0", "--port", "8000"]`
- [X] T006 [P] Create `pnpm-workspace.yaml` at repo root: `packages: ["apps/*", "packages/*"]`; add stub `package.json` files in `apps/client/` and `packages/api-client/` so pnpm recognises the workspaces
- [X] T007 [P] Create `.pre-commit-config.yaml`: hooks for `ruff check`, `ruff format --check`, `mypy --strict apps/api/src/`; add commitlint hook enforcing Conventional Commits
- [X] T008 [P] Update `.gitignore`: add `__pycache__/`, `.venv/`, `*.pyc`, `*.pyo`, `.env`, `infra/volumes/`, `*.db`; ensure `uv.lock` and `pnpm-lock.yaml` are NOT ignored (lock files must be committed)
- [X] T009 [P] Create `docs/adr/001-tech-stack.md`: record FastAPI + Pydantic v2 + SQLAlchemy 2.0 + Alembic + PostgreSQL 16 + argon2-cffi + python-jose decisions per `research.md`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure — database schema for entities shared across all stories,
auth primitives, error handling, logging. No user story can start until this phase is complete.

**⚠️ CRITICAL**: Complete and commit each migration in FK-dependency order (organisations →
clients → sites → users → refresh_tokens). Alembic `downgrade()` MUST be implemented on
every migration.

- [X] T010 Create `apps/api/src/hds/__init__.py` and sub-package `__init__.py` files for: `core/`, `models/`, `repositories/`, `services/`, `routers/`, `scripts/`, `locales/`
- [X] T011 Create `apps/api/src/hds/core/config.py`: Pydantic `BaseSettings` with fields `DATABASE_URL` (str), `JWT_SECRET_KEY` (SecretStr), `JWT_ALGORITHM` (str, default `"HS256"`), `ACCESS_TOKEN_TTL_SECONDS` (int, default 900), `REFRESH_TOKEN_TTL_DAYS` (int, default 7), `LOGIN_MAX_FAILURES` (int, default 5), `LOGIN_LOCKOUT_MINUTES` (int, default 15)
- [X] T012 Create `apps/api/src/hds/core/database.py`: async SQLAlchemy engine from `config.DATABASE_URL`; `async_sessionmaker`; `AsyncSession` type alias; `get_db` async generator FastAPI dependency
- [X] T013 Create `apps/api/src/hds/models/base.py`: `TimestampedBase` mixin — `id` (UUID, `default=uuid4`, primary key), `created_at` (DateTime timezone=True, `server_default=func.now()`), `updated_at` (DateTime timezone=True, `server_default=func.now()`, `onupdate=func.now()`); `Base = declarative_base()`
- [X] T014 Initialise Alembic in `apps/api/`: `alembic init alembic`; configure `alembic/env.py` for async SQLAlchemy (`asyncio.run`); set `target_metadata` from `hds.models`; configure `alembic.ini` to read `DATABASE_URL` from environment via `os.environ`
- [X] T015 Create Alembic migration `001_init_organisations`: `CREATE TYPE user_role AS ENUM ('employee','manager','admin','client')`, `project_type AS ENUM ('security','training')`, `project_status AS ENUM ('active','ended')`; `CREATE TABLE organisations (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name VARCHAR(255) NOT NULL, timezone VARCHAR(64) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`; reversible downgrade drops table then types
- [X] T016 Create Alembic migration `002_clients`: `CREATE TABLE clients (id UUID PK, organization_id UUID NOT NULL REFERENCES organisations(id), name VARCHAR(255) NOT NULL, contact_name VARCHAR(255), contact_email VARCHAR(320), contact_phone VARCHAR(32), contract_start DATE NOT NULL, contract_end DATE, is_active BOOLEAN NOT NULL DEFAULT true, is_deleted BOOLEAN NOT NULL DEFAULT false, deleted_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`; `CREATE INDEX idx_clients_org ON clients(organization_id)`
- [X] T017 [P] Create Alembic migration `003_sites`: `CREATE TABLE sites (id UUID PK, organization_id UUID NOT NULL REFERENCES organisations(id), name VARCHAR(255) NOT NULL, address TEXT NOT NULL, timezone VARCHAR(64) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`; `CREATE INDEX idx_sites_org ON sites(organization_id)`
- [X] T018 Create Alembic migration `004_users`: `CREATE TABLE users (id UUID PK, organization_id UUID NOT NULL REFERENCES organisations(id), email VARCHAR(320) NOT NULL, password_hash VARCHAR(255) NOT NULL, full_name VARCHAR(255) NOT NULL, phone VARCHAR(32), role user_role NOT NULL, is_active BOOLEAN NOT NULL DEFAULT true, is_deleted BOOLEAN NOT NULL DEFAULT false, deleted_at TIMESTAMPTZ, default_hourly_rate NUMERIC(12,4) CHECK(default_hourly_rate > 0), client_id UUID REFERENCES clients(id), failed_login_attempts SMALLINT NOT NULL DEFAULT 0, locked_until TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`; `CREATE UNIQUE INDEX uq_users_org_email ON users(organization_id, email) WHERE is_deleted = false`; `CREATE INDEX idx_users_org ON users(organization_id)`; `CREATE INDEX idx_users_client ON users(client_id) WHERE client_id IS NOT NULL`
- [X] T019 [P] Create Alembic migration `005_refresh_tokens`: `CREATE TABLE refresh_tokens (id UUID PK, user_id UUID NOT NULL REFERENCES users(id), token_hash VARCHAR(64) NOT NULL UNIQUE, expires_at TIMESTAMPTZ NOT NULL, revoked_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now())`; `CREATE INDEX idx_refresh_tokens_user ON refresh_tokens(user_id)`
- [X] T020 [P] Create `apps/api/src/hds/models/organisation.py`: `Organisation` SQLAlchemy model extending `TimestampedBase`; `__tablename__ = "organisations"`; columns `name`, `timezone`; import in `hds/models/__init__.py`
- [X] T021 [P] Create `apps/api/src/hds/models/client.py`: `Client` model; all columns per migration 002; `is_deleted` + `deleted_at` soft-delete; `organization_id` FK relationship; import in `hds/models/__init__.py`
- [X] T022 [P] Create `apps/api/src/hds/models/site.py`: `Site` model per migration 003; import in `hds/models/__init__.py`
- [X] T023 Create `apps/api/src/hds/models/user.py`: `UserRole` Python Enum mirroring DB type; `User` model per migration 004; `default_hourly_rate` mapped with `Numeric(12, 4, asdecimal=True)` — never `Float`; `client_id` nullable FK; `relationship("Client")` back-populates `users`; import in `hds/models/__init__.py`
- [X] T024 [P] Create `apps/api/src/hds/models/refresh_token.py`: `RefreshToken` model per migration 005; `relationship("User")`; import in `hds/models/__init__.py`
- [X] T025 Wire all model imports in `apps/api/src/hds/models/__init__.py`: import `Organisation`, `Client`, `Site`, `User`, `RefreshToken` so that `Base.metadata` contains all tables when Alembic's `env.py` references `target_metadata`; run `alembic check` to confirm no drift after this task
- [X] T026 Create `apps/api/src/hds/repositories/base.py`: `BaseRepository[ModelT]`; `__init__(self, session: AsyncSession)`; `get_by_id(id: UUID) -> ModelT | None`; `list(org_id: UUID, **filters) -> list[ModelT]`; no auth logic, no business rules
- [X] T027 [P] Create `apps/api/src/hds/repositories/organisation.py`: `OrganisationRepository(BaseRepository[Organisation])`: `get_by_id`; all DB access only here (Constitution Principle II)
- [X] T028 Create `apps/api/src/hds/core/security.py`: `hash_password(plain: str) -> str` via argon2-cffi `PasswordHasher` (time_cost=2, memory_cost=65536, parallelism=2); `verify_password(plain, hashed) -> bool`; `create_access_token(sub: UUID, org: UUID, role: UserRole, ttl_seconds: int) -> str` via python-jose; `decode_access_token(token: str) -> dict`; `generate_refresh_token() -> tuple[str, str]` returns (raw_token, sha256_hex_hash)
- [X] T029 Create `apps/api/src/hds/core/exceptions.py`: `AuthError(code: str, detail: str)`; `ForbiddenError(code: str, detail: str)`; `NotFoundError(resource: str, id: UUID)`; `ConflictError(code: str, detail: str, field: str | None)`; `DomainValidationError(code: str, detail: str)`
- [X] T030 Create `apps/api/src/hds/core/error_handler.py`: FastAPI exception handlers for each domain error; response body is RFC 9457 `ProblemDetail` (type, title, status, detail, code, instance); `Content-Type: application/problem+json`; HTTP status mapping: AuthError→401, ForbiddenError→403, NotFoundError→404, ConflictError→409, DomainValidationError→422; account-locked AuthError→423
- [X] T031 Create `apps/api/src/hds/core/logging.py`: configure structlog or stdlib logging for JSON output; include `request_id` (set by middleware) and `user_id` when available; filter: NEVER log `password`, `token`, `email`, `phone` field values
- [X] T032 Create `apps/api/src/hds/main.py`: `create_app() -> FastAPI`; register CORS middleware; register request-id middleware (generate UUID4 per request, add to response header `X-Request-Id`); register exception handlers from `error_handler.py`; expose `app = create_app()` for uvicorn; routers added in later phases

**Checkpoint**: `docker compose up` starts PostgreSQL; `alembic upgrade head` runs migrations 001–005; `uvicorn hds.main:app` starts with 0 errors; `alembic check` reports no drift.

---

## Phase 3: User Story 1 — Sign In and Role-Based Access (Priority: P1) 🎯 MVP

**Goal**: Users of all four roles can sign in, receive tokens, and are denied access to screens outside their role.

**Independent Test**: Four test accounts sign in; each gets a 200 with tokens; Employee calling `GET /employees` gets 403; Client calling `GET /employees` gets 403; Manager gets 200.

- [X] T033 [P] [US1] Write unit tests for `AuthService` in `apps/api/tests/unit/test_auth_service.py` using fake repositories (no DB): sign-in with correct credentials returns `TokenResponse`; wrong password increments `failed_login_attempts`; 5th failure sets `locked_until`; sign-in while locked raises `AuthError(code="auth.account_locked")`; successful sign-in resets counter; sign-out revokes token; sign-out-all revokes all tokens for user; refresh rotates token. **RUN TESTS — confirm ALL FAIL before T038.**
- [X] T034 [P] [US1] Create `apps/api/tests/conftest.py`: `async_client` fixture (httpx `AsyncClient` + `ASGITransport(app)`); `test_db` fixture (PostgreSQL test container or isolated schema per session); `seed_org` fixture (create one `Organisation`); `seed_users` fixture (one `User` per role linked to `seed_org`)
- [X] T035 [P] [US1] Write integration tests for role boundaries in `apps/api/tests/integration/test_auth_roles.py`: each seeded role signs in → 200; Employee `GET /api/v1/employees` → 403; Client `GET /api/v1/employees` → 403; Manager `GET /api/v1/employees` → 200 (after T067 registers the router)
- [X] T036 [US1] Create `apps/api/src/hds/repositories/user.py`: `UserRepository(BaseRepository[User])`: `get_by_email_and_org(email, org_id) -> User | None`; `get_by_id(id: UUID) -> User | None`; `create(org_id, data) -> User` (used by both employee and client-user creation); `increment_failed_attempts(user_id)`; `reset_failed_attempts(user_id)`; `set_locked_until(user_id, until: datetime)`; `update(user_id, **fields) -> User`; `soft_delete(user_id)` (sets `is_deleted=True`, `deleted_at=now()`)
- [X] T037 [P] [US1] Create `apps/api/src/hds/repositories/refresh_token.py`: `RefreshTokenRepository`: `create(user_id, token_hash, expires_at) -> RefreshToken`; `get_by_hash(hash) -> RefreshToken | None`; `revoke(token_id)`; `revoke_all_for_user(user_id)` (sets `revoked_at=now()` on all non-revoked rows)
- [X] T038 [US1] Create `apps/api/src/hds/services/auth.py`: `AuthService(user_repo, token_repo, config)`: `login(email, org_id, password) -> TokenResponse` — check `locked_until` first (raise `AuthError(code="auth.account_locked")` with HTTP 423); verify password; on failure increment + lock if ≥ `LOGIN_MAX_FAILURES`; on success reset counter, issue access token (TTL from config) + refresh token; `refresh(raw_token) -> TokenResponse` — hash, look up, check `revoked_at` and `expires_at`, rotate (revoke old, issue new); `logout(raw_token)`; `logout_all(user_id)`. **After implementing: run T033 tests — ALL must PASS.**
- [X] T039 [US1] Create `apps/api/src/hds/core/deps.py`: `get_current_user(token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)) -> User` — decode JWT, load live `User` by calling `UserRepository(db).get_by_id(sub)` — MUST NOT use `db.get` or `db.execute` directly in `deps.py` (Constitution Principle II); raise `AuthError` if invalid/expired/inactive; `require_manager(user: User = Depends(get_current_user)) -> User` — raise `ForbiddenError` if `user.role not in {manager, admin}`; `require_admin(user) -> User` — raise `ForbiddenError` if `user.role != admin`; `get_current_user_or_self(id: UUID, user: User = Depends(get_current_user)) -> User` — allow manager+ OR `user.id == id`
- [X] T040 [US1] Create `apps/api/src/hds/routers/auth.py`: Pydantic schemas `LoginRequest`, `TokenResponse`, `RefreshRequest` per `contracts/openapi.yaml`; `POST /auth/login` → `AuthService.login`; `POST /auth/refresh`; `POST /auth/logout` (auth required); `POST /auth/logout-all` (auth required); register in `main.py`
- [X] T041 [P] [US1] Create `apps/api/src/hds/routers/me.py`: Pydantic `UserRead` schema per `contracts/openapi.yaml` — include `default_hourly_rate` as a read-only field (FR-017); `GET /me` → returns `get_current_user`; register in `main.py`. PATCH /me is added in T048.
- [X] T042 [US1] Create `apps/api/src/hds/scripts/seed_dev.py`: seed one `Organisation` (name="HDS Dev", timezone="Asia/Jerusalem") and three named internal users with credentials from `quickstart.md` (`admin@hds.test` role=admin, `manager@hds.test` role=manager, `employee@hds.test` role=employee) plus one `Client` with its client user (`client@hds.test`); idempotent (skip if already exists). Project and Site seeding is deferred to T115.
- [ ] T043 [US1] Run integration tests `T035` against PostgreSQL — confirm role boundaries pass; fix any failures before continuing. **Depends on T070 (employees router registered) and T132 (strict 200 assertion); execute after Phase 5.**
- [X] T044 [P] [US1] Add Hebrew/English translation strings for auth screens to `apps/api/src/hds/locales/he.json` and `apps/api/src/hds/locales/en.json`: keys `auth.sign_in`, `auth.sign_out`, `auth.invalid_credentials`, `auth.account_locked`, `auth.account_locked_detail`
- [X] T045 [P] [US1] Create Playwright test scaffold `apps/client/tests/e2e/test_us1_signin.spec.ts`: sign-in with each role → correct redirect; sign-out invalidates session; locked account shows message; both LTR and RTL project configurations — **NOTE: pending `apps/client` implementation; scaffold file created, tests marked `test.skip` until client is built**

**Checkpoint US1**: `POST /api/v1/auth/login` works for all four roles; role boundaries return 403 as expected; `locked_until` activates after 5 failures; `GET /me` returns correct data including `default_hourly_rate`.

---

## Phase 4: User Story 2 — Employee Views and Edits Own Profile (Priority: P2)

**Goal**: An Employee can read their own profile and update name and phone. Any other field in the PATCH body is rejected.

**Independent Test**: Employee updates phone → 200 with new phone; Employee sends `{role: "manager"}` → rejected; Employee hits `GET /employees/{other_id}` → 403.

- [X] T046 [P] [US2] Write unit tests for employee self-service in `apps/api/tests/unit/test_employee_self_service.py`: `update_own_profile({full_name, phone})` succeeds; `update_own_profile({role: "manager"})` raises `ForbiddenError`; `update_own_profile({is_active: false})` raises `ForbiddenError`; get profile returns correct `User` including `default_hourly_rate`. **RUN TESTS — confirm ALL FAIL before T047.**
- [X] T047 [US2] Create `apps/api/src/hds/services/employee.py` (initial stub): `EmployeeService(user_repo, config)`; `get_profile(user_id, org_id) -> User`; `update_own_profile(current_user: User, data: dict) -> User` — whitelist: only `full_name` and `phone` allowed; any other key raises `ForbiddenError(code="profile.field_not_allowed")`; delegate write to `user_repo.update`. **Run T046 — ALL must PASS.**
- [X] T048 [US2] Add `PATCH /me` endpoint to `apps/api/src/hds/routers/me.py`: Pydantic update schema accepting only `{full_name?: str, phone?: str}`; wire `EmployeeService.update_own_profile` via `Depends`; field whitelist enforced at **service layer**, not just router schema
- [X] T049 [P] [US2] Write integration tests in `apps/api/tests/integration/test_me.py`: Employee `PATCH /me {phone: "+972501234567"}` → 200 with new phone; Employee `PATCH /me {role: "manager"}` → 403; Employee `GET /api/v1/employees/{other_id}` → 403
- [X] T050 [P] [US2] Add Hebrew/English strings for profile screens to `apps/api/src/hds/locales/he.json` and `apps/api/src/hds/locales/en.json`: `profile.my_profile`, `profile.full_name`, `profile.phone`, `profile.save`, `profile.field_not_allowed`
- [X] T051 [P] [US2] Create stub `apps/client/src/locales/he.json` and `apps/client/src/locales/en.json` with the same keys as the backend locale files (populated when client is built)
- [X] T052 [P] [US2] Create Playwright test scaffold `apps/client/tests/e2e/test_us2_profile.spec.ts`: open My Profile, edit phone, save, verify persisted; attempt to navigate to other employee profile → blocked — **pending client implementation; marked `test.skip`**

**Checkpoint US2**: `PATCH /me` updates name/phone; role/active fields rejected at service layer; no cross-employee access.

---

## Phase 5: User Story 3 — Manager Manages Employees, Groups, and Qualifications (Priority: P3)

**Goal**: Managers can create/search/edit/promote/deactivate/reactivate/soft-delete employees, manage groups and qualifications, and assign them to employees.

**Independent Test**: Manager creates employee → appears in list; duplicate email in org → 409; promote → role changes; deactivate → cannot sign in; reactivate → can sign in; last-Admin guard → 409; non-Admin attempting to promote to Admin → 403; group/qualification assignment via PUT endpoints.

- [X] T053 [P] [US3] Write unit tests for `EmployeeService` manager operations in `apps/api/tests/unit/test_employee_manager.py`: `create_employee` success; `create_employee` duplicate email in org → `ConflictError(code="users.duplicate_email")`; same email in different org → success; `promote_to_manager` by Manager → success; `promote_to_admin` by non-Admin → `ForbiddenError(code="users.admin_required")`; `deactivate`; `reactivate`; `soft_delete` when last Admin → `ConflictError(code="users.last_admin")`; `soft_delete` non-last admin → success. **RUN — confirm ALL FAIL before T064.**
- [X] T054 [P] [US3] Write unit tests for `GroupService` in `apps/api/tests/unit/test_group_service.py`: `create_group` success; duplicate name in org → `ConflictError`; `rename_group`; `delete_group`; `replace_employee_groups` (full replacement); empty list clears all memberships. **RUN — confirm FAIL before T065.**
- [X] T055 [P] [US3] Write unit tests for `QualificationService` in `apps/api/tests/unit/test_qualification_service.py`: create success; duplicate name → `ConflictError`; rename; delete; `replace_employee_qualifications`. **RUN — confirm FAIL before T066.**
- [X] T056 [US3] Create Alembic migration `006_groups`: `CREATE TABLE groups (id UUID PK, organization_id UUID NOT NULL REFERENCES organisations(id), name VARCHAR(255) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`; `CREATE UNIQUE INDEX uq_groups_org_name ON groups(organization_id, name)`; reversible
- [X] T057 [P] [US3] Create Alembic migration `007_qualifications`: `CREATE TABLE qualifications (id UUID PK, organization_id UUID NOT NULL REFERENCES organisations(id), name VARCHAR(255) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`; `CREATE UNIQUE INDEX uq_qualifications_org_name ON qualifications(organization_id, name)`; reversible
- [X] T058 [P] [US3] Create Alembic migration `008_user_groups`: `CREATE TABLE user_groups (user_id UUID NOT NULL REFERENCES users(id), group_id UUID NOT NULL REFERENCES groups(id), PRIMARY KEY(user_id, group_id))`; reversible
- [X] T059 [P] [US3] Create Alembic migration `009_user_qualifications`: `CREATE TABLE user_qualifications (user_id UUID NOT NULL REFERENCES users(id), qualification_id UUID NOT NULL REFERENCES qualifications(id), PRIMARY KEY(user_id, qualification_id))`; reversible
- [X] T060 [P] [US3] Create `apps/api/src/hds/models/group.py`: `Group` model (`__tablename__="groups"`); `user_groups` association table object; add `User.groups` relationship via `secondary=user_groups`; import `Group` in `hds/models/__init__.py`
- [X] T061 [P] [US3] Create `apps/api/src/hds/models/qualification.py`: `Qualification` model; `user_qualifications` association table; add `User.qualifications` relationship; import `Qualification` in `hds/models/__init__.py`
- [X] T062 [P] [US3] Create `apps/api/src/hds/repositories/group.py`: `GroupRepository`: `list_by_org(org_id) -> list[Group]`; `create(org_id, name) -> Group`; `rename(id, name) -> Group`; `delete(id)`; `get_groups_for_user(user_id) -> list[Group]`; `replace_user_groups(user_id, group_ids: list[UUID])`
- [X] T063 [P] [US3] Create `apps/api/src/hds/repositories/qualification.py`: `QualificationRepository`: same interface as `GroupRepository` adapted for qualifications
- [X] T064 [US3] Extend `apps/api/src/hds/services/employee.py` with manager methods: `create_employee(org_id, data) -> User` — check `user_repo.get_by_email_and_org`; raise `ConflictError(code="users.duplicate_email", field="email")` if exists per FR-012; `list_employees(org_id, search?, role?, is_active?, group_id?, qualification_id?) -> Page[User]`; `edit_employee(id, org_id, data, current_user) -> User` — if `data.role == "admin"` and `current_user.role != "admin"`: raise `ForbiddenError(code="users.admin_required")`; `promote(id, org_id, current_user) -> User` — same admin-role guard; `deactivate(id, org_id) -> User` — count active admins if target is admin, raise `ConflictError(code="users.last_admin")` if count == 1 per FR-011; `reactivate(id, org_id) -> User`; `soft_delete(id, org_id)` — same last-admin guard. **Run T053 — ALL must PASS.**
- [X] T065 [P] [US3] Create `apps/api/src/hds/services/group.py`: `GroupService`: `create_group(org_id, name) -> Group` — raise `ConflictError` if name exists in org; `rename_group(id, org_id, name) -> Group`; `delete_group(id, org_id)`; `replace_employee_groups(employee_id, org_id, group_ids)`. **Run T054 — PASS.**
- [X] T066 [P] [US3] Create `apps/api/src/hds/services/qualification.py`: `QualificationService` — same interface as `GroupService` for qualifications. **Run T055 — PASS.**
- [X] T067 [US3] Create `apps/api/src/hds/routers/employees.py`: Pydantic schemas `UserCreateRequest`, `UserUpdateRequest`, `UserListResponse`, `IDListRequest` per `contracts/openapi.yaml`; `UserRead` schema includes `default_hourly_rate` (FR-017); endpoints: `GET /employees` (require_manager, cursor pagination, all filters); `POST /employees` (require_manager); `GET /employees/{id}` (get_current_user_or_self); `PATCH /employees/{id}` (require_manager or self with whitelist); `DELETE /employees/{id}` (require_admin); `POST /employees/{id}/deactivate` (require_manager); `POST /employees/{id}/reactivate` (require_manager); `GET /employees/{id}/groups` (require_manager or self); `PUT /employees/{id}/groups` (require_manager); `GET /employees/{id}/qualifications` (require_manager or self); `PUT /employees/{id}/qualifications` (require_manager)
- [X] T068 [P] [US3] Create `apps/api/src/hds/routers/groups.py`: Pydantic `GroupRead`, `GroupWriteRequest`, `GroupListResponse` per `contracts/openapi.yaml`; `GET /groups`, `POST /groups`, `PATCH /groups/{id}`, `DELETE /groups/{id}` (all require_manager)
- [X] T069 [P] [US3] Create `apps/api/src/hds/routers/qualifications.py`: `GET /qualifications`, `POST /qualifications`, `PATCH /qualifications/{id}`, `DELETE /qualifications/{id}` (all require_manager)
- [X] T070 [US3] Register `employees`, `groups`, `qualifications` routers in `apps/api/src/hds/main.py`; confirm `alembic check` still reports no drift
- [X] T071 [P] [US3] Write integration tests in `apps/api/tests/integration/test_employees.py`: create employee → 201; duplicate email → 409 `code="users.duplicate_email"`; promote → role updated; deactivate → login returns 401; reactivate → login returns 200; last-admin deactivation → 409 `code="users.last_admin"`; non-Admin promoting to Admin → 403 `code="users.admin_required"`; soft-delete → not in list; `PUT /employees/{id}/groups` assigns correctly
- [X] T072 [P] [US3] Add Hebrew/English strings for employee management to locales: `employees.create`, `employees.deactivate`, `employees.reactivate`, `employees.delete`, `employees.last_admin_error`, `employees.duplicate_email`, `employees.admin_required`, `groups.create`, `qualifications.create`
- [X] T073 [P] [US3] Create Playwright test scaffold `apps/client/tests/e2e/test_us3_employees.spec.ts` — **pending client implementation; marked `test.skip`**

**Checkpoint US3**: Full CRUD for employees, groups, qualifications; access boundaries and last-admin guard verified by integration tests.

---

## Phase 6: User Story 4 — Manager Manages Clients, Sites, and Projects (Priority: P4)

**Goal**: Managers create/edit clients (with cascade deactivation), sites, and projects (with billing_rate as exact Decimal > 0, bidirectional status). Admin can delete a client only when it has no active projects (FR-009a).

**Independent Test**: Create client, site, project → all appear in lists; `billing_rate="0"` → 422; project `ended` → `active` → succeeds; deactivate client → client users deactivated; soft-delete project → hidden; soft-delete client with existing projects → 409.

- [X] T074 [P] [US4] Write unit tests for `ClientService` in `apps/api/tests/unit/test_client_service.py`: `create_client` success; `edit_client(is_active=False)` → calls `repo.deactivate_client_users` per FR-024; `soft_delete_client` with no projects → success; `soft_delete_client` with existing non-deleted projects → `ConflictError(code="clients.has_projects")` per FR-009a; `create_client_user` duplicate email in org → `ConflictError`. **RUN — confirm FAIL before T083.**
- [X] T075 [P] [US4] Write unit tests for `SiteService` in `apps/api/tests/unit/test_site_service.py`: `create_site` success; `edit_site` success. **RUN — confirm FAIL before T084.**
- [X] T076 [P] [US4] Write unit tests for `ProjectService` manager scope in `apps/api/tests/unit/test_project_service.py`: `create_project` with `billing_rate=Decimal("75.00")` → success; `billing_rate=Decimal("0")` → `DomainValidationError` per FR-028; `billing_rate=Decimal("-1")` → `DomainValidationError`; `set_status(active→ended)` → status="ended"; `set_status(ended→active)` → status="active" per FR-026; `soft_delete` → `is_deleted=True`. **RUN — confirm FAIL before T085.**
- [X] T077 [US4] Create Alembic migration `010_projects`: `CREATE TABLE projects (id UUID PK, organization_id UUID NOT NULL REFERENCES organisations(id), client_id UUID NOT NULL REFERENCES clients(id), site_id UUID NOT NULL REFERENCES sites(id), name VARCHAR(255) NOT NULL, type project_type NOT NULL, billing_rate NUMERIC(12,4) NOT NULL CHECK(billing_rate > 0), start_date DATE NOT NULL, end_date DATE, status project_status NOT NULL DEFAULT 'active', is_deleted BOOLEAN NOT NULL DEFAULT false, deleted_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`; `CREATE INDEX idx_projects_org ON projects(organization_id)`; `CREATE INDEX idx_projects_client ON projects(client_id)`; reversible
- [X] T078 [P] [US4] Create `apps/api/src/hds/models/project.py`: `ProjectType` and `ProjectStatus` Python Enums; `Project` model; `billing_rate` mapped as `Numeric(12, 4, asdecimal=True)` — **never `Float`**; relationships to `Client` and `Site`; import `Project` in `hds/models/__init__.py`
- [X] T079 [P] [US4] Create `apps/api/src/hds/repositories/client.py`: `ClientRepository`: `list_by_org(org_id, is_active?) -> list[Client]`; `create`; `update`; `get_by_id`; `soft_delete`; `deactivate_client_users(client_id)` (sets `is_active=False` on all users with that `client_id`); `count_non_deleted_projects(client_id) -> int` (used by ClientService for FR-009a guard)
- [X] T080 [P] [US4] Create `apps/api/src/hds/repositories/site.py`: `SiteRepository`: `list_by_org`, `create`, `update`, `get_by_id`
- [X] T081 [P] [US4] Create `apps/api/src/hds/repositories/project.py`: `ProjectRepository`: `list_by_org(org_id, status?, client_id?) -> list[Project]`; `list_by_client(client_id) -> list[Project]`; `create`; `update`; `get_by_id`; `soft_delete`
- [X] T082 [P] [US4] Verify `UserRepository.create` (added in T036) supports the `client_id` field needed by `ClientService.create_client_user`; no new method required — confirm the existing implementation handles both employee creation (client_id=None) and client-user creation (client_id set)
- [X] T083 [US4] Create `apps/api/src/hds/services/client.py`: `ClientService`: `create_client(org_id, data) -> Client`; `edit_client(id, org_id, data) -> Client` — if `is_active` flips to `False` call `client_repo.deactivate_client_users(id)` per FR-024; `create_client_user(client_id, org_id, data) -> User` — check email unique in org; `remove_client_user(client_id, user_id, org_id)`; `soft_delete(id, org_id)` — check `client_repo.count_non_deleted_projects(id) > 0` → raise `ConflictError(code="clients.has_projects")` per FR-009a, then cascade deactivation. **Run T074 — PASS.**
- [X] T084 [P] [US4] Create `apps/api/src/hds/services/site.py`: `SiteService`: `create_site(org_id, data) -> Site`; `edit_site(id, org_id, data) -> Site`. **Run T075 — PASS.**
- [X] T085 [P] [US4] Create `apps/api/src/hds/services/project.py`: `ProjectService`: `create_project(org_id, data) -> Project` — validate `billing_rate > 0` (raise `DomainValidationError(code="projects.billing_rate_invalid")` if not per FR-028); `edit_project(id, org_id, data) -> Project`; `set_status(id, org_id, status: ProjectStatus) -> Project` — both `active→ended` and `ended→active` permitted per FR-026; `soft_delete(id, org_id)`. **Run T076 — PASS.**
- [X] T086 [US4] Create `apps/api/src/hds/routers/clients.py`: Pydantic `ClientRead`, `ClientCreateRequest`, `ClientUpdateRequest`, `ClientListResponse` per `contracts/openapi.yaml`; `GET /clients` (require_manager, cursor pagination); `POST /clients` (require_manager); `GET /clients/{id}` (require_manager); `PATCH /clients/{id}` (require_manager); `DELETE /clients/{id}` (require_admin; implements FR-009a — ClientService.soft_delete raises 409 `code="clients.has_projects"` if non-deleted projects exist); `GET /clients/{id}/users` (require_manager); `POST /clients/{id}/users` (require_manager); `DELETE /clients/{id}/users/{user_id}` (require_manager)
- [X] T087 [P] [US4] Create `apps/api/src/hds/routers/sites.py`: Pydantic `SiteRead`, `SiteCreateRequest`, `SiteUpdateRequest`, `SiteListResponse`; `GET /sites`, `POST /sites`, `GET /sites/{id}`, `PATCH /sites/{id}` (all require_manager)
- [X] T088 [P] [US4] Create `apps/api/src/hds/routers/projects.py`: Pydantic `ProjectRead`, `ProjectCreateRequest`, `ProjectUpdateRequest`, `ProjectListResponse` per `contracts/openapi.yaml`; `GET /projects` (get_current_user — both Manager+ and Client allowed, scope enforced in service); `POST /projects` (require_manager); `GET /projects/{id}` (get_current_user); `PATCH /projects/{id}` (require_manager); `DELETE /projects/{id}` (require_admin)
- [X] T089 [US4] Register `clients`, `sites`, `projects` routers in `apps/api/src/hds/main.py`; run `alembic check`
- [X] T090 [P] [US4] Write integration tests in `apps/api/tests/integration/test_clients_sites_projects.py`: create client → 201; create site → 201; create project `billing_rate="75.0000"` → 201; create project `billing_rate="0"` → 422 `code="projects.billing_rate_invalid"`; `PATCH /projects/{id} {status: "ended"}` → `status="ended"`; `PATCH /projects/{id} {status: "active"}` → `status="active"`; deactivate client → client user `is_active=false`; soft-delete project → 404 on subsequent GET; soft-delete client with existing project → 409 `code="clients.has_projects"`
- [X] T091 [P] [US4] Add Hebrew/English strings for client/site/project screens to locales: `clients.create`, `clients.has_projects`, `projects.billing_rate_invalid`, `projects.status_active`, `projects.status_ended`, `sites.create`
- [X] T092 [P] [US4] Create Playwright test scaffold `apps/client/tests/e2e/test_us4_clients_projects.spec.ts` — **pending client implementation; marked `test.skip`**

**Checkpoint US4**: All client/site/project CRUD works; `billing_rate` accepted as Decimal string, rejected as zero/negative; project status is fully bidirectional; client cascade deactivation verified; client deletion blocked when projects exist.

---

## Phase 7: User Story 5 — Client Views Own Projects (Priority: P5)

**Goal**: A Client-role user can only see their own client's projects. All other resources return 403.

**Independent Test**: Client lists projects → only own client's; Client fetches unowned project → 403; Client calls `GET /employees` → 403.

- [X] T093 [P] [US5] Write unit tests for `ProjectService` client scope in `apps/api/tests/unit/test_project_client_scope.py`: `list_projects` with client user returns only projects where `client_id == user.client_id`; `list_projects` with client user and `client_id` filter for different client → empty; `get_project` with unowned project_id → `ForbiddenError`. **RUN — confirm FAIL before T094.**
- [X] T094 [US5] Extend `apps/api/src/hds/services/project.py`: `list_projects(org_id, current_user, status?, client_id?) -> list[Project]` — if `current_user.role == 'client'`: override `client_id = current_user.client_id` regardless of filter; `get_project(id, org_id, current_user) -> Project` — if `current_user.role == 'client'` and `project.client_id != current_user.client_id`: raise `ForbiddenError(code="projects.access_denied")`. **Run T093 — PASS.**
- [X] T095 [US5] Update `apps/api/src/hds/routers/projects.py` `GET /projects` and `GET /projects/{id}` handlers: pass `current_user` into `ProjectService.list_projects` and `ProjectService.get_project` so the service can apply Client-scope filtering per FR-010. The dependency (`get_current_user`) was already set in T088; this task wires the parameter through to the service call.
- [X] T096 [P] [US5] Write integration tests in `apps/api/tests/integration/test_client_portal.py`: client lists projects → only seeded project; client `GET /projects/{unowned_id}` → 403; client `POST /projects` → 403; client `GET /employees` → 403; client `GET /clients` → 403
- [X] T097 [P] [US5] Add Hebrew/English strings for client portal: `portal.my_projects`, `portal.project_detail`
- [X] T098 [P] [US5] Create Playwright test scaffold `apps/client/tests/e2e/test_us5_client_portal.spec.ts` — **pending client implementation; marked `test.skip`**

**Checkpoint US5**: Client role sees only own projects; every cross-boundary call returns 403.

---

## Phase 8: User Story 6 — Password Reset and Invitation (*Proposed*) (Priority: P6)

*Implements FR-006. Marked Proposed — can be deferred or cut without affecting US1–US5.*

- [X] T099 [P] [US6] Write unit tests for `InvitationService` in `apps/api/tests/unit/test_invitation_service.py`: `create_invitation` returns raw token; `accept_invitation` within window activates user; `accept_invitation` after expiry → `AuthError(code="auth.token_expired")`; second use of same token → `AuthError(code="auth.token_used")`; `request_password_reset` with unknown email → silent (no error); `reset_password` valid → success; expired → `AuthError`. **RUN — confirm FAIL before T103.**
- [X] T100 [US6] Create Alembic migration `011_invitation_tokens`: `CREATE TABLE invitation_tokens (id UUID PK, user_id UUID NOT NULL REFERENCES users(id), token_hash VARCHAR(64) NOT NULL UNIQUE, purpose VARCHAR(16) NOT NULL CHECK(purpose IN ('invite','reset')), expires_at TIMESTAMPTZ NOT NULL, used_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now())`; `CREATE INDEX idx_invitation_tokens_user ON invitation_tokens(user_id)`; reversible
- [X] T101 [P] [US6] Create `apps/api/src/hds/models/invitation_token.py`: `InvitationToken` model per migration 011; `relationship("User")`; import `InvitationToken` in `hds/models/__init__.py`
- [X] T102 [P] [US6] Create `apps/api/src/hds/repositories/invitation_token.py`: `InvitationTokenRepository`: `create(user_id, token_hash, purpose, expires_at) -> InvitationToken`; `get_by_hash(hash) -> InvitationToken | None`; `mark_used(id)`
- [X] T103 [US6] Create `apps/api/src/hds/services/invitation.py`: `InvitationService`: `create_invitation(manager_user, email, role, org_id) -> str` (raw token — email delivery is caller/env concern per Assumptions); `accept_invitation(raw_token, new_password) -> User` — check `used_at` is None (else `AuthError(code="auth.token_used")`), check `expires_at` (else `AuthError(code="auth.token_expired")`), set password hash, activate user, mark token used; `request_password_reset(email, org_id) -> str | None` (silent if email not found); `reset_password(raw_token, new_password)`. **Run T099 — PASS.**
- [X] T104 [US6] Create `apps/api/src/hds/routers/invitations.py`: `POST /auth/invite` (require_manager); `POST /auth/invite/accept` (no auth, body `{token, password}`); `POST /auth/password-reset/request` (no auth, body `{email}`); `POST /auth/password-reset/confirm` (no auth, body `{token, new_password}`); register in `main.py`
- [X] T105 [P] [US6] Write integration tests in `apps/api/tests/integration/test_invitations.py`: create invitation → token returned; accept within window → user can sign in; accept again → 401 `code="auth.token_used"`; expired token → 401 `code="auth.token_expired"`; password reset flow end-to-end
- [X] T106 [P] [US6] Create Playwright test scaffold `apps/client/tests/e2e/test_us6_invite.spec.ts` — **pending client implementation; marked `test.skip`**

**Checkpoint US6**: Invitation and password-reset flows work end-to-end; token invalidation enforced.

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: CI pipeline, import-graph enforcement, locale consolidation, validation.

- [X] T107 [P] Create `apps/api/src/hds/tools/check_imports.py`: script using `ast` to walk `apps/api/src/hds/`; error if any `services/` module imports from `routers/`; error if any `repositories/` module imports from `services/`; `exit(1)` on violation; enforces Constitution Principle II
- [X] T108 [P] Consolidate all locale strings added in T044, T050, T072, T091, T097 into canonical `apps/api/src/hds/locales/he.json` and `apps/api/src/hds/locales/en.json`; verify identical key sets between Hebrew and English files (no missing translations)
- [X] T109 [P] Create `.github/workflows/ci.yml`: jobs — `lint` (ruff check + ruff format --check); `typecheck` (mypy --strict apps/api/src/); `test-unit` (pytest apps/api/tests/unit/ with SQLite); `test-integration` (pytest apps/api/tests/integration/ with PostgreSQL 16 service container); `contract` (schemathesis run /api/v1/openapi.json --checks all after starting app); `migration-check` (alembic upgrade head + alembic check); `import-graph` (python -m hds.tools.check_imports); **TypeScript client regeneration job is skipped until `packages/api-client` has a real generator — add a TODO comment in the workflow: "TODO: enable ts-client-check job in the apps/client feature (Constitution Principle V)"**; all other jobs block merge per Constitution Principle IX
- [ ] T110 Run `alembic check` from `apps/api/`: expected output `"No new upgrade operations detected."`; fix any model/migration drift before marking done
- [X] T111 [P] Run full unit test suite: `uv run pytest apps/api/tests/unit/ -v --tb=short`; all tests must pass; fix any failures
- [ ] T112 [P] Run full integration test suite: `uv run pytest apps/api/tests/integration/ -v --tb=short` (requires Docker Compose stack); all tests must pass
- [ ] T113 Run Schemathesis against running server: `uv run schemathesis run http://localhost:8000/api/v1/openapi.json --checks all --validate-schema true`; fix any schema mismatches between `contracts/openapi.yaml` and actual FastAPI-generated output; 0 failures required per Constitution Principle V
- [X] T114 [P] Create `docs/adr/002-soft-delete.md` and `docs/adr/003-jwt-refresh.md` per Engineering Standards definition-of-done (decision record for each major design choice)
- [ ] T115 [P] Update `apps/api/src/hds/scripts/seed_dev.py` to cover all entities introduced across phases: one Organisation; one Client with one client-user (`client@hds.test`); one Site; one Project linking the Client and Site (requires migrations 001–010 to be applied); three named internal users matching `quickstart.md` (`admin@hds.test` role=admin, `manager@hds.test` role=manager, `employee@hds.test` role=employee) plus one additional unnamed employee pre-assigned to the seeded Group for group-membership testing; one Group; one Qualification; verify `docker compose up && alembic upgrade head && python -m hds.scripts.seed_dev` runs clean against a fresh database

---

## Phase 10: Remediation (from `/speckit-analyze`, 2026-11-01)

**Purpose**: Close gaps found by cross-artifact analysis. Spec/plan were amended first (single-org sign-in, initial password, lockout disclosure, Admin protection). Tasks marked `[X]` are verified by static gates and unit tests only; anything needing PostgreSQL, Docker or CI stays `[ ]` until run.

- [X] T116 Create `apps/api/src/hds/core/authz.py` with `ensure_role(actor, *roles)` raising the 403 problem-details error; add an `actor: User` parameter to every mutating service method and call `ensure_role` inside the service (Constitution VI); update unit tests with fake repos to cover each role
- [X] T117 Single-org sign-in (FR-001): add `UserRepository.get_by_email(email)`; change `AuthService.login(email, password)`; remove `organization_id` from `LoginRequest` in schemas, router and `contracts/openapi.yaml`; update unit/integration tests and `quickstart.md`
- [X] T118 Initial password (FR-012, FR-022): add `initial_password` (`min_length=12`) to employee and client-user create schemas; hash with Argon2id in the service; update contract and tests
- [X] T119 Admin protection (FR-011, FR-011a): in `EmployeeService` reject removing, deactivating, deleting or demoting the last active Admin, and any edit/deactivate/delete/demote of an Admin by a non-Admin; unit tests including Hypothesis role-boundary cases
- [X] T120 Token revocation: revoke all refresh tokens on deactivate, delete and role change; `AuthService.refresh` rejects inactive or deleted users; tests for each path. Tests: `tests/integration/test_token_revocation.py` (refresh rejects inactive and soft-deleted users even with an unrevoked token; deactivate, delete and role change each revoke all of that user's tokens and no one else's; a plain edit keeps them; deactivated user's access token is rejected) and unit tests in `test_auth_service.py`. Passes on PostgreSQL; mutation-checked
- [X] T121 [Proposed, US6] Create `apps/api/src/hds/core/mail.py` `MailSender` interface with a logging implementation; password-reset and invite endpoints return `202` with empty body regardless of account existence; invite creates an inactive user; update contract and tests
- [ ] T122 Migration `013_membership_cascade` (012 is `012_global_unique_email`, T135): `ON DELETE CASCADE` on `user_groups.group_id` and `user_qualifications.qualification_id`; verify with `alembic check`; integration test for FR edge case "deleting an assigned group or qualification removes memberships"
- [ ] T123 Validation (FR-028): add error codes `projects.date_range_invalid` and `users.hourly_rate_invalid` (he/en locale keys); validate `end_date >= start_date` and rates `> 0` in services; map `IntegrityError` to 422 problem-details instead of 500
- [X] T124 Create `apps/api/src/hds/scripts/export_openapi.py`; add a CI step that regenerates the OpenAPI document and diffs it against `specs/001-foundation-auth-org/contracts/openapi.yaml` (fail on drift)
- [ ] T125 Time-invariant tests (Constitution VII): timestamps are timezone-aware UTC; rates and durations are positive; no `float` in money paths
- [ ] T126 Create `apps/api/src/hds/core/i18n.py` message lookup; rename locale keys to match error `code` values; add an AST test asserting every `code` used in routers/services exists in both `he.json` and `en.json`
- [ ] T127 Hypothesis role-matrix test: for every (role, endpoint) pair assert the allowed/forbidden outcome from the spec role matrix
- [ ] T128 Schemathesis as a pytest module in `apps/api/tests/contract/` driven from the OpenAPI document
- [ ] T129 Performance smoke test: read endpoints p95 < 300 ms and sign-in < 3 s against the seeded PostgreSQL stack (SC-001)
- [X] T130 Extend `hds/tools/check_imports.py`: forbid `sqlalchemy` query constructs and `.execute(` in `routers/` and `services/`; forbid `models/` importing upward; add tests
- [ ] T131 CI hardening in `.github/workflows/ci.yml` (workflow edited; confirm on the first CI run): fix lint paths, set `JWT_SECRET_KEY` for DB-backed jobs, replace `sleep` with curl retry, add `pip-audit` and commitlint jobs
- [ ] T132 Strengthen `tests/integration/test_auth_roles.py`: assert strict `200` (not "not 403") for permitted roles
- [ ] T133 Re-verify against Docker/PostgreSQL: T043, T110, T112, T113, T115 — run each, fix failures, and only then mark them `[X]`
- [X] T134 Organisation scoping (tests first): `OrgScopedRepository.get_by_id(id, org_id)` and `list(org_id)` filter by the caller's organisation for user, client, site, project, group and qualification repositories (child listings `list_users`, `list_by_client`, `get_groups_for_user`, `get_qualifications_for_user` take `org_id` too); `get_client`/`get_site` routes and all id-bearing routes pass `current_user.organization_id`; token-owner lookups use the single unscoped `UserRepository.get_for_auth`. `tests/integration/test_org_scoping.py` asserts a UUID from another organisation returns 404 for every resource and route and leaves the other organisation's rows untouched. Passes on PostgreSQL
- [X] T135 Global email uniqueness (tests first): migration `012_global_unique_email` replaces `uq_users_org_email` with partial unique index `uq_users_email` on `users(email) WHERE is_deleted = false`; model, `data-model.md` and ADR 002 updated; `tests/integration/test_email_uniqueness.py` (DB rejects a duplicate across organisations, API returns 409, reuse after soft delete allowed). Passes on PostgreSQL
- [X] T136 Password reset revokes sessions (tests first): `InvitationService.reset_password` calls `RefreshTokenRepository.revoke_all_for_user`; integration test `tests/integration/test_password_reset_revocation.py` (old refresh tokens return 401, other users unaffected) plus unit tests. Passes on PostgreSQL
- [X] T137 Foreign-key ids belong to the caller's organisation (tests first, `tests/integration/test_foreign_key_ids.py`): every endpoint taking an FK id in a body, path or query returns 404 (not 403) for another organisation's id and writes nothing, on create and update. Audit of all routes found: (1) `POST /api/v1/projects` body `client_id`, `site_id` — fixed in `ProjectService.create_project`; (2) `GET /api/v1/projects?client_id=` query — fixed in `ProjectService.list_projects`; (3) `PUT /api/v1/employees/{id}/groups` and `/qualifications` body `ids` and path user id — covered (T134), foreign and mixed ids tested; (4) `POST /api/v1/clients/{id}/users` path client id — covered, tested; (5) `DELETE /api/v1/clients/{id}/users/{user_id}` path user id — covered, tested; (6) update routes (`PATCH` projects, clients, sites, employees, groups, qualifications) accept no FK id; a test proves `client_id`/`site_id` in a project PATCH body cannot re-point it. No other route takes an FK id. Passes on PostgreSQL

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 10 (Remediation)**: Requires Phases 1–9 code in place. T116 precedes T117–T121 and T119; T122 precedes T133; T132 precedes T043; T133 runs last.

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Requires Phase 1 complete — BLOCKS all user stories
- **Phases 3–8 (User Stories)**: All require Phase 2 complete; execute in priority order or in parallel across team members
- **Phase 9 (Polish)**: Requires all desired user story phases complete

### Migration Ordering (strict FK dependency)

```
001_init_organisations
002_clients           (FK → organisations)
003_sites             (FK → organisations)
004_users             (FK → organisations, clients)
005_refresh_tokens    (FK → users)
006_groups            (FK → organisations)
007_qualifications    (FK → organisations)
008_user_groups       (FK → users, groups)
009_user_qualifications (FK → users, qualifications)
010_projects          (FK → organisations, clients, sites)
011_invitation_tokens (FK → users) [Proposed]
```

### User Story Dependencies

- **US1 (P1)**: Can start immediately after Phase 2 — blocks nothing but is needed by all
- **US2 (P2)**: Can start after Phase 2; shares `EmployeeService` stub with US3 — independent
- **US3 (P3)**: Can start after Phase 2 and US2 (shares `EmployeeService`)
- **US4 (P4)**: Can start after Phase 2 — fully independent of US2/US3
- **US5 (P5)**: Requires US4 complete (needs `projects` router to exist); extends `ProjectService`
- **US6 (P6)**: Can start after Phase 2 — fully independent; *Proposed*, can be dropped

### Parallel Opportunities Within Each Phase

```
Phase 2:  T016 ∥ T017   (clients ∥ sites migrations)
          T020 ∥ T021 ∥ T022 ∥ T023 ∥ T024   (all models; T025 follows after all complete)
          T019 ∥ T024   (refresh_tokens migration ∥ model)

Phase 3:  T033 ∥ T034 ∥ T035   (write all tests before implementing)
          T036 ∥ T037           (user repo ∥ token repo)
          T040 ∥ T041           (auth router ∥ me router GET /me)

Phase 5:  T053 ∥ T054 ∥ T055   (all unit tests first)
          T056 ∥ T057 ∥ T058 ∥ T059   (migrations)
          T060 ∥ T061           (group model ∥ qualification model)
          T062 ∥ T063           (group repo ∥ qualification repo)
          T065 ∥ T066           (group service ∥ qualification service)
          T068 ∥ T069           (groups router ∥ qualifications router)

Phase 6:  T074 ∥ T075 ∥ T076   (all unit tests first)
          T079 ∥ T080 ∥ T081   (repositories)
          T084 ∥ T085           (site service ∥ project service)
          T087 ∥ T088           (sites router ∥ projects router)
```

---

## Parallel Example: Phase 3 (US1)

```bash
# Step 1 — write and run tests (confirm they ALL fail)
Task: "T033 Write unit tests for AuthService"
Task: "T034 Create conftest.py"
Task: "T035 Write integration tests for role boundaries"

# Step 2 — implement (can parallelise repos and services)
Task: "T036 UserRepository"
Task: "T037 RefreshTokenRepository"

# Step 3 — services and routers (after repos)
Task: "T038 AuthService (run unit tests T033 — must pass)"
Task: "T039 deps.py"
Task: "T040 auth router"   ∥   Task: "T041 me router (GET /me)"
```

---

## Implementation Strategy

### MVP (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: US1 (Sign In + Role Boundaries)
4. **STOP and VALIDATE**: All four roles sign in; role guards return 403 correctly; brute-force lockout works
5. Demo with `quickstart.md` curl commands

### Incremental Delivery

1. Setup + Foundational → monorepo + DB ready
2. US1 → auth works, GET /me works → MVP
3. US2 → employee self-service (PATCH /me)
4. US3 → manager employee/group/qualification management
5. US4 → client/site/project management
6. US5 → client portal
7. US6 *(optional)* → invitation + password reset

### Proposed Cut

US6 (T099–T106) can be dropped from the first delivery. Cut it by removing those tasks and the migration 011. US1–US5 are unaffected.

---

## Notes

- `[P]` tasks touch different files; they can be run in parallel without conflicts
- Unit tests MUST be written first and confirmed to fail before the service implementation
- `billing_rate` and `default_hourly_rate` MUST use `Numeric(12,4,asdecimal=True)` — `Float` anywhere in the stack is a Constitution VII violation
- Role checks live in **service methods** (via `ensure_role`, T116), not only in router dependencies (Constitution VI)
- Spelling convention (I6): `organisations` (table, model, docs) is British; the `organization_id` column and attribute stay as shipped to avoid a rename migration. Do not mix new names.
- T082 is a verify-only task folded into T036 (D1); no separate work remains.
- Password-reset and invitation tokens must never be returned to unauthenticated callers (U3); the mail sender (T121) is the only delivery path.
- UX timings SC-003 and SC-006 are deferred to `apps/client`; API timings SC-001 and the 300 ms read target are covered by T129 (G2).
- All locale strings must be added in the same PR as the feature that introduces them (Constitution VIII)
- `deps.py` MUST call `UserRepository(db).get_by_id(sub)` — never `db.get` or `db.execute` directly (Constitution II)
- Commit after each task or logical group using Conventional Commits (`feat:`, `fix:`, `refactor:`)
