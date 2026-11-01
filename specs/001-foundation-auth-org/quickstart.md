# Quickstart Validation Guide: Foundation — Authentication and Organisation

**Branch**: `001-foundation-auth-org` | **Date**: 2026-11-01

This guide describes how to stand up the service locally, seed it, and verify each user
story end-to-end. It is not an implementation guide — it assumes the tasks in `tasks.md`
have been executed and the code exists.

---

## Prerequisites

| Requirement | Check |
|-------------|-------|
| Docker and Docker Compose installed | `docker compose version` |
| `uv` installed | `uv --version` |
| `pnpm` installed (for TS client generation) | `pnpm --version` |
| The repository cloned | `git status` |

---

## 1. Start local services

```bash
# From the repo root
docker compose up -d          # starts PostgreSQL 16 on port 5432

# Install Python dependencies
cd apps/api
uv sync

# Run Alembic migrations
uv run alembic upgrade head

# Seed the database with one organisation and one test account per role
uv run python -m hds.scripts.seed_dev
```

After seeding, the following accounts exist:

| Email | Password | Role |
|-------|----------|------|
| `admin@hds.test` | `Dev1234!` | Admin |
| `manager@hds.test` | `Dev1234!` | Manager |
| `employee@hds.test` | `Dev1234!` | Employee |
| `client@hds.test` | `Dev1234!` | Client |

The client user is linked to a pre-seeded client "Acme Security Ltd" with one active project
"North Site — Security" and one test site "North Site".

---

## 2. Start the API server

```bash
cd apps/api
uv run uvicorn hds.main:app --reload --port 8000
```

OpenAPI UI: `http://localhost:8000/docs`
OpenAPI JSON: `http://localhost:8000/openapi.json`

---

## 3. Verify the OpenAPI contract is current

```bash
# From repo root — regenerate TS client and fail if committed copy differs
pnpm --filter api-client generate
git diff --exit-code packages/api-client/src/
```

A clean exit confirms Principle V (API-First) is satisfied.

---

## 4. Run the automated test suites

### Unit tests (services with fake repositories)
```bash
cd apps/api
uv run pytest tests/unit/ -v
```
Expected: all role-boundary tests pass, including `test_last_admin_protection` and
`test_role_enforced_on_every_call`.

### Integration tests (real PostgreSQL)
```bash
uv run pytest tests/integration/ -v
```
Requires the Docker Compose stack to be running. Tests create isolated schemas per test
session; they do not pollute the dev seed data.

### Contract tests (Schemathesis against live server)
```bash
# Server must be running on port 8000
uv run schemathesis run http://localhost:8000/openapi.json \
    --checks all \
    --validate-schema true
```
Expected: 0 failures. Any response that does not match the schema causes a test failure.

### Playwright smoke tests (LTR + RTL passes)
```bash
cd apps/client
pnpm playwright test --project=chromium --project=chromium-rtl
```
Each user journey runs twice: once in LTR (English) and once in RTL (Hebrew). A layout
breakage in RTL fails the `chromium-rtl` project.

---

## 5. User story validation scenarios

### Story 1 — Sign In and Role-Based Access

```bash
# Sign in as Admin
curl -s -X POST http://localhost:8000/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"admin@hds.test","password":"Dev1234!"}' | jq .

# Expected: { "access_token": "...", "token_type": "bearer", "expires_in": 900, "refresh_token": "..." }
```

Verify denial — Employee attempts Manager route:
```bash
# Get token for employee
EMPLOYEE_TOKEN=$(curl -s -X POST http://localhost:8000/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"employee@hds.test","password":"Dev1234!"}' | jq -r .access_token)

# Attempt to list all employees (Manager+ only)
curl -s -o /dev/null -w "%{http_code}" \
  http://localhost:8000/api/v1/employees \
  -H "Authorization: Bearer $EMPLOYEE_TOKEN"
# Expected: 403
```

Verify brute-force lockout:
```bash
for i in {1..6}; do
  curl -s -X POST http://localhost:8000/api/v1/auth/login \
    -H 'Content-Type: application/json' \
    -d '{"email":"employee@hds.test","password":"wrongpassword"}' | jq .code
done
# 6th attempt expected: "auth.account_locked"
```

### Story 2 — Employee Self-Service Profile

```bash
# Employee reads own profile
curl -s http://localhost:8000/api/v1/me \
  -H "Authorization: Bearer $EMPLOYEE_TOKEN" | jq .

# Employee updates own profile
curl -s -X PATCH http://localhost:8000/api/v1/me \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $EMPLOYEE_TOKEN" \
  -d '{"phone":"+972501234567"}' | jq .phone
# Expected: "+972501234567"

# Employee attempts to view another user (use any known UUID from the seed)
curl -s -o /dev/null -w "%{http_code}" \
  http://localhost:8000/api/v1/employees/<other_user_id> \
  -H "Authorization: Bearer $EMPLOYEE_TOKEN"
# Expected: 403
```

### Story 3 — Manager Manages Employees

```bash
MANAGER_TOKEN=$(curl -s -X POST http://localhost:8000/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"manager@hds.test","password":"Dev1234!"}' | jq -r .access_token)

# Create employee
NEW_USER=$(curl -s -X POST http://localhost:8000/api/v1/employees \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $MANAGER_TOKEN" \
  -d '{"email":"newguard@hds.test","full_name":"New Guard","role":"employee","initial_password":"Dev1234!Dev1234!"}')
echo $NEW_USER | jq .id

NEW_ID=$(echo $NEW_USER | jq -r .id)

# Assign to a group (replace with seeded group UUID)
curl -s -X PUT http://localhost:8000/api/v1/employees/$NEW_ID/groups \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $MANAGER_TOKEN" \
  -d '{"ids":["<seeded-group-uuid>"]}'

# Deactivate
curl -s -X POST http://localhost:8000/api/v1/employees/$NEW_ID/deactivate \
  -H "Authorization: Bearer $MANAGER_TOKEN" | jq .is_active
# Expected: false

# Reactivate
curl -s -X POST http://localhost:8000/api/v1/employees/$NEW_ID/reactivate \
  -H "Authorization: Bearer $MANAGER_TOKEN" | jq .is_active
# Expected: true
```

### Story 4 — Manager Manages Clients, Sites, Projects

```bash
# Create a client
CLIENT=$(curl -s -X POST http://localhost:8000/api/v1/clients \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $MANAGER_TOKEN" \
  -d '{"name":"Beta Corp","contract_start":"2026-11-01"}')
CLIENT_ID=$(echo $CLIENT | jq -r .id)

# Create a site
SITE=$(curl -s -X POST http://localhost:8000/api/v1/sites \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $MANAGER_TOKEN" \
  -d '{"name":"Beta Site","address":"1 Test Street, Tel Aviv","timezone":"Asia/Jerusalem"}')
SITE_ID=$(echo $SITE | jq -r .id)

# Create a project
PROJECT=$(curl -s -X POST http://localhost:8000/api/v1/projects \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $MANAGER_TOKEN" \
  -d "{\"client_id\":\"$CLIENT_ID\",\"site_id\":\"$SITE_ID\",\"name\":\"Beta Security\",\"type\":\"security\",\"billing_rate\":\"75.0000\",\"start_date\":\"2026-11-01\"}")
PROJECT_ID=$(echo $PROJECT | jq -r .id)

# Mark as ended
curl -s -X PATCH http://localhost:8000/api/v1/projects/$PROJECT_ID \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $MANAGER_TOKEN" \
  -d '{"status":"ended"}' | jq .status
# Expected: "ended"

# Reactivate
curl -s -X PATCH http://localhost:8000/api/v1/projects/$PROJECT_ID \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $MANAGER_TOKEN" \
  -d '{"status":"active"}' | jq .status
# Expected: "active"
```

### Story 5 — Client Views Own Projects

```bash
CLIENT_TOKEN=$(curl -s -X POST http://localhost:8000/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"client@hds.test","password":"Dev1234!"}' | jq -r .access_token)

# Client sees own projects only
curl -s http://localhost:8000/api/v1/projects \
  -H "Authorization: Bearer $CLIENT_TOKEN" | jq '.items | length'
# Expected: ≥ 1 (seeded project only)

# Client cannot access employee list
curl -s -o /dev/null -w "%{http_code}" \
  http://localhost:8000/api/v1/employees \
  -H "Authorization: Bearer $CLIENT_TOKEN"
# Expected: 403
```

---

## 6. RTL / i18n validation

Run the Playwright RTL pass:

```bash
cd apps/client
pnpm playwright test --project=chromium-rtl --reporter=html
```

Open `playwright-report/index.html` and confirm:
- No text truncation in Hebrew layout.
- Navigation icons mirror correctly.
- Form fields align right.
- All text is translated (no English fallback strings in the Hebrew pass).

---

## 7. Constitution compliance spot-checks

| Principle | Spot-check command |
|-----------|-------------------|
| II. Layered Backend | `uv run python -m hds.tools.check_imports` (custom mypy plugin or script that errors on layer violations) |
| VII. Money not float | `grep -r "Float\|float" apps/api/src/hds/models/` — expect 0 matches |
| X. Migrations only | `uv run alembic check` — expect "No new upgrade operations detected." |
| IX. Quality gates | `uv run ruff check . && uv run mypy src/` — expect 0 errors |

---

## 8. Artifacts reference

| Artifact | Path |
|----------|------|
| OpenAPI contract | `specs/001-foundation-auth-org/contracts/openapi.yaml` |
| Data model | `specs/001-foundation-auth-org/data-model.md` |
| Research notes | `specs/001-foundation-auth-org/research.md` |
| Feature spec | `specs/001-foundation-auth-org/spec.md` |
