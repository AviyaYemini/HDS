# Quickstart Validation Guide: Security Hardening

**Branch**: `002-security-hardening` | **Date**: 2026-11-02

How to prove each user story once the tasks are done. It is a run guide, not an implementation
guide. Contracts: [error codes](contracts/error-codes.md), [configuration](contracts/configuration.md).
Data: [data-model.md](data-model.md).

## Prerequisites

- Docker, `uv`, and the repository cloned.
- PostgreSQL running: `docker compose -f infra/docker-compose.yml up -d postgres`.
- From `apps/api`: `uv sync --extra dev`.

## Phasing (so the riskiest change is proven first)

1. **Phase A, no behaviour change**: move the commit out of repositories (D1) and run the existing
   suite unchanged. It must pass before anything else is touched.
2. **Phase B**: Story 1, Story 2, Story 3 (the High findings).
3. **Phase C**: Story 4 tests, then Stories 5 to 8.
4. **Phase D**: Story 9, documentation, ADRs.

Each phase ends with every gate below green and a commit on the feature branch.

## Gates (run after every phase)

```bash
uv run ruff check src tests alembic
uv run ruff format --check src tests
uv run interrogate src
uv run mypy --strict src/
uv run python -m hds.tools.check_imports
uv run alembic check
TEST_DATABASE_URL=postgresql+asyncpg://hds:<password>@localhost:5432/hds_test uv run pytest
```

The OpenAPI drift check from feature 001 must stay green:
`uv run python -m hds.scripts.export_openapi --check ../../specs/001-foundation-auth-org/contracts/openapi.yaml <generated.json>`.

## Story checks

| Story | How to prove it | Expected |
| --- | --- | --- |
| 1 Start-up refusal (SC-201) | Build the settings with `ENVIRONMENT` unset; with `ENVIRONMENT=production` and the placeholder secret; with a 20-byte secret; with production and no mail backend | Each fails with a message naming the setting |
| 1 No leaked links (SC-202) | Integration test: invite and reset with log capture on | No log record contains the token or an unmasked address; the outbox file contains the message |
| 2 Pending only (SC-204) | Integration tests: redeem a link for a deactivated, deleted and already-active account; invite a deactivated address | Redemptions refused with no data change; invite returns 409 `invitations.account_deactivated` |
| 2 Role on re-invite | Re-invite a pending account with a different role as Manager and as Admin | Manager to Manager succeeds; Manager to Admin is 403 |
| 3 Single use (SC-203) | Concurrency tests: 100 rounds of two simultaneous refreshes, and of two simultaneous link redemptions | Exactly one success per round |
| 3 Replay | Use a refresh token, wait past the grace window (configurable down to 1 s in tests), use it again | 401 and every refresh token of the user is revoked; within the window sessions are kept |
| 4 Atomicity | Failure-injection tests: force the last step of refresh, password reset and client update to fail | The earlier steps are not applied |
| 5 Last Admin (SC-205) | Concurrency test: two Admins deactivate each other at once, 100 rounds | An active Admin remains each time |
| 6 Lockout (SC-206) | Integration test on PostgreSQL for limits 2 to 10: N-1 failures then success; N failures then attempt | Success after N-1; locked after N |
| 7 Equal work (SC-207) | Unit test: the hash check runs once on the unknown-email path; optional timing comparison, 500 samples each | Called exactly once; distributions indistinguishable |
| 8 Email case (SC-208) | Integration test: sign in, reset and duplicate-create with different case; run migration 016 against data with a clash | Same account every time; the migration stops and lists the clash |
| 9 Time zone (SC-209) | Create and edit a site with `Asia/Jerusalem`, `UTC`, `Mars/Base`, `` | First two accepted; others 422 `sites.timezone_invalid` |

## Operational checks

```bash
uv run python -m hds.scripts.find_email_case_duplicates   # run before migration 016
uv run alembic upgrade head
uv run python -m hds.scripts.check_site_timezones         # lists invalid stored zones
```

Both scripts only read data. Migration 016 must be run only when the first one prints nothing.
