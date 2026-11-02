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

The phases are the phases of [tasks.md](tasks.md):

1. **Phases 1 and 2** (Setup, Foundational): move the commit out of repositories (research D1) and
   run the existing suite unchanged. Checkpoint A, the last task of Phase 2, must be green before
   anything else is touched.
2. **Phases 3 to 5** (Stories 1, 2 and 3, the P1 stories).
3. **Phases 6 to 10** (Stories 4 to 8, the P2 stories).
4. **Phases 11 and 12** (Story 9, then Polish with the documentation and ADR checks).

Each phase ends with every gate below green and a commit on the feature branch.

## Gates (run after every phase)

```bash
uv run ruff check src tests alembic
uv run ruff format --check src tests
uv run interrogate src
uv run mypy --strict src/ tests/
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
| 1 No leaked links (SC-202) | Integration test: invite and reset with log capture on, using the outbox backend | No log record contains the token or an unmasked address; the outbox file holds a record with `[REDACTED]` in place of the token |
| 1 Manual link | In a development environment, run `uv run python -m hds.scripts.dev_issue_link <email> invite`; then run it with `ENVIRONMENT=production` | First prints a working link once and writes it nowhere; the second refuses to run |
| 2 Invariant | Insert an active user with no activation time | The database rejects it (`ck_users_active_has_activated_at`) |
| 2 Reactivate pending | `POST /api/v1/employees/{id}/reactivate` for an invited, never-activated account | 409 `users.pending_cannot_reactivate`, account still pending; not a 422 constraint answer |
| 2 Concurrent invites | Concurrency test: two staff invite the same new address at once | One pending account, both requests succeed |
| 2 Pending only (SC-204) | Integration tests: redeem a link for a deactivated, deleted and already-active account; invite a deactivated address | Redemptions refused with no data change; invite returns 409 `invitations.account_deactivated` |
| 2 Role on re-invite | Re-invite a pending account with a different role as Manager and as Admin | Manager to Manager succeeds; Manager to Admin is 403 |
| 3 Single use (SC-203) | Concurrency tests: 100 rounds of two simultaneous refreshes, and of two simultaneous link redemptions | Exactly one success per round |
| 3 Replay | Use a refresh token, wait past the grace window (configurable down to 1 s in tests), use it again | 401 and every refresh token of the user is revoked; within the window sessions are kept |
| 4 Atomicity | Failure-injection tests: force the last step of refresh, password reset and client update to fail | The earlier steps are not applied |
| 5 Last Admin (SC-205) | Concurrency test: two Admins deactivate each other at once, 100 rounds | An active Admin remains each time |
| 6 Lockout (SC-206) | Integration test on PostgreSQL for limits 2 to 10: N-1 failures then success; N failures then attempt; N failures, expire the lock, then N new attempts | Success after N-1; locked after N; a full N attempts after expiry |
| 7 Equal work (SC-207) | Unit test: the hash check runs exactly once on the unknown-email, known-email and inactive-account paths with identical answers. Before a release, run by hand: `uv run pytest -m timing` (500 samples each) | Called exactly once on every path; timing distributions indistinguishable |
| 8 Email case (SC-208) | Integration test: sign in, reset and duplicate-create with different case; run migration 016 against data with a clash | Same account every time; the migration stops and lists the clash |
| 9 Time zone (SC-209) | Create and edit a site with `Asia/Jerusalem`, `UTC`, `Mars/Base`, `` | First two accepted; others 422 `sites.timezone_invalid` |

## Operational checks

```bash
uv run python -m hds.scripts.find_email_case_duplicates   # run before migration 016
uv run alembic upgrade head
uv run python -m hds.scripts.check_site_timezones         # lists invalid stored zones
```

Both scripts only read data. Migration 016 must be run only when the first one prints nothing.


---

# Part 2: PR 4 fixes, validation

Run after each phase of Part 2 of [plan.md](plan.md). Gates are the ones above, plus
`uv run mypy --strict src/ tests/`. Contracts: [error codes, Part 2](contracts/error-codes.md);
data: [data model, Part 2](data-model.md).

| Item | How to prove it | Expected |
| --- | --- | --- |
| F01 | Request two reset links, complete with the newer, then use the older | Older refused `auth.token_used`; password unchanged |
| F02 | Sign in as a portal user, deactivate the client, reactivate the user, refresh with the old token | Reactivation refused while the client is inactive; old token refused after any later reactivation |
| F03 | Invite, deactivate the pending account, try to reactivate | Deactivation refused 409; account stays pending; link still valid |
| F04 | 20 concurrent unknown-address sign-ins while timing a plain request | The plain request is answered promptly; at most `HASH_CONCURRENCY` hashes run at once |
| FR-221 | Fail sign-in for one address 10 times (known, then an unknown one, mixed case) | Both get 429 with `Retry-After`; no password check over the limit; a success is unaffected |
| F05 | Log `{"password": "ab\"cd"}` and values with `'` and `\` | No fragment of the secret in the output; redacting twice changes nothing |
| F06 | Slow sender on password-reset request; compare registered and unknown addresses (`pytest -m timing`) | Response returns before delivery starts; ratio below 1.15 |
| F07 | Run migration 017 on a scratch database at 016 with a deactivated account that has an unused invite and a used reset link | It ends deactivated; a genuine pending invitee stays pending; undecidable ids are printed |
| F08 | Replay a rotated token after its expiry; replay one that is unexpired | `auth.token_expired`, other sessions live; the second ends them |
| F09 | Concurrency tests: refresh racing reset, logout everywhere, theft response and account change | No live token afterwards in any round |
| F10 | Create a site with `posixrules`, `Factory`, `asia/jerusalem`, `ASIA/JERUSALEM`; run `check_site_timezones` | 422 `sites.timezone_invalid`; the script lists stored values that fail |
| F11 | Lock an account, complete a reset, sign in with the new password | 200 |
| F12 | Unit test with a rollback that raises | The original exception propagates; the rollback error is logged by type |
| F13 to F16 | Run the named tests; read the edited documents | Test fails when the index is absent; dev tool refuses a deactivated account; numbering and environment named |
| F17 | `PATCH` each optional field with `null`, and omit it | Cleared with `null`; untouched when omitted; `null` for a required field is 422 |
| F18, F19, F23, F24, F25 | The named tests | As in the fix list; F23 and F24 follow the proposed defaults unless reversed |
| F20 to F22 | Full suite and the OpenAPI comparison | No behaviour or document change |
| F26 | Start two test runs at once | Both pass; no leftover database after either |

Operational: run `python -m hds.scripts.purge_expired_records` on a schedule; run
`python -m hds.scripts.check_site_timezones` after F10; read migration 017's output before accepting
it on real data.
