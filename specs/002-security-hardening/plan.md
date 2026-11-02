# Implementation Plan: Security Hardening of Sign-In, Tokens and Account Rules

**Branch**: `002-security-hardening` | **Date**: 2026-11-02 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/002-security-hardening/spec.md`

## Summary

Correct ten defects found in the foundation feature (001). No endpoint is added or removed. The
work falls into five technical moves:

1. **One transaction per request.** Repositories stop committing; a unit of work around each
   request commits once, rolls back on any error, and runs deferred actions (sending email) only
   after a successful commit. This is the base for the rest (research D1).
2. **Conditional writes for single use.** Refresh tokens and one-time links are spent with a
   guarded update that must change exactly one row; a spent refresh token that is replayed outside
   a grace window revokes all of the user's tokens (D2, D3).
3. **Explicit account state.** A new `activated_at` marks an account as having been activated, so
   an invitation can complete only a pending account, and outstanding links are cancelled when an
   account is deactivated, deleted or activated (D4).
4. **Safer start-up and logging.** An explicit environment allow-list for weak secrets, a
   development-only mail outbox instead of logging messages, and a production start that is
   refused without a real mail sender (D5, D6).
5. **Sign-in and data rules.** Lockout counted in the database, equalised sign-in work for
   unknown emails, case-insensitive emails (with a stop-on-duplicates migration), a row lock for
   the last-Admin check, and validated site time zones (D7 to D11).

## Technical Context

**Language/Version**: Python 3.12 (unchanged)

**Primary Dependencies**: FastAPI 0.162 (the `scope="function"` option on `Depends` is required,
see D1), Pydantic v2, SQLAlchemy 2.1 async, Alembic, argon2-cffi, PyJWT. One new runtime
dependency: `tzdata`, as a fallback source of time-zone data (D11).

**Storage**: PostgreSQL 16. Three new migrations (014 to 016); no table is dropped.

**Testing**: pytest and pytest-asyncio on a real PostgreSQL database for everything that depends
on locking, conditional updates or constraints. New: concurrency tests that run two sessions at
once, a failure-injection test per multi-step action, and a lockout test against the real
database. Hypothesis and Schemathesis stay as they are.

**Target Platform**: Linux server in Docker (unchanged).

**Project Type**: web-service (backend only; `apps/client` is untouched).

**Performance Goals**: no regression in the existing budgets (read p95 < 300 ms, sign-in < 3 s).
The extra work is one dummy password check for unknown emails and one `SELECT ... FOR UPDATE`
on a handful of Admin rows.

**Constraints**: the generated OpenAPI document must stay consistent with the contract of feature
001 (operations, success codes and query parameters unchanged; the drift check stays green);
Constitution X (new migrations only, reversible, no edits to applied code); Constitution VIII (every
new error code has English and Hebrew text); Constitution XI (docstrings, ADRs and READMEs ship in
the same pull request).

**Scale/Scope**: unchanged (about 50 to 200 employees per deployment).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-checked after Phase 1 design.*

| Principle | Check | Result |
| --- | --- | --- |
| I. Spec as source of truth | Everything is labelled Proposed; it corrects feature 001 and adds no capability | Pass |
| II. Layered backend | The unit of work lives in `core`; services stay free of SQL; the new row lock and guarded updates live in repositories. No new upward import | Pass |
| III. Pure scheduling package | Not touched | Pass |
| IV. Test-first | Each fix gets a test that fails first (FR-219); concurrency and lockout tests run on PostgreSQL | Pass |
| V. API-first | No operation, parameter or success code changes. New problem codes are listed in `contracts/error-codes.md`; the drift check must stay green | Pass |
| VI. Security | Strengthens it: Argon2id unchanged, secrets only from the environment, JWT lifetime unchanged | Pass |
| VII. Money and time | No money change; time stays UTC. Time-zone names are validated | Pass |
| VIII. Internationalisation | New error codes get `en` and `he` text; `test_i18n` enforces parity | Pass |
| IX. Quality gates | All gates stay required. Migration check covers the three new migrations | Pass |
| X. Migrations only | Three new reversible migrations; data steps that cannot be reversed are documented | Pass, one noted limit (D9) |
| XI. Documentation | Docstrings, two ADRs, README and configuration updates ship in the same pull request | Pass |

**Post-design re-check**: the same result. The one cross-cutting risk is that moving the commit
out of repositories touches every write path; it is mitigated by running the full existing suite
unchanged before any new behaviour is added (see tasks phasing in `quickstart.md`).

## Project Structure

### Documentation (this feature)

```text
specs/002-security-hardening/
├── plan.md              # This file
├── research.md          # Phase 0: decisions D1 to D11
├── data-model.md        # Phase 1: new columns, states, migrations
├── quickstart.md        # Phase 1: how to prove each story
├── contracts/
│   ├── error-codes.md   # New problem codes and their translations
│   └── configuration.md # New and changed settings
├── checklists/
│   └── requirements.md
└── tasks.md             # Phase 2 (/speckit-tasks, not created here)
```

### Source Code (repository root)

```text
apps/api/
├── alembic/versions/
│   ├── 014_user_activation.py            # users.activated_at + backfill
│   ├── 015_refresh_revoked_reason.py     # refresh_tokens.revoked_reason
│   └── 016_case_insensitive_email.py     # stop on duplicates, lowercase, new unique index
├── src/hds/
│   ├── core/
│   │   ├── config.py        # environment allow-list, mail backend, reuse grace window
│   │   ├── database.py      # unit of work (commit once, rollback, after-commit actions)
│   │   ├── logging.py       # redact format arguments, mask email addresses
│   │   ├── mail.py          # development outbox sender; production refuses to start without one
│   │   └── types.py         # Email (normalised), no change to Timezone type itself
│   ├── models/              # user.activated_at, refresh_token.revoked_reason
│   ├── repositories/        # no commits; guarded updates; row lock for Admins; cancel links
│   ├── services/            # auth, invitation, employee, client, site
│   ├── routers/             # switch to the unit-of-work dependency (46 endpoints)
│   ├── scripts/
│   │   ├── find_email_case_duplicates.py   # list clashes before migration 016
│   │   └── check_site_timezones.py         # report invalid stored zones (FR-218)
│   └── locales/             # en.json, he.json: new error codes
├── tests/
│   ├── unit/                # services with fakes; config and logging rules
│   ├── integration/         # PostgreSQL: lockout, single use, activation, email case, atomicity
│   └── concurrency/         # two sessions at once: refresh, links, last Admin
docs/adr/
├── 005-request-scoped-transactions.md
└── 006-refresh-token-reuse-detection.md
infra/docker-compose.yml     # explicit environment, no default secret
.env.example                 # explicit environment, empty secret placeholder
```

**Structure Decision**: keep the existing four-layer backend. All changes are inside `apps/api`,
plus two ADRs, the compose file and the example settings. One new test folder
(`tests/concurrency`) separates tests that need real parallelism and a real database.

## Complexity Tracking

No constitution violations. Two choices add moving parts and are justified in `research.md`: the
after-commit action list (D1, needed so email is never sent for a rolled-back change) and the
`revoked_reason` column (D2, needed so only a rotated token triggers the theft response).
