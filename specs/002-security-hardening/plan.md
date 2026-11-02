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
   development-only mail outbox that stores no link in clear (plus a one-time "show me the link"
   action for manual testing), and a production start that is refused without a real mail sender
   (D5, D6).
5. **Sign-in and data rules.** Lockout counted in the database and reset when the lock expires,
   equalised sign-in work for unknown emails, case-insensitive emails (with a stop-on-duplicates migration), a row lock for
   the last-Admin check, and validated site time zones (D7 to D11).

## Technical Context

**Language/Version**: Python 3.12 (unchanged)

**Primary Dependencies**: FastAPI 0.121.0 or newer (the `scope="function"` option on `Depends` is
required, see D1; the lower bound in `pyproject.toml` is raised to 0.121.0, and 0.162 is what is
installed today), Pydantic v2, SQLAlchemy 2.1 async, Alembic, argon2-cffi, PyJWT. One new runtime
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

**Constraints**: ADR 004 stays in force (PyJWT, algorithm fixed by configuration, `exp` required;
no token-library or signature change); the generated OpenAPI document must stay consistent with the contract of feature
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
| VI. Security | Strengthens it: Argon2id unchanged, secrets only from the environment, JWT lifetime and ADR 004 unchanged, links never stored in clear | Pass |
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
│   ├── 014_user_activation.py            # users.activated_at + backfill + check constraint
│   ├── 015_refresh_revoked_reason.py     # refresh_tokens.revoked_reason
│   └── 016_case_insensitive_email.py     # stop on duplicates, lowercase, new unique index
├── src/hds/
│   ├── core/
│   │   ├── config.py        # environment allow-list, mail backend, reuse grace window
│   │   ├── database.py      # unit of work (commit once, rollback, after-commit actions)
│   │   ├── logging.py       # redact format arguments, mask email addresses
│   │   ├── mail.py          # redacting development outbox; production refuses to start without a real sender
│   │   └── types.py         # Email (normalised), no change to Timezone type itself
│   ├── models/              # user.activated_at, refresh_token.revoked_reason
│   ├── repositories/        # no commits; guarded updates; row lock for Admins; cancel links
│   ├── services/            # auth, invitation, employee, client, site
│   ├── routers/             # switch to the unit-of-work dependency (46 endpoints)
│   ├── scripts/
│   │   ├── find_email_case_duplicates.py   # list clashes before migration 016
│   │   ├── check_site_timezones.py         # report invalid stored zones (FR-218)
│   │   └── dev_issue_link.py               # development only: show a fresh link once
│   └── locales/             # en.json, he.json: new error codes
├── tests/
│   ├── unit/                # services with fakes; config and logging rules
│   ├── integration/         # PostgreSQL: lockout, single use, activation, email case, atomicity
│   └── concurrency/         # two sessions at once: refresh, links, last Admin
docs/adr/
├── 005-request-scoped-transactions.md
└── 006-refresh-token-reuse-detection.md
infra/docker-compose.yml     # explicit environment, no default secret
.github/workflows/ci.yml     # environment on every app job; tests/concurrency in the PostgreSQL job
.env.example                 # explicit environment, empty secret placeholder
```

**Structure Decision**: keep the existing four-layer backend. All changes are inside `apps/api`,
plus two ADRs, the compose file and the example settings. One new test folder
(`tests/concurrency`) separates tests that need real parallelism and a real database.

## Complexity Tracking

No constitution violations. Two choices add moving parts and are justified in `research.md`: the
after-commit action list (D1, needed so email is never sent for a rolled-back change) and the
`revoked_reason` column (D2, needed so only a rotated token triggers the theft response).

Accepted limit on Principle X: migration 016 is reversible for the schema (the old index is
restored on downgrade) but the lowercasing of stored email addresses is not undone. The step is
idempotent and a lowercase address stays valid under the old rule, so leaving it in place on
downgrade is harmless; the migration docstring says so (D9).


---

# Part 2: PR 4 fixes

**Date**: 2026-11-03 | **Input**: [pr4-fix-list.md](pr4-fix-list.md) (F01 to F26) and the clarifications
of session 2026-11-03 in [spec.md](spec.md) | **Research**: D12 to D26 in [research.md](research.md)

Part 1 above is the plan for the ten original findings and is complete (T201 to T304). Part 2 plans
the 26 open items from the PR 4 review. All of them are **Proposed**: none comes from the original
product spec. No tasks and no code are written at this stage.

## Summary

Every item is in scope for PR 4, including the older gaps F17 to F26 (clarification Q1). Four
decisions shape the work:

1. **Pending accounts are never "deactivated"** (Q2): staff deactivation of a pending account is a
   refusal, and `activated_at` is only ever written by a real activation (F03).
2. **An expired credential ends no sessions** (Q3): FR-209 is amended; expired records are purged
   (F08).
3. **Sign-in gets a database-backed rate limit** (Q4, Q5, new FR-221) and its password hashing moves
   off the event loop behind a bound (F04). This is the largest item and adds a table.
4. **Three defaults are proposed, not yet confirmed** (quota reached): a completed reset clears the
   lockout (F11), a Client account's role cannot change (F23), a client's contract end may not
   precede its start (F24). Each can be reversed before its phase starts.

## Technical Context (changes only)

**Primary Dependencies**: no new runtime dependency. `tzdata` (already required) becomes the
authoritative zone list (D21). `asyncio.to_thread` and a semaphore carry the hashing bound (D15).

**Storage**: two new migrations. **017** is a data migration that corrects migration 014's
"pending" backfill (F07, D18). **018** adds the `login_attempts` table for the rate limit
(F04, D16). Migration 014 is already committed and the project hook forbids editing it.

**Testing**: each item gets a test that fails first, on PostgreSQL where it touches locking,
constraints or migrations. New: a concurrency test per revoke-all path (F09), an event-loop
responsiveness test (F04), a delivery-ordering test (F06), per-run test databases (F26). Mutation
checks accompany every security fix, as in Part 1.

**Constraints**: the OpenAPI drift check stays green (operations, success codes and query
parameters unchanged); `429` is declared on login as a response code and described; the generated
document may differ from the pre-feature baseline in descriptions and in the added error
responses only. Constitution X: no edit to an applied migration. Constitution VIII: every new
error code has English and Hebrew text.

**Behaviour changes to existing endpoints** (each is listed in `contracts/error-codes.md`, Part 2):
`POST /auth/login` (429), `POST /auth/refresh` (expired rotated token), password reset request and
confirm (older links cancelled; lockout cleared), employee deactivate and reactivate (pending
refused; portal user of an inactive client refused), client edit and delete (sessions ended),
`PATCH` on me, employees, clients, projects and sites (explicit `null` clears), logout (ownership),
client create and edit (date order), employee edit (Client role locked).

## Constitution Check (Part 2)

| Principle | Check | Result |
| --- | --- | --- |
| I. Spec as source of truth | Items are labelled Proposed; FR-209 is amended and FR-221 added through the clarify stage, not silently | Pass |
| II. Layered backend | Hashing bound and rate-limit logic live in `core` and a repository; F20 moves router reads back into services, which restores the rule | Pass (F20 repairs an existing breach) |
| III. Pure scheduling package | Not touched | Pass |
| IV. Test-first | Every item has a failing-first test; security items add mutation checks | Pass |
| V. API-first | New response code `429` and new problem codes are declared; drift check stays green | Pass |
| VI. Security | Strengthens it (F01, F02, F03, F05, F08, F09, F04). The rate-limit table stores hashes of addresses, not addresses | Pass |
| VII. Money and time | F10 makes time-zone names canonical; no money change | Pass |
| VIII. Internationalisation | New codes need `en` and `he`; `test_i18n` enforces parity | Pass |
| IX. Quality gates | `mypy --strict src tests`, ruff, interrogate, import graph, `alembic check` stay required | Pass |
| X. Migrations only | 017 and 018 are new and reversible (017's data change is documented as not reversible) | Pass, one noted limit (D18) |
| XI. Documentation | ADR 007 (sign-in abuse controls) and ADR 008 (delivery after the response), amendments to ADR 005 and 006, README and `docs/hds-api.md` | Pass |

**Post-design re-check**: same result. The cross-cutting risk is F04 and F06 together touching the
sign-in and request lifecycle; they are sequenced after the schema work and each ships with the
existing equal-work and timing tests still green.

## Phases

Order: security fixes that need no schema, then the migrations, then the work that depends on them,
then the smaller fixes, then the older gaps. Every phase ends with all gates green and a commit; a
phase does not start before the previous checkpoint passes. Item ids are from `pr4-fix-list.md`.

| Phase | Items | Why here | Migration |
| --- | --- | --- | --- |
| **P1 Credentials and sessions** | F01, F02, F03, F08, F09, F11 | Highest-risk account-takeover and session bugs; none needs schema | none |
| **P2 Data migrations** | F07 (017), rate-limit storage (018) | Schema before the code that uses it; 017 corrects stored data | 017, 018 |
| **P3 Sign-in under load and abuse** | F04 (executor bound, rate limit FR-221), F06 | Needs 018; touches the same path as the equal-work tests | uses 018 |
| **P4 Input, logging and reliability** | F05, F10, F12 | Independent and small; F10 also adds a stored-data report | none |
| **P5 Tests, tool and documents** | F13, F14, F15, F16 | No production behaviour change | none |
| **P6 Older gaps** | F17, F18, F19, F20, F21, F22, F23, F24, F25, F26 | Contract-affecting changes (F17, F23, F24, F25) last, refactors (F20 to F22) after the behaviour is pinned by tests, F26 first within the phase because it makes every later run safe to repeat | none |

### P1 Credentials and sessions

- **F01** (D12). Tests first: two reset links, complete with the newer, the older is refused
  `auth.token_used` and the password is unchanged (fails today). Change `reset_password` to cancel
  every other outstanding link of the account, and `request_password_reset` to cancel earlier unused
  reset links only. Mutation: drop either cancellation.
- **F02** (D13). Tests first: client deactivation, client delete and portal-user removal each end the
  user's refresh tokens (`account`) and cancel links; a pre-deactivation token stays refused after a
  later reactivation; reactivating a portal user of an inactive or deleted client is refused with a
  new translated code. `ClientService` gains a refresh-token repository.
- **F03** (D14, Q2). Tests first: deactivate then reactivate a pending account never leaves it
  active; the same through client deactivation. Deactivating a pending account is refused 409; client
  deactivation skips pending portal users; `deactivate_client_users` stops stamping `activated_at`.
- **F08** (D19, Q3). Tests first: an expired rotated token replayed returns `auth.token_expired` and
  leaves other sessions live; an unexpired one still ends them. Check expiry before the revoked
  state. Add expired-record cleanup (D19). FR-209 text is already amended in the spec.
- **F09** (D20). Concurrency tests first: refresh racing each revoke-all path (reset, logout
  everywhere, theft response, account change) leaves no live token. Take the user-row lock in refresh
  and in every revoke-all.
- **F11** (D12, proposed default). Test first: lock the account, complete a reset, sign in with the
  new password at once. Reset clears the failure count and the lock.

Checkpoint P1: full suite, concurrency suite three times, mutation checks for F01, F02, F03, F08, F09.

### P2 Data migrations

- **017 `pending_backfill_correction`** (F07, D18). New migration, tests first on a scratch database
  at revision 016 with: an inactive account with an unused invitation and a used reset link (must
  end deactivated), one with a refresh-token history (deactivated), a genuine pending invitee
  (stays pending), and an undecidable row (reported, not guessed). Also shown on realistic data,
  upgrade and downgrade, as for 014 to 016.
- **018 `login_attempts`** (rate limit, D16). New table and index; tests for upgrade, downgrade and
  the retention rule. `alembic check` must agree with the models.

Checkpoint P2: `alembic check`, migration tests, round trip 013 to head and back.

### P3 Sign-in under load and abuse

- **F04 hashing** (D15). Test first: during a burst of unknown-address sign-ins another request is
  answered promptly (the loop is not blocked); the number of concurrent hashes is bounded. Async
  wrappers over `hash_password`, `verify_password` and `verify_against_dummy` run in a thread behind
  a semaphore; callers in four services await them. The work-counting tests (SC-207) still pass.
- **F04 rate limit, FR-221** (D16). Tests first: N failures for one address (known or
  unknown, any letter case) give 429 with `Retry-After`, identical for both, and no password
  verification happens over the limit; per-network-address limit likewise; a success is not
  limited; old rows are purged. New settings with defaults, new code `auth.rate_limited` (en, he),
  429 declared on login, `docs/hds-api.md` corrected (D4).
- **F06** (D17). Test first: with a deliberately slow sender, `POST /auth/password-reset/request`
  returns before delivery starts, and a registered and an unregistered address take comparable time
  (opt-in timing test extended). After-commit actions run as a background task after the response.

Checkpoint P3: SC-207 unit and opt-in timing test, new load test, contract suite and the Schemathesis
CLI script (which must tolerate 429 on login; the script's login exclusion already covers it).

### P4 Input, logging and reliability

- **F05** (D22). Test first with values containing `"`, `'` and `\`; idempotence. Pattern change in
  `core/logging.py`.
- **F10** (D21). Test first: `posixrules`, `Factory`, `asia/jerusalem`, `ASIA/JERUSALEM` give 422;
  valid zones pass; `check_site_timezones` reports stored values failing the new rule.
- **F12** (D23). Test first: a failing rollback leaves the original exception; the rollback error is
  logged by type only.

### P5 Tests, tool and documents

- **F13** the duplicate-address test gains `role` and asserts `uq_users_email_ci`; shown to fail
  when the index is absent. **F14** `dev_issue_link` issues invitations only for pending accounts;
  fixture made truly pending. **F15** scenario renumbered. **F16** quickstart row names
  `ENVIRONMENT=production`.

### P6 Older gaps

Order inside the phase: **F26**, then **F18**, **F19**, **F25**, then the contract-affecting
**F17**, **F23** (D5), **F24** (D6), then the refactors **F20**, **F21**, **F22**.

- **F26** (D24). Each run creates and drops its own database; a leftover from a killed run is
  recognised by name and removed on the next start. Proves itself by running two runs at once.
- **F17** (D25). Explicit `null` clears optional fields; omitted fields are left alone; `null` for a
  required field is 422. Tests per endpoint and field.
- **F18, F19, F23, F24, F25**: small service changes, each with its failing test; D5 and D6 are the
  proposed defaults above; F25 reuses the savepoint insert already used for invitations.
- **F20 to F22**: pure refactors with the behaviour already pinned; OpenAPI must not change (the
  shared `UserRead` keeps its component name).

Checkpoint P6 is the merge gate: every gate, the full suite on PostgreSQL, concurrency three times,
the contract script, the opt-in timing test, the OpenAPI comparison.

## Project Structure (additions)

```text
apps/api/
  alembic/versions/
    017_pending_backfill_correction.py   # F07
    018_login_attempts.py                # rate limit
  src/hds/
    core/passwords.py        # async hashing behind a bound (F04); security.py keeps the sync core
    core/rate_limit.py       # window check and recording (FR-221)
    models/login_attempt.py
    repositories/login_attempt.py
    scripts/purge_expired_records.py     # expired credentials and old attempts (F08, FR-221)
  tests/
    concurrency/test_revoke_all_races.py        # F09
    integration/test_rate_limit.py, test_event_loop_not_blocked.py, test_migration_017.py,
    integration/test_migration_018.py, test_reset_cancels_links.py, test_client_sessions.py
    unit/test_log_redaction_quotes.py, test_rollback_failure.py, test_site_timezone_canonical.py
docs/adr/007-sign-in-abuse-controls.md, 008-delivery-after-the-response.md
```

## Complexity Tracking

| Item | Why it is bigger than a fix | Simpler option rejected |
| --- | --- | --- |
| Rate limit in the database (FR-221) | Chosen at clarification: shared by workers, survives restart, counts unknown addresses | In-memory per worker; per-network only |
| Post-response delivery (F06) | A request-lifecycle change that ADR 005 described the other way | A delay or a fake send on the unknown path |
| Per-run test databases (F26) | The only way two runs can coexist | A lock that refuses the second run |
