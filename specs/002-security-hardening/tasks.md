---

description: "Task list for 002-security-hardening"
---

# Tasks: Security Hardening of Sign-In, Tokens and Account Rules

**Input**: Design documents from `specs/002-security-hardening/`

**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md),
[data-model.md](data-model.md), [contracts/](contracts/), [quickstart.md](quickstart.md)

**Tests**: Required. FR-219 demands a test that fails before each fix, and Constitution IV
requires tests first. Within every story the test tasks come first; write them, watch them fail,
then implement.

**Organization**: Grouped by user story. Task ids start at T201 so they cannot collide with
feature 001's T001 to T147 (the same reason requirement ids use the 2xx block).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel (different files, no dependency on an unfinished task)
- **[Story]**: US1 to US9, matching [spec.md](spec.md)
- All paths are relative to the repository root. `api/` below means `apps/api/`.

## Rules that apply to every task

- Commit after each phase on branch `002-security-hardening` (Conventional Commits).
- Every new or changed public function, class and module gets a docstring that states rules, error
  codes and side effects (Constitution XI); every new error code gets `en` and `he` text.
- Never edit an applied migration; new migrations are 014, 015 and 016 only (Constitution X).
- Each phase ends green: `uv run ruff check src tests alembic`, `ruff format --check`,
  `interrogate src`, `mypy --strict src/`, `python -m hds.tools.check_imports`, `alembic check`,
  the OpenAPI drift check, and the test suite on PostgreSQL (commands in
  [quickstart.md](quickstart.md)).

---

## Phase 1: Setup

**Purpose**: dependencies and test scaffolding the later phases need.

- [X] T201 Add `tzdata` to `dependencies` in `apps/api/pyproject.toml` and refresh `apps/api/uv.lock` with `uv lock` (zone data fallback, research D11), and raise the lower bound of `fastapi` from `>=0.115` to `>=0.121.0`, the first release whose `Depends` accepts `scope` (verified by installing 0.120.4, which lacks it, and 0.121.0, which has it, and inspecting the signature; research D1)
- [X] T202 [P] Create `apps/api/tests/concurrency/__init__.py` and `apps/api/tests/concurrency/conftest.py` with a helper that opens two independent sessions on the test database and starts two coroutines at the same moment (use an `asyncio.Barrier`), returning both outcomes
- [X] T203 [P] Add the default development outbox folder `apps/api/.dev-mail/` to `.gitignore` (research D6)

---

## Phase 2: Foundational — one transaction per request (blocks every story)

**Purpose**: research D1. Move the commit out of repositories without changing behaviour. Stories 3
and 5 depend on it; Story 4 is largely proven by it.

**⚠️ CRITICAL**: no story work starts until T216 is green.

### Tests first

- [X] T204 [P] Write `apps/api/tests/unit/test_unit_of_work.py` for `unit_of_work(session)`: commits exactly once after success; rolls back and re-raises on any exception; after-commit actions run only after a successful commit, in order; they are discarded after a rollback; an action that raises is logged (no secrets) and does not fail the request
- [X] T205 [P] Write `apps/api/tests/unit/test_deferred_mailer.py` for `DeferredMailer`: `send()` queues only; nothing is delivered before commit; delivered once after commit; discarded on rollback

### Implementation

- [X] T206 Implement `unit_of_work(session)` (async context manager), `after_commit(session, action)` (queue stored in `session.info`) and the dependency `get_unit_of_work` (depends on `get_db`, yields the session inside `unit_of_work`) in `apps/api/src/hds/core/database.py`. Routers declare it as `Depends(get_unit_of_work, scope="function")` so the commit happens before the response is sent. Make T204 pass
- [X] T207 Implement `DeferredMailer` (wraps a `MailSender`, queues each send with `after_commit`) in `apps/api/src/hds/core/mail.py`. Make T205 pass
- [X] T208 Remove every `await self.session.commit()` from `apps/api/src/hds/repositories/base.py` (`_update_scoped`, `_soft_delete_scoped`, `_delete_scoped`): use `flush()` and `refresh()` instead. Add a documented `commit()` method on `BaseRepository` whose docstring says it is allowed only for effects that must survive a refusal (failed-sign-in count, refresh-reuse response)
- [X] T209 [P] Remove the commits from `apps/api/src/hds/repositories/user.py`, `refresh_token.py` and `invitation_token.py` (use `flush()`), keeping behaviour identical
- [X] T210 [P] Remove the commits from `apps/api/src/hds/repositories/client.py`, `group.py`, `qualification.py`, `project.py` and `site.py` (use `flush()`)
- [X] T211 Switch `apps/api/src/hds/core/deps.py` (`get_current_user`) and `apps/api/src/hds/routers/auth.py`, `me.py`, `invitations.py` from `Depends(get_db)` to `Depends(get_unit_of_work, scope="function")`
- [X] T212 [P] Same switch in `apps/api/src/hds/routers/clients.py`, `groups.py`, `qualifications.py`
- [X] T213 [P] Same switch in `apps/api/src/hds/routers/employees.py`, `sites.py`, `projects.py`. After T211 to T213 no router imports `get_db` directly
- [X] T214 [P] In `.github/workflows/ci.yml` add `apps/api/tests/concurrency/` to the `uv run pytest` command of the `test-integration` job (the PostgreSQL 16 job) and add `ENVIRONMENT: test` to that job's `env`, so concurrency tests run on every pull request (Constitution IV and IX; FR-219). Add the path next to the existing paths, which collect tests, because pytest exits with code 5 when it is given only an empty folder
- [X] T215 Audit `apps/api/src/hds/services/*.py` for code that relied on an intermediate commit (for example reading back a row written earlier in the same call) and fix with `flush()`; write one line per change in the Phase 2 commit message, or the single line "no such code found"
- [X] T216 **Checkpoint A**: run the unchanged existing suite in `apps/api/tests/` (unit, integration, contract, performance) on PostgreSQL plus the OpenAPI drift check. All 422 tests must pass with no test edits. Fix regressions before continuing

**Checkpoint**: foundation ready. No behaviour has changed yet.

---

## Phase 3: User Story 1 — Secrets and one-time links never leak (Priority: P1) 🎯 MVP

**Goal**: unsafe configurations refuse to start; links and tokens are never stored in clear
(FR-201 to FR-204).

**Independent Test**: start the settings in each unsafe configuration; send an invitation and a
reset with log capture and the outbox backend ([quickstart.md](quickstart.md), Story 1 rows).

### Tests for User Story 1 (write first, they must fail)

- [X] T217 [P] [US1] Write `apps/api/tests/unit/test_settings_environment.py`: `ENVIRONMENT` unset fails; `development` and `test` accept the placeholder secret; `production`, `staging` and `qa` reject the placeholder and a secret shorter than 32 bytes; a non-development environment without a real `MAIL_BACKEND` fails; the error text names the setting and never contains the secret value
- [X] T218 [P] [US1] Write `apps/api/tests/unit/test_logging_redaction.py`: secrets in `%`-style format arguments (tuple and dict) are redacted; email addresses are masked as `a***@domain`; no log record is produced with a message body
- [X] T219 [P] [US1] Write `apps/api/tests/unit/test_mail_outbox.py`: an outbox line holds the masked recipient, subject, purpose, expiry and the body with the token replaced by `[REDACTED]`; the file never contains the raw token; the sender refuses to be built outside `development` and `test`
- [X] T220 [P] [US1] Write `apps/api/tests/integration/test_no_token_in_logs.py` (SC-202): run invite, accept, reset request and reset confirm with `caplog` at DEBUG and the outbox backend; assert no log record contains a token or an unmasked address
- [X] T221 [P] [US1] Write `apps/api/tests/unit/test_dev_issue_link.py`: the script refuses to run when `ENVIRONMENT=production`; in development it prints a working link once and writes it to no file and no log

### Implementation for User Story 1

> **Order matters**: do the environment updates first (conftest, startup test, CI), because once `ENVIRONMENT` is required every test fails until they are in place. Then do the config change.

- [X] T222 [US1] In `apps/api/tests/conftest.py` set `ENVIRONMENT=test` and a test `JWT_SECRET_KEY` with `os.environ.setdefault` before any `hds` import, and make the shared `OutboxMailSender` accept the new `secret` keyword
- [X] T223 [US1] Update `apps/api/tests/unit/test_startup_and_cors.py` for the required `ENVIRONMENT` (build `Settings` explicitly in each test)
- [X] T224 [P] [US1] In `.github/workflows/ci.yml` set `ENVIRONMENT: test` and a non-secret test `JWT_SECRET_KEY` on the jobs that import the application and are not handled by T214: `test-unit`, `migration-check` and `contract` (`test-integration` already has it from T214)
- [X] T225 [US1] In `apps/api/src/hds/core/config.py` make `ENVIRONMENT` required with no default; define development environments as exactly `development` and `test`; enforce start-up rules 1 to 3 of [contracts/configuration.md](contracts/configuration.md) (secret not the placeholder and at least 32 bytes; real `MAIL_BACKEND`); add settings `MAIL_BACKEND` (`outbox`) and `MAIL_OUTBOX_PATH`. Error messages name the setting, never the value. Make T217 pass
- [X] T226 [P] [US1] In `apps/api/src/hds/core/logging.py` make `SensitiveDataFilter` redact string values in `record.args` (tuple and dict) and mask email addresses. Make T218 pass
- [X] T227 [US1] In `apps/api/src/hds/core/mail.py` replace `LoggingMailSender` with `RedactingOutboxMailSender`: add an optional keyword `secret: str | None = None` to `MailSender.send`; the sender replaces every occurrence of `secret` in the body with `[REDACTED]` before writing the outbox line; it logs only the masked recipient and the subject. `get_mail_sender()` picks the backend from settings. Make T219 pass
- [X] T228 [US1] In `apps/api/src/hds/services/invitation.py` pass the raw token as `secret=` in every `self._mailer.send(...)` call (`create_invitation`, `request_password_reset`) so the sender can redact it, and extend `apps/api/tests/unit/test_invitation_service.py` to assert that `secret` equals the emailed token. This lets the Story 1 outbox and log checks pass without waiting for Story 4
- [X] T229 [P] [US1] Create `apps/api/src/hds/scripts/dev_issue_link.py` (`python -m hds.scripts.dev_issue_link <email> <invite|reset>`): refuses outside a development environment; issues a fresh link for the account and prints it once to standard output; stores it nowhere. Make T221 pass
- [X] T230 [P] [US1] In `infra/docker-compose.yml` set `ENVIRONMENT: development` explicitly and replace the signing-secret default with `${JWT_SECRET_KEY:?set JWT_SECRET_KEY}` (no default)
- [X] T231 [P] [US1] In the root `.env.example` set `ENVIRONMENT=development` and leave `JWT_SECRET_KEY=` empty with a comment `generate one with: openssl rand -hex 32` (FR-204)
- [X] T232 [US1] Update the configuration tables and quick start in `apps/api/README.md`, `infra/README.md` and the root `README.md` (set `ENVIRONMENT`, generate the secret, redacted outbox, `dev_issue_link`) in the same change, and list the new scripts in the layout section of `apps/api/README.md`

**Checkpoint**: Story 1 passes on its own. MVP of the security fixes.

### Scope additions recorded during implementation (Phase 3)

Work done in Phase 3 that goes beyond the task text above. Each was needed to make the story's
requirements true; none changes an endpoint or the OpenAPI document.

- **T227, mail interface**: `MailSender.send` gained `purpose` and `expires_at` as well as the
  `secret` keyword named in the task, because the outbox record must hold the purpose and expiry
  ([contracts/configuration.md](contracts/configuration.md)). `DeferredMailer` forwards all three,
  and T228 passes all three from the invitation service. The test double in `tests/conftest.py`
  accepts them.
- **T227, removal**: `LoggingMailSender` is deleted, not kept as a fallback. It logged the whole
  message body, and nothing else referenced it.
- **T225, settings**: besides the start-up rules, the settings now hide the raw input in
  validation errors (a refused start used to print the secret), ignore unknown keys (the shared
  example file holds `POSTGRES_*` and `TEST_DATABASE_URL`, which made copying it fail), and refuse an
  empty or blank secret in every environment. The constant `PLACEHOLDER_JWT_SECRET` is renamed
  `PUBLISHED_PLACEHOLDER`, because the secret-detection hook flagged the old name; it is the public
  value the settings refuse, not a secret.
- **T226, logging**: redaction runs on the final text of a record, and the JSON formatter also
  redacts exception text; `mask_email` and `redact` are public helpers in `hds.core.logging`.
- **T229, constants**: `INVITE_TTL_DAYS` and `RESET_TTL_MINUTES` in `services/invitation.py` are
  public so the development link script uses the same lifetimes.
- **T230 and T231, tests**: `tests/unit/test_deployment_files.py` reads the compose and example
  files and checks that using them unchanged does not start.
- **Harness**: the pre-bash hook no longer blocks reading `.env.example` or `.env.sample`
  (`chore(harness): allow reading env example files`), with `tests/unit/test_harness_env_pattern.py`.

---

## Phase 4: User Story 2 — Invitations cannot bring back a deactivated account (Priority: P1)

**Goal**: an invitation completes only a pending account; links are cancelled when account state
changes (FR-205 to FR-207).

**Independent Test**: deactivate an account that holds an unused link, redeem it, confirm nothing
changes.

### Tests for User Story 2 (write first)

- [X] T233 [P] [US2] Extend `apps/api/tests/unit/test_invitation_service.py`: inviting a deactivated address raises `ConflictError` with code `invitations.account_deactivated`; re-inviting a pending account sets the new role and replaces the earlier link; a Manager re-inviting a pending account as Admin is forbidden and the role is unchanged; accepting for an account that is not pending raises `auth.token_used`; accepting cancels the account's other links; a reset link is redeemable only for an active account
- [X] T234 [P] [US2] Extend `apps/api/tests/unit/test_employee_manager.py` and `apps/api/tests/unit/test_client_service.py`: `deactivate` and `soft_delete` cancel the user's outstanding links; deactivating or deleting a client cancels the links of its portal users
- [X] T235 [P] [US2] Write `apps/api/tests/integration/test_invitation_activation.py` on PostgreSQL covering the review's paths: redeem a leftover link for a deactivated account; a Manager invites a deactivated Admin's email; a portal user of a deactivated client redeems a link; a leftover link for an active user must not change the password or end sessions (SC-204)
- [X] T236 [P] [US2] Write `apps/api/tests/integration/test_migration_014.py`: build rows at revision 013 (active, deactivated after use, pending with an unused invite link), upgrade to 014, assert `activated_at` is set for all but the pending one; the check constraint `ck_users_active_has_activated_at` rejects an active user with no `activated_at`; downgrade drops the constraint and the column
- [X] T237 [P] [US2] Write `apps/api/tests/concurrency/test_concurrent_invite.py`: 50 rounds of two staff members inviting the same new address at the same moment; exactly one pending account exists and both calls succeed (spec Story 2 scenario 7)
- [X] T238 [US2] Add tests that a pending account cannot be reactivated (not parallel: it edits the same unit-test file as T234): extend `apps/api/tests/unit/test_employee_manager.py` (reactivating an account whose `activated_at` is null raises `ConflictError` with code `users.pending_cannot_reactivate` and changes nothing; reactivating a deactivated account still works) and write `apps/api/tests/integration/test_reactivate_pending.py` (`POST /api/v1/employees/{id}/reactivate` for an invited, never-activated account returns 409 with that code, not the generic 422 `validation.constraint_violation`; the account stays pending and its invitation link still works)

### Implementation for User Story 2

- [X] T239 [US2] In `apps/api/src/hds/models/user.py` add `activated_at` with this constraint verbatim: `timestamp with time zone, null`; set when an invitation is accepted and when staff create an account that is active from the start; null means the account has never been active; also declare `CheckConstraint("is_active = false OR activated_at IS NOT NULL", name="ck_users_active_has_activated_at")` on the model
- [X] T240 [US2] Create `apps/api/alembic/versions/014_user_activation.py`: add `users.activated_at`; backfill `activated_at = created_at` for every user except those that are inactive, hold an invitation link and never used one; then add `ck_users_active_has_activated_at`, `CHECK (is_active = false OR activated_at IS NOT NULL)`, after the backfill; downgrade drops the constraint and then the column; docstring says what changes and why. `alembic check` must pass
- [X] T241 [US2] In `apps/api/src/hds/scripts/seed_dev.py` set `activated_at` to the current time in the defaults of every seeded active user, so `ck_users_active_has_activated_at` holds and running the seed twice stays idempotent
- [X] T242 [P] [US2] Set `activated_at` to the current time on every `User(...)` that the tests build directly, so the new check constraint holds: `apps/api/tests/conftest.py`, `apps/api/tests/integration/conftest.py`, `apps/api/tests/integration/test_email_uniqueness.py`, `apps/api/tests/integration/test_employee_filters.py`, `apps/api/tests/performance/test_smoke_performance.py`; run each file's tests to confirm
- [X] T243 [US2] In `apps/api/src/hds/repositories/user.py` make `create` set `activated_at` when `is_active` is true and add `mark_activated(user_id, org_id)`; in `apps/api/src/hds/repositories/invitation_token.py` add `cancel_for_user(user_id)` and `cancel_for_users(user_ids)` that mark outstanding links used
- [X] T244 [US2] Implement the pending-account rules in `apps/api/src/hds/services/invitation.py` per [data-model.md](data-model.md) (account states): create, replace-link and role update for pending, duplicate for active, `invitations.account_deactivated` for deactivated; accept only for pending, reset only for active; cancel other links on accept. Run the insert of a new pending account inside a savepoint (`begin_nested()`); on a unique violation roll back the savepoint, reload the account by address and continue with the existing-account rules, so a concurrent invitation succeeds (spec Story 2 scenario 7). Update the method docstrings (rules, error codes, side effects)
- [X] T245 [US2] Give `EmployeeService` and `ClientService` an `InvitationTokenRepository` and cancel links in `deactivate`, `soft_delete`, `edit_client` (deactivation) and client `soft_delete`; also make `deactivate` and the bulk deactivation of client users set `activated_at = COALESCE(activated_at, now)` in the same update; wire the new constructor argument in `apps/api/src/hds/routers/employees.py`, `apps/api/src/hds/routers/clients.py` and `apps/api/src/hds/routers/me.py`
- [X] T246 [US2] In `apps/api/src/hds/services/employee.py` make `reactivate` refuse an account whose `activated_at` is null: raise `ConflictError(code="users.pending_cannot_reactivate", ...)` before any write (FR-220); update its docstring and the `description` of `POST /api/v1/employees/{id}/reactivate` in `apps/api/src/hds/routers/employees.py`
- [X] T247 [P] [US2] Update the existing constructor calls of `EmployeeService` and `ClientService` to pass a fake `InvitationTokenRepository`: `apps/api/tests/unit/test_employee_manager.py`, `apps/api/tests/unit/test_employee_self_service.py` (6 calls) and `apps/api/tests/unit/test_client_service.py`
- [X] T248 [P] [US2] Add `invitations.account_deactivated` and `users.pending_cannot_reactivate` to `apps/api/src/hds/locales/en.json` and `he.json` (English and Hebrew text for each)
- [X] T249 [US2] Update the `description` text of `POST /auth/invite`, `/auth/invite/accept` and `/auth/password-reset/confirm` in `apps/api/src/hds/routers/invitations.py` and the matching section of `docs/hds-api.md`

**Checkpoint**: Stories 1 and 2 pass independently.

### Scope additions recorded during implementation (Phase 4)

Work done in Phase 4 that differs from, or goes beyond, the task text above.

- **T240, migration pre-check**: the migration has a public function `ensure_no_rejected_rows`
  that runs between the backfill and the constraint. It lists the count and the first 20 account
  ids (identifiers only, no addresses) and stops, and because PostgreSQL DDL is transactional
  nothing is left half-applied. `tests/integration/test_migration_014.py` runs the migration on
  its own scratch database (`hds_migration_014_test`, created and dropped by the fixture) seeded at
  revision 013 with active, deactivated, re-invited and pending accounts.
- **T242, one more fixture**: `tests/unit/test_dev_issue_link.py` also builds `User` rows and
  needed `activated_at` (the SQLite database enforces the check constraint too).
- **T243, `mark_activated` dropped**: it is not needed, because accepting an invitation passes
  `activated_at` through the existing `update`. Instead the repository gained `create_if_absent`
  (the savepoint insert that T244 requires) and a shared `_with_activation` helper used by `create`
  and `create_if_absent`. `create_if_absent` replaces `create` on the invitation path, so the
  existing invitation unit tests were updated to mock it.
- **T244, two guards the task text did not name**: an address that belongs to *another
  organisation* is a duplicate (409) and is left untouched, because email is unique across the
  deployment but one organisation must not edit another's accounts (NFR-001); and a Manager cannot
  change the role of a *pending Admin* (only an Admin may change an Admin, FR-011a). Both are
  covered by tests.
- **T245, client portal users**: `ClientService` gained a private helper that lists a client's
  portal users, cancels their links and deactivates them, used by both client deactivation and
  deletion; `ClientRepository.deactivate_client_users` keeps `activated_at` or fills it with
  `COALESCE`. A mutation check showed the integration test fails without it.
- **T249, `docs/hds-api.md`**: that file is a product-level overview with no per-endpoint section,
  so a row for the invitation endpoints and one convention line about the account states were
  added, both marked *Proposed*.

---

---

## Phase 5: User Story 3 — Sessions and one-time links are single-use (Priority: P1)

**Goal**: at most one success per credential or link, even under simultaneous use; replay after
the grace window ends all sessions (FR-208, FR-209).

**Independent Test**: two simultaneous refreshes, 100 rounds, exactly one success each; a replay
outside the window revokes every token of the user.

### Tests for User Story 3 (write first)

- [X] T250 [P] [US3] Write `apps/api/tests/concurrency/test_refresh_single_use.py`: 100 rounds of two simultaneous refreshes with one token; exactly one succeeds each round and the loser does not revoke other tokens (SC-203)
- [X] T251 [P] [US3] Write `apps/api/tests/concurrency/test_link_single_use.py`: 100 rounds of two simultaneous invitation acceptances and two simultaneous reset confirmations; exactly one succeeds each round
- [X] T252 [P] [US3] Write `apps/api/tests/integration/test_refresh_reuse.py`: reuse after the grace window revokes all of the user's tokens; reuse within the window is refused and keeps sessions (set `REFRESH_REUSE_GRACE_SECONDS` to 1 and wait); reuse of a token revoked by logout is refused with no revoke-all; a legacy token with no reason is refused with no revoke-all; a token revoked with reason `account` (user deactivated) is refused with no other effect
- [X] T253 [P] [US3] Extend `apps/api/tests/unit/test_auth_service.py` with the decision table of research D3 using fake repositories
- [X] T254 [P] [US3] Write `apps/api/tests/integration/test_migration_015.py`: the check constraint accepts `rotated`, `logout`, `all`, `account` and null and rejects any other value; downgrade drops the column

### Implementation for User Story 3

- [X] T255 [US3] In `apps/api/src/hds/models/refresh_token.py` add `revoked_reason` with this constraint verbatim: `short text, null. One of rotated, logout, all, account. Null for tokens revoked before this change`, plus a check constraint for those values
- [X] T256 [US3] Create `apps/api/alembic/versions/015_refresh_revoked_reason.py` (add column and check constraint; reversible; docstring says what and why)
- [X] T257 [US3] Add `REFRESH_REUSE_GRACE_SECONDS: int = 10` to `apps/api/src/hds/core/config.py` (must be positive)
- [X] T258 [US3] In `apps/api/src/hds/repositories/refresh_token.py` add `revoke_if_live(token_id, reason) -> bool` implemented as `UPDATE ... WHERE id = :id AND revoked_at IS NULL` returning whether a row changed; give `revoke_all_for_user` a `reason` argument; replace the unguarded `revoke`
- [X] T259 [P] [US3] In `apps/api/src/hds/repositories/invitation_token.py` replace `mark_used` with `spend(id) -> bool` implemented as `UPDATE ... SET used_at = now WHERE id = :id AND used_at IS NULL` returning whether a row changed
- [X] T260 [US3] Rewrite `AuthService.refresh` in `apps/api/src/hds/services/auth.py` per research D3: guarded rotation; on a lost race re-read and decide (rotated and outside the window: `revoke_all_for_user(reason="all")`, call the repository's explicit `commit()`, then refuse; otherwise refuse with no effect). `logout` uses reason `logout`, `logout_all` uses `all`. Update the docstrings
- [X] T261 [US3] Make `apps/api/src/hds/services/invitation.py` redeem links through `spend()` (a false result means refused as used) and make `apps/api/src/hds/services/employee.py` pass reason `account` when revoking tokens on deactivate, delete and role change; reset passes `all`
- [X] T262 [US3] Cross-organisation sweep (beyond the ten findings; feature 001 NFR-001 and FR-012): write `apps/api/tests/integration/test_cross_organisation_sweep.py`. It holds an inventory of every operation that takes an id or an email from the request (path, query, body), generated from the application's OpenAPI document and checked against it, so a new endpoint cannot be forgotten: each operation is either a sweep case or an exempt entry with a stated reason (public operations that authenticate by credential or token). For each sweep case the test sends another organisation's id or address as an authorised caller and asserts the refusal (404 for an id, so it is indistinguishable from a missing one; 409 `users.duplicate_email` for an address) and that nothing changed, by comparing a digest of every table before and after. Also record in `spec.md` Assumptions that a 409 for an address held by another organisation reveals that the address exists in the deployment, which email uniqueness across the deployment makes unavoidable
- [X] T263 [US3] Fix every gap the sweep finds, test first: for each failing case, show it failing for the right reason, fix the service or repository, and list the cases and fixes in the commit message (write "none found" if there are none). A refused request must change nothing in any organisation
- [X] T264 [US3] Write `docs/adr/006-refresh-token-reuse-detection.md` (context, decision, grace window, consequences) and add it to the ADR table in `docs/README.md`

### Scope additions recorded during implementation (Phase 5)

Work done in Phase 5 that differs from, or goes beyond, the task text above.

- **Redeeming links, spend first**: accepting an invitation and confirming a reset call `spend()`
  before any check of the owner's state, so the single-use decision is made by the database alone.
  A mutation that removed the `used_at IS NULL` condition was masked by the pending-account check
  for accept-invitation, so a second mutation (M2b) removed both and was caught.
- **`RevocationReason`**: a `str` enum in `models/refresh_token.py` (`rotated`, `logout`, `all`,
  `account`) instead of bare strings, so a caller cannot pass a reason the check constraint rejects.
- **`tests/integration/migration_support.py`**: the scratch-database helper that migration 014's
  test built is now shared with the migration 015 test (create, upgrade to a revision, drop).
- **Throwaway checks**: the 100/100 double wins before the fix and the mutation runs are scripts
  outside the repository; their results are in the commit message of the single-use change.
- **T262, sweep result**: the sweep found two gaps (`GET /employees?group_id=` and
  `?qualification_id=` answered 200 with an empty list for another organisation's id and for an
  id that does not exist). Case variants of an address are not in the sweep; they belong to US8
  (T283), because case-insensitive addresses do not exist until then.
- **T263, fix**: `EmployeeService` now takes the group and qualification repositories and refuses
  an unknown or foreign filter id with 404. The two feature 001 tests that asserted the old empty
  answer (`test_employee_filters.py`) now assert 404, as NFR-001 requires.
- **T264, numbering**: ADR 006 was written before ADR 005 because the task order fixes the names;
  ADR 005 followed in Phase 6 (T269).

**Checkpoint**: all three P1 stories pass. Highest-risk security work is done.

---

## Phase 6: User Story 4 — A change either fully happens or does not happen (Priority: P2)

**Goal**: multi-step actions are all-or-nothing and email is sent only after commit (FR-210).

**Independent Test**: force a failure in the last step of each action and confirm no earlier step
took effect.

### Tests for User Story 4 (write first)

- [X] T265 [P] [US4] Write `apps/api/tests/integration/test_atomicity.py` on PostgreSQL with failure injection in the last step: refresh (old token still valid, no new token), password reset (old password, link unspent, sessions intact), client update with portal-user deactivation (users unchanged), role change with token revocation (role and tokens unchanged)
- [X] T266 [P] [US4] Write `apps/api/tests/integration/test_mail_after_commit.py`: an invitation whose transaction fails sends no email and leaves no usable link; a successful one sends exactly one email, after the commit

### Implementation for User Story 4

- [X] T267 [US4] Make `InvitationService` queue every email through `DeferredMailer`: build `DeferredMailer(db, mailer)` in `apps/api/src/hds/routers/invitations.py`; update `apps/api/src/hds/services/invitation.py` docstrings (email is sent after commit)
- [X] T268 [US4] Review the order of steps in `apps/api/src/hds/services/auth.py` (`refresh`), `apps/api/src/hds/services/invitation.py` (`reset_password`) and `apps/api/src/hds/services/client.py` (`edit_client`) so each is a single unit of work with no step that needs an earlier commit; fix any that do
- [X] T269 [US4] Write `docs/adr/005-request-scoped-transactions.md` (decision D1, the explicit-commit exception, `scope="function"`, alternatives) and add it to the ADR table in `docs/README.md`

### Scope additions recorded during implementation (Phase 6)

- **T268, review result**: no step needed an earlier commit, so no code changed. `refresh`,
  `reset_password` and `edit_client` (and the role change in `employee.py`) are each one unit of
  work; the only explicit commits left in `src/` are the documented ones (failed-sign-in count and
  lock in `repositories/user.py`, the theft response in `services/auth.py`).
- **T265, failing-first by mutation**: the atomicity tests already pass after Phase 2, because the
  unit of work gives atomicity. Their evidence is a mutation check: an early `commit()` placed
  after the first step of refresh, reset, client change and role change makes the matching test
  fail for the right reason (four mutations, all caught, files restored).
- **T266/T267**: the three mail tests failed before the change (mail sent with 0 links visible to
  another connection; mail sent for a change whose commit failed). Making the router's
  `DeferredMailer` a plain mailer again is caught by all three (mutation M9).

**Checkpoint**: Story 4 passes; D1 is documented.

---

## Phase 7: User Story 5 — The last Admin survives simultaneous actions (Priority: P2)

**Goal**: at least one active Admin always remains (FR-211).

**Independent Test**: two Admins deactivate each other at once, 100 rounds, an active Admin remains
each time.

- [X] T270 [P] [US5] Write `apps/api/tests/concurrency/test_last_admin.py`: 100 rounds of two simultaneous deactivations between the only two Admins, plus simultaneous demotions; exactly one action succeeds and an active Admin remains (SC-205)
- [X] T271 [P] [US5] Extend `apps/api/tests/unit/test_employee_manager.py`: the last-Admin guard counts the ids returned by the locking query and raises `users.last_admin` at one
- [X] T272 [US5] In `apps/api/src/hds/repositories/user.py` add `lock_active_admins(org_id) -> list[UUID]`: `SELECT id ... WHERE organization_id = :org AND role = 'admin' AND is_active AND NOT is_deleted ORDER BY id FOR UPDATE`; document that the lock lasts until the unit of work ends
- [X] T273 [US5] Make `_guard_last_admin` in `apps/api/src/hds/services/employee.py` use `lock_active_admins` instead of `count_active_admins`; remove the unused count method if nothing else calls it

### Scope additions recorded during implementation (Phase 7)

- **T270, evidence**: before the change both tests failed in round 0 with `[200, 200]` (both
  Admins removed). After it, 100 rounds of each pass. Mutation M10 (remove `FOR UPDATE`) and M11
  (guard no longer uses the locked ids) are both caught.
- **T271**: the five existing guard tests in `test_employee_manager.py` now supply a list of ids
  from `lock_active_admins` instead of a count; one id raises `users.last_admin`, two pass.

**Checkpoint**: Story 5 passes.

---

## Phase 8: User Story 6 — Sign-in lockout matches the rule (Priority: P2)

**Goal**: lock after exactly N failures; the count restarts when the lock expires (FR-212, FR-213).

**Independent Test**: N-1 failures then success; N failures then locked; after expiry N attempts.

- [X] T274 [P] [US6] Write `apps/api/tests/integration/test_lockout.py` on PostgreSQL for limits 2 to 10: N-1 failures then the correct password signs in; N failures then the next attempt returns `auth.account_locked`; after the lock is expired by setting `locked_until` in the past, N new attempts are allowed and the account locks only after the N-th (SC-206)
- [X] T275 [P] [US6] Extend `apps/api/tests/unit/test_auth_service.py`: the service uses the count returned by the repository and does not add one itself; an expired lock is cleared before the password check
- [X] T276 [US6] In `apps/api/src/hds/repositories/user.py` add `register_failed_login(user_id, max_failures, lock_until) -> tuple[int, datetime | None]` as one `UPDATE ... SET failed_login_attempts = failed_login_attempts + 1, locked_until = CASE WHEN failed_login_attempts + 1 >= :max THEN :until ELSE locked_until END ... RETURNING` with session synchronisation off, followed by the explicit `commit()`; add `clear_expired_lock(user_id, now)` as `UPDATE ... SET failed_login_attempts = 0, locked_until = NULL WHERE id = :id AND locked_until IS NOT NULL AND locked_until <= :now`. Remove `increment_failed_attempts` and `set_locked_until`
- [X] T277 [US6] Rewrite the failure path of `AuthService.login` in `apps/api/src/hds/services/auth.py`: clear an expired lock first, then check the lock, then verify; use `register_failed_login` and its returned count; delete the `user.failed_login_attempts + 1` line. Update the docstring (lock after N, restart on expiry)

### Scope additions recorded during implementation (Phase 8)

- **T274, evidence**: before the change 18 of the 27 lockout cases failed. For every limit from 2
  to 10, N-1 failures already answered 423 (locked one failure early), and the post-expiry case
  kept the old count. After the change all 27 pass. Mutations M17 (lock at count+1), M18 (expired
  lock never cleared), M19 (clear keeps the count) and M20 (count not committed before the
  refusal) are all caught.
- **T277**: the service no longer reads the count back: the single statement in
  `register_failed_login` decides, and the returned `(failures, locked_until)` is available to
  callers but unused by the service. The address is matched through `get_by_email`, which is
  case-insensitive since Phase 10, so the lock follows the account whatever case is typed.
- **Unknown addresses**: they cannot be locked (there is no account row to hold a count), so the
  locked-account answer (423) remains the one accepted difference (feature 001 FR-005, spec
  Story 7). Counting failures per unknown address would need a new table and is not in the spec.
- **Contract**: no status code, parameter or schema changed; the login description now states the
  exact-N rule, the restart on expiry and case-insensitive matching. The OpenAPI document differs
  from the pre-feature baseline only in descriptions.

**Checkpoint**: Story 6 passes.

---

## Phase 9: User Story 7 — Sign-in does not reveal which accounts exist (Priority: P2)

**Goal**: equal work for unknown and known emails (FR-214).

- [X] T278 [P] [US7] Write `apps/api/tests/unit/test_login_equal_work.py`: with a spy on the password check, an unknown email, a known email with a wrong password and an inactive account each cause exactly one check, and all return the same code and message (this is the automated check of SC-207)
- [X] T279 [P] [US7] Write `apps/api/tests/integration/test_login_timing.py` (SC-207): 500 samples each for unknown and known emails with a wrong password, compared with a statistical test; mark it `timing`, register the marker in `apps/api/pyproject.toml` and deselect it by default; it is opt-in and is run by hand in the final phase before a release
- [X] T280 [US7] In `apps/api/src/hds/core/security.py` create `DUMMY_PASSWORD_HASH` at import with the same Argon2id parameters as real hashes and a `verify_against_dummy(plain)` helper; docstring explains why
- [X] T281 [US7] In `AuthService.login` (`apps/api/src/hds/services/auth.py`) call `verify_against_dummy` when no account matches, then return the same `auth.invalid_credentials`

### Scope additions recorded during implementation (Phase 9)

- **T278, work counted, not timed**: both new tests wrap the Argon2 hasher and count verifications.
  Before the change an unknown address caused 0 (known and inactive accounts caused 1); after it
  every refusal causes exactly 1. `tests/integration/test_login_responses.py` (added to T278)
  drives the real application and compares the whole response of four refusals: status, reason
  phrase, body (without the request id) and headers (without the request id and date). The
  locked-account answer (423) is the accepted difference and is not compared.
- **T279, opt-in timing**: on a development machine without the dummy check the medians are 18 ms
  (unknown) against 55 ms (known), 3.0x, and the test fails; with it they are 54.6 ms and 57.4 ms,
  1.05x, and it passes. The test asserts the ratio (below 1.15) and prints the p-value instead of
  asserting it: with 500 samples each, a real difference of about 3 ms (a known account also
  writes and commits its failure count) is always statistically significant, so a significance
  test would fail on a difference nobody can use. The marker `timing` is registered in
  `pyproject.toml` and deselected by default (`addopts = "-m 'not timing'"`).
- **Residual difference**: that 3 ms commit for a known account cannot be removed without writing
  something for unknown addresses; it is small against the Argon2 cost and is stated here and in
  the test's docstring.
- **Contract**: only the login description changed ("after the same amount of work").

**Checkpoint**: Story 7 passes.

---

## Phase 10: User Story 8 — Email addresses ignore letter case (Priority: P2)

**Goal**: one address, one account, whatever the case (FR-215, FR-216).

- [X] T282 [P] [US8] Write `apps/api/tests/unit/test_email_type.py`: the `Email` type trims and lowercases; invalid addresses are still rejected; the generated OpenAPI schema for the field is unchanged (`format: email`)
- [X] T283 [P] [US8] Write `apps/api/tests/integration/test_email_case.py`: sign in with a different case; create a duplicate with a different case (409 `users.duplicate_email`); request a reset with a different case and receive the email; invite with a different case (SC-208); and re-add the letter-case address cases to the cross-organisation sweep in `apps/api/tests/integration/test_cross_organisation_sweep.py` (an upper-cased address held by another organisation is refused with 409 `users.duplicate_email` on `POST /employees`, `POST /auth/invite` and `POST /clients/{id}/users`, and nothing changes)
- [X] T284 [P] [US8] Write `apps/api/tests/integration/test_migration_016.py`: data with two addresses that differ only by case makes the migration stop with a message listing both; clean data is lowercased and the old index is replaced by `uq_users_email_ci`; downgrade restores `uq_users_email`
- [X] T285 [P] [US8] Write `apps/api/tests/unit/test_find_email_case_duplicates.py` for the listing script (read-only; prints nothing when clean)
- [X] T286 [US8] Add the `Email` type to `apps/api/src/hds/core/types.py`: an `EmailStr` with an after-validator that trims and lowercases, so OpenAPI is unchanged
- [X] T287 [US8] Use `Email` instead of `EmailStr` in the request models of `apps/api/src/hds/routers/auth.py`, `invitations.py`, `employees.py`, `clients.py` (user requests only; contact emails of clients are not account identifiers)
- [X] T288 [US8] In `apps/api/src/hds/repositories/user.py` lowercase the address in `get_by_email` and `create` as a second line of defence for non-HTTP callers
- [X] T289 [US8] In `apps/api/src/hds/models/user.py` replace the `uq_users_email` index with `uq_users_email_ci` with this constraint verbatim: `unique on lower(email) where is_deleted = false`; `alembic check` must agree with migration 016
- [X] T290 [US8] Create `apps/api/alembic/versions/016_case_insensitive_email.py`: first find addresses that differ only by case and, if any, raise with a message that lists them and tells staff to resolve them (nothing is merged or deactivated); otherwise lowercase all addresses, drop `uq_users_email`, create `uq_users_email_ci`; downgrade recreates `uq_users_email` and its docstring states that the lowercasing is not reversed
- [X] T291 [P] [US8] Create `apps/api/src/hds/scripts/find_email_case_duplicates.py` (read-only; lists clashing addresses and exits non-zero if any)
- [X] T292 [US8] Confirm the duplicate checks in `services/employee.py`, `services/client.py` and `services/invitation.py` now match case-insensitively through `get_by_email`; add a unit test per service if one is missing

### Scope additions recorded during implementation (Phase 10)

- **T283, sweep**: the three letter-case address cases were re-added to the cross-organisation
  sweep (`POST /employees`, `POST /auth/invite`, `POST /clients/{id}/users`); they failed before the
  change (an upper-cased foreign address was accepted) and pass now.
- **T284/T290, ids only**: the migration's stop message lists account ids and organisation ids,
  not addresses (the same choice as migration 014), and the listing script does the same. A
  deleted account is not compared. Shown on a database with realistic data: upgrade with a clash
  stops and changes nothing; after staff delete the duplicate it upgrades, a duplicate in another
  case is refused by `uq_users_email_ci`; downgrade restores `uq_users_email` and keeps the
  lowercase addresses; upgrading again works.
- **T288**: lowercasing sits in `UserRepository.get_by_email` (compared as `lower(email)`, the same
  expression as the index) and in `_with_activation`, which both `create` and `create_if_absent`
  use.
- **T285**: the listing script is split into a pure `find_clashes`, a `run(fetch)` and a thin
  database `fetch`, so the unit test needs no database.
- **`tests/integration/migration_support.py`**: gained `alembic_run`, which returns the finished
  process instead of asserting success, so a test can inspect a migration that stops on purpose.
- **ADR 002** now names `uq_users_email_ci` in its example.
- **Mutation checks** (all caught, files restored): the type no longer lowercases; `get_by_email`
  case-sensitive again; the repository stores the address as given; the migration skips the clash
  check; the new index not on `lower(email)`.

**Checkpoint**: Story 8 passes.

---

## Phase 11: User Story 9 — Site time zones are real time zones (Priority: P3)

**Goal**: only loadable IANA zone names are stored (FR-217, FR-218).

- [X] T293 [P] [US9] Extend `apps/api/tests/unit/test_site_service.py`: `Asia/Jerusalem` and `UTC` and `Asia/Kolkata` (a zone without daylight saving) accepted on create and edit; `Mars/Base`, an empty string and a name with path characters raise `DomainValidationError` with code `sites.timezone_invalid`
- [X] T294 [P] [US9] Write `apps/api/tests/integration/test_site_timezone.py`: `POST` and `PATCH /api/v1/sites` return 422 with code `sites.timezone_invalid` for invalid zones and store nothing (SC-209)
- [X] T295 [US9] In `apps/api/src/hds/services/site.py` add the zone check (`ZoneInfo(name)`; any failure raises `sites.timezone_invalid`) to `create_site` and `edit_site`; update docstrings
- [X] T296 [P] [US9] Add `sites.timezone_invalid` to `apps/api/src/hds/locales/en.json` and `he.json`
- [X] T297 [P] [US9] Create `apps/api/src/hds/scripts/check_site_timezones.py` (read-only; lists sites and organisations with an invalid zone; FR-218) with a test in `apps/api/tests/unit/test_check_site_timezones.py`

### Scope additions recorded during implementation (Phase 11)

- **T295, one function**: the check is `validate_timezone` in `services/site.py`, a real
  `ZoneInfo` load (not a pattern), also used by the report script so both apply the same rule. An
  edit that leaves the zone out is not checked, so a site that already holds a bad zone can still
  be renamed; the report script lists such sites.
- **Evidence**: 21 new cases failed before the change (the service accepted every bad name); after
  it all pass. Three mutations (create not validating, edit not validating, a pattern check
  instead of a load) are all caught.
- **T297**: the script is split into a pure `find_invalid`, `run(fetch)` and a thin database
  `fetch`; it prints site and organisation ids only. The sites table has no `is_deleted` column, so
  it lists every site.
- **Contract**: the 422 status was already declared on both operations; only the descriptions
  changed. No migration. The schemathesis contract test treats any short string as a valid time
  zone, so the two site writes join the two project writes in skipping the single check "valid
  data must be accepted" (every other check still runs on them), as for the project date order.

**Checkpoint**: all nine stories pass.

---

## Phase 12: Polish & Cross-Cutting

- [X] T298 [P] Update `docs/hds-api.md` (error table, sign-in, refresh, invitation and site sections) to match [contracts/error-codes.md](contracts/error-codes.md)
- [X] T299 [P] Update `docs/README.md` ADR table (005 and 006 present) and the ADR text if any decision changed during implementation
- [X] T300 Final docstring pass over `apps/api/src/hds/`: every changed or added public symbol states rules, error codes and side effects; `uv run interrogate src` stays at 100%; every endpoint whose behaviour changed has an updated `description`
- [X] T301 Run every gate from `apps/api/` and the full suite on PostgreSQL, including `apps/api/tests/concurrency/`, and the OpenAPI drift check against `specs/001-foundation-auth-org/contracts/openapi.yaml`; the generated document may differ from the pre-feature baseline only in descriptions
- [X] T302 Run `apps/api/tests/concurrency/` three times in a row to catch flaky timing; fix or tighten any test that fails once
- [X] T303 Run the validation steps of `specs/002-security-hardening/quickstart.md` end to end (start-up refusals, scripts, migrations 014 to 016 on a copy of seeded data) and record the result in the pull request; also run `uv run pytest -m timing` by hand and record that the timing comparison (SC-207) found no difference
- [X] T304 Extend the cross-organisation sweep to callers other than Admin: run every case in `apps/api/tests/integration/test_cross_organisation_sweep.py` as a Manager (same refusals as for the Admin, except cases that need the Admin role, which must be refused 403) and, for the Employee and Client roles, assert that every operation is refused by role (403) before any organisation lookup, so another organisation's id never gets a different answer from a missing one; nothing may change in any case. Body ids are already covered by the existing cases (`ids` of the employee group and qualification replacements, `client_id` and `site_id` of a new project, `email` of invitations, employees and client users)

### Scope additions recorded during implementation (Phase 12)

- **T304, result**: no gap found. As a Manager every case gets the Admin's refusal, except the
  three Admin-only deletes (employee, client, project), which are refused 403 before any lookup.
  Employee and Client users get 403 on every operation, except that a Client user may read
  projects: the list ignores a foreign `client_id` and shows only their own, and a foreign project
  id is 404 like a missing one. 165 sweep cases pass.
- **T299**: ADR 005 text updated (the lockout write is now `register_failed_login`).
- **T300**: descriptions updated for refresh (single use, grace window), password-reset request
  (case, mail after save) and the employee list (404 for a foreign filter). Interrogate 100%.
- **T301**: ruff, format (src, tests, alembic), interrogate, `mypy --strict src/`, import graph and
  `alembic check` clean; 909 tests pass including `tests/concurrency/`; the drift check agrees with
  the contract and the document differs from the pre-feature baseline only in descriptions.
  `mypy --strict tests/` (not a CI gate) reports 127 errors against 26 on `main`; not touched, as
  it is outside this task list.
- **T302**: `tests/concurrency/` three runs in a row, 9 passed each.
- **T303**: start-up refusals checked by hand (unset `ENVIRONMENT`, unset secret, the published
  placeholder, a 20-byte secret, production without a mail backend: each refuses and names the
  setting, none prints the secret); migrations 013 to head on seeded data, downgrade to 013 and up
  again all exit 0; `find_email_case_duplicates` prints nothing, `check_site_timezones` lists the
  one bad site by id and exits 1; `dev_issue_link` prints a token in development and refuses in
  production. `pytest -m timing`: median 54.8 ms unknown against 57.7 ms known (ratio 1.05).

---

# Part 2: PR 4 fixes (T305 onward)

Input: [pr4-fix-list.md](pr4-fix-list.md), plan Part 2, research D12 to D26, the 2026-11-03 clarifications and FR-221 to FR-223. Labels name the fix-list item (`[F01]` to `[F26]`); sign-in work for FR-221 to FR-223 carries `[F04]`. Every group is: failing tests first, then the change, then mutation checks for security fixes, then one commit. A phase does not start before the previous checkpoint passes.


## Phase P1: Credentials and sessions (F01, F02, F03, F08, F09, F11)

**Goal**: Close the account-takeover and session-lifetime bugs that need no schema (plan Part 2, P1).

**Independent Test**: Each group's tests fail first for the right reason, pass after the change, and every security change is caught by its mutation check.


### Group A: a completed reset ends older links and clears the lockout (F01, F11; research D12)

- [ ] T305 [P] [F01] [F11] Write `apps/api/tests/integration/test_reset_cancels_links.py` on PostgreSQL: (1) request two reset links, complete the reset with the NEWER one, then use the older with a different password: it is refused 401 `auth.token_used`, the password stays the newer one's and sign-in with the older's password fails; (2) a pending invitation link of the same account also stops working after a completed reset; (3) requesting a new reset link cancels the earlier UNUSED reset links of that account but leaves an unused invitation link of a pending account alone; (4) F11: lock the account with `LOGIN_MAX_FAILURES` wrong passwords, complete a reset, then sign in with the new password at once (200; `failed_login_attempts` is 0 and `locked_until` is empty). Show it failing
- [ ] T306 [P] [F01] [F11] Extend `apps/api/tests/unit/test_invitation_service.py`: `reset_password` calls `cancel_for_user` for the owner (all purposes) and clears the lock and count; `request_password_reset` calls it with purpose `reset` only
- [ ] T307 [F01] In `apps/api/src/hds/repositories/invitation_token.py` give `cancel_for_user(user_id, purpose=None)` and `cancel_for_users(user_ids, purpose=None)` an optional `purpose` that limits cancellation to that purpose (default: all); update the docstrings
- [ ] T308 [F01] [F11] In `apps/api/src/hds/services/invitation.py` make `reset_password` cancel every other outstanding link of the owner (`cancel_for_user`, all purposes) after spending the presented one, and clear the failure count and lock (`UserRepository.reset_failed_attempts`); make `request_password_reset` cancel the account's earlier unused `reset` links before storing the new one. Update the docstrings and the `description` of the confirm and request operations in `apps/api/src/hds/routers/invitations.py`
- [ ] T309 [F01] [F11] Mutation checks for Group A (scratch script outside the repository, as in Phase 5): drop the cancellation in `reset_password`; drop it in `request_password_reset`; drop the lock clearing. Each must make a test in `test_reset_cancels_links.py` fail; restore the files byte for byte and record the three results in the commit message
- [ ] T310 [F01] [F11] Commit Group A as `fix(api): a completed password reset ends older links and clears the lockout` (message lists the failing-first evidence and the mutation results)

### Group B: portal users when a client changes (F02; research D13)

- [ ] T311 [P] [F02] Write `apps/api/tests/integration/test_client_sessions.py` on PostgreSQL: as a portal user sign in and keep the refresh token; then (a) deactivate the client, (b) delete the client, (c) remove the portal user with `DELETE /api/v1/clients/{id}/users/{user_id}`: in each case the old refresh token is refused and stays refused after a Manager reactivates the user; the token rows carry reason `account`; unused links of that user are cancelled (`auth.token_used`, FR-207). Also: reactivating a portal user whose client is inactive or deleted returns 409 `users.client_inactive`; reactivating one whose client is active still works. Show it failing
- [ ] T312 [P] [F02] Extend `apps/api/tests/unit/test_client_service.py` and `apps/api/tests/unit/test_employee_manager.py`: `ClientService` revokes with `RevocationReason.account` and cancels links on deactivate, delete and removal; `EmployeeService.reactivate` raises `users.client_inactive` for a Client-role user whose client is inactive or deleted
- [ ] T313 [F02] In `apps/api/src/hds/services/client.py` inject `RefreshTokenRepository` into `ClientService`; make `_deactivate_portal_users` (used by `edit_client` and `soft_delete`) revoke each portal user's tokens with `RevocationReason.account` and cancel their links, and make `remove_client_user` do both too. Give `ClientRepository.list_users` in `apps/api/src/hds/repositories/client.py` a fixed `order_by(User.id)` and revoke the portal users' tokens in ascending id order, so two simultaneous changes to one client cannot lock the same rows in opposite orders and deadlock (T328 adds the row lock itself); add a test that runs two simultaneous client deactivations 50 times without a deadlock error. Update the wiring in `apps/api/src/hds/routers/clients.py` and the docstrings
- [ ] T314 [F02] In `apps/api/src/hds/services/employee.py` inject `ClientRepository` into `EmployeeService` (wiring in `apps/api/src/hds/routers/employees.py` and `me.py`; update the constructor calls in `apps/api/tests/unit/test_employee_self_service.py` and `test_employee_manager.py`) and make `reactivate` refuse a Client-role user whose client is inactive or deleted with `ConflictError(code="users.client_inactive")`. Add `users.client_inactive` to `apps/api/src/hds/locales/en.json` and `he.json` (`test_i18n` enforces parity). Update the reactivate operation `description`
- [ ] T315 [F02] Mutation checks for Group B: drop the revocation in each of the three client paths; drop the reactivation guard. Each must fail a test; restore files; record results
- [ ] T316 [F02] Commit Group B as `fix(api): ending a client ends its portal users' sessions and blocks their reactivation`

### Group C: a pending account is never activated by deactivation (F03; research D14, clarification Q2)

- [ ] T317 [P] [F03] Write `apps/api/tests/integration/test_pending_deactivate.py` on PostgreSQL: invite an address (pending), `POST /api/v1/employees/{id}/deactivate` returns 409 `users.pending_cannot_deactivate`, the account stays pending (`is_active` false, `activated_at` NULL) and its invitation link still works (accept succeeds). Same for a pending portal user when its client is deactivated: the pending user is skipped (stays pending, link intact) while active portal users are deactivated. Finally a guard test: after any sequence of deactivate and reactivate calls on a pending account `activated_at` is still NULL. Show it failing
- [ ] T318 [P] [F03] Extend `apps/api/tests/unit/test_employee_manager.py` and add `apps/api/tests/integration/test_activated_at_writers.py`: a static test that no code under `apps/api/src/hds` other than accept-invitation, create-active (`UserRepository._with_activation`) and the migrations sets `activated_at` (scan the AST for assignments and `values(activated_at=...)`)
- [ ] T319 [F03] In `apps/api/src/hds/services/employee.py` make `deactivate` refuse a pending account (not active, not deleted, `activated_at` empty) with `ConflictError(code="users.pending_cannot_deactivate")`; remove the `coalesce` that stamps `activated_at` from the user update in `deactivate` (about line 360) and from `apps/api/src/hds/repositories/client.py` `deactivate_client_users`, which must also skip pending users (`activated_at IS NOT NULL`). Add the code to `en.json` and `he.json`. Update the docstrings and the deactivate operation `description` (a pending account can only be removed by an Admin's delete: accepted consequence, spec assumptions)
- [ ] T320 [F03] Mutation checks for Group C: restore the stamp; remove the refusal; remove the pending skip in `deactivate_client_users`. Each must fail a test; restore files; record results
- [ ] T321 [F03] Commit Group C as `fix(api): deactivating a pending account is refused and never stamps an activation time`

### Group D: expired credentials end no sessions (F08; research D19, clarification Q3)

- [ ] T322 [P] [F08] Extend `apps/api/tests/integration/test_refresh_reuse.py` on PostgreSQL (window set to 1 second as in that file): (1) a rotated token replayed AFTER ITS EXPIRY returns 401 `auth.token_expired`, leaves the user's other tokens live and rewrites no reason; (2) an expired token that was never rotated returns `auth.token_expired`; (3) an UNEXPIRED rotated token replayed after the window still ends all sessions (existing behaviour). Add `apps/api/tests/integration/test_expired_cleanup.py`: expired refresh tokens and links past `expires_at` are deleted for the user after a SUCCESSFUL sign-in and at refresh, and a failed sign-in deletes nothing; a spent or cancelled link still inside its lifetime is KEPT and still answers `auth.token_used`. Show them failing
- [ ] T323 [P] [F08] Extend `apps/api/tests/unit/test_auth_service.py` decision table: expiry is checked before the revoked state
- [ ] T324 [F08] In `apps/api/src/hds/services/auth.py` `refresh`, check expiry before the revoked state (an expired credential, rotated or not, raises `auth.token_expired` and changes nothing). Add `delete_expired_for_user(user_id, now)` to `apps/api/src/hds/repositories/refresh_token.py` and to `apps/api/src/hds/repositories/invitation_token.py` (links: only rows with `expires_at` in the past) and call them for the user on the success path of sign-in, AFTER verification (T348 forbids any write before verification finishes, and `release_connection()` would raise), and on the success path of refresh. Update the docstrings and the refresh operation `description`; add a note to `docs/adr/006-refresh-token-reuse-detection.md` (expired credentials)
- [ ] T325 [F08] Mutation checks for Group D: restore the old check order; delete spent links inside their lifetime. Each must fail a test; restore files; record results
- [ ] T326 [F08] Commit Group D as `fix(api): an expired refresh token is refused as expired and ends no session`

### Group E: sessions that start while every session is being ended (F09; research D20)

- [ ] T327 [P] [F09] Write `apps/api/tests/concurrency/test_revoke_all_races.py` using the `race` fixture, 50 rounds each: a refresh racing (a) a password-reset confirm, (b) `POST /auth/logout-all`, (c) the theft response (replay of a rotated token after the window), (d) an account change that revokes (role change); and a LOGIN with the OLD password racing a password-reset confirm. After each round no refresh token of the user is live, except: in the login race either the login is refused with 401 `auth.invalid_credentials` or its session was revoked by the reset; the test asserts the refusal outcome is seen and no live token remains (so a stale re-read fails it). Show it failing
- [ ] T328 [F09] Add `lock_user(user_id)` to `apps/api/src/hds/repositories/user.py` (`SELECT ... FOR UPDATE` on the user row, documented: held until the unit of work ends; lock order is user row first) and make `RefreshTokenRepository.revoke_all_for_user` in `apps/api/src/hds/repositories/refresh_token.py` take it before revoking. The client paths from T313 therefore lock portal users one at a time in ascending id order; rerun T313's deadlock test here, because this is the task that makes it meaningful
- [ ] T329 [F09] In `apps/api/src/hds/services/auth.py`: `refresh` takes `lock_user` first and THEN reads the user (`get_for_auth` with `populate_existing()`), so it never uses a stale `is_active`; `login`, after verifying the password, takes `lock_user`, re-reads the user with `populate_existing()` and refuses with `auth.invalid_credentials` if the account is no longer active or `password_hash` differs from the verified one, before creating the token. Update the docstrings. Run `tests/unit/test_auth_service.py`, `test_login_equal_work.py`, `tests/integration/test_lockout.py`, `test_login_responses.py` and `test_refresh_reuse.py` afterwards, because T348 and T356 rewrite `login` again
- [ ] T330 [F09] Mutation checks for Group E: remove the lock in refresh; remove it in `revoke_all_for_user`; replace `populate_existing()` with a plain re-read in login; reverse the portal-user order from T313 (the deadlock test must fail). Each must fail its test; restore files; record results
- [ ] T331 [F09] Commit Group E as `fix(api): no session can outlive an end-every-session`

### Group F: documents for P1

- [ ] T332 [F01] [F02] [F03] [F08] [F11] Update `docs/hds-api.md` (error table and conventions: reset ends older links and clears the lockout; client changes end portal sessions; pending accounts cannot be deactivated; expired tokens end no sessions) and add the new rows to the requirement coverage table at the end of this file. Run `uv run pytest tests/unit/test_i18n.py`
- [ ] T333 [F01] [F02] [F03] [F08] [F11] Commit as `docs: describe the P1 session and link rules`

**Checkpoint P1: ruff, format, interrogate, `mypy --strict src/ tests/`, import graph, `alembic check`, the OpenAPI comparison (descriptions and error responses only), the full suite on PostgreSQL, `tests/concurrency/` three runs in a row, and every mutation result recorded. Stop and report before P2**

---

## Phase P2: Data migrations (F07 migration 017; sign-in throttle migration 018)

**Goal**: Schema and stored-data changes, before the code that uses them (plan Part 2, P2).

**Independent Test**: Both migrations upgrade, downgrade and re-upgrade on a scratch database with realistic data; `alembic check` agrees with the models.


### Group A: migration 017 corrects the pending backfill (F07; research D18)

- [ ] T334 [P] [F07] Write `apps/api/tests/integration/test_migration_017.py` on a scratch database seeded at revision 016 (use `scratch_database` from `apps/api/tests/integration/migration_support.py`): (1) an inactive account with `activated_at` empty, an unused invitation link and a reset link (used OR unused) ends with `activated_at = created_at` (deactivated, not pending); (2) one with a refresh-token history ends deactivated; (3) a RE-INVITED pending account (its earlier invitation link is cancelled, so `used_at` is set, and a newer link is unused) STAYS PENDING, and so does a pending account whose only link is a used or cancelled invitation link: invitation links are not evidence, because cancelling a link sets `used_at`; (4) a genuine pending invitee (inactive, unused unexpired invite, no evidence) stays pending; (5) a pending account with no evidence whose only link already expired (older than `INVITE_TTL_DAYS`) stays pending and its id is LISTED in the migration output (ids only, no addresses); (6) active accounts and accounts with `activated_at` already set are untouched; (7) downgrade is a documented no-op and the data is unchanged; (8) running the data step twice changes nothing more. Show it failing (revision does not exist)
- [ ] T335 [F07] Create `apps/api/alembic/versions/017_pending_backfill_correction.py` (down revision `016_case_insensitive_email`): a public function `reclassify_pending(connection)` that sets `activated_at = created_at` for users with `is_active = false`, `is_deleted = false`, `activated_at IS NULL` and any evidence of past activity: a row in `refresh_tokens`, or an `invitation_tokens` row with purpose `reset` (used or not, because reset links are only ever sent to active accounts). Do NOT use invitation links as evidence: cancelling a link also sets `used_at`, and re-inviting a pending account cancels its earlier link, so a genuine invitee would be wrongly marked deactivated, and a public function `undecidable_pending(connection)` returning the ids of the remaining pending accounts whose latest invitation link (purpose `invite`, latest by `created_at`) has `expires_at` in the past; `upgrade()` runs the first, prints the count reclassified and the undecidable ids (ids only) and changes nothing else; `downgrade()` is a no-op and its docstring says the data change is not reversible. Docstring states what changes, why, existing rows and the downgrade. Do NOT edit migration 014
- [ ] T336 [F07] Show migration 017 on a database with realistic data (several organisations, pending, deactivated, re-invited and staff-created accounts, links and refresh tokens): upgrade, the printed report, downgrade, upgrade again; keep the output for the commit message
- [ ] T337 [F07] Mutation check: make `reclassify_pending` ignore used reset links; scenario (1) must fail; restore the file
- [ ] T338 [F07] Commit as `fix(api): migration 017 corrects accounts that migration 014 left pending`

### Group B: migration 018 creates the sign-in throttle table (FR-221; research D16; data-model)

- [ ] T339 [P] [F04] Write `apps/api/tests/integration/test_migration_018.py` on a scratch database at revision 017: upgrade creates `login_throttle` with columns `kind` (short text, part of the primary key, check constraint `ck_login_throttle_kind`: `kind IN ('address', 'network')`), `key_hash` (text, 64 characters, part of the primary key), `failures` (integer, not null, default 0, check `failures >= 0`), `window_started_at` (timestamptz, null), `locked_until` (timestamptz, null), `updated_at` (timestamptz, not null, default now), and index `ix_login_throttle_updated_at` on `updated_at`; the check rejects another `kind` and a negative `failures`; the primary key rejects a duplicate (`kind`, `key_hash`); downgrade drops the table; existing tables are untouched. Show it failing
- [ ] T340 [F04] Create `apps/api/src/hds/models/login_throttle.py` (`LoginThrottle`, tenant-free: no `organization_id`, no foreign key, because unregistered addresses have no account; docstring states the layer and the rules) and register it in `apps/api/src/hds/models/__init__.py`; create `apps/api/alembic/versions/018_login_throttle.py` with the schema above and a reversible downgrade (drops the table; counts are disposable); `uv run alembic check` must report no differences
- [ ] T341 [F04] Commit as `feat(api): add the login_throttle table (migration 018)`

**Checkpoint P2: `alembic check`; migration tests 014 to 018; the round trip 013 to head and back on seeded data (record the output); full suite. Stop and report before P3 (migrations 017 and 018 are new)**

---

## Phase P3: Sign-in under load and abuse (F04, FR-221, FR-222, FR-223, F06)

**Goal**: Bounded hashing without held connections, the same lockout for every address, a windowed network limit, and delivery after the response (plan Part 2, P3).

**Independent Test**: Hashing never blocks the loop or holds a connection on any of the six paths; a registered and an unregistered address answer identically; SC-207 (work count and opt-in timing) still passes.


### Group A: bounded hashing that holds no connection (F04; FR-222; research D15)

- [ ] T342 [P] [F04] Write `apps/api/tests/unit/test_passwords.py`: the async `hash_password`, `verify_password` and `verify_against_dummy` return what the synchronous core returns; at most `HASH_CONCURRENCY` run at once (count concurrent entries with a wrapper); more than `HASH_CONCURRENCY + HASH_QUEUE_LIMIT` outstanding raises `ServiceBusyError` immediately; the limiter is created per running event loop (use it from two different loops in one test without an error)
- [ ] T343 [P] [F04] Write `apps/api/tests/integration/test_event_loop_not_blocked.py`: during a burst of 20 concurrent unknown-address sign-ins a plain `GET /api/v1/openapi.json` stays responsive: measure the idle median of 9 probes first, then the median of 9 probes during the burst, and assert the burst median is at most 10 times the idle median with a floor of 50 ms (a fixed ceiling would be flaky in CI), and the burst needs no more than `HASH_CONCURRENCY` hashes at a time
- [ ] T344 [P] [F04] Write `apps/api/tests/integration/test_hash_pool.py`: with the application's engine limited to a pool of 5 connections (override `get_db` with an engine of `pool_size=5, max_overflow=0, pool_timeout=2`), run 50 concurrent sign-ins and, separately, 50 concurrent `POST /api/v1/employees` creations; while they run a plain authenticated read succeeds within 2 s (no connection is held while hashing); when the hash queue is full the excess requests get 503 `service.busy` with `Retry-After` and change nothing; for each of `POST /auth/invite`, `POST /auth/invite/accept`, `POST /auth/password-reset/confirm` and `POST /api/v1/clients/{id}/users` one call under a full queue also returns 503 and changes nothing. Also test `release_connection()` on a session wrapped by `unit_of_work`: it ends a read-only transaction (the connection returns to the pool); it RAISES after an ORM `add` plus flush, after a direct `update(...)` statement, a direct `insert(...)` and a direct `delete(...)` (these do not appear in `session.new` or `session.dirty`, which is why the writes are tracked by a `do_orm_execute` listener); a `select` does not count; after a `commit()` or `rollback()` the record is cleared
- [ ] T345 [F04] Create `apps/api/src/hds/core/passwords.py`: `async hash_password`, `verify_password`, `verify_against_dummy` that run the synchronous functions of `apps/api/src/hds/core/security.py` with `asyncio.to_thread` behind a limiter (`HASH_CONCURRENCY` running, `HASH_QUEUE_LIMIT` waiting, kept per running event loop in a `WeakKeyDictionary`); when full raise `ServiceBusyError` (add to `apps/api/src/hds/core/exceptions.py` with code `service.busy`). Add settings `HASH_CONCURRENCY: int = Field(default=2, ge=1)` and `HASH_QUEUE_LIMIT: int = Field(default=8, ge=0)` to `apps/api/src/hds/core/config.py` and document them
- [ ] T346 [F04] Register a handler in `apps/api/src/hds/core/error_handler.py` mapping `ServiceBusyError` to 503 problem+json with a `Retry-After` header (seconds, default 1); add `service.busy` to `en.json` and `he.json`
- [ ] T347 [F04] Add `release_connection()` to `BaseRepository` in `apps/api/src/hds/repositories/base.py`: ends the current read-only transaction (`session.commit()`), and raises `RuntimeError` if anything has been written in this transaction (document: use only before the request's first write, because `commit()` also saves earlier writes). Track writes in `unit_of_work` in `apps/api/src/hds/core/database.py`: register a `do_orm_execute` listener on `session.sync_session` that sets `session.info['hds.wrote']` for any statement that is not a SELECT (this sees direct UPDATE, INSERT and DELETE statements, which `session.new` and `session.dirty` do not) and an `after_flush` listener for ORM changes; `commit()` and `rollback()` clear it; skip the registration for sessions without `sync_session` (the unit-test `FakeSession`)
- [ ] T348 [F04] Use the async functions and release the connection BEFORE hashing, before the first write and before any row lock, on all six paths: `login` in `apps/api/src/hds/services/auth.py` (read the user and the throttle rows, decide the lock state from what was read with an expired lock treated as cleared IN MEMORY, `release_connection()`, verify, and only then write: the failure upsert, or `lock_user` and the session as in P1 Group E; nothing is written before verification finishes); `accept_invitation`, `reset_password` and `create_invitation` (placeholder password, hashed before its first statement) in `apps/api/src/hds/services/invitation.py`; `create_employee` in `apps/api/src/hds/services/employee.py` and `create_client_user` in `apps/api/src/hds/services/client.py` (validate, release, hash, then write). Keep the synchronous functions for `apps/api/src/hds/scripts/seed_dev.py`. The work-counting tests in `apps/api/tests/unit/test_login_equal_work.py` and `tests/integration/test_login_responses.py` must still pass; adapt their hasher wrapper to the new call path without weakening the assertions. Run the same login test files as T329 afterwards
- [ ] T349 [F04] Declare 503 (and its `Retry-After`) in the `responses=` of the six operations (`POST /auth/login`, `/auth/invite`, `/auth/invite/accept`, `/auth/password-reset/confirm`, `POST /api/v1/employees`, `POST /api/v1/clients/{id}/users`) in `apps/api/src/hds/routers/auth.py`, `invitations.py`, `employees.py`, `clients.py`; update their `description` text. The OpenAPI drift check and `tests/contract` must stay green
- [ ] T350 [F04] Mutation checks for Group A: remove the limiter; remove the queue bound; call `release_connection` after hashing instead of before on one path; make `release_connection` ignore a direct UPDATE statement. Each must fail a test; restore files; record results
- [ ] T351 [F04] Commit as `fix(api): hash passwords off the event loop, bounded, without holding a database connection`

### Group B: the same lockout for every address, and a windowed network limit (FR-221, FR-223; research D16)

- [ ] T352 [P] [F04] Write `apps/api/tests/integration/test_login_throttle.py` on PostgreSQL (set `LOGIN_MAX_FAILURES` and the network limit through the settings object, as in `test_lockout.py`): (1) for a REGISTERED and an UNREGISTERED address (also in a different letter case) `LOGIN_MAX_FAILURES` wrong passwords answer 401 `auth.invalid_credentials` and the next attempt answers 423 `auth.account_locked`, and the two responses have the same status, body shape, headers and number of Argon2 verifications at every step; (2) while locked a correct password is refused 423 and is NOT verified (verification count unchanged); (3) after the lock expires (set `locked_until` in the past) the count restarts: `LOGIN_MAX_FAILURES - 1` failures do not lock, ONE wrong password after the cooldown leaves the count at 1 (not `LOGIN_MAX_FAILURES + 1`), and a correct password right after the cooldown signs in with 200 (no 500, no leftover lock) for a registered address; with a statement recorder on the engine, a sign-in whose stored lock has expired issues no INSERT, UPDATE or DELETE before the Argon2 verification starts (so `release_connection()` is legal there); (4) a successful sign-in clears the ADDRESS count (nine typos, a success, one typo: not locked); (5) NETWORK: `LOGIN_RATE_LIMIT_PER_NETWORK` failures inside one window from one client address give 429 `auth.rate_limited` with `Retry-After` for ANY submitted address, answered before any lookup; failures spread across two windows (move `window_started_at` back) do not add up; 29 failures, a successful sign-in, then one more failure DOES lock (a success does not clear the network count); a limit of 0 turns the network limit off; (6) rows store only hashes (no address or network address appears in the table); (7) each recorded failure deletes up to 50 unlocked rows older than `LOGIN_THROTTLE_RETENTION_HOURS` and never a locked row. Show it failing
- [ ] T353 [P] [F04] Write `apps/api/tests/unit/test_client_address.py` for `client_address(request, trusted_proxy_count)`: count 0 ignores `X-Forwarded-For` (a forged header changes nothing); count 1 takes the last entry; count 2 takes the second from the right; a header with fewer entries than the count falls back to the connecting address; a malformed chosen entry falls back to the connecting address; an IPv6 address is reduced to its /64 prefix (two addresses in one /64 give the same key); an IPv4 address keeps its full value
- [ ] T354 [F04] Add `LoginThrottleRepository` in `apps/api/src/hds/repositories/login_throttle.py`: `register_failure(kind, key_hash, limit, lock_until, window_minutes, now)` as ONE atomic `INSERT ... ON CONFLICT DO UPDATE ... RETURNING` (for `address`: increment `failures`, set `locked_until` when the new count reaches `limit`; for `network`: first restart `failures` and `window_started_at` if `window_started_at + window_minutes <= now`, then increment, and set `locked_until` to the window end when the limit is reached), committed at once through the explicit `commit()` so a refusal does not undo it; in the same statement a stored lock that has expired (`locked_until <= now`) restarts the count at 1 and clears the lock, so there is no separate write to clear an expired lock before verification; give `UserRepository.register_failed_login` in `apps/api/src/hds/repositories/user.py` the same expiry rule (`failed_login_attempts = CASE WHEN locked_until IS NOT NULL AND locked_until <= :now THEN 1 ELSE failed_login_attempts + 1 END`) and remove `clear_expired_lock` from the sign-in path (keep the method only if another caller needs it); `get(kind, key_hash)`; `clear_failures(kind, key_hash)` (address rows only); `purge_stale(older_than, limit=50)` deleting unlocked rows by the `updated_at` index. Rewrite the existing `test_an_expired_lock_is_cleared_before_the_password_is_checked` in `apps/api/tests/unit/test_auth_service.py`, which asserts the order clear, verify, register: the new order is verify then register, with no write before verification. Docstrings state the layer and that keys are SHA-256 hex
- [ ] T355 [F04] Create `apps/api/src/hds/core/client_address.py` (`client_address(request, trusted_proxy_count)` and `network_key`: IPv6 reduced to /64, malformed entries ignored) and `apps/api/src/hds/services/throttle.py` (service layer, because it coordinates two repositories; the rule shared by registered and unregistered addresses: a small interface with `clear_expired`, `locked_until`, `register_failure`, `clear_on_success`, implemented over the users columns for a registered address and over `LoginThrottleRepository` for an unregistered one). Add settings `LOGIN_RATE_LIMIT_PER_NETWORK: int = Field(default=30, ge=0)`, `TRUSTED_PROXY_COUNT: int = Field(default=0, ge=0)` and `LOGIN_THROTTLE_RETENTION_HOURS: int = Field(default=24, gt=0)` to `apps/api/src/hds/core/config.py`
- [ ] T356 [F04] Rewrite the front of `AuthService.login` in `apps/api/src/hds/services/auth.py` and pass the client address from `apps/api/src/hds/routers/auth.py`: (a) if the network limit is on and the network key is locked, raise `AuthError(code="auth.rate_limited")` mapped to 429 with `Retry-After`, before any lookup; (b) decide the address state from the row just read, treating an expired lock as cleared IN MEMORY (no write), and refuse with `auth.account_locked` (423, same detail format and lock-end rule) while locked, WITHOUT verifying the password; (c) call `release_connection()` (no write has happened yet), then verify (the real hash, or `verify_against_dummy` for an unregistered address), on failure register the failure for the address and the network and purge stale rows, on success clear the address count and NOT the network count. Add `auth.rate_limited` to `en.json` and `he.json`, map it in `apps/api/src/hds/core/error_handler.py`, declare 423 and 429 in the login `responses=` and update the login `description`. Run the same login test files as T329 afterwards
- [ ] T357 [F04] Mutation checks for Group B: lock unregistered addresses one failure late; skip the lock for unregistered addresses; let a success clear the network count; remove the window restart; key IPv6 by the full address; trust the forwarded header at count 0; remove the stale-row purge; write the expired-lock clear before verification again (the no-write-before-verification test must fail). Each must fail a test; restore files; record results
- [ ] T358 [F04] Commit as `fix(api): lock unregistered addresses like registered ones and limit failures per network`

- [ ] T407 [F04] Keep the new limits out of tests that are not about them. In `apps/api/tests/conftest.py`, before any `hds` import, assign `os.environ["LOGIN_RATE_LIMIT_PER_NETWORK"] = "0"` (an assignment, not `setdefault`, so a developer's shell cannot change it) with a comment saying why; turn the limit on only in `tests/integration/test_login_throttle.py`, through the settings object. In `tests/integration/test_login_timing.py` also set `LOGIN_MAX_FAILURES` very high on the settings object: the test sends 500 wrong sign-ins for one unregistered address and asserts 401, which now locks after 5, and a success cannot clear it. In `tests/contract/test_openapi_contract.py` set `LOGIN_MAX_FAILURES` very high for the module, because a repeated generated address could otherwise answer 423, which the "valid data" check rejects and login is exempt from only in the CLI suite. Add a settings test that the default of `LOGIN_RATE_LIMIT_PER_NETWORK` is 30 (delete the variable, no settings file)
- [ ] T408 [F04] `apps/api/scripts/contract_cli.sh` does not start the server, so the setting belongs to whoever does: add `LOGIN_RATE_LIMIT_PER_NETWORK: "0"` to the `env` of the `contract` job in `.github/workflows/ci.yml` (the "Start API" step inherits it), add it to the server-start lines the script prints when it refuses, and state it in `apps/api/README.md` beside the script. Run the script twice in a row against a scratch database: the second run must not fail at its first sign-in

### Group C: delivery after the response (F06; research D17)

- [ ] T359 [P] [F06] Write `apps/api/tests/integration/test_delivery_after_response.py`. Do NOT use the httpx test client for the ordering check: it waits for the whole ASGI app, background tasks included, before it returns a response, so a sender that blocks would hang the test. Instead call the full application (`hds.main.app`, middleware included) directly as an ASGI callable with a hand-built HTTP scope and receive/send functions that append every message to a shared event log: with a sender whose `send` appends `delivery_started` then waits on an event, `POST /auth/password-reset/request` for a registered address must log the final `http.response.body` message BEFORE `delivery_started` (assert the log order while the sender is still blocked, then set the event and await the app); delivery then runs exactly once; on a failed commit nothing is delivered; a delivery that raises is logged by exception type only (no message, no address, no token) and does not change the response. Show it failing
- [ ] T360 [P] [F06] Extend `apps/api/tests/integration/test_login_timing.py` (opt-in, marker `timing`) or add `apps/api/tests/integration/test_reset_request_timing.py`: 300 samples each for a registered and an unregistered address on `POST /auth/password-reset/request`, interleaved, with a pass mark of median ratio below 1.15; print both medians
- [ ] T361 [F06] In `apps/api/src/hds/core/database.py` change `get_unit_of_work` to take FastAPI's `BackgroundTasks`: after a successful commit it adds ONE background task that runs the session's after-commit actions, so they run after the response is sent, still only after the commit and still dropped on rollback; keep failure logging by type. Adapt `apps/api/tests/unit/test_unit_of_work.py`, `test_unit_of_work_dependency.py`, `test_deferred_mailer.py` and `tests/integration/test_mail_after_commit.py` to the new ordering without weakening them
- [ ] T362 [F06] Write `docs/adr/008-delivery-after-the-response.md` (context, decision, the residual token-write difference, alternatives) and add a pointer to `docs/adr/005-request-scoped-transactions.md`; add both to the ADR table in `docs/README.md`
- [ ] T363 [F06] Mutation check: run the actions before the response again; the ordering test must fail; restore the file
- [ ] T364 [F06] Commit as `fix(api): send mail after the response so a reset request takes the same time for every address`

### Group D: bulk purge, settings and documents for P3

- [ ] T365 [F08] [F04] Write `apps/api/tests/unit/test_purge_expired_records.py` and create `apps/api/src/hds/scripts/purge_expired_records.py` (`python -m hds.scripts.purge_expired_records`): deletes expired refresh tokens, links past `expires_at` and unlocked `login_throttle` rows older than the retention; spent or cancelled links inside their lifetime are kept; prints counts only; exit 0. Split into a pure decision, `run(session_factory)` and a thin database entry as in `check_site_timezones.py`
- [ ] T366 [F04] Add `HASH_CONCURRENCY`, `HASH_QUEUE_LIMIT`, `LOGIN_RATE_LIMIT_PER_NETWORK`, `TRUSTED_PROXY_COUNT` and `LOGIN_THROTTLE_RETENTION_HOURS` to `.env.example` (as comments) and to the configuration table of `apps/api/README.md` (the existing `tests/unit/test_deployment_files.py` enforces both); state in the table that behind a reverse proxy `TRUSTED_PROXY_COUNT` must be set (or the network limit set to 0) and that a wrong value cannot be detected at start-up
- [ ] T367 [F04] Write `docs/adr/007-sign-in-abuse-controls.md` (bounded hashing and why no connection is held, the same lockout for every address, the windowed network limit and the proxy rule, alternatives) and add it to `docs/README.md`; correct `docs/hds-api.md` (rate limiting is now specified; remove the claim that the FR-005 exception stands); record the deployment note that the future ingress feature must set `TRUSTED_PROXY_COUNT` and check it in its steps
- [ ] T368 [F04] [F06] Commit as `docs: describe the sign-in abuse controls and the new settings`

### Group E: the contract records error responses, and the drift check compares them (Constitution V, research D15 to D17)

- [ ] T409 [P] [F04] Extend `apps/api/tests/unit/test_export_openapi.py` first: `diff` reports a response code that only the contract declares, and one that only the generated document declares, for error codes as well as 2xx. Show it failing
- [ ] T410 [F04] In `apps/api/src/hds/scripts/export_openapi.py` make `surface` compare every declared response code, not only 2xx, and remove the comments and docstring text saying error responses are not declared per route (the generated document declares them per route). Update the module docstring and the sentence in `apps/api/README.md` listing what CI compares
- [ ] T411 [F04] Update `specs/001-foundation-auth-org/contracts/openapi.yaml` so its response codes match the generated document on all 46 operations (none match today; this includes 400, 409 and 422 where the application declares them, 429 on login and 503 on the six hashing operations). Before copying, review the generated code sets: where one is wrong for an operation (for example the blanket 409 on `POST /auth/logout-all`), correct the application's declaration instead of writing it into the contract, and list those corrections in the commit message. Add each operation's new responses with the shared problem+json schema. `export_openapi --check` must pass
- [ ] T412 [F04] Commit as `feat(api): the contract records error responses and the drift check compares them`

**Checkpoint P3: all gates; SC-207 unit and responses tests; `uv run pytest -m timing` for login and for the reset request; the Schemathesis script `apps/api/scripts/contract_cli.sh` against a scratch database until it exits 0 (login now answers 423 and 429; the hooks file must not break); the OpenAPI comparison; `tests/concurrency/` three runs. Stop and report before P4**

---

## Phase P4: Input, logging and reliability (F05, F10, F12)

**Goal**: Small independent fixes (plan Part 2, P4).

**Independent Test**: Each fix has a test that fails first; the logging fix also has a mutation check.


### Group A: log redaction keeps no fragment (F05; research D22)

- [ ] T369 [P] [F05] Write `apps/api/tests/unit/test_log_redaction_quotes.py`: `redact('{"password": "ab\\"cd"}')` leaves no `cd` fragment and no part of the secret; values containing `"`, `'` and `\` (single and double quoted, JSON and key=value forms) never leave a fragment longer than one character of the secret; redacting twice equals redacting once (property test with Hypothesis). Show it failing
- [ ] T370 [F05] In `apps/api/src/hds/core/logging.py` change `_SENSITIVE_PAIR` (line 22) so the value group consumes backslash escapes and stops only at an unescaped matching delimiter; unquoted values still stop at whitespace, comma or closing brace. Update the docstring
- [ ] T371 [F05] Mutation check: restore the old pattern; the test must fail; restore the file
- [ ] T372 [F05] Commit as `fix(api): log redaction removes values that contain quotes and backslashes`

### Group B: canonical time-zone names (F10; research D21, amends D11)

- [ ] T373 [P] [F10] Write `apps/api/tests/unit/test_site_timezone_canonical.py` and extend `apps/api/tests/integration/test_site_timezone.py`: `posixrules`, `Factory`, `asia/jerusalem`, `ASIA/JERUSALEM` are refused with 422 `sites.timezone_invalid` on create and edit through the application; `Asia/Jerusalem`, `UTC`, `Asia/Kolkata` and `America/Argentina/Buenos_Aires` still pass; the result does not depend on the platform's file system (the check uses the `tzdata` package list, case-sensitive); `check_site_timezones` reports stored values that fail the new rule. Show it failing
- [ ] T374 [F10] In `apps/api/src/hds/services/site.py` change `validate_timezone` (line 33) to accept a name only if it is in the `tzdata` package's zone list (read once from `importlib.resources.files('tzdata') / 'zones'`), is not `Factory`, and loads with `ZoneInfo`; matching is case-sensitive. Make `apps/api/src/hds/scripts/check_site_timezones.py` use the same function. Update the docstrings and the note in `specs/002-security-hardening/research.md` D11 if wording changed
- [ ] T375 [F10] Mutation check: use `available_timezones()` instead of the package list on a case-insensitive comparison; the four named names must be accepted again and the test fail; restore the file
- [ ] T376 [F10] Commit as `fix(api): accept only canonical IANA time-zone names`

### Group C: a failed rollback does not hide the original error (F12; research D23)

- [ ] T377 [P] [F12] Write `apps/api/tests/unit/test_rollback_failure.py` with the `FakeSession` from `apps/api/tests/unit/conftest.py`: when the block raises and `rollback()` also raises, the ORIGINAL exception propagates unchanged and the rollback error is logged by exception type only (no message); when the commit fails and rollback fails too, the commit error propagates. Show it failing
- [ ] T378 [F12] In `unit_of_work` in `apps/api/src/hds/core/database.py` run the rollback inside its own `try`: log a rollback failure by exception type only and re-raise the original error. Update the docstring
- [ ] T379 [F12] Commit as `fix(api): a failing rollback no longer replaces the request's real error`

**Checkpoint P4: all gates, full suite, the OpenAPI comparison. Stop and report before P5**

---

## Phase P5: Tests, the development tool and documents (F13, F14, F15, F16)

**Goal**: Corrections with no production behaviour change (plan Part 2, P5).

**Independent Test**: The corrected tests fail when the thing they guard is removed.


### Group A: one commit for four corrections

- [ ] T380 [F13] F13: in `apps/api/tests/integration/test_email_case.py` (the repository test near line 176) add `role` to the second insert and assert `uq_users_email_ci` appears in the `IntegrityError`; show by a mutation (drop the index in a scratch database or monkeypatch it away) that the test fails when the index is absent; restore
- [ ] T381 [F14] F14: in `apps/api/src/hds/scripts/dev_issue_link.py` (line 77) issue an invitation token only for a PENDING account (not active, not deleted, `activated_at` empty) and refuse a deactivated one with exit status 1; in `apps/api/tests/unit/test_dev_issue_link.py` (line 61) make the "pending" fixture account truly pending (`activated_at` None) and add a deactivated one; tests: deactivated refused with status 1 and a message, pending gets a token that `accept_invitation` can redeem. Show the new tests failing first
- [ ] T382 [P] [F15] F15: in `specs/002-security-hardening/spec.md` renumber the last scenario of User Story 3 (about line 129) from `3.` to `4.` and update any reference to it (search `tasks.md`, `plan.md`, `quickstart.md`)
- [ ] T383 [P] [F16] F16: in `specs/002-security-hardening/quickstart.md` (about line 47) change the start-up row to say `ENVIRONMENT=production` for the 20-byte-secret case
- [ ] T384 [F13] [F14] [F15] [F16] Commit as `test(api): fix the duplicate-address and pending-account tests, and two document errors`

**Checkpoint P5: all gates and the full suite. Stop and report before P6**

---

## Phase P6: Older gaps (F17 to F26)

**Goal**: Gaps not introduced by this PR, in scope by clarification Q1 (plan Part 2, P6). Order: F26 first, then the small fixes, then the contract-affecting ones, then the refactors.

**Independent Test**: Behaviour changes are pinned by failing-first tests before any refactor; the refactors change no behaviour and no OpenAPI output.


### Group A: each test run uses its own database (F26; research D24)

- [ ] T385 [P] [F26] Write `apps/api/tests/unit/test_run_database.py` (pure functions): the per-run database name is `hds_<process id>_test`, always ends in `_test`, an explicit `TEST_DATABASE_URL` wins, and a database named `hds_<pid>_test` whose process id no longer exists is recognised as stale (and one whose process exists is not). Add a scripted check (not collected by default) in `apps/api/scripts/two_runs_at_once.sh` that starts two `pytest tests/integration/test_me.py` runs at the same time and requires both to pass and no `hds_*_test` database to be left behind. Show the two runs failing at random before the change
- [ ] T386 [F26] In `apps/api/tests/conftest.py` derive the database name from the run, create and migrate it in `migrated_test_db`, drop it at the end of the session, and drop stale `hds_<pid>_test` databases at start; keep the `_assert_safe_test_db` guard; keep `apps/api/tests/integration/migration_support.py` scratch databases as they are. Update `apps/api/README.md` (Tests) and `README.md`
- [ ] T387 [F26] Commit as `test(api): give each test run its own database`

### Group B: small service fixes (F18, F19, F25)

- [ ] T388 [P] [F18] [F19] [F25] Write failing tests: F18 `apps/api/tests/integration/test_client_without_client.py` (a Client-role user whose `client_id` is NULL gets an empty project list and 404 on a project id; seed the row with SQL); F19 extend `apps/api/tests/integration/test_token_revocation.py` (`POST /auth/logout` with a refresh token owned by ANOTHER user answers 204 and revokes nothing; with the caller's own token it still revokes); F25 `apps/api/tests/concurrency/test_duplicate_create.py` (two simultaneous creations of one address through `POST /api/v1/employees`, and through `POST /api/v1/clients/{id}/users`, give exactly one 201 and one 409 `users.duplicate_email`, 50 rounds each, never a 422). Show them failing
- [ ] T389 [F18] [F19] [F25] F18: in `apps/api/src/hds/services/project.py` (`list_projects`, about line 182, and `get_project`) make a Client-role user without `client_id` see no projects. F19: in `apps/api/src/hds/services/auth.py` (`logout`, about line 206) revoke the token only if it belongs to the caller; the answer stays 204 either way; the router passes the caller. F25: in `apps/api/src/hds/services/employee.py` `create_employee` and `apps/api/src/hds/services/client.py` `create_client_user` use `UserRepository.create_if_absent` and raise `users.duplicate_email` (409) on a lost race. Update docstrings and the three operations' `description`
- [ ] T390 [F18] [F19] Mutation checks for F18 and F19: remove the empty-list rule; remove the ownership check. Each must fail a test; restore files; record results
- [ ] T391 [F18] [F19] [F25] Commit as `fix(api): logout checks ownership, portal users without a client see nothing, simultaneous creation returns 409`

### Group C: clearing optional fields (F17; research D25)

- [ ] T392 [P] [F17] Write `apps/api/tests/integration/test_patch_null_clears.py` on PostgreSQL: for each of `PATCH /me` (phone), `PATCH /api/v1/employees/{id}` (phone, `default_hourly_rate`), `PATCH /api/v1/clients/{id}` (`contact_name`, `contact_email`, `contact_phone`, `contract_end`), `PATCH /api/v1/projects/{id}` (`end_date`) and `PATCH /api/v1/sites/{id}`: an explicit `null` clears an optional field (stored NULL, response shows null), an omitted field is left alone, and `null` for a required field (`full_name`, `role`, `name`, `address`, `timezone`, `billing_rate`, `status`, `contract_start`, as each endpoint has them) returns 422 `validation.request_invalid` and changes nothing; also assert on the generated OpenAPI document AND on `specs/001-foundation-auth-org/contracts/openapi.yaml` that these request fields are optional and NOT nullable while the clearable fields (`phone`, `default_hourly_rate`, `contact_*`, `contract_end`, `end_date`) still allow `null`. Show it failing (today `null` is dropped and the answer is 200, and the schema allows `null`)
- [ ] T393 [F17] Replace `exclude_none=True` by `exclude_unset=True` in the PATCH handlers of `apps/api/src/hds/routers/me.py`, `employees.py`, `clients.py`, `projects.py` and `sites.py`; make the required fields of the five PATCH request models OPTIONAL BUT NOT NULLABLE (omitted is allowed, `null` is rejected with 422): declare them with their plain type and a default that is never used because `exclude_unset=True` drops it (do not leave `| None` in the type, which is what makes the schema allow `null` today); make the services and repositories pass `None` through for the clearable columns only. The generated OpenAPI document DOES change here: those request fields lose `null` (the schema must say what the API does, and Schemathesis treats a request the schema allows as valid, so a 422 would otherwise fail its `positive_data_acceptance` check). Do NOT add these operations to `apps/api/tests/contract/unprovable_operations.json`. Make the same schema change in `specs/001-foundation-auth-org/contracts/openapi.yaml`. Run the comparison script and require that the only differences from the baseline are descriptions and the removed `null` options on those request fields; run `tests/contract` and the Schemathesis script. Update the five operations' `description`
- [ ] T394 [F17] Mutation check: restore `exclude_none=True` in one router; the test must fail; restore the file
- [ ] T395 [F17] Commit as `fix(api): PATCH with an explicit null clears an optional field`

### Group D: Client accounts and client dates (F23, F24; confirmed defaults D5 and D6)

- [ ] T396 [P] [F23] [F24] Write `apps/api/tests/integration/test_client_rules.py` on PostgreSQL: F23 any role change on a Client-role account (`PATCH /api/v1/employees/{id}` with `role`, and the promote operations) returns 409 `users.client_role_locked` and changes nothing; F24 `POST /api/v1/clients` and `PATCH /api/v1/clients/{id}` with `contract_end` earlier than `contract_start` return 422 `clients.date_range_invalid` (on PATCH the stored value is used for the field not sent) and equal dates are accepted. Show it failing
- [ ] T397 [F23] [F24] F23: in `apps/api/src/hds/services/employee.py` (`edit_employee`, `promote_to_manager`, `promote_to_admin`) refuse any role change on a user whose role is `client` with `ConflictError(code="users.client_role_locked")`. F24: in `apps/api/src/hds/services/client.py` (`create_client`, `edit_client`) validate the date order with the stored values merged in, raising `DomainValidationError(code="clients.date_range_invalid")`. Add both codes to `en.json` and `he.json`; update the operations' `description`; keep the service rule only (no new constraint, existing rows may violate it)
- [ ] T398 [F23] [F24] Mutation checks: remove each rule; each must fail a test; restore files; record results
- [ ] T399 [F23] [F24] Commit as `fix(api): Client accounts keep their role and a client's contract dates are ordered`

### Group E: refactors with behaviour already pinned (F20, F21, F22)

- [ ] T400 [F20] [F21] [F22] Before touching code, save the generated OpenAPI document (`uv run python -m hds.scripts.export_openapi`) and the full-suite result as the baseline for this group
- [ ] T401 [P] [F20] [F21] [F22] F20 tests first: add `apps/api/tests/unit/test_site_service.py`, `test_client_service.py` and `test_employee_manager.py` cases for the service methods the routers will call (`SiteService.list_sites`, `SiteService.get_site`, the client and employee reads): each requires the right role (403 `users.manager_required` otherwise) and returns 404 for another organisation's id. They fail until the methods exist. F21 and F22 are pinned by the existing suite and the OpenAPI baseline
- [ ] T402 [F20] F20: move the repository reads in `apps/api/src/hds/routers/sites.py`, `clients.py` and `employees.py` (for example `SiteRepository(db).list_by_org`, `get_by_id`, `_require_user_in_org`) into the matching services (`SiteService.list_sites` and `get_site`, `ClientService`, `EmployeeService`), each with its role check, and make the routers call only services. Constitution II: `uv run python -m hds.tools.check_imports` must pass and the routers must not import repositories for reads. Add or extend unit tests per new service method (role check and not-found)
- [ ] T403 [F21] F21: replace the two copies of `UserRead` in `apps/api/src/hds/routers/me.py` and `employees.py` with one definition in `apps/api/src/hds/routers/user_schemas.py`; remove the "keep identical" comment; the OpenAPI component keeps the name `UserRead`
- [ ] T404 [F22] F22: remove the duplicate helpers: one user lookup for `get_by_id_for_token` and `get_for_auth` in `apps/api/src/hds/repositories/user.py`; one token-hash function in `apps/api/src/hds/core/security.py` used by `apps/api/src/hds/services/auth.py` and `invitation.py`; delete `_verify_password` in `auth.py` and make the tests that patch it patch `hds.core.passwords` (or the single verification function) instead
- [ ] T405 [F20] [F21] [F22] Compare the OpenAPI document and the suite with the baseline: the document must be identical, and every test that passed before must pass. Commit as `refactor(api): routers use services, one UserRead, no duplicate helpers`

**Checkpoint P6 (merge gate): ruff, format (src, tests, alembic, scripts), interrogate 100%, `mypy --strict src/ tests/`, import graph, `alembic check`; the full suite on PostgreSQL; `tests/concurrency/` three runs in a row; the Schemathesis script against a scratch database until it exits 0; `uv run pytest -m timing` for login and reset request; the OpenAPI comparison; the round trip of migrations 013 to head and back; update the coverage table below; push and report the PR 4 head commit**

---

- [ ] T406 Run the merge gate of Checkpoint P6 once more on the final head, push `002-security-hardening`, and report the PR number, the CI result and anything that could not be run

---

## Dependencies & Execution Order

### Phase dependencies

- **Phase 1** Setup: none.
- **Phase 2** Foundational: after Phase 1. **Blocks every story.** T216 (Checkpoint A) must be green.
- **Phases 3 to 11**: after Phase 2.
- **Phase 12** Polish: after the stories you ship.

### Story dependencies

- **US1** independent: its logging and outbox checks no longer depend on US4 (the token is passed to the sender inside US1). Within US1, do T222, T223 and T224 before T225. Can run in parallel with US2 (different files) once Phase 2 is done.
- **US2** after Phase 2. Touches `invitation.py`, `invitation_token.py`, `user.py`. Do the fixture fixes (T241, T242) immediately after migration 014 (T240), otherwise the suite fails on the new check constraint; do T247 right after T245. T246 follows T245 (same file).
- **US3** after US2 (same files: `invitation.py`, `invitation_token.py`, `refresh_token.py`; guarded spend replaces the code US2 touched).
- **US4** after US3 (the atomicity tests exercise refresh and reset as rewritten there; `DeferredMailer` wiring edits `invitation.py`).
- **US5** after Phase 2 only (needs the unit of work, not the other stories). Edits `user.py` and `employee.py`, so do not run it at the same time as US2 or US3.
- **US6 then US7** (both edit `AuthService.login` and `security.py`); each after Phase 2. US6 should follow US3 because both edit `services/auth.py`.
- **US8** after Phase 2; it edits `user.py`, the routers and adds migration 016. Do it after US2 so migration numbering stays 014, 015, 016.
- **US9** independent after Phase 2.

### Within each story

Tests first and failing, then models and migrations, repositories, services, routers and docs.

### Parallel opportunities

- Phase 1: T202 and T203.
- Phase 2: T204 with T205; T209 with T210; T212 with T213; T214 with any of them.
- US1: T217 to T221 together; T226, T229, T230, T231 and T224 together.
- US2: T233 to T237 together (T238 follows T234, same file); T242 and T247; T248.
- US3: T250 to T254 together; T259.
- US4: T265 with T266. US5: T270 with T271. US6: T274 with T275. US7: T278 with T279.
- US8: T282 to T285 together; T291.
- US9: T293, T294, T296, T297.
- US1 and US9 can be done by a second person while the first does US2 and US3.

### Parallel example: User Story 3 tests

```bash
Task: "Write tests/concurrency/test_refresh_single_use.py (T250)"
Task: "Write tests/concurrency/test_link_single_use.py (T251)"
Task: "Write tests/integration/test_refresh_reuse.py (T252)"
Task: "Extend tests/unit/test_auth_service.py with the grace-window decision table (T253)"
Task: "Write tests/integration/test_migration_015.py (T254)"
```

---

## Implementation Strategy

### MVP first (User Story 1)

1. Phase 1, then Phase 2 through Checkpoint A (no behaviour change, all 422 tests pass).
2. Phase 3 (US1). Stop and validate with the Story 1 rows of the quickstart.

### Incremental delivery

1. Setup and Foundational.
2. US1, then US2, then US3: the three P1 stories close the account-takeover paths. Commit after each.
3. US4, then US5, US6, US7, US8 (P2). US9 (P3) last, but before any scheduling work starts.
4. Polish. Open the pull request with the docs checkbox ticked.

### Rollout notes

- Deploying needs `ENVIRONMENT` and `JWT_SECRET_KEY` set (US1); a deployment without them stops by
  design.
- Run `find_email_case_duplicates` before applying migration 016 (US8).
- Run `check_site_timezones` after migration to list invalid stored zones (US9).
- Production cannot start until a mail provider feature adds a real `MAIL_BACKEND` (FR-202).
- Migration 014 adds a check constraint: any other writer of active users (scripts, fixtures, future code) must set `activated_at`.

---

## Requirement → task coverage

| Requirement | Tasks |
| --- | --- |
| FR-201 | T217, T225 |
| FR-202 | T217, T225, T227 |
| FR-203 | T218, T219, T220, T221, T226, T227, T228, T229 |
| FR-204 | T230, T231 |
| FR-205 | T233, T235, T236, T239, T240, T241, T242, T244 |
| FR-206 | T233, T235, T237, T244, T248 |
| FR-207 | T234, T243, T245, T247 |
| FR-208 | T250, T251, T214, T258, T259, T261 |
| FR-209 | T252, T253, T257, T260 |
| FR-210 | T204 to T216, T265, T266, T267, T268 |
| FR-211 | T270, T271, T272, T273 |
| FR-212 | T274, T275, T276, T277 |
| FR-213 | T274, T275, T276, T277 |
| FR-214 | T278, T280, T281 |
| FR-215 | T282, T283, T286, T287, T288, T292 |
| FR-216 | T284, T285, T290, T291 |
| FR-217 | T293, T294, T295 |
| FR-218 | T297 |
| FR-219 | every test task above |
| FR-220 | T238, T246, T248 |
| SC-201 | T217 |
| SC-202 | T220 |
| SC-203 | T250, T251 |
| SC-204 | T235, T236 |
| SC-205 | T270 |
| SC-206 | T274 |
| SC-207 | T278 (automated), T279 (opt-in, run by hand in T303) |
| SC-208 | T283, T284 |
| SC-209 | T294, T297 |

104 tasks (T201 to T304; T262 and T263 are extra hardening against feature 001 NFR-001 and FR-012 and map to no FR-2xx). Every requirement FR-201 to FR-220 and every success criterion SC-201 to
SC-209 has at least one implementing task and one test task.

## Notes

- [P] tasks touch different files and have no dependency on an unfinished task.
- A failing test must be seen to fail before its implementation task starts (FR-219).
- Stop at any checkpoint to validate a story on its own.
- Avoid: two people editing `services/auth.py`, `services/invitation.py` or `repositories/user.py` at the same time.

## Part 2 coverage: fix-list item → tasks

| Item | Tasks |
| --- | --- |
| F01 | T305, T306, T307, T308, T309, T310, T332, T333 |
| F02 | T311, T312, T313, T314, T315, T316, T332, T333 |
| F03 | T317, T318, T319, T320, T321, T332, T333 |
| F04 | T339, T340, T341, T342, T343, T344, T345, T346, T347, T348, T349, T350, T351, T352, T353, T354, T355, T356, T357, T358, T365, T366, T367, T368, T407, T408, T409, T410, T411, T412 |
| F05 | T369, T370, T371, T372 |
| F06 | T359, T360, T361, T362, T363, T364, T368 |
| F07 | T334, T335, T336, T337, T338 |
| F08 | T322, T323, T324, T325, T326, T332, T333, T365 |
| F09 | T327, T328, T329, T330, T331 |
| F10 | T373, T374, T375, T376 |
| F11 | T305, T306, T308, T309, T310, T332, T333 |
| F12 | T377, T378, T379 |
| F13 | T380, T384 |
| F14 | T381, T384 |
| F15 | T382, T384 |
| F16 | T383, T384 |
| F17 | T392, T393, T394, T395 |
| F18 | T388, T389, T390, T391 |
| F19 | T388, T389, T390, T391 |
| F20 | T400, T401, T402, T405 |
| F21 | T400, T401, T403, T405 |
| F22 | T400, T401, T404, T405 |
| F23 | T396, T397, T398, T399 |
| F24 | T396, T397, T398, T399 |
| F25 | T388, T389, T391 |
| F26 | T385, T386, T387 |

## Part 2 coverage: requirement → tasks

FR-221 to FR-223 share the `[F04]` label; their tasks are T339 to T358 and T407 to T412.

| Requirement | Item | Tasks |
| --- | --- | --- |
| FR-209 | F08 | T322, T323, T324, T325, T326, T332, T333, T365 |
| FR-221 | F04 | T339, T340, T341, T342, T343, T344, T345, T346, T347, T348, T349, T350, T351, T352, T353, T354, T355, T356, T357, T358, T365, T366, T367, T368, T407, T408, T409, T410, T411, T412 |
| FR-222 | F04 | T339, T340, T341, T342, T343, T344, T345, T346, T347, T348, T349, T350, T351, T352, T353, T354, T355, T356, T357, T358, T365, T366, T367, T368, T407, T408, T409, T410, T411, T412 |
| FR-223 | F04 | T339, T340, T341, T342, T343, T344, T345, T346, T347, T348, T349, T350, T351, T352, T353, T354, T355, T356, T357, T358, T365, T366, T367, T368, T407, T408, T409, T410, T411, T412 |
| FR-224 | F01 | T305, T306, T307, T308, T309, T310, T332, T333 |
| FR-225 | F02 | T311, T312, T313, T314, T315, T316, T332, T333 |
| FR-226 | F03 | T317, T318, T319, T320, T321, T332, T333 |
| FR-227 | F05 | T369, T370, T371, T372 |
| FR-228 | F06 | T359, T360, T361, T362, T363, T364, T368 |
| FR-229 | F07 | T334, T335, T336, T337, T338 |
| FR-230 | F09 | T327, T328, T329, T330, T331 |
| FR-231 | F10 | T373, T374, T375, T376 |
| FR-232 | F11 | T305, T306, T308, T309, T310, T332, T333 |
| FR-233 | F12 | T377, T378, T379 |
| FR-234 | F13 | T380, T384 |
| FR-235 | F14 | T381, T384 |
| FR-236 | F17 | T392, T393, T394, T395 |
| FR-237 | F18 | T388, T389, T390, T391 |
| FR-238 | F19 | T388, T389, T390, T391 |
| FR-239 | F20 | T400, T401, T402, T405 |
| FR-240 | F21 | T400, T401, T403, T405 |
| FR-241 | F22 | T400, T401, T404, T405 |
| FR-242 | F23 | T396, T397, T398, T399 |
| FR-243 | F24 | T396, T397, T398, T399 |
| FR-244 | F25 | T388, T389, T391 |
| FR-245 | F26 | T385, T386, T387 |
| SC-210 | FR-221 | T352, T356 |
| SC-211 | FR-222 | T343, T344, T348 |
| SC-212 | FR-228 | T359, T360, T361 |

Part 2 adds 108 tasks (T305 to T412; T407 to T412 sit inside P3); the file now holds 212 tasks. Every item F01 to F26 has at least one test task and one implementation task; every security item has a mutation-check task. Task numbers were kept stable when tasks were added.

## Part 2 dependencies and execution order

- **P1 to P6 run in order.** P1 touches `services/invitation.py`, `client.py`, `employee.py`, `auth.py` and the token repositories; P3 edits the same files again, so do not start P3 before P1's checkpoint.
- **Within P1** the five groups are independent of each other except that Group E (`lock_user`, login and refresh edits in `auth.py`) comes after Group D (also `auth.py`). Groups A to C may run in parallel by different people only if they avoid `invitation.py` (A) and `employee.py` (B, C); do B before C.
- **P2** needs P1 only for the revoke-all behaviour that migration 017's evidence rule reads; the two migrations are independent of each other (017 before 018 for numbering).
- **P3 Group A** (hashing) precedes **Group B** (throttle), because Group B's `login` builds on the released-connection path; **Group C** (delivery) is independent of A and B and may run in parallel with B except that both edit `core/database.py`'s neighbours (`error_handler.py`); do C after B. Group D closes P3.
- **P4** groups are independent; Group C edits `core/database.py` again, so it follows P3 Group C.
- **P5** has no dependencies beyond P4's checkpoint.
- **P6**: Group A first (it makes repeated runs safe), then B, C and D (D after B because both edit `services/employee.py` and `client.py`), then the refactors in Group E last, with the baseline taken first.

### Parallel opportunities (Part 2)

Inside a group, every test-writing task marked `[P]` can be written together. Across groups: P1 Group A with Group D; P3 Group C with the documentation task of Group D; P4 Groups A and B; P5 tasks F15 and F16.

### Implementation strategy (Part 2)

1. P1 first: it closes the account-takeover and session paths and needs no schema.
2. P2 next, then stop and report: two new migrations.
3. P3 is the largest and riskiest (it rewrites the sign-in path). Keep SC-207 green after every task.
4. P4 and P5 are small; P6 last because its contract-affecting items change existing endpoints and its refactors are safest once behaviour is pinned.
5. Open follow-up (not a task here): the deployment feature that adds the ingress must set `TRUSTED_PROXY_COUNT` and check it.
