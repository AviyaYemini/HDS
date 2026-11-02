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
