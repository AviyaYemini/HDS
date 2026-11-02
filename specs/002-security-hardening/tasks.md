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

- [ ] T217 [P] [US1] Write `apps/api/tests/unit/test_settings_environment.py`: `ENVIRONMENT` unset fails; `development` and `test` accept the placeholder secret; `production`, `staging` and `qa` reject the placeholder and a secret shorter than 32 bytes; a non-development environment without a real `MAIL_BACKEND` fails; the error text names the setting and never contains the secret value
- [ ] T218 [P] [US1] Write `apps/api/tests/unit/test_logging_redaction.py`: secrets in `%`-style format arguments (tuple and dict) are redacted; email addresses are masked as `a***@domain`; no log record is produced with a message body
- [ ] T219 [P] [US1] Write `apps/api/tests/unit/test_mail_outbox.py`: an outbox line holds the masked recipient, subject, purpose, expiry and the body with the token replaced by `[REDACTED]`; the file never contains the raw token; the sender refuses to be built outside `development` and `test`
- [ ] T220 [P] [US1] Write `apps/api/tests/integration/test_no_token_in_logs.py` (SC-202): run invite, accept, reset request and reset confirm with `caplog` at DEBUG and the outbox backend; assert no log record contains a token or an unmasked address
- [ ] T221 [P] [US1] Write `apps/api/tests/unit/test_dev_issue_link.py`: the script refuses to run when `ENVIRONMENT=production`; in development it prints a working link once and writes it to no file and no log

### Implementation for User Story 1

> **Order matters**: do the environment updates first (conftest, startup test, CI), because once `ENVIRONMENT` is required every test fails until they are in place. Then do the config change.

- [ ] T222 [US1] In `apps/api/tests/conftest.py` set `ENVIRONMENT=test` and a test `JWT_SECRET_KEY` with `os.environ.setdefault` before any `hds` import, and make the shared `OutboxMailSender` accept the new `secret` keyword
- [ ] T223 [US1] Update `apps/api/tests/unit/test_startup_and_cors.py` for the required `ENVIRONMENT` (build `Settings` explicitly in each test)
- [ ] T224 [P] [US1] In `.github/workflows/ci.yml` set `ENVIRONMENT: test` and a non-secret test `JWT_SECRET_KEY` on the jobs that import the application and are not handled by T214: `test-unit`, `migration-check` and `contract` (`test-integration` already has it from T214)
- [ ] T225 [US1] In `apps/api/src/hds/core/config.py` make `ENVIRONMENT` required with no default; define development environments as exactly `development` and `test`; enforce start-up rules 1 to 3 of [contracts/configuration.md](contracts/configuration.md) (secret not the placeholder and at least 32 bytes; real `MAIL_BACKEND`); add settings `MAIL_BACKEND` (`outbox`) and `MAIL_OUTBOX_PATH`. Error messages name the setting, never the value. Make T217 pass
- [ ] T226 [P] [US1] In `apps/api/src/hds/core/logging.py` make `SensitiveDataFilter` redact string values in `record.args` (tuple and dict) and mask email addresses. Make T218 pass
- [ ] T227 [US1] In `apps/api/src/hds/core/mail.py` replace `LoggingMailSender` with `RedactingOutboxMailSender`: add an optional keyword `secret: str | None = None` to `MailSender.send`; the sender replaces every occurrence of `secret` in the body with `[REDACTED]` before writing the outbox line; it logs only the masked recipient and the subject. `get_mail_sender()` picks the backend from settings. Make T219 pass
- [ ] T228 [US1] In `apps/api/src/hds/services/invitation.py` pass the raw token as `secret=` in every `self._mailer.send(...)` call (`create_invitation`, `request_password_reset`) so the sender can redact it, and extend `apps/api/tests/unit/test_invitation_service.py` to assert that `secret` equals the emailed token. This lets the Story 1 outbox and log checks pass without waiting for Story 4
- [ ] T229 [P] [US1] Create `apps/api/src/hds/scripts/dev_issue_link.py` (`python -m hds.scripts.dev_issue_link <email> <invite|reset>`): refuses outside a development environment; issues a fresh link for the account and prints it once to standard output; stores it nowhere. Make T221 pass
- [ ] T230 [P] [US1] In `infra/docker-compose.yml` set `ENVIRONMENT: development` explicitly and replace the signing-secret default with `${JWT_SECRET_KEY:?set JWT_SECRET_KEY}` (no default)
- [ ] T231 [P] [US1] In the root `.env.example` set `ENVIRONMENT=development` and leave `JWT_SECRET_KEY=` empty with a comment `generate one with: openssl rand -hex 32` (FR-204)
- [ ] T232 [US1] Update the configuration tables and quick start in `apps/api/README.md`, `infra/README.md` and the root `README.md` (set `ENVIRONMENT`, generate the secret, redacted outbox, `dev_issue_link`) in the same change, and list the new scripts in the layout section of `apps/api/README.md`

**Checkpoint**: Story 1 passes on its own. MVP of the security fixes.

---

## Phase 4: User Story 2 — Invitations cannot bring back a deactivated account (Priority: P1)

**Goal**: an invitation completes only a pending account; links are cancelled when account state
changes (FR-205 to FR-207).

**Independent Test**: deactivate an account that holds an unused link, redeem it, confirm nothing
changes.

### Tests for User Story 2 (write first)

- [ ] T233 [P] [US2] Extend `apps/api/tests/unit/test_invitation_service.py`: inviting a deactivated address raises `ConflictError` with code `invitations.account_deactivated`; re-inviting a pending account sets the new role and replaces the earlier link; a Manager re-inviting a pending account as Admin is forbidden and the role is unchanged; accepting for an account that is not pending raises `auth.token_used`; accepting cancels the account's other links; a reset link is redeemable only for an active account
- [ ] T234 [P] [US2] Extend `apps/api/tests/unit/test_employee_manager.py` and `apps/api/tests/unit/test_client_service.py`: `deactivate` and `soft_delete` cancel the user's outstanding links; deactivating or deleting a client cancels the links of its portal users
- [ ] T235 [P] [US2] Write `apps/api/tests/integration/test_invitation_activation.py` on PostgreSQL covering the review's paths: redeem a leftover link for a deactivated account; a Manager invites a deactivated Admin's email; a portal user of a deactivated client redeems a link; a leftover link for an active user must not change the password or end sessions (SC-204)
- [ ] T236 [P] [US2] Write `apps/api/tests/integration/test_migration_014.py`: build rows at revision 013 (active, deactivated after use, pending with an unused invite link), upgrade to 014, assert `activated_at` is set for all but the pending one; the check constraint `ck_users_active_has_activated_at` rejects an active user with no `activated_at`; downgrade drops the constraint and the column
- [ ] T237 [P] [US2] Write `apps/api/tests/concurrency/test_concurrent_invite.py`: 50 rounds of two staff members inviting the same new address at the same moment; exactly one pending account exists and both calls succeed (spec Story 2 scenario 7)
- [ ] T238 [US2] Add tests that a pending account cannot be reactivated (not parallel: it edits the same unit-test file as T234): extend `apps/api/tests/unit/test_employee_manager.py` (reactivating an account whose `activated_at` is null raises `ConflictError` with code `users.pending_cannot_reactivate` and changes nothing; reactivating a deactivated account still works) and write `apps/api/tests/integration/test_reactivate_pending.py` (`POST /api/v1/employees/{id}/reactivate` for an invited, never-activated account returns 409 with that code, not the generic 422 `validation.constraint_violation`; the account stays pending and its invitation link still works)

### Implementation for User Story 2

- [ ] T239 [US2] In `apps/api/src/hds/models/user.py` add `activated_at` with this constraint verbatim: `timestamp with time zone, null`; set when an invitation is accepted and when staff create an account that is active from the start; null means the account has never been active; also declare `CheckConstraint("is_active = false OR activated_at IS NOT NULL", name="ck_users_active_has_activated_at")` on the model
- [ ] T240 [US2] Create `apps/api/alembic/versions/014_user_activation.py`: add `users.activated_at`; backfill `activated_at = created_at` for every user except those that are inactive, hold an invitation link and never used one; then add `ck_users_active_has_activated_at`, `CHECK (is_active = false OR activated_at IS NOT NULL)`, after the backfill; downgrade drops the constraint and then the column; docstring says what changes and why. `alembic check` must pass
- [ ] T241 [US2] In `apps/api/src/hds/scripts/seed_dev.py` set `activated_at` to the current time in the defaults of every seeded active user, so `ck_users_active_has_activated_at` holds and running the seed twice stays idempotent
- [ ] T242 [P] [US2] Set `activated_at` to the current time on every `User(...)` that the tests build directly, so the new check constraint holds: `apps/api/tests/conftest.py`, `apps/api/tests/integration/conftest.py`, `apps/api/tests/integration/test_email_uniqueness.py`, `apps/api/tests/integration/test_employee_filters.py`, `apps/api/tests/performance/test_smoke_performance.py`; run each file's tests to confirm
- [ ] T243 [US2] In `apps/api/src/hds/repositories/user.py` make `create` set `activated_at` when `is_active` is true and add `mark_activated(user_id, org_id)`; in `apps/api/src/hds/repositories/invitation_token.py` add `cancel_for_user(user_id)` and `cancel_for_users(user_ids)` that mark outstanding links used
- [ ] T244 [US2] Implement the pending-account rules in `apps/api/src/hds/services/invitation.py` per [data-model.md](data-model.md) (account states): create, replace-link and role update for pending, duplicate for active, `invitations.account_deactivated` for deactivated; accept only for pending, reset only for active; cancel other links on accept. Run the insert of a new pending account inside a savepoint (`begin_nested()`); on a unique violation roll back the savepoint, reload the account by address and continue with the existing-account rules, so a concurrent invitation succeeds (spec Story 2 scenario 7). Update the method docstrings (rules, error codes, side effects)
- [ ] T245 [US2] Give `EmployeeService` and `ClientService` an `InvitationTokenRepository` and cancel links in `deactivate`, `soft_delete`, `edit_client` (deactivation) and client `soft_delete`; also make `deactivate` and the bulk deactivation of client users set `activated_at = COALESCE(activated_at, now)` in the same update; wire the new constructor argument in `apps/api/src/hds/routers/employees.py`, `apps/api/src/hds/routers/clients.py` and `apps/api/src/hds/routers/me.py`
- [ ] T246 [US2] In `apps/api/src/hds/services/employee.py` make `reactivate` refuse an account whose `activated_at` is null: raise `ConflictError(code="users.pending_cannot_reactivate", ...)` before any write (FR-220); update its docstring and the `description` of `POST /api/v1/employees/{id}/reactivate` in `apps/api/src/hds/routers/employees.py`
- [ ] T247 [P] [US2] Update the existing constructor calls of `EmployeeService` and `ClientService` to pass a fake `InvitationTokenRepository`: `apps/api/tests/unit/test_employee_manager.py`, `apps/api/tests/unit/test_employee_self_service.py` (6 calls) and `apps/api/tests/unit/test_client_service.py`
- [ ] T248 [P] [US2] Add `invitations.account_deactivated` and `users.pending_cannot_reactivate` to `apps/api/src/hds/locales/en.json` and `he.json` (English and Hebrew text for each)
- [ ] T249 [US2] Update the `description` text of `POST /auth/invite`, `/auth/invite/accept` and `/auth/password-reset/confirm` in `apps/api/src/hds/routers/invitations.py` and the matching section of `docs/hds-api.md`

**Checkpoint**: Stories 1 and 2 pass independently.

---

## Phase 5: User Story 3 — Sessions and one-time links are single-use (Priority: P1)

**Goal**: at most one success per credential or link, even under simultaneous use; replay after
the grace window ends all sessions (FR-208, FR-209).

**Independent Test**: two simultaneous refreshes, 100 rounds, exactly one success each; a replay
outside the window revokes every token of the user.

### Tests for User Story 3 (write first)

- [ ] T250 [P] [US3] Write `apps/api/tests/concurrency/test_refresh_single_use.py`: 100 rounds of two simultaneous refreshes with one token; exactly one succeeds each round and the loser does not revoke other tokens (SC-203)
- [ ] T251 [P] [US3] Write `apps/api/tests/concurrency/test_link_single_use.py`: 100 rounds of two simultaneous invitation acceptances and two simultaneous reset confirmations; exactly one succeeds each round
- [ ] T252 [P] [US3] Write `apps/api/tests/integration/test_refresh_reuse.py`: reuse after the grace window revokes all of the user's tokens; reuse within the window is refused and keeps sessions (set `REFRESH_REUSE_GRACE_SECONDS` to 1 and wait); reuse of a token revoked by logout is refused with no revoke-all; a legacy token with no reason is refused with no revoke-all; a token revoked with reason `account` (user deactivated) is refused with no other effect
- [ ] T253 [P] [US3] Extend `apps/api/tests/unit/test_auth_service.py` with the decision table of research D3 using fake repositories
- [ ] T254 [P] [US3] Write `apps/api/tests/integration/test_migration_015.py`: the check constraint accepts `rotated`, `logout`, `all`, `account` and null and rejects any other value; downgrade drops the column

### Implementation for User Story 3

- [ ] T255 [US3] In `apps/api/src/hds/models/refresh_token.py` add `revoked_reason` with this constraint verbatim: `short text, null. One of rotated, logout, all, account. Null for tokens revoked before this change`, plus a check constraint for those values
- [ ] T256 [US3] Create `apps/api/alembic/versions/015_refresh_revoked_reason.py` (add column and check constraint; reversible; docstring says what and why)
- [ ] T257 [US3] Add `REFRESH_REUSE_GRACE_SECONDS: int = 10` to `apps/api/src/hds/core/config.py` (must be positive)
- [ ] T258 [US3] In `apps/api/src/hds/repositories/refresh_token.py` add `revoke_if_live(token_id, reason) -> bool` implemented as `UPDATE ... WHERE id = :id AND revoked_at IS NULL` returning whether a row changed; give `revoke_all_for_user` a `reason` argument; replace the unguarded `revoke`
- [ ] T259 [P] [US3] In `apps/api/src/hds/repositories/invitation_token.py` replace `mark_used` with `spend(id) -> bool` implemented as `UPDATE ... SET used_at = now WHERE id = :id AND used_at IS NULL` returning whether a row changed
- [ ] T260 [US3] Rewrite `AuthService.refresh` in `apps/api/src/hds/services/auth.py` per research D3: guarded rotation; on a lost race re-read and decide (rotated and outside the window: `revoke_all_for_user(reason="all")`, call the repository's explicit `commit()`, then refuse; otherwise refuse with no effect). `logout` uses reason `logout`, `logout_all` uses `all`. Update the docstrings
- [ ] T261 [US3] Make `apps/api/src/hds/services/invitation.py` redeem links through `spend()` (a false result means refused as used) and make `apps/api/src/hds/services/employee.py` pass reason `account` when revoking tokens on deactivate, delete and role change; reset passes `all`
- [ ] T262 [US3] Write `docs/adr/006-refresh-token-reuse-detection.md` (context, decision, grace window, consequences) and add it to the ADR table in `docs/README.md`

**Checkpoint**: all three P1 stories pass. Highest-risk security work is done.

---

## Phase 6: User Story 4 — A change either fully happens or does not happen (Priority: P2)

**Goal**: multi-step actions are all-or-nothing and email is sent only after commit (FR-210).

**Independent Test**: force a failure in the last step of each action and confirm no earlier step
took effect.

### Tests for User Story 4 (write first)

- [ ] T263 [P] [US4] Write `apps/api/tests/integration/test_atomicity.py` on PostgreSQL with failure injection in the last step: refresh (old token still valid, no new token), password reset (old password, link unspent, sessions intact), client update with portal-user deactivation (users unchanged), role change with token revocation (role and tokens unchanged)
- [ ] T264 [P] [US4] Write `apps/api/tests/integration/test_mail_after_commit.py`: an invitation whose transaction fails sends no email and leaves no usable link; a successful one sends exactly one email, after the commit

### Implementation for User Story 4

- [ ] T265 [US4] Make `InvitationService` queue every email through `DeferredMailer`: build `DeferredMailer(db, mailer)` in `apps/api/src/hds/routers/invitations.py`; update `apps/api/src/hds/services/invitation.py` docstrings (email is sent after commit)
- [ ] T266 [US4] Review the order of steps in `apps/api/src/hds/services/auth.py` (`refresh`), `apps/api/src/hds/services/invitation.py` (`reset_password`) and `apps/api/src/hds/services/client.py` (`edit_client`) so each is a single unit of work with no step that needs an earlier commit; fix any that do
- [ ] T267 [US4] Write `docs/adr/005-request-scoped-transactions.md` (decision D1, the explicit-commit exception, `scope="function"`, alternatives) and add it to the ADR table in `docs/README.md`

**Checkpoint**: Story 4 passes; D1 is documented.

---

## Phase 7: User Story 5 — The last Admin survives simultaneous actions (Priority: P2)

**Goal**: at least one active Admin always remains (FR-211).

**Independent Test**: two Admins deactivate each other at once, 100 rounds, an active Admin remains
each time.

- [ ] T268 [P] [US5] Write `apps/api/tests/concurrency/test_last_admin.py`: 100 rounds of two simultaneous deactivations between the only two Admins, plus simultaneous demotions; exactly one action succeeds and an active Admin remains (SC-205)
- [ ] T269 [P] [US5] Extend `apps/api/tests/unit/test_employee_manager.py`: the last-Admin guard counts the ids returned by the locking query and raises `users.last_admin` at one
- [ ] T270 [US5] In `apps/api/src/hds/repositories/user.py` add `lock_active_admins(org_id) -> list[UUID]`: `SELECT id ... WHERE organization_id = :org AND role = 'admin' AND is_active AND NOT is_deleted ORDER BY id FOR UPDATE`; document that the lock lasts until the unit of work ends
- [ ] T271 [US5] Make `_guard_last_admin` in `apps/api/src/hds/services/employee.py` use `lock_active_admins` instead of `count_active_admins`; remove the unused count method if nothing else calls it

**Checkpoint**: Story 5 passes.

---

## Phase 8: User Story 6 — Sign-in lockout matches the rule (Priority: P2)

**Goal**: lock after exactly N failures; the count restarts when the lock expires (FR-212, FR-213).

**Independent Test**: N-1 failures then success; N failures then locked; after expiry N attempts.

- [ ] T272 [P] [US6] Write `apps/api/tests/integration/test_lockout.py` on PostgreSQL for limits 2 to 10: N-1 failures then the correct password signs in; N failures then the next attempt returns `auth.account_locked`; after the lock is expired by setting `locked_until` in the past, N new attempts are allowed and the account locks only after the N-th (SC-206)
- [ ] T273 [P] [US6] Extend `apps/api/tests/unit/test_auth_service.py`: the service uses the count returned by the repository and does not add one itself; an expired lock is cleared before the password check
- [ ] T274 [US6] In `apps/api/src/hds/repositories/user.py` add `register_failed_login(user_id, max_failures, lock_until) -> tuple[int, datetime | None]` as one `UPDATE ... SET failed_login_attempts = failed_login_attempts + 1, locked_until = CASE WHEN failed_login_attempts + 1 >= :max THEN :until ELSE locked_until END ... RETURNING` with session synchronisation off, followed by the explicit `commit()`; add `clear_expired_lock(user_id, now)` as `UPDATE ... SET failed_login_attempts = 0, locked_until = NULL WHERE id = :id AND locked_until IS NOT NULL AND locked_until <= :now`. Remove `increment_failed_attempts` and `set_locked_until`
- [ ] T275 [US6] Rewrite the failure path of `AuthService.login` in `apps/api/src/hds/services/auth.py`: clear an expired lock first, then check the lock, then verify; use `register_failed_login` and its returned count; delete the `user.failed_login_attempts + 1` line. Update the docstring (lock after N, restart on expiry)

**Checkpoint**: Story 6 passes.

---

## Phase 9: User Story 7 — Sign-in does not reveal which accounts exist (Priority: P2)

**Goal**: equal work for unknown and known emails (FR-214).

- [ ] T276 [P] [US7] Write `apps/api/tests/unit/test_login_equal_work.py`: with a spy on the password check, an unknown email, a known email with a wrong password and an inactive account each cause exactly one check, and all return the same code and message (this is the automated check of SC-207)
- [ ] T277 [P] [US7] Write `apps/api/tests/integration/test_login_timing.py` (SC-207): 500 samples each for unknown and known emails with a wrong password, compared with a statistical test; mark it `timing`, register the marker in `apps/api/pyproject.toml` and deselect it by default; it is opt-in and is run by hand in the final phase before a release
- [ ] T278 [US7] In `apps/api/src/hds/core/security.py` create `DUMMY_PASSWORD_HASH` at import with the same Argon2id parameters as real hashes and a `verify_against_dummy(plain)` helper; docstring explains why
- [ ] T279 [US7] In `AuthService.login` (`apps/api/src/hds/services/auth.py`) call `verify_against_dummy` when no account matches, then return the same `auth.invalid_credentials`

**Checkpoint**: Story 7 passes.

---

## Phase 10: User Story 8 — Email addresses ignore letter case (Priority: P2)

**Goal**: one address, one account, whatever the case (FR-215, FR-216).

- [ ] T280 [P] [US8] Write `apps/api/tests/unit/test_email_type.py`: the `Email` type trims and lowercases; invalid addresses are still rejected; the generated OpenAPI schema for the field is unchanged (`format: email`)
- [ ] T281 [P] [US8] Write `apps/api/tests/integration/test_email_case.py`: sign in with a different case; create a duplicate with a different case (409 `users.duplicate_email`); request a reset with a different case and receive the email; invite with a different case (SC-208)
- [ ] T282 [P] [US8] Write `apps/api/tests/integration/test_migration_016.py`: data with two addresses that differ only by case makes the migration stop with a message listing both; clean data is lowercased and the old index is replaced by `uq_users_email_ci`; downgrade restores `uq_users_email`
- [ ] T283 [P] [US8] Write `apps/api/tests/unit/test_find_email_case_duplicates.py` for the listing script (read-only; prints nothing when clean)
- [ ] T284 [US8] Add the `Email` type to `apps/api/src/hds/core/types.py`: an `EmailStr` with an after-validator that trims and lowercases, so OpenAPI is unchanged
- [ ] T285 [US8] Use `Email` instead of `EmailStr` in the request models of `apps/api/src/hds/routers/auth.py`, `invitations.py`, `employees.py`, `clients.py` (user requests only; contact emails of clients are not account identifiers)
- [ ] T286 [US8] In `apps/api/src/hds/repositories/user.py` lowercase the address in `get_by_email` and `create` as a second line of defence for non-HTTP callers
- [ ] T287 [US8] In `apps/api/src/hds/models/user.py` replace the `uq_users_email` index with `uq_users_email_ci` with this constraint verbatim: `unique on lower(email) where is_deleted = false`; `alembic check` must agree with migration 016
- [ ] T288 [US8] Create `apps/api/alembic/versions/016_case_insensitive_email.py`: first find addresses that differ only by case and, if any, raise with a message that lists them and tells staff to resolve them (nothing is merged or deactivated); otherwise lowercase all addresses, drop `uq_users_email`, create `uq_users_email_ci`; downgrade recreates `uq_users_email` and its docstring states that the lowercasing is not reversed
- [ ] T289 [P] [US8] Create `apps/api/src/hds/scripts/find_email_case_duplicates.py` (read-only; lists clashing addresses and exits non-zero if any)
- [ ] T290 [US8] Confirm the duplicate checks in `services/employee.py`, `services/client.py` and `services/invitation.py` now match case-insensitively through `get_by_email`; add a unit test per service if one is missing

**Checkpoint**: Story 8 passes.

---

## Phase 11: User Story 9 — Site time zones are real time zones (Priority: P3)

**Goal**: only loadable IANA zone names are stored (FR-217, FR-218).

- [ ] T291 [P] [US9] Extend `apps/api/tests/unit/test_site_service.py`: `Asia/Jerusalem` and `UTC` and `Asia/Kolkata` (a zone without daylight saving) accepted on create and edit; `Mars/Base`, an empty string and a name with path characters raise `DomainValidationError` with code `sites.timezone_invalid`
- [ ] T292 [P] [US9] Write `apps/api/tests/integration/test_site_timezone.py`: `POST` and `PATCH /api/v1/sites` return 422 with code `sites.timezone_invalid` for invalid zones and store nothing (SC-209)
- [ ] T293 [US9] In `apps/api/src/hds/services/site.py` add the zone check (`ZoneInfo(name)`; any failure raises `sites.timezone_invalid`) to `create_site` and `edit_site`; update docstrings
- [ ] T294 [P] [US9] Add `sites.timezone_invalid` to `apps/api/src/hds/locales/en.json` and `he.json`
- [ ] T295 [P] [US9] Create `apps/api/src/hds/scripts/check_site_timezones.py` (read-only; lists sites and organisations with an invalid zone; FR-218) with a test in `apps/api/tests/unit/test_check_site_timezones.py`

**Checkpoint**: all nine stories pass.

---

## Phase 12: Polish & Cross-Cutting

- [ ] T296 [P] Update `docs/hds-api.md` (error table, sign-in, refresh, invitation and site sections) to match [contracts/error-codes.md](contracts/error-codes.md)
- [ ] T297 [P] Update `docs/README.md` ADR table (005 and 006 present) and the ADR text if any decision changed during implementation
- [ ] T298 Final docstring pass over `apps/api/src/hds/`: every changed or added public symbol states rules, error codes and side effects; `uv run interrogate src` stays at 100%; every endpoint whose behaviour changed has an updated `description`
- [ ] T299 Run every gate from `apps/api/` and the full suite on PostgreSQL, including `apps/api/tests/concurrency/`, and the OpenAPI drift check against `specs/001-foundation-auth-org/contracts/openapi.yaml`; the generated document may differ from the pre-feature baseline only in descriptions
- [ ] T300 Run `apps/api/tests/concurrency/` three times in a row to catch flaky timing; fix or tighten any test that fails once
- [ ] T301 Run the validation steps of `specs/002-security-hardening/quickstart.md` end to end (start-up refusals, scripts, migrations 014 to 016 on a copy of seeded data) and record the result in the pull request; also run `uv run pytest -m timing` by hand and record that the timing comparison (SC-207) found no difference

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
- US4: T263 with T264. US5: T268 with T269. US6: T272 with T273. US7: T276 with T277.
- US8: T280 to T283 together; T289.
- US9: T291, T292, T294, T295.
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
| FR-210 | T204 to T216, T263, T264, T265, T266 |
| FR-211 | T268, T269, T270, T271 |
| FR-212 | T272, T273, T274, T275 |
| FR-213 | T272, T273, T274, T275 |
| FR-214 | T276, T278, T279 |
| FR-215 | T280, T281, T284, T285, T286, T290 |
| FR-216 | T282, T283, T288, T289 |
| FR-217 | T291, T292, T293 |
| FR-218 | T295 |
| FR-219 | every test task above |
| FR-220 | T238, T246, T248 |
| SC-201 | T217 |
| SC-202 | T220 |
| SC-203 | T250, T251 |
| SC-204 | T235, T236 |
| SC-205 | T268 |
| SC-206 | T272 |
| SC-207 | T276 (automated), T277 (opt-in, run by hand in T301) |
| SC-208 | T281, T282 |
| SC-209 | T292, T295 |

101 tasks (T201 to T301). Every requirement FR-201 to FR-220 and every success criterion SC-201 to
SC-209 has at least one implementing task and one test task.

## Notes

- [P] tasks touch different files and have no dependency on an unfinished task.
- A failing test must be seen to fail before its implementation task starts (FR-219).
- Stop at any checkpoint to validate a story on its own.
- Avoid: two people editing `services/auth.py`, `services/invitation.py` or `repositories/user.py` at the same time.
