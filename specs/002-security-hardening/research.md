# Research: Security Hardening of Sign-In, Tokens and Account Rules

Each decision answers one technical question the spec leaves open. "Finding" numbers refer to the
2026-11-02 code review; "Story" numbers refer to [spec.md](spec.md).

## D1. Who owns the transaction? (Finding 9, Story 4; base for D2, D3, D10)

**Decision**: One transaction per request, owned by a unit of work in `core`.

- Repositories stop calling `commit()`. They `flush()` where they need generated values.
- A new dependency wraps the session: commit once after the handler returns, roll back on any
  exception. It is declared with `Depends(..., scope="function")`, which ends before the response
  is sent. The option first appears in FastAPI 0.121.0 (checked by installing 0.120.4, which lacks
it, and 0.121.0, which has it, and inspecting the `Depends` signature; the changelog was not
used). The lower bound in `pyproject.toml` is raised to 0.121.0; 0.162 is installed today. A failed commit therefore produces an
  error response, never a success that was not saved.
- The tests already replace the plain session dependency (`get_db`). The new unit-of-work
  dependency is built on top of `get_db`, so the same override keeps working and the commit and
  rollback logic is exercised by the integration suite without changing how tests provide a
  session.
- **After-commit actions**: services never send email directly. A deferred mailer queues the send
  on the session; the unit of work runs the queue only after a successful commit. This is what
  guarantees "no usable link for a rolled-back change" (spec edge case). A send that fails after
  commit is logged (no token, masked address) and does not fail the request, because the change
  is already saved.
- **Effects that must survive a refusal**: two writes must persist even though the request then
  fails with a domain error: the failed-sign-in counter (D7) and the revoke-everything response to
  a replayed refresh token (D3). They call an explicit, documented `commit()` on the repository
  before raising. Nothing else may.

**Rationale**: it is the smallest change that makes every multi-step action atomic, and the
review's fix list ("the service or request owns the transaction") asks for exactly this. Doing it
first means D2, D3 and D10 can rely on it.

**Alternatives considered**:
- *Service-owned `async with uow:` blocks.* More explicit, but every service constructor and all
  46 route handlers change shape, and a forgotten block silently loses writes.
- *Commit in an HTTP middleware.* Middleware cannot see which handler errors are domain errors and
  runs outside FastAPI's dependency scope; harder to test.
- *Roll back only on unexpected errors, commit on domain errors.* Rejected: a domain error raised
  after a partial write would persist half an action.

## D2. Single use of refresh tokens and one-time links (Finding 4, Story 3)

**Decision**: Spend a credential with one guarded statement and trust its row count.

- Refresh tokens: `UPDATE ... SET revoked_at = now, revoked_reason = 'rotated' WHERE id = :id AND
  revoked_at IS NULL`. If no row changes, someone else spent it first.
- Invitation and reset links: `UPDATE ... SET used_at = now WHERE id = :id AND used_at IS NULL`,
  same rule.
- The redeeming change (new password, activation) happens in the same transaction (D1), so a link
  is never spent without its effect, or the reverse.

**Rationale**: a check followed by a separate update leaves a gap in which two requests both pass
the check. A guarded update closes it in the database without needing locks in application code.

**Alternatives considered**: `SELECT ... FOR UPDATE` then update (also correct, but it holds a row
lock across several steps and adds a round trip); an application-level mutex (does not work across
processes).

## D3. Replay of a spent refresh token, with a grace window (Finding 4, Story 3, clarified)

**Decision**: Record why a token was revoked and use that to decide the response.

- New column `refresh_tokens.revoked_reason` with values `rotated`, `logout`, `all`, `account`
  (null for tokens revoked before this change).
- When the guarded update in D2 changes no row, the service re-reads the token:
  - reason `rotated` and revoked **more than** `REFRESH_REUSE_GRACE_SECONDS` ago (default 10):
    revoke all of the user's tokens (reason `all`), commit that, then refuse (FR-209).
  - reason `rotated` and within the window: refuse, change nothing (an honest retry or the loser
    of a simultaneous pair).
  - any other reason, or null: refuse, change nothing. Using a token after sign-out is not
    treated as theft.
- The grace window also covers the loser of two simultaneous renewals, so "exactly one wins"
  (SC-203) does not log the user out.

**Rationale**: matches the clarified rule and the foundation's own design note (reuse revokes
everything), while not punishing a lost response on a weak connection.

**Alternatives considered**: strict, no window (logs honest users out); refuse only (today's
behaviour, leaves a stolen session alive); a "replaced by" pointer chain per token (more precise
about which token replaced which, but more schema and logic than the requirement needs).

## D4. Pending accounts and outstanding links (Finding 1, Story 2)

**Decision**: Make the pending state explicit and cancel links when state changes.

- New nullable column `users.activated_at`. An account is **pending** when `activated_at` is null,
  `is_active` is false and it is not deleted. It is set when an invitation is accepted and when
  staff create an account that is active from the start.
- Redeeming an invitation requires a pending account; otherwise it is refused with the same answer
  as a spent link (`auth.token_used`), so an anonymous caller learns nothing about the account's
  state. Redeeming a reset link requires an active account.
- Inviting an address: no account creates a pending one; a pending one takes the new role under the
  normal permission rules and gets a new link that replaces the old one; an active one is a
  duplicate; a deactivated one is refused with the new code `invitations.account_deactivated`.
- Cancelling means marking outstanding links used (`used_at`). It happens when an account is
  deactivated or deleted (including client users deactivated with their client), when an
  invitation is accepted, and when a link is replaced.
- **Backfill** (migration 014): `activated_at = created_at` for every existing user except those
  that are inactive, have an invitation link, and have never used one. Those remain pending.
- **Invariant enforced by the database**: after the backfill, migration 014 adds
  `CHECK (is_active = false OR activated_at IS NOT NULL)`, so an active account can never lack an
  activation time. Without it, any writer that forgot to set the column (the seed script, a test
  fixture, a future code path) would create an account that looks *pending* once deactivated,
  which is the original defect. Deactivation also sets `activated_at` when it is still null, as
  a second line of defence.
- **Reactivating a pending account**: `EmployeeService.reactivate` checks `activated_at` before
  writing. A pending account (null `activated_at`) is refused with the dedicated 409 code
  `users.pending_cannot_reactivate`. Without this check the new database constraint would reject
  the write and the caller would see the generic constraint answer (422), which hides the cause.
- **Concurrent invitations**: two simultaneous invitations for a new address both try to insert
  it. The insert runs inside a savepoint; the loser catches the unique violation, rolls back the
  savepoint, reloads the account by address and continues with the existing-account rules (a
  pending account is re-invited). Neither request fails.

**Rationale**: "was this account ever active?" cannot be derived from today's columns, and the
review's attack paths all come from not knowing it.

**Alternatives considered**: infer pending from a placeholder password (fragile); a separate
`status` enum (a larger change touching every query that tests `is_active`).

## D5. Start-up safety for secrets and environment (Findings 3 and 2, Story 1)

**Decision**: Fail closed on the environment.

- `ENVIRONMENT` has no default. Only `development` and `test` are treated as development;
  `production`, `staging` and any other value get the strict checks (secret not the placeholder, at
  least 32 bytes; real mail sender).
- The compose file sets `ENVIRONMENT` explicitly and no longer defaults the signing secret
  (`${JWT_SECRET_KEY:?...}` fails when it is missing or empty). The example settings file ships an
  empty secret with instructions to generate one, so nothing in the repository is a usable secret
  (FR-204).
- CI and the test suite set `ENVIRONMENT=test` explicitly.

**Rationale**: the review's finding is that the check is skipped unless someone remembers to turn
it on. Making the safe behaviour the default and the unsafe one explicit reverses that.

**Alternatives considered**: keep `development` as the default and only document it (the original
defect); detect production from other hints such as hostnames (unreliable).

## D6. Mail delivery and logging (Finding 2, Story 1)

**Decision**: Nothing that persists a message may hold a link or token in clear. The development
sender writes a redacted record to an outbox file; a separate development-only action shows a
working link once; production refuses to start without a real sender.

- New setting `MAIL_BACKEND`. Value `outbox` appends one JSON line per message to
  `MAIL_OUTBOX_PATH` (default under a git-ignored folder) holding the masked recipient, subject,
  purpose, expiry and the body with the token replaced by `[REDACTED]`. Its constructor refuses to
  run unless the environment is development or test. The log gets only the masked address and the
  subject.
- A developer who needs a real link for a manual test runs a script
  (`hds.scripts.dev_issue_link`, refuses to run outside development and test) that issues a fresh
  link for a given account and prints it to the terminal once. It writes it to no file and no log.
  Automated tests keep using the in-memory test double, which never persists anything.
- Sending code receives the real body only inside the sender call; redaction happens in the
  sender, so no caller can forget it.
- No real provider exists yet, and choosing one is out of scope (spec). So in production the
  settings check fails with a clear message until a provider feature adds a backend. This is a
  deliberate consequence: there is no production deployment yet (feature 05).
- The log filter also scans format arguments (it only scanned the message text) and masks email
  addresses.

**Rationale**: the requirement is that links are never stored in clear. A redacted outbox still
shows that a message was produced and to whom; the script covers the one legitimate need to see a
link, without leaving it on disk.

**Alternatives considered**: a clear-text outbox file in development (rejected: it is a copy of
account-access links on disk, and the habit carries into other environments); logging the link in
development only (still a log, and easy to enable by mistake in production); returning the link in
the API response in development (changes the contract); encrypting the outbox (key management for
a development convenience).

## D7. Lockout counted in the database (Finding 5, Story 6)

**Decision**: Increment and decide in one statement.

- A single `UPDATE users SET failed_login_attempts = failed_login_attempts + 1, locked_until =
  CASE WHEN failed_login_attempts + 1 >= :max THEN :until ELSE locked_until END ... RETURNING ...`
  with session synchronisation switched off, so the in-memory user object is not changed behind
  the service's back (the cause of the off-by-one). It commits itself (D1 exception).
- **Reset on expiry** (decided at planning, superseding the earlier clarification): at the start of
  each sign-in, before the password is checked, one guarded statement clears the count and the lock
  when the lock has expired (`... SET failed_login_attempts = 0, locked_until = NULL WHERE id = :id
  AND locked_until IS NOT NULL AND locked_until <= :now`). A successful sign-in also clears the
  count. After a cooldown the user therefore has the full N attempts again (FR-213).
- An integration test on PostgreSQL covers N-1 and N failures for several limits, and the full N
  attempts after expiry (SC-206).

**Rationale**: one atomic statement is correct under concurrent failures as well, which the
current read-modify-write is not.

**Alternatives considered**: keep the two-step flow and subtract the extra one (hides the real
bug); a Python-side counter in a cache (new infrastructure).

## D8. Equal work for unknown and known emails (Finding 6, Story 7)

**Decision**: When no account matches, verify the password against a fixed dummy Argon2id hash
created at start-up with the same parameters as real hashes, then return the same error.

**Rationale**: the cost of Argon2id dominates the timing; doing it in both paths removes the
difference without changing responses. The locked-account notice remains the accepted exception
(FR-005).

**Testing note**: a statistical timing test is noisy in CI. The gating test asserts that the hash
function is called exactly once on every path (deterministic). A separate marked test compares
timing distributions and runs on demand.

**Alternatives considered**: add a random sleep (adds latency without equalising work); rate
limiting (useful, out of scope).

## D9. Case-insensitive emails (Finding 7, Story 8)

**Decision**: Normalise on input and enforce in the database.

- A shared `Email` type strips and lowercases at the schema edge; repositories also lowercase the
  value they search for and store, as a second line of defence for non-HTTP callers. The OpenAPI
  schema for the field is unchanged (still an email string).
- Migration 016 first looks for accounts whose addresses differ only by case. If it finds any, it
  stops with a message listing them (clarified: staff resolve them; nothing is merged
  automatically). Otherwise it lowercases all addresses, drops the old unique index and creates a
  unique index on `lower(email)` for non-deleted users.
- A script lists the clashes before the migration is run.
- Downgrade restores the previous index. The lowercasing of stored addresses is not reversed; this
  is the one irreversible data step and is documented in the migration.

**Rationale**: storing lowercase makes ordinary lookups simple and indexed; the functional index
keeps the rule true even if some future writer forgets to normalise.

**Alternatives considered**: `citext` extension (a new database extension to install and
operate); compare with `lower()` in queries only (no stored guarantee, slower lookups).

## D10. Last-Admin guard under concurrency (Finding 8, Story 5)

**Decision**: Lock the organisation's active Admin rows for the rest of the transaction before
counting (`SELECT id ... FOR UPDATE`, ordered by id so two requests lock in the same order). Count
the locked rows. Row locks were chosen over serializable transactions (below). This depends on D1, because the lock lasts until commit.

**Rationale**: under PostgreSQL's default isolation, the second request waits for the first to
commit and then re-evaluates the rows, so it sees one Admin and is refused. A concurrency test
with two sessions proves it.

**Alternatives considered**: serialisable isolation for the whole request (broader than needed and
needs retry logic); a database trigger that rejects the last-Admin removal (strong, but moves a
business rule into a trigger and duplicates the service check).

## D11. Site time zone validation (Finding 10, Story 9)

**Decision**: A site's time zone is valid if the IANA time zone database can load it
(`ZoneInfo(name)`, which reads the system copy of the IANA database and falls back to the
`tzdata` package), checked in the site service so the error carries a stable code
(`sites.timezone_invalid`, HTTP 422). The package `tzdata` is added as a dependency so zone data
exists even on systems that ship none.

**Rationale**: the future scheduler will load zones the same way, so "valid" means exactly "will
load". Measured on this machine: 598 zone names are listed on the development host and 486 inside
the slim container image, so a fixed list would disagree with the environment that actually runs;
loading the zone does not.

A script reports sites (and organisations) already holding an invalid zone (FR-218); it changes
nothing.

**Alternatives considered**: a hard-coded list (drifts); checking against
`available_timezones()` at validation time (set differs per environment); accepting any string
(the defect).
