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

## D11. Site time zone validation (amended by D21: the zone list now comes from the tzdata package) (Finding 10, Story 9)

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


---

# Part 2: PR 4 fixes (D12 to D26)

Decisions for the 26 items of [pr4-fix-list.md](pr4-fix-list.md), after the clarifications of
2026-11-03. Each states the decision, the reason and what was rejected. D11 is amended by D21.

## D12. Completing a reset (F01, F11)

**Decision**: `reset_password` spends the presented link first (as now), then in the same
transaction cancels **every other outstanding link** of the account (reset and invitation),
revokes all refresh tokens (reason `all`), and clears the failure count and the lock. Issuing a new
reset link first cancels the account's earlier **unused reset** links only. `cancel_for_user`
gains an optional `purpose` argument.

**Rationale**: a reset means the owner regained control, so every earlier credential is stale
(F01). The owner proved control of the mailbox, so a lockout from earlier guessing should not
stand (F11; proposed default, reversible). Cancelling only reset links on a new request keeps a
pending invitation alive, which is a different flow.

**Alternatives**: cancel nothing and rely on the 60-minute expiry (the defect); cancel all
purposes on request (would destroy a pending account's invitation).

## D13. Portal users when a client changes (F02)

**Decision**: `ClientService` receives the refresh-token repository. Deactivating or deleting a
client, and removing a portal user, revoke each affected user's tokens with reason `account` and
cancel their links (FR-207). `EmployeeService.reactivate` refuses a Client-role user whose client
is inactive or deleted, with a new translated code `users.client_inactive` (409). That needs the
client repository in `EmployeeService`.

**Rationale**: the same guarantee employees already have. The refusal closes the path in which a
Manager reactivates a portal user under a dead client.

**Alternatives**: revoke only on deactivation (misses delete and removal); allow reactivation and
let the client check happen at sign-in (the account would look active in the list).

## D14. Pending accounts and deactivation (F03, clarification Q2)

**Decision**: `EmployeeService.deactivate` refuses a pending account with a dedicated translated
409 (`users.pending_cannot_deactivate`); the account and its link are untouched. Client
deactivation skips pending portal users. `UserRepository` and `ClientRepository` stop stamping
`activated_at` with `coalesce(...)` on deactivation: **only activation writes it**. A guard test
asserts that no code path other than accept-invitation, create-active and migration backfill sets
`activated_at`.

**Rationale**: FR-220 says only accepting an invitation activates a pending account; a stamp on
deactivation turns "never activated" into "was activated".

**Alternatives**: keep pending and cancel the link (adds a state nobody can see); a new
"withdrawn" state (schema and UI cost for no requirement).

## D15. Password hashing off the event loop (F04)

**Decision**: new `core/passwords.py` exposes `async` `hash_password`, `verify_password` and
`verify_against_dummy` that run the synchronous Argon2 call with `asyncio.to_thread` behind an
`asyncio.Semaphore(HASH_CONCURRENCY)` (setting, default 2, at least 1). `core/security.py` keeps the
synchronous core for scripts and the seed. The four services `await` the new functions. The
work-counting tests wrap the synchronous core, so one verification per refusal is still counted.

**Rationale**: measured 36 ms per check and 724 ms of stalled loop for 20 concurrent unknown-address
sign-ins. Threads release the loop (argon2-cffi releases the GIL while hashing). The bound keeps
the machine from running more hashes than it has cores, which also protects memory (64 MiB each).

**Alternatives**: a process pool (more memory, start-up and pickling for no gain); a smaller
cost parameter (weakens stored hashes); unbounded threads (an attacker chooses the memory use).

## D16. Sign-in rate limit (FR-221; F04, clarification Q4, Q5)

**Decision**: new table `login_attempts` (migration 018), one row per **failed** sign-in:
`id`, `kind` (`address` or `network`), `key_hash` (SHA-256 of the lowercased submitted address, or
of the client network address), `attempted_at` (UTC). Before any lookup or hashing, sign-in counts
rows for both keys inside `LOGIN_RATE_WINDOW_SECONDS` (default 900); at or above
`LOGIN_RATE_LIMIT_PER_ADDRESS` (default 10) or `LOGIN_RATE_LIMIT_PER_NETWORK` (default 30) it
answers **429 `auth.rate_limited` with `Retry-After`** and does no password work. Failures are
recorded through the explicit-commit path so the refusal does not undo them. Old rows are deleted
when a key is written and by `purge_expired_records`.

**Rationale**: counting before the lookup makes the answer identical for registered and
unregistered addresses and costs one indexed count. Hashing the keys keeps addresses and network
addresses out of the table. The network address is `request.client.host`; a forwarded header is
trusted only if `TRUST_FORWARDED_FOR` is set (default off), so a client cannot dodge the limit by
sending the header.

**Residual**: an account lock (423, after 5 failures) and the rate limit (429, after 10) differ, so
a determined caller can still tell a locked registered address from an unregistered one. This is the
accepted notice of feature 001 FR-005. Making the per-address limit answer exactly like the lock
(423) would close it; it is **not** what was clarified, so it is recorded here as an open
alternative for the tasks stage to confirm.

**Alternatives**: in-memory per worker (resets on restart, unshared); per-network only (does not
stop a distributed guess at one address); a reverse proxy limit (not available in every deployment).

## D17. Delivery after the response (F06)

**Decision**: `get_unit_of_work` receives FastAPI's `BackgroundTasks`; after a successful commit it
queues one task that runs the session's after-commit actions. They therefore run **after the
response is sent**, still only after the commit and still dropped on rollback. A delivery failure is
logged by exception type only, as now. ADR 008 records the change to ADR 005.

**Rationale**: the dependency's `scope="function"` ended before the response, so delivery sat on the
request's critical path and made a registered address slower than an unknown one. A background task
removes delivery from that path without a queue service.

**Residual**: a registered address still does a token write and a commit that an unknown address
skips (a few milliseconds, the same order as the sign-in difference in SC-207). The opt-in timing
test is extended to this endpoint with the same pass mark (ratio below 1.15).

**Alternatives**: a real queue (new infrastructure, no provider exists yet); a dummy write for
unknown addresses (doubles the work for every probe and is easy to get wrong); do nothing until a
provider exists (the gap grows to a network call).

## D18. Correcting migration 014's backfill (F07)

**Decision**: new migration **017** (014 is applied and protected). For accounts that are inactive
and have `activated_at` empty (migration 014 called them pending), set `activated_at = created_at`
when there is evidence of past activity: any refresh token ever issued, a used password-reset link,
or a used invitation link. Accounts with none of these stay pending. Accounts the rule cannot
decide (pending, no evidence, but older than the invitation lifetime, so their only link has
expired) are **listed by id in the migration's output and left pending**, never guessed. The
downgrade is a no-op and says so.

**Rationale**: a used reset link or a session history proves the account was active, which is what
the old rule ignored. Listing the undecidable rows follows the project rule of reporting, not
guessing.

**Alternatives**: edit 014 (forbidden); deactivate everything ambiguous (would lock out genuine
invitees).

## D19. Expired credentials (F08, clarification Q3)

**Decision**: `refresh` checks expiry **before** the revoked state. An expired credential, rotated or
not, answers `auth.token_expired` and changes nothing. Expired refresh tokens and spent or expired
links are deleted: opportunistically for the user at sign-in and refresh, and in bulk by
`purge_expired_records`. ADR 006 gets a note.

**Rationale**: a credential past its expiry cannot be used for anything, so presenting it is not
evidence of theft, and unbounded replay of an old token must not be a way to sign the owner out.

**Alternatives**: keep revoking and rate-limit replays (still a harassment path); purge only by cron
(replays work until the first run).

## D20. Refresh in flight versus "end every session" (F09)

**Decision**: a repository method `lock_user(user_id)` runs `SELECT ... FOR UPDATE` on the user
row. `refresh` takes it before spending the token, and every revoke-all path
(`revoke_all_for_user` callers: reset, logout everywhere, theft response, account changes) takes it
before revoking. One waits for the other; the later one sees the earlier one's rows.

**Rationale**: the new token row is invisible to a concurrent revoke-all until commit; serialising
on the user row removes the window. The lock order is always user row first, so there is no
deadlock with the last-Admin lock (which locks other rows, ordered by id).

**Alternatives**: revoke by `issued_at` cut-off (needs a new column and a check on every refresh);
serialisable isolation (a retry loop in every request).

## D21. Canonical time-zone names (F10; amends D11)

**Decision**: valid means the name is in the `tzdata` package's own zone list (read from its
`zones` resource), is not `Factory`, and loads with `ZoneInfo`. Matching is **case-sensitive**.
`check_site_timezones` applies the same rule.

**Rationale**: D11 rejected `available_timezones()` because it differs per environment (598 on the
development host, 486 in the slim image). The `tzdata` package is a declared dependency and the same
on every platform (598 names; `Factory` is the only non-zone; `posixrules` is absent), so a
package-based list gives one answer everywhere, and the case-sensitive match removes the dependence
on a case-insensitive file system. The load check stays so "valid" still means "will load".

**Alternatives**: a hard-coded list (drifts); `available_timezones()` (environment-dependent, the
reason D11 rejected it).

## D22. Log redaction (F05)

**Decision**: the value group of `_SENSITIVE_PAIR` consumes backslash escapes and stops only at an
unescaped matching delimiter; unquoted values stop at whitespace, comma or closing brace as now.
Property test: for generated values containing `"`, `'` and `\`, no fragment longer than one
character survives, and redacting twice equals redacting once.

**Alternatives**: parse every log message as JSON (messages are free text).

## D23. Rollback failure (F12)

**Decision**: in `unit_of_work`, the rollback runs inside its own `try`; if it raises, the
exception type is logged (no message, as for after-commit failures) and the original exception is
re-raised unchanged.

## D24. Per-run test database (F26)

**Decision**: the suite derives its database name from the run (`hds_<process id>_test`), creates
it, migrates it and drops it at the end; an explicit `TEST_DATABASE_URL` still wins. A database left
behind by a killed run matches the name pattern and its process no longer exists, so the next start
drops it. The `*_test` safety guard is kept.

**Rationale**: two runs at once (one by hand, one by a tool) already corrupted each other during PR
4 work; separate databases are the only fix that allows both.

**Alternatives**: an advisory lock that makes the second run wait or refuse (still serial); a
schema per run (migrations assume `public`).

## D25. Clearing optional fields (F17)

**Decision**: PATCH handlers use `model_dump(exclude_unset=True)`, so an omitted field is left
alone and an explicit `null` is passed on. The services accept `null` only for columns that allow
it (phone, default hourly rate, project end date, client contract end and contact fields); `null`
for a required field is a 422 with the existing validation code. The OpenAPI schemas already allow
`null` on these fields, so the document does not change.

**Alternatives**: a sentinel value for "clear" (new contract); keep dropping nulls and document it
(the defect).

## D26. Remaining older gaps (F18 to F25)

**Decision**, per item:

- **F18**: a Client-role user without `client_id` sees no projects (an empty list, not an error).
- **F19**: logout revokes the presented token only if it belongs to the caller; the answer stays 204
  either way, so it reveals nothing.
- **F20, F21, F22**: refactors with behaviour already pinned by tests. Reads move into services;
  one shared `UserRead` keeps the OpenAPI component name; one helper each for the token lookup, the
  token hash and password verification (tests patch the underlying function instead).
- **F23 (D5, proposed default)**: any role change on a Client-role account is refused (409
  `users.client_role_locked`); portal users are created and removed through the client.
- **F24 (D6, proposed default)**: a client's `contract_end` before `contract_start` is a 422
  `clients.date_range_invalid`; on PATCH the stored value is used for the field not sent.
- **F25**: employee and client-user creation use the savepoint insert already used for invitations,
  so simultaneous creation of one address returns 409 `users.duplicate_email`.

**Rationale**: smallest change per item that satisfies its stated behaviour; F23 and F24 are the
ones that need the confirmation noted in the spec assumptions.
