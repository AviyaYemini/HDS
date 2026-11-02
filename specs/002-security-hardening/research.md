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
stand (F11; confirmed default). Cancelling only reset links on a new request keeps a
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

## D15. Password hashing off the event loop, without holding a connection (F04, FR-222)

**Decision**:

- New `core/passwords.py` exposes `async` `hash_password`, `verify_password` and
  `verify_against_dummy`. Each runs the synchronous Argon2 call with `asyncio.to_thread`.
  `core/security.py` keeps the synchronous core for scripts and the seed.
- **All six paths release the connection.** Sign-in, accept invitation, confirm reset, create
  employee, create client user and create invitation (whose placeholder password is hashed) each
  hash with no connection held. A request's connection is taken lazily at its first statement, so a
  path that can hash before its first statement does (create invitation) simply does so first; a
  path that must read first (validate a link, check the address is free, load the user) ends that
  read-only transaction with `commit()` before hashing, and only then writes. **Rule: release only
  before the request's first write**, because `commit()` also saves any earlier write. A helper
  `release_connection()` on the base repository does it and refuses (raises) if anything was written
  in the transaction. Writes are tracked on the session itself: `unit_of_work` registers a
  `do_orm_execute` listener that records any non-SELECT statement (this sees direct `UPDATE`,
  `INSERT` and `DELETE`, which `session.new` and `session.dirty` do not) and an `after_flush`
  listener for ORM changes, in `session.info`; `commit()` and `rollback()` clear the record. The pool test covers sign-in **and** a non-login path (50 concurrent creations
  against a pool of 5).
- **Bounded, with a bounded queue.** At most `HASH_CONCURRENCY` (default 2) hashes run at once, and
  at most `HASH_QUEUE_LIMIT` (default 8) more may wait. A request that would join a full queue is
  refused at once with 503 `service.busy` and `Retry-After`. The limiter is created **per running
  event loop** (a `WeakKeyDictionary` keyed by the loop), because a semaphore made at import and
  used from several loops raises in tests.
- **No connection held while hashing.** `login` does its reads, ends the transaction
  (`commit()` on a read-only transaction returns the connection to the pool), then hashes; the
  next database call begins a new transaction. The pool default is 15 connections; before this
  change about 16 simultaneous sign-ins would have held all of them while waiting for a hash and
  every other endpoint would have waited for a connection. A test runs 50 concurrent sign-ins
  against a pool of 5 and requires a plain request to be answered meanwhile.
- **Hash before locking.** `accept_invitation` and `reset_password` compute the new password's hash
  after validating the link by a read and **before** any row lock or write, so no lock is held
  while a request waits in the hash queue.
- The work-counting tests wrap the synchronous core, so one verification per refusal is still
  counted (SC-207).

**Rationale**: measured 36 ms per check and 724 ms of stalled loop for 20 concurrent unknown-address
sign-ins. Threads release the loop (argon2-cffi releases the GIL while hashing). The bound keeps
CPU and memory (64 MiB per hash) predictable, and refusing when the queue is full turns an outage
into a clear, fast answer for the excess.

**Alternatives**: a process pool (more memory, start-up and pickling for no gain); a smaller cost
parameter (weakens stored hashes); unbounded threads (an attacker chooses the memory use); an
unbounded wait queue (moves the outage to latency and, with held connections, to the pool).

## D16. Sign-in lockout for every address, and a network limit (FR-221; F04)

**Decision (revised 2026-11-03; supersedes the 429-for-both choice of the first draft)**:

- New table `login_throttle` (migration 018), one row per key: `kind` (`address` or `network`),
  `key_hash` (HMAC-SHA-256 hex, keyed by a server secret, of the lowercased submitted address or of
  the client network address),
  `failures`, `locked_until`, `updated_at`; primary key (`kind`, `key_hash`). It mirrors the columns
  the users table already has for the account lock, so an address with no account is treated by the
  same rule as one with an account.
- **Per address.** The limit is `LOGIN_MAX_FAILURES` and the lock lasts `LOGIN_LOCKOUT_MINUTES`; no
  separate setting. For a registered address the existing account columns remain the state; for an
  unregistered one the `address` row is. Both go through one small interface in `services/throttle.py` (it
  coordinates two repositories, so it is service work), so the sign-in code
  has a single path: read the state and treat an expired lock as cleared **in memory** (no write),
  refuse with **423 `auth.account_locked`** while locked (same body shape, lock-end time computed
  the same way), otherwise verify (a real or the dummy hash), and on failure register the failure
  with one atomic upsert that locks at the limit and is committed at once so the refusal does not
  undo it. **Nothing is written before verification finishes**, so the connection can be released
  first: the upsert restarts the count when the stored lock has expired (`failures = CASE WHEN
  locked_until <= now THEN 1 ELSE failures + 1`), and a success clears the state with the existing
  reset. There is no separate "clear expired lock" write on the sign-in path. A successful sign-in clears the count (nine
  typos followed by a good sign-in followed by a typo is one failure, not ten). The count restarts
  when a lock ends (FR-213).
- **Said openly.** While an address is locked a correct password is refused and is not verified,
  as the existing account lock already does. A real user who is locked out waits for the lock to end.
- **Per network address, in a fixed window.** The row also has `window_started_at`. A failure first
  restarts the count if the window (`LOGIN_LOCKOUT_MINUTES`) has ended, then adds one; at
  `LOGIN_RATE_LIMIT_PER_NETWORK` (default 30; **0 disables it**) failures inside one window the
  address is refused until the window ends: **429 `auth.rate_limited` with `Retry-After`**, answered
  before any lookup and for every submitted address alike. This is the only 429. One atomic upsert
  does the restart, the increment and the decision. Because the window is fixed, typos spread over
  days never accumulate, and the 24-hour clean only removes rows that already expired.
- **A successful sign-in does not clear the network count** (it clears only the address count).
  Otherwise an attacker holding one valid account could sign in every 29 guesses and never reach
  the limit. Tests: failures spread across two windows do not add up; 29 failures, a success, then
  one more failure locks the network.
- **Which network address.** `TRUSTED_PROXY_COUNT` (default 0). At 0 the forwarded header is
  ignored and the connecting address is used, which is right for the shipped compose file (no
  proxy). Behind N reverse proxies set it to N: the client address is the Nth entry from the right
  of `X-Forwarded-For` (each trusted proxy appends the address it saw), and if the header has fewer
  than N entries, or the chosen entry is not a valid address, the connecting address is used (a
  malformed entry is ignored, not trusted). An IPv6 address is keyed by its **/64 prefix** (a client
  controls the whole /64 and could otherwise rotate addresses inside it); an IPv4 address by the
  full address. A forwarded header is never read at 0, so a client cannot pick its own address. **Deployment risk, stated in the README and the settings table:**
  behind a proxy with the value left at 0 every user shares the proxy's address and 30 failures
  inside a window would lock everyone out; either set the count or set the network limit to 0.
  Nothing is deployed behind a proxy yet, so the default stays 0; the deployment feature that adds
  the ingress must set the count and add a check to its steps (a wrong value cannot be detected at
  start-up). Tests cover 0, 1, 2, a short header, a forged header, a malformed entry and IPv6 /64
  keying.
- **Staleness without a scheduler.** Each recorded failure also deletes up to 50 rows whose
  `updated_at` is older than `LOGIN_THROTTLE_RETENTION_HOURS` (default 24) and that are unlocked
  (`locked_until` empty or in the past, because `network` and `reset` rows keep the value after
  hitting a limit), using the index on `updated_at`, so a row outlives its retention only briefly and the table is bounded by the request rate over that period. Rows are one
  per distinct key (not per attempt), and random addresses are the only way to add many. The script
  `purge_expired_records` stays for a bulk clean and for expired credentials, but nothing depends
  on it being scheduled.
- Keys are an HMAC keyed by a server secret (`THROTTLE_SECRET_KEY`, or derived from
  `JWT_SECRET_KEY`), so a copy of the table alone cannot confirm an address. They are protected, not
  anonymous: whoever holds the secret can still check a known address.

**Reset requests (F28)**: a third key kind, `reset`, counts requests per lowercased address in a
fixed window of `RESET_TTL_MINUTES`, limit `RESET_REQUEST_LIMIT_PER_ADDRESS` (default 5; 0 turns it
off). It is checked before any lookup, so a registered and an unregistered address behave alike.
The decision uses the returned count: over the limit means a count above it, so the 5th of 5 still
works. This path also deletes stale rows on every request, because it has no hashing cost and no
network limit and could otherwise add rows far faster than sign-in. The table is then bounded by
the request rate over `LOGIN_THROTTLE_RETENTION_HOURS`, not by a fixed size. The count is committed
before the account is looked at, so it survives a later failure and its row lock is not held while
the request waits for the user's row.
Over the limit the answer is still 202 and nothing is issued or sent, which keeps the promise that
this endpoint reveals nothing. A request no longer cancels earlier links (see D17), so the limit is
about mail volume, not about protecting a link.

**Reset requests per network**: `RESET_REQUEST_LIMIT_PER_NETWORK` (default 30 in the same window; 0
turns it off) counts under kind `reset` with a `network:` key prefix, which no address can collide
with, so no migration is needed. Both counts always run, so a request over one limit does the same
work as any other. The first request over the network limit in a window logs one warning: behind a
proxy with a wrong `TRUSTED_PROXY_COUNT` every user shares one address, so this would silently stop
reset mail for everyone. A warning per request would let an attacker fill the log.

**Rationale**: the first draft answered 429 above a limit of 10 while a registered address locked
with 423 at 5, so anyone could still tell them apart and FR-221's "same answer" did not hold.
Mirroring the account lock for unregistered addresses removes the difference instead of
describing it, and supersedes the exception feature 001 (FR-005) accepted.

**Residual**: a registered address that is locked skips the password check while an unregistered
locked address does too, so the work is equal. Retention (24 hours) clears an unregistered
address's partial count sooner than a registered account's count, which persists until success; an
attacker would need a day's patience to observe it, and it is documented.

**Alternatives**: an event table with one row per failed attempt (grows with attack volume);
in-memory per worker (resets on restart, unshared); 429 for the per-address limit (the rejected
draft); a reverse-proxy limit (not available in every deployment).

## D17. Delivery after the response (F06)

**Amended on 2026-11-03, after the timing test failed**: see "All address work after the response"
at the end of this section. The residual below no longer applies to the reset request.

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

**All address work after the response**: the residual was larger than estimated: a ratio of 1.2 with
a new connection per request and about 1.7 with a pool, because this path has no password check to
hide the gap as sign-in has. So the request now only counts (per address and per network), removes
stale rows and answers. When it is within its limits, it queues one job as an after-commit action,
whatever the address. The job has its own session and unit of work, from `get_session_factory`, which
tests can override. It looks the address up, takes the user's row first, reads the account again,
stores a link and sends the mail once its transaction has committed.

`RESET_JOB_CONCURRENCY` (2) jobs run at once per worker and `RESET_JOB_QUEUE_LIMIT` (100) more may
wait, so a burst cannot take every database connection. Past that the job is dropped and logged by
type, without the address; the answer is already sent. A job costs an unregistered address a lookup,
so the cap also holds under random addresses. A crash after the response loses that one mail, and
the requester can ask again. ADR 008 records this.

Rejected: a dummy write or a delay on the unknown path (D17 above, and plan.md); a real queue (new
infrastructure, and no provider exists yet).

**A request no longer cancels links**: if a public request can cancel a link, anyone can keep
replacing it. An account can now hold several live reset links, at most the per-address limit per
window. Each lasts 60 minutes, and a completed reset cancels every other link (FR-224).

## D18. Correcting migration 014's backfill (F07)

**Decision**: new migration **017** (014 is applied and protected). For accounts that are inactive
and have `activated_at` empty (migration 014 called them pending), set `activated_at = created_at`
when there is evidence of past activity: any refresh token ever issued, or any password-reset link
(used or not, because reset links are only ever sent to active accounts). **Invitation links are
not evidence**: cancelling a link also sets `used_at`, and re-inviting a pending account cancels its
earlier link, so a used invitation link can belong to a genuine invitee (since migration 014 an
accepted invitation always sets `activated_at`, so an empty `activated_at` with a used invitation
link can only mean a cancelled link). Accounts with none of the evidence stay pending. Accounts the rule cannot
decide (pending, no evidence, but older than the invitation lifetime, so their only link has
expired) are **listed by id in the migration's output and left pending**, never guessed. The
downgrade is a no-op and says so.

**Rationale**: a reset link or a session history proves the account was active, which is what
the old rule ignored. Listing the undecidable rows follows the project rule of reporting, not
guessing.

**Alternatives**: edit 014 (forbidden); deactivate everything ambiguous (would lock out genuine
invitees).

## D19. Expired credentials (F08, clarification Q3)

**Decision**: `refresh` checks expiry **before** the revoked state. An expired credential, rotated or
not, answers `auth.token_expired` and changes nothing. Cleanup deletes **only records past their
expiry**: expired refresh tokens, and links whose `expires_at` has passed. A spent or cancelled link
that is still inside its lifetime is **kept**, because cancelling marks a link used and its answer
must stay `auth.token_used` (F01's acceptance check and the error contract). Cleanup runs
opportunistically for the user on the success path of sign-in, after verification and never
before it (D15: nothing is written before verification), at refresh, and in bulk by
`purge_expired_records`.
ADR 006 gets a note.

**Consequence, stated**: once a link is past its expiry and has been deleted, it answers
`auth.token_expired` whether or not it had been spent, and a deleted refresh token answers
`auth.invalid_token`. All are 401, and nothing about the account is revealed either way.

**Rationale**: a credential past its expiry cannot be used for anything, so presenting it is not
evidence of theft, and unbounded replay of an old token must not be a way to sign the owner out.

**Alternatives**: keep revoking and rate-limit replays (still a harassment path); delete spent links
at once (would turn `auth.token_used` into `auth.token_expired` inside the lifetime).

## D20. Sessions that start while "end every session" runs (F09)

**Decision**: a repository method `lock_user(user_id)` runs `SELECT ... FOR UPDATE` on the user row,
and every path that creates a session or ends all sessions takes it:

- **Refresh** takes the lock first and **then reads the user** (`is_active`, deletion), so it never
  acts on a stale copy read before the lock.
- **Login** verifies the password without a lock (so no lock is held during hashing), then takes
  the lock, re-reads the user **with `populate_existing()`** (the session keeps the values it
  loaded before the lock, so a plain re-read would return the old password hash and active flag),
  and refuses with `auth.invalid_credentials` if the account is no longer active **or its password
  hash changed since it was verified**; only then does it create the token. The race test asserts
  the refusal, so a plain re-read fails it. A login that verified the old password therefore cannot create a session after a reset
  finishes.
- **Every revoke-all** (`revoke_all_for_user` callers: reset, logout everywhere, theft response,
  account changes) takes the same lock first.

One side waits for the other and the later one sees the earlier one's rows. The lock order is
always user row first; the last-Admin guard locks Admin rows ordered by id and only ever waits for
rows another request holds briefly, and sign-in holds just one user row, so no cycle is possible.

**Lock order for links (F27)**: the same rule covers links. Redeeming a link used to spend it and
then update the user, while staff actions updated the user and then cancelled the link, which
deadlocked in about half of the rounds. `reset_password` and `accept_invitation` now take the user's
row before `spend`, `request_password_reset` and a re-invitation take it before cancelling links,
and a re-invitation reads the account again after the lock. A link insert waits on the user's row
through its foreign key, so a path that cancels a link and then inserts one is exposed too. The
order inside a redemption is: read the link, hash, lock the user, spend.

**Rationale**: the new token row is invisible to a concurrent revoke-all until commit; serialising on
the user row removes the window for refresh and for login alike.

**Alternatives**: revoke by `issued_at` cut-off (a new column and a check on every refresh);
serialisable isolation (a retry loop in every request).

## D21. Canonical time-zone names (F10; amends D11)

**Decision**: valid means the name is in the `tzdata` package's own zone list (read from its
`zones` resource), is not `Factory`, and loads with `ZoneInfo`. Matching is **case-sensitive**.
`check_site_timezones` applies the same rule.

**Rationale**: D11 rejected `available_timezones()` because it depends on the environment (D11 recorded 598 names
on the development host and 486 in the slim image; the 486 figure has not been re-checked and this
decision does not rest on it). The `tzdata` package is a declared dependency and the same
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
for a required field is a 422 with the existing validation code. **The OpenAPI document does
change here (corrected 2026-11-03)**: today the PATCH schemas allow `null` on required columns
(`full_name`, `name`, `billing_rate`, `timezone`, `address`, `status`, `role`, `contract_start`), and
Schemathesis treats such a request as valid, so a 422 would fail its "valid data must be accepted"
check. Those request fields are made **optional but not nullable** (omitted is allowed, `null` is
not) so the schema says what the API does; the fields that may be cleared keep `null`. The
exceptions list `unprovable_operations.json` is not used to hide this. The change only makes
requests stricter, so the 001 contract's operations, success codes and parameters are unchanged.

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
- **F23 (D5, confirmed default)**: any role change on a Client-role account is refused (409
  `users.client_role_locked`); portal users are created and removed through the client.
- **F24 (D6, confirmed default)**: a client's `contract_end` before `contract_start` is a 422
  `clients.date_range_invalid`; on PATCH the stored value is used for the field not sent.
- **F25**: employee and client-user creation use the savepoint insert already used for invitations,
  so simultaneous creation of one address returns 409 `users.duplicate_email`.

**Rationale**: smallest change per item that satisfies its stated behaviour; F23 and F24 are the
ones that need the confirmation noted in the spec assumptions.
