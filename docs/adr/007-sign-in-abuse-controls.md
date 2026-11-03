# ADR 007: Sign-in abuse controls

**Date**: 2026-11-03
**Status**: Accepted

## Context

Sign-in lets anyone ask for an Argon2 check, which is slow and memory-hungry on purpose. Run inside an async handler, one check stopped every other request in the worker, and holding a database connection during it let 50 requests take the whole pool. An address with no account answered 401 for ever while a registered one locked with 423, so anyone could tell them apart. Nothing limited guesses across many addresses from one client. Password-reset requests had no limit at all.

## Decision

**Hashing is bounded and holds no connection** (FR-222). Each hash runs in a thread. At most `HASH_CONCURRENCY` (2) run at once per worker and `HASH_QUEUE_LIMIT` (8) more may wait; past that a request is refused at once with 503 `service.busy` and `Retry-After`. Every path that hashes reads what it needs and gives its connection back before it hashes (`BaseRepository.release_connection`). Sign-in, accepting an invitation and confirming a reset then lock the user and read again, so anything that changed during the hash is refused. Creating an employee or a client user has no row to lock: the unique address index decides, and the loser answers 409.

**One lockout rule for every address, in one store**. Every address, registered or not, is counted in `login_throttle`; the account's own columns are no longer used. A count restarts when its lock ends and after `LOGIN_THROTTLE_RETENTION_HOURS` (24) without a failure, and a success or a completed reset removes it. This was first built with registered addresses counted on the account row, which never expired while an unknown address's row did, so a caller who waited a day could tell them apart; it was moved before merging. Both kinds give the same answers after the same number of password checks. The 423 for an unknown address supersedes the accepted FR-005 exception of feature 001.

**A network limit in a fixed window**. Both are FR-221. Failures from one network address count in a window of `LOGIN_LOCKOUT_MINUTES`, and over `LOGIN_RATE_LIMIT_PER_NETWORK` (30) every address answers 429 `auth.rate_limited`, before any lookup. A success does not clear this count, or one valid account could guess forever. An IPv6 address counts by its /64.

**The proxy rule** (FR-223). `TRUSTED_PROXY_COUNT` (0) says how many proxies stand in front of the API. At 0 the forwarded header is ignored, so a client cannot pick its own address. Behind N proxies the Nth entry from the right is believed. A malformed entry is ignored.

**Throttle keys are protected, not anonymous.** A key is an HMAC keyed by `THROTTLE_SECRET_KEY`, or by a key derived from `JWT_SECRET_KEY`. A copy of the table alone cannot confirm an address; someone with the secret can.

**Reset requests are limited too** (FR-247): per address and per network, under kind `reset`, in a 60-minute window. See ADR 008.

**No scheduled job.** Each counted failure or request removes up to 50 stale rows, and `purge_expired_records` does the same in bulk.

## Consequences

- **A wrong `TRUSTED_PROXY_COUNT` cannot be detected at start-up.** Too low, and every user shares the proxy's address: one bad window locks everyone out of sign-in and silently stops reset mail for everyone. Too high, and a client can pick its own address. **The future ingress feature must set the count and check it in its deployment steps.** `infra/README.md` says so too.
- Creating a client user checks that the client is active before hashing, so a client deactivated while the hash waits in its queue can still get a new portal user. The spec accepts a very small window here, and the queue makes it wider.
- Changing either secret restarts every count, so a lock can end early on a rotation. Rotation is rare and deliberate.
- Every address does the same writes on a failure. A registered address's sign-in still reads a little more, so a small timing gap remains, which the password check hides: the opt-in timing test shows a ratio of about 1.1 against a pass mark of 1.15.
- Anyone can lock any address for `LOGIN_LOCKOUT_MINUTES` by failing on purpose, which is what a lockout means. The owner can still reset the password, which clears the lock.
- The table is bounded by the request rate over `LOGIN_THROTTLE_RETENTION_HOURS`, not by a fixed size.
- The limits are per deployment, not per worker, because they live in the database. The hashing and job caps are per worker.

## Alternatives considered

- **Counters in memory.** Lost on restart and split across workers.
- **429 for a locked unknown address.** A registered one answers 423, so that would show which addresses have accounts.
- **Limit by network only.** Leaves the 401 and 423 difference in place.
- **Refuse a missing proxy count at start-up.** Nothing can tell a proxied deployment from a direct one.
- **Sliding windows.** A fixed window is one atomic upsert, and typos spread over days never add up.
