# ADR 008: Work on a reset request's address runs after the response

**Date**: 2026-11-03
**Status**: Accepted. Changes ADR 005 for after-commit actions.

## Context

ADR 005 ran after-commit actions before the response, so a registered address waited for its mail and an unknown one did not. The reset request promises it cannot show who has an account (FR-228). The first fix moved only delivery after the response. The timing test then still failed: a registered address also takes a lock, stores a link and commits. That gave a median ratio of 1.2 with a new connection per request, and 1.6 to 1.7 with a connection pool, against a pass mark of 1.15. Sign-in has the same kind of gap, but about 55 ms of password check hides it. This path has nothing to hide it.

## Decision

**After-commit actions run after the response.** `get_unit_of_work` takes FastAPI's `BackgroundTasks` and, after a successful commit, adds one task that runs the session's actions. On rollback they are dropped, as before. `unit_of_work` outside a request still runs them at once. A failure is logged by exception type only.

**The reset request only counts and answers.** It counts per address and per network, removes stale throttle rows and answers 202. It reads no account and no link, so its time cannot depend on the address.

**Everything else is a job.** When the request is within its limits it queues one job, for every address, as an after-commit action. Over a limit nothing is queued, and the answer does not change. The job:

- uses its own session and unit of work, from `get_session_factory`, because the request's session is finished;
- looks the address up, takes the user's row first (FR-246), reads the account again, stores a link and sends the mail after its own commit;
- reports nothing to the caller, who already has the answer.

**Jobs are capped per worker.** `RESET_JOB_CONCURRENCY` (2) run at once and `RESET_JOB_QUEUE_LIMIT` (100) more may wait, so a burst cannot take every database connection. Past that a job is dropped and logged by type, never with the address. A job running longer than `RESET_JOB_TIMEOUT_SECONDS` (30) is stopped, so a stalled mail provider cannot hold every place. A link stored before that stays valid, but its mail is lost.

**A network over its limit only reads.** The network is checked first. Over its limit a request takes no lock, removes no rows and counts no address, so a flood cannot queue on one row and hold every connection, and one client cannot use up every address's allowance. What it skips depends on the network and on no address.

**A request cancels no link.** Anyone can make one, so it must not be able to take a link away. A completed reset still cancels every other link (FR-224). An account can hold several live reset links, bounded by the limits.

## Consequences

- The median ratio is about 1.02 on a pooled engine, measured by `tests/integration/test_reset_request_timing.py`. That test uses a pool and times to the end of the response on purpose (SC-212).
- **A crash after the response loses that one mail.** The job lives only in the worker's memory, so a crash or a restart loses it, and everything queued with it. A job dropped past the cap is lost the same way. The caller was told 202, so the owner sees no mail and must ask again, which uses one of their requests. This is accepted: a password reset can simply be retried.
- **Invitation mail moves after the response too**, because it is an after-commit action like any other. A crash after the 202 leaves the pending account and its link with no mail, and the manager sees no sign of it. Inviting again re-issues the link. Unlike the reset job, this mail has no cap: only signed-in staff can cause it. The timing argument does not apply to it, but a slow provider now cannot hold a manager's request open either.
- A failed job is visible only in the log. It looks the same as an unknown address from the outside, which is the point.
- A burst of requests for random addresses can fill the queue and cost a real user their mail. The network limit makes that harder for one client, not impossible for many.
- Jobs compete for the same database as requests, so heavy reset traffic could in principle be seen in other requests' timing. It cannot be tied to one address.
- **Behind a proxy with a wrong `TRUSTED_PROXY_COUNT`** every user shares one network address, so the network limit silently stops reset mail for everyone. One warning per window is the only sign. The deployment feature must set the count (ADR 007).
- A test through httpx cannot see any of this, because the client waits for background work. `tests/asgi_support.py` calls the application directly. A failed job does not fail a request, so race tests check the log.

## Alternatives considered

- **Move only delivery after the response.** Tried first, and the timing test failed.
- **A dummy write or a delay for unknown addresses.** Doubles the work for every probe, and has to match a cost that changes.
- **A real queue.** Survives a crash, but it is new infrastructure, and no mail provider exists yet. A job in memory can be replaced by one without changing the request.
- **Keep cancelling earlier links.** Lets anyone replace an owner's link every 12 minutes within the limit, which the limit was meant to stop.
