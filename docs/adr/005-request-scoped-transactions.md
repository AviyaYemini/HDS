# ADR 005: One transaction per request

**Date**: 2026-11-02
**Status**: Accepted

## Context

Feature 001 let repositories call `commit()` after each write. A multi-step action (refresh a token, reset a password, deactivate a client with its portal users, change a role and end sessions) could therefore stop half-way with the earlier steps already saved: an old refresh token revoked with no new one issued, a link spent with the password unchanged. Email was sent from inside the service, so a request that later failed could still mail a link to a change that was never saved (review finding 9).

## Decision

**The request owns the transaction.** `hds.core.database.get_unit_of_work` wraps the session: it commits once when the handler returns and rolls back on any exception. Repositories only `flush`.

- It is declared as `Depends(get_unit_of_work, scope="function")`. That scope ends before the response is sent, so a commit that fails becomes an error response (500), never a success that was not saved. The option first appears in FastAPI 0.121.0, which is now the lower bound in `pyproject.toml`.
- It is built on `get_db`, so the tests that replace `get_db` with a PostgreSQL session keep working unchanged.
- **Email is queued, not sent.** Services send through `DeferredMailer`, which queues the send on the session (`after_commit`). The unit of work runs the queue only after a successful commit and drops it on rollback. A send that fails after the commit is logged (exception type only, no token, no traceback) and does not undo the saved change. The routers build the `DeferredMailer`; the services only know the `MailSender` interface.
- **The explicit-commit exception.** Two kinds of write must survive a refusal raised afterwards, because the request then fails and would roll them back: the failed-sign-in counter and lock (`register_failed_login`, which counts and locks in one statement), and the revoke-everything response to a replayed refresh token (ADR 006). They call the documented `BaseRepository.commit()` before raising. Nothing else may; a commit anywhere else would also save every earlier step of that request, which is exactly the half-done state this decision removes.

## Consequences

- Every multi-step action is all or nothing. `tests/integration/test_atomicity.py` makes the last step of each fail and reads the database through a second connection; replacing any of those with an early `commit()` makes the matching test fail (mutation checks).
- `tests/integration/test_mail_after_commit.py` shows a message is sent once, only after the commit, and not at all when the commit fails.
- A handler that forgets a repository call loses nothing silently: the whole request is one unit, so the missing write shows in the same test as any other.
- Work after the commit (sending mail) cannot change the response. A failed send is visible in the log only; delivery is not guaranteed by this decision.
- A long request holds its transaction (and any row locks, such as the last-Admin guard's) until it returns. Handlers are short, so this is accepted.

## Alternatives considered

- **Service-owned `async with uow:` blocks.** More explicit, but every service constructor and all route handlers change shape, and a forgotten block silently loses writes.
- **Commit in an HTTP middleware.** Middleware cannot tell which handler errors are domain errors and runs outside the dependency scope; harder to test.
- **Roll back only on unexpected errors, commit on domain errors.** Rejected: a domain error raised after a partial write would save half an action.
