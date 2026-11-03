# ADR 006: Refresh-token reuse detection with a grace window

**Date**: 2026-11-02
**Status**: Accepted
**Builds on**: ADR 003 (short-lived access tokens with rotating refresh tokens). Rotation itself is unchanged.

## Context

ADR 003 rotates a refresh token on every use: the old one is revoked and a new one issued. Two weaknesses remained (review findings 4 and 10, feature 002):

- **Two uses at the same moment both succeeded.** The service read the token, saw it live, and revoked it in a later statement. Two requests presenting the same token could both pass the check, so one stolen copy could renew alongside its owner. A test that fired two simultaneous renewals saw two wins in 100 of 100 rounds.
- **A replayed token was only refused.** Presenting an already-rotated token is either an honest retry (the response with the new token was lost) or a thief using a copy. Refusing both and doing nothing else leaves the thief's own renewed session alive.

## Decision

**Spend atomically.** `RefreshTokenRepository.revoke_if_live` runs `UPDATE refresh_tokens SET revoked_at = now(), revoked_reason = ... WHERE id = :id AND revoked_at IS NULL RETURNING id`. Under PostgreSQL's default isolation (READ COMMITTED) the second writer waits for the first to commit, re-checks the condition, finds it false and updates nothing. Exactly one request gets the row; the other gets no row back and is refused. No application lock is needed. Invitation and reset links use the same pattern (`InvitationTokenRepository.spend`).

**Record why a token ended.** `refresh_tokens.revoked_reason` holds `rotated`, `logout`, `all` or `account`, or null for rows revoked before the column existed (migration 015). Null is read as "unknown" and is never treated as evidence of theft.

**Decide on reuse by reason and age.** When the atomic update loses, the service re-reads the token:

| Stored state | Response |
|---|---|
| reason `rotated`, revoked **within** `REFRESH_REUSE_GRACE_SECONDS` | refuse (`auth.token_revoked`), change nothing |
| reason `rotated`, revoked **longer ago** than the window, token not expired | revoke every live token of the user (reason `all`), commit that, then refuse |
| past its expiry, whatever its state | refuse (`auth.token_expired`), change nothing |
| any other reason, or null | refuse, change nothing |

Using a token after sign-out, or after the account was changed, is not theft, so it ends nothing else. Expiry is checked first: an expired token cannot be used for anything, so presenting it is not evidence of theft, and without this rule anyone holding one old token could sign the owner out again and again (F08). Expired token rows are deleted after a successful sign-in or refresh; a deleted one is answered as unknown (`auth.invalid_token`).

**The grace window** is `REFRESH_REUSE_GRACE_SECONDS`, default 10, and must be greater than zero. It covers an honest retry and the loser of two simultaneous renewals, so "exactly one wins" does not sign the owner out.

**The theft response must survive its own refusal.** Raising the error rolls the request's transaction back, which would undo the revocation. The service therefore calls the repository's documented explicit `commit()` (the explicit-commit exception in ADR 005) after `revoke_all_for_user` and before raising.

## Consequences

- N simultaneous renewals of one token give exactly one success; this is tested over 100 rounds against PostgreSQL, and removing the `AND revoked_at IS NULL` condition makes the test fail (mutation check).
- A stolen refresh token that is replayed after the window ends the thief's session and the owner's other sessions. The owner signs in again with the password; the password is not touched.
- A lost response followed by an immediate retry is refused, so a client must start again from its stored current token or sign in. The window is deliberately short; widening it trades detection for tolerance.
- Rows revoked before migration 015 cannot trigger the theft response. That is safe (they are only refused) and fades as old rows expire.
- Every call that revokes tokens now names a reason, so the code that revokes cannot forget to say why.

## Alternatives considered

- **Strict, no window.** Any second use signs everyone out. Simple, but a dropped response on a weak connection ends the user's sessions.
- **Refuse only (the earlier behaviour).** Leaves a stolen, already-renewed session alive.
- **A "replaced by" chain per token.** More precise about which token replaced which, but more schema and logic than the requirement needs.
- **An application-level lock (advisory lock or `SELECT ... FOR UPDATE` then update).** Works, but a single conditional `UPDATE` is shorter and cannot be bypassed by a code path that forgets the lock.
