# ADR 003: JWT Access + Rotating Refresh Token Strategy

**Date**: 2026-11-01
**Status**: Accepted

## Context

HDS requires short-lived sessions with seamless renewal and the ability to revoke all sessions for a user (logout-all, account compromise).

## Decision

- **Access token**: HS256 JWT, TTL ≤ 15 minutes, contains `sub` (user UUID), `org`, `role`, `iat`, `exp`. Stateless — not stored in DB.
- **Refresh token**: Opaque UUID4 string, TTL 7 days, stored as SHA-256 hex hash in `refresh_tokens` table. On use: old token revoked (`revoked_at` set), new token issued (rotation).
- **`get_current_user` dep**: Always loads the live `User` row via `UserRepository(db).get_by_id(sub)` — role changes take effect on the next request without re-login.

## Consequences

- **Pro**: Short access-token TTL limits exposure of a leaked JWT.
- **Pro**: Refresh rotation invalidates stolen tokens (reuse of revoked token → immediate revocation of the new token family is not yet implemented but the detection point exists).
- **Pro**: `logout_all` revokes all refresh tokens in one query.
- **Con**: Requires one DB read per authenticated request (live user load); acceptable for this scale.
- **Con**: If a refresh token is stolen and used first, the legitimate client's token is invalidated — they must re-login.

## Alternatives Considered

- **Opaque session tokens only**: Simpler but no stateless verification capability.
- **Long-lived JWTs**: Higher blast radius on token leak; rejected per Constitution Principle VI.
