# ADR 016: Phone sessions last 30 days and renew on use; the client declares its kind

**Date**: 2026-11-07
**Status**: Accepted.

## Context

Guards sign in on their phones and expect to stay signed in; a silent sign-out stops notifications
and leads to missed shifts (research report E15). HDS issued every refresh token for 7 days
(`REFRESH_TOKEN_TTL_DAYS`, ADR 003), with rotation and reuse detection (ADR 006). The owner chose 30
days rolling on phones, unlocked by biometrics, and 7 days on the web (007 clarification Q5, FR-010).

## Decision

**A kind on each session.** `POST /auth/login` takes an optional `client_kind`, `web` (the default)
or `native`. A native refresh token lasts `REFRESH_TOKEN_TTL_DAYS_NATIVE` days (30) and a web one
`REFRESH_TOKEN_TTL_DAYS` (7). The kind is stored on the token (`refresh_tokens.client_kind`, migration
025), and a rotation copies it, so a phone that keeps refreshing stays signed in while it is used.
Access tokens stay at 15 minutes or less (Constitution VI).

**Sessions are listed and ended one by one.** `GET /me/sessions` lists the caller's live refresh
tokens, one per device; `DELETE /me/sessions/{id}` revokes one; `POST /auth/logout-all` still ends
them all.

**Biometrics stay on the device.** The phone app puts a biometric prompt in front of the stored
refresh token. No biometric data reaches the API.

## Accepted limit: the kind is self-declared

The API cannot tell a phone from a browser, so a browser can send `client_kind: native` and receive a
30-day refresh token. This is acceptable because:

- rotation and reuse detection still apply to that token (ADR 006): each refresh token works once,
  and replaying a spent one ends every session of the user;
- the web app keeps its tokens in memory only (ADR 010), so a browser that claims `native` gains a
  longer-lived token only if its own code chooses to store it;
- biometric unlock is a convenience gate on the phone, not a security boundary; the security
  boundary is the refresh token and its rotation.

A test pins that a web sign-in without `client_kind` gets 7 days.

## Consequences

- One new setting, `REFRESH_TOKEN_TTL_DAYS_NATIVE`, documented in `.env.example` and the API README.
- Existing tokens became `web` when migration 025 ran, the lifetime they were issued with.

## Alternatives considered

- **Device attestation** (App Attest, Play Integrity) to prove a phone: strong, but heavy for a
  closed service of tens of users; it can replace the self-declared kind later.
- **30 days for everyone**: longer web sessions without the in-memory protection a phone's secure
  store gives.
- **Keep 7 days and add only biometrics**: the owner chose 30 days.
