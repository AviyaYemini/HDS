# Contract: Error Codes

Feature 001's operations, parameters and success codes are **unchanged**, so
`specs/001-foundation-auth-org/contracts/openapi.yaml` and the drift check are untouched. This
file lists the error answers that are new or whose conditions change. All use the existing
`application/problem+json` format with a stable `code`, and every code needs English and Hebrew
text in `apps/api/src/hds/locales` (Constitution VIII; `test_i18n` enforces parity).

## New codes

| Code | HTTP | When | Operation |
| --- | --- | --- | --- |
| `invitations.account_deactivated` | 409 | Inviting an address that belongs to a deactivated account (FR-206) | `POST /auth/invite` |
| `users.pending_cannot_reactivate` | 409 | Reactivating an account that was invited but never activated (FR-220). The account stays pending | `POST /api/v1/employees/{id}/reactivate` |
| `sites.timezone_invalid` | 422 | A site is saved with a time zone that cannot be loaded (FR-217) | `POST /api/v1/sites`, `PATCH /api/v1/sites/{id}` |

## Existing codes with changed conditions

| Code | HTTP | New condition | Operation |
| --- | --- | --- | --- |
| `auth.token_used` | 401 | An invitation link is redeemed for an account that is not pending, or a link was cancelled (FR-205, FR-207). The answer is the same as for a spent link, so an anonymous caller learns nothing about the account | `POST /auth/invite/accept`, `POST /auth/password-reset/confirm` |
| `auth.token_revoked` | 401 | A refresh token was already used. After the grace window this also ends all of the user's sessions (FR-209) | `POST /auth/refresh` |
| `auth.account_locked` | 423 | Returned after exactly N failures, not N-1 (FR-212) | `POST /auth/login` |
| `users.duplicate_email` | 409 | Emails that differ only by letter case are duplicates (FR-215) | employee, client-user and invitation creation |
| `users.admin_required` | 403 | A Manager re-inviting a pending account as Admin (FR-206) | `POST /auth/invite` |

## Behaviour that is not an error code

- `POST /auth/login` for an unknown email takes the same time and returns the same
  `auth.invalid_credentials` as a known email with a wrong password (FR-214).
- `POST /auth/password-reset/request` still answers 202 for any address; matching ignores letter
  case (FR-215).
- A concurrent double use of a refresh token or link: one request succeeds, the other receives the
  codes above (FR-208).

## Documentation to update in the same change

- `docs/hds-api.md`: error table and the sign-in, refresh and invitation sections.
- Operation `description` text for the endpoints above (they feed the OpenAPI document).


---

# Part 2: PR 4 fixes

No operation, parameter or success code changes. The drift check against
`specs/001-foundation-auth-org/contracts/openapi.yaml` now compares error responses too, and that
file is updated to match (P3 Group E, and P6 for the PATCH fields). The items below are new or
changed error answers; each needs English and Hebrew text where it is a new code.

## New codes and responses

| Code | HTTP | When | Operation | Item |
| --- | --- | --- | --- | --- |
| `auth.rate_limited` | 429 + `Retry-After` | Too many failed sign-ins from one network address inside one fixed window of `LOGIN_LOCKOUT_MINUTES` (limit `LOGIN_RATE_LIMIT_PER_NETWORK`, 0 turns it off); a successful sign-in does not clear the count; answered before any lookup, for every submitted address alike | `POST /auth/login` | FR-221 |
| `service.busy` | 503 + `Retry-After` | The password-hashing queue is full; nothing was checked or changed | `POST /auth/login`, `POST /auth/invite`, `POST /auth/invite/accept`, `POST /auth/password-reset/confirm`, `POST /api/v1/employees`, `POST /api/v1/clients/{id}/users`; 503 is declared on all six | FR-222 |
| `users.pending_cannot_deactivate` | 409 | Staff deactivate an account that was invited and never accepted | `POST /api/v1/employees/{id}/deactivate` | F03 |
| `users.client_inactive` | 409 | Reactivating a portal user whose client is inactive or deleted, or creating one under an inactive client | `POST /api/v1/employees/{id}/reactivate`, `POST /api/v1/clients/{id}/users` | F02 |
| `users.client_role_locked` | 409 | Changing the role of a Client account (confirmed default D5) | employee edit, promote operations | F23 |
| `clients.date_range_invalid` | 422 | `contract_end` earlier than `contract_start` (confirmed default D6) | `POST /api/v1/clients`, `PATCH /api/v1/clients/{id}` | F24 |

## Existing answers whose conditions change

| Answer | New condition | Operation | Item |
| --- | --- | --- | --- |
| `auth.token_used` 401 | Any older link of an account after a reset completes (inside the link's lifetime; a link past its expiry and cleaned up answers `auth.token_expired`) | confirm reset, accept invitation | F01 |
| `auth.account_locked` 423 | Also for an address that has no account, after `LOGIN_MAX_FAILURES` failures; same body shape and lock-end rule as for a registered address; a correct password is refused while locked (FR-221) | `POST /auth/login` | FR-221 |
| `auth.token_expired` 401 | A rotated or revoked refresh token that is also past its expiry; ends no session | `POST /auth/refresh` | F08 |
| 204 | Logout with a token owned by someone else: nothing is revoked, the answer is unchanged | `POST /auth/logout` | F19 |
| 409 `users.duplicate_email` | Simultaneous creation of one address | employee and client-user creation | F25 |
| 422 `sites.timezone_invalid` | Non-canonical names (`posixrules`, `Factory`, wrong case) | site create and edit | F10 |
| 422 `validation.request_invalid` | A money field (`default_hourly_rate`, `billing_rate`) that is not a plain decimal string in the schema's shape: JSON numbers, `1e3`, `+5`, surrounding spaces, `75.`, `.5` and non-ASCII digits. Zero and negative strings still reach the services and get their own codes | employee and project create and edit | Money |
| 200 / 422 | An explicit `null` clears an optional field (phone, hourly rate, project end date, contract end, contact fields); omitted fields are untouched; `null` for a required field is 422, and the request schema no longer lists `null` for those fields | `PATCH` on me, employees, clients, projects, sites | F17 |

## Behaviour that is not an error code

- A completed reset clears the lockout and the failure count (F11).
- Deactivating or deleting a client, or removing a portal user, ends that user's sessions (F02).
- Password-reset request returns before delivery runs (F06); delivery failures are logged by type.
- A Client-role user without a client sees no projects (F18).
- Password-reset requests over the per-address limit still answer 202; nothing is cancelled or sent (F28).

## New settings

`HASH_CONCURRENCY` (default 2), `HASH_QUEUE_LIMIT` (8), `LOGIN_RATE_LIMIT_PER_NETWORK` (30 per
fixed window of `LOGIN_LOCKOUT_MINUTES`; 0 turns the network limit off), `TRUSTED_PROXY_COUNT` (0:
ignore the forwarded header; N behind N reverse proxies; set by the deployment feature), `LOGIN_THROTTLE_RETENTION_HOURS` (24), `THROTTLE_SECRET_KEY` (optional, at least 32 bytes outside development; derived from `JWT_SECRET_KEY` when unset), `RESET_REQUEST_LIMIT_PER_ADDRESS` (5 per `RESET_TTL_MINUTES`; 0 turns it off). The per-address limit and lock length
reuse `LOGIN_MAX_FAILURES` and `LOGIN_LOCKOUT_MINUTES`. All go into `.env.example` and the
configuration table; the existing test enforces both. Behind a proxy with `TRUSTED_PROXY_COUNT`
left at 0 every user shares one address: set the count or set the network limit to 0.
