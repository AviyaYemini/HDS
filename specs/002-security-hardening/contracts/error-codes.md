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
