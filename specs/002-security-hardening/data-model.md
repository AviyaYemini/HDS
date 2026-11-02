# Data Model: Security Hardening

Only changes to feature 001's model ([data-model.md](../001-foundation-auth-org/data-model.md)) are
listed. No table is added or dropped. All three migrations are new files; applied migrations are
not edited (Constitution X).

## Changed tables

### `users`

| Column | Change | Type | Rules |
| --- | --- | --- | --- |
| `activated_at` | **added** (migration 014) | timestamp with time zone, null | Set when an invitation is accepted, and when staff create an account that is active from the start. Null means the account has never been active |
| constraint `ck_users_active_has_activated_at` | **added** (migration 014, after the backfill) | check | `CHECK (is_active = false OR activated_at IS NOT NULL)`: an active account always has an activation time |
| `email` | **rule changed** (migration 016) | text | Always stored lowercase and trimmed |

| Index | Change |
| --- | --- |
| `uq_users_email` | **dropped** (migration 016) |
| `uq_users_email_ci` | **added**: unique on `lower(email)` where `is_deleted = false` |

### `refresh_tokens`

| Column | Change | Type | Rules |
| --- | --- | --- | --- |
| `revoked_reason` | **added** (migration 015) | short text, null | One of `rotated`, `logout`, `all`, `account`. Null for tokens revoked before this change. Checked by a database constraint |

### `invitation_tokens`

No schema change. `used_at` now also records *cancellation* (a link cancelled when its account is
deactivated, deleted, activated or re-invited is marked used at that moment).

## Account states

An account is in exactly one of these states, derived from existing columns plus `activated_at`:

| State | `is_deleted` | `is_active` | `activated_at` |
| --- | --- | --- | --- |
| Pending | false | false | null |
| Active | false | true | set |
| Deactivated | false | false | set |
| Deleted | true | any | any |

```text
 (invite) ─▶ Pending ──accept──▶ Active ◀──reactivate── Deactivated
                │                  │  └─────deactivate────▲
                └──── delete ──────┴───────────▶ Deleted
```

Reactivation is available **only from Deactivated**. From Pending it is refused with
`users.pending_cannot_reactivate` (409) and the account stays Pending; only accepting an invitation
leaves Pending.

Rules by state:

- **Invitation link redeemable**: Pending only (FR-205).
- **Reset link redeemable**: Active only.
- **Invite an address**: none → create Pending; Pending → update role and replace link; Active →
  duplicate (409); Deactivated → refused (`invitations.account_deactivated`) (FR-206).
- **Entering Deactivated or Deleted, or leaving Pending** cancels all outstanding links (FR-207).
- Reactivation stays the staff action of feature 001 (FR-015); it does not happen by invitation.
- **Reactivate**: Deactivated → Active. Pending → refused (`users.pending_cannot_reactivate`,
  FR-220). Active → unchanged, as today.

## Refresh-token states

```text
 live ──rotate──▶ revoked(rotated) ──reuse after grace──▶ all of the user's tokens → revoked(all)
  │                       └──reuse within grace──▶ refused, no other effect
  ├──logout──────▶ revoked(logout)
  ├──logout-all / password reset──▶ revoked(all)
  └──deactivate / delete / role change──▶ revoked(account)
```

Only a reuse of a token revoked as `rotated` can trigger the revoke-everything response (D3).

## Migrations

| Revision | What it does | Reversible |
| --- | --- | --- |
| `014_user_activation` | Adds `users.activated_at`; backfills it from `created_at` for every user except those that are inactive, hold an invitation link and never used one; then adds `ck_users_active_has_activated_at` | Yes: drops the constraint and the column |
| `015_refresh_revoked_reason` | Adds `refresh_tokens.revoked_reason` with a check constraint | Yes: drops the column |
| `016_case_insensitive_email` | Stops with a list if any addresses clash by case; lowercases all addresses; replaces `uq_users_email` with `uq_users_email_ci` | Index: yes. The lowercasing of stored data is not reversed |

## Validation rules added

- Email (create, sign-in, invite, reset): trimmed and lowercased before use.
- Site time zone (create, edit): must load as a zone; otherwise 422 `sites.timezone_invalid`.
- Settings: described in [contracts/configuration.md](contracts/configuration.md).


---

# Part 2: PR 4 fixes

## New table: `login_throttle` (migration 018, FR-221, research D16)

Failure count and lock for an address or a network address that has no account row to hold them. It
mirrors the `failed_login_attempts` and `locked_until` columns of `users`, so an unregistered address
is locked by the same rule as a registered one. Registered addresses keep using the `users`
columns; a network address always uses this table.

| Column | Type | Rule |
| --- | --- | --- |
| `kind` | short text | `address`, `network` or `reset`; check constraint `ck_login_throttle_kind`; part of the primary key |
| `key_hash` | text (64) | HMAC-SHA-256 hex, keyed by a server secret, of the lowercased submitted address or of the client network address (an IPv6 address reduced to its /64); never the value itself, and a copy of the table alone cannot confirm one; part of the primary key |
| `failures` | integer | consecutive failures, at least 0; restarts at 0 when a lock ends; for `address` rows cleared by a successful sign-in, for `network` rows never cleared by one |
| `window_started_at` | timestamptz, null | `network` and `reset` rows only: start of the fixed counting window; a failure after `LOGIN_LOCKOUT_MINUTES` restarts it and the count |
| `locked_until` | timestamptz, null | UTC; set when `failures` reaches the limit (`address`) or the window limit (`network`) |
| `updated_at` | timestamptz | UTC; index `ix_login_throttle_updated_at` for the batch clean |

No foreign key: unregistered addresses have no account. One row per distinct key, not per attempt.
Each recorded failure or reset request deletes up to 50 unlocked rows (`locked_until` empty or in
the past) older than `LOGIN_THROTTLE_RETENTION_HOURS` (default 24), so growth is bounded without a scheduled job; `purge_expired_records` does the same in
bulk. Downgrade drops the table (the counts are disposable).

## Changed rules on existing data

- **`users.activated_at`** (F03, F07): written only by a real activation (accepting an invitation,
  creating an active account, the 014 backfill). Deactivation never writes it. Migration 017
  clears the cases where 014 left an account pending despite evidence of earlier activity
  (D18). The constraint `ck_users_active_has_activated_at` is unchanged.
- **`refresh_tokens`** (F08, F09): expired rows are deleted; `revoked_reason` values are unchanged.
  Refresh and every revoke-all take a row lock on the owning user (no schema change).
- **`invitation_tokens`** (F01, F02, F08): `cancel_for_user` gains an optional purpose; a completed
  reset cancels every other link of the account; a new reset request cancels earlier unused reset
  links. Cleanup deletes only links past `expires_at`; spent and cancelled links inside their
  lifetime are kept so their answer stays `auth.token_used`.
- **Client accounts** (F23, F24): a user with role `client` has no role change; a client's
  `contract_end` is on or after `contract_start` (service rule, not a new constraint, because
  existing rows might violate it; a check constraint is left to a later migration after a report).

## State transitions affected

| From | Action | Before | After |
| --- | --- | --- | --- |
| pending | staff deactivate | becomes deactivated, can be reactivated | refused 409; stays pending |
| pending | client deactivated | portal user deactivated | skipped; stays pending |
| active portal user | client deactivated, deleted, or user removed | tokens live | tokens revoked (`account`), links cancelled |
| deactivated portal user | reactivate while client inactive | succeeds | refused 409 |
| locked | password reset completes | still locked up to 15 min | lock and count cleared |
