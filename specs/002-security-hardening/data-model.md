# Data Model: Security Hardening

Only changes to feature 001's model ([data-model.md](../001-foundation-auth-org/data-model.md)) are
listed. No table is added or dropped. All three migrations are new files; applied migrations are
not edited (Constitution X).

## Changed tables

### `users`

| Column | Change | Type | Rules |
| --- | --- | --- | --- |
| `activated_at` | **added** (migration 014) | timestamp with time zone, null | Set when an invitation is accepted, and when staff create an account that is active from the start. Null means the account has never been active |
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

Rules by state:

- **Invitation link redeemable**: Pending only (FR-205).
- **Reset link redeemable**: Active only.
- **Invite an address**: none → create Pending; Pending → update role and replace link; Active →
  duplicate (409); Deactivated → refused (`invitations.account_deactivated`) (FR-206).
- **Entering Deactivated or Deleted, or leaving Pending** cancels all outstanding links (FR-207).
- Reactivation stays the staff action of feature 001 (FR-015); it does not happen by invitation.

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
| `014_user_activation` | Adds `users.activated_at`; backfills it from `created_at` for every user except those that are inactive, hold an invitation link and never used one | Yes: drops the column |
| `015_refresh_revoked_reason` | Adds `refresh_tokens.revoked_reason` with a check constraint | Yes: drops the column |
| `016_case_insensitive_email` | Stops with a list if any addresses clash by case; lowercases all addresses; replaces `uq_users_email` with `uq_users_email_ci` | Index: yes. The lowercasing of stored data is not reversed |

## Validation rules added

- Email (create, sign-in, invite, reset): trimmed and lowercased before use.
- Site time zone (create, edit): must load as a zone; otherwise 422 `sites.timezone_invalid`.
- Settings: described in [contracts/configuration.md](contracts/configuration.md).
