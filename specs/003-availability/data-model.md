# Data Model: Availability, Part 1 (leftovers from 002)

## Migration 019 (`019_expiry_index_retire_locks`)

| Table | Change | Downgrade |
| --- | --- | --- |
| `refresh_tokens` | index `idx_refresh_tokens_expires_at` on `expires_at` | drop the index |
| `invitation_tokens` | index `idx_invitation_tokens_expires_at` on `expires_at` | drop the index |
| `users` | drop `failed_login_attempts` and `locked_until` | add both back as in 004, empty (count 0, no lock) |

No data moves. Every address's sign-in count has been in `login_throttle` since 002.

## Client (unchanged columns, new rule)

- `contract_start`: required date, now editable; never empty.
- `contract_end`: optional date; when set, not before `contract_start`, judged on the pair after a change
  that sets either date.
