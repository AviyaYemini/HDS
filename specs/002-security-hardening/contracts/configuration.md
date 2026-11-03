# Contract: Configuration

Settings are read from the environment by `hds.core.config`. This file lists what is new or
changed. Unchanged settings are in `apps/api/README.md`.

## Changed

| Setting | Before | After |
| --- | --- | --- |
| `ENVIRONMENT` | Optional, default `development` | **Required, no default.** `development` and `test` are development environments. Any other value (`production`, `staging`, ...) gets the strict checks below |
| `JWT_SECRET_KEY` | Default placeholder in code and in the compose file | **No default anywhere.** Required in every environment. In a non-development environment it must not be the placeholder and must be at least 32 bytes |

## New

| Setting | Meaning | Default |
| --- | --- | --- |
| `MAIL_BACKEND` | How messages are delivered. `outbox` writes a redacted record to a local file and is accepted only in a development environment. No other backend exists yet | `outbox` in development, none otherwise |
| `MAIL_OUTBOX_PATH` | File that the `outbox` backend appends JSON lines to. Each line holds the masked recipient, subject, purpose, expiry and a body with the token replaced by `[REDACTED]`; it never holds a usable link | a git-ignored path under the API folder |
| `REFRESH_REUSE_GRACE_SECONDS` | Window after a refresh token is used in which a second use is refused without ending sessions (FR-209) | `10` |

## Start-up rules (the system refuses to start when any fails)

| # | Rule | Requirement |
| --- | --- | --- |
| 1 | `ENVIRONMENT` is set | FR-201 |
| 2 | In a non-development environment, `JWT_SECRET_KEY` is not the placeholder and is at least 32 bytes | FR-201 |
| 3 | In a non-development environment, `MAIL_BACKEND` names a real delivery backend. **Until a provider feature adds one, production cannot start** | FR-202 |
| 4 | `CORS_ORIGINS` contains no `*` (unchanged) | feature 001 NFR-002 |

Every refusal names the failing setting and never prints a secret value.

## Files that change with this contract

- `infra/docker-compose.yml`: sets `ENVIRONMENT` explicitly; the signing secret has no default.
- The example settings file at the repository root: sets `ENVIRONMENT=development`, leaves the
  secret empty with an instruction to generate one (FR-204).
- `.github/workflows/ci.yml`: every job that builds the app sets `ENVIRONMENT=test`.
- `apps/api/README.md` and `infra/README.md`: configuration tables and quick start.
