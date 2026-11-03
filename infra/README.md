# Infrastructure

Local development infrastructure. Nothing here is a production deployment.

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | PostgreSQL 16 and the API, for local development |
| `Dockerfile.api` | Builds the API image from `apps/api` |
| `volumes/postgres` | PostgreSQL data directory (a bind mount) |

## Usage

Run from the repository root; the compose file is not in the root, so pass it explicitly.

```bash
docker compose -f infra/docker-compose.yml up -d postgres        # database only
docker compose -f infra/docker-compose.yml up --build           # database and API
docker compose -f infra/docker-compose.yml exec api alembic upgrade head
docker compose -f infra/docker-compose.yml down                  # data in volumes/ is kept
```

PostgreSQL listens on port 5432 and the API on 8000. The API container mounts
`apps/api/src`, so code changes are visible in it, but its command does not enable reload;
restart the container to pick them up.

## Before adding a proxy or ingress

Nothing here sits behind a proxy. The feature that adds one **must** set `TRUSTED_PROXY_COUNT` to the
number of proxies and check it in its deployment steps, because a wrong value cannot be detected at
start-up. Left at 0, every user shares the proxy's address: one bad window locks everyone out of
sign-in and silently stops reset mail for everyone. Too high, and a client can pick its own address.
See [ADR 007](../docs/adr/007-sign-in-abuse-controls.md).

## Configuration

Compose takes these from the environment of the shell that runs it:

| Variable | Default |
| --- | --- |
| `POSTGRES_DB`, `POSTGRES_USER` | `hds` |
| `POSTGRES_PASSWORD` | a development value |
| `JWT_SECRET_KEY` | **none, required**: compose stops when it is unset or empty |

The API container is started with `ENVIRONMENT=development`, set in the compose file. Generate a
secret with `openssl rand -hex 32` and export it before any `docker compose` command, for example
`export JWT_SECRET_KEY="$(openssl rand -hex 32)"`. Compose reads the whole file, so the variable
is needed even when you start only PostgreSQL. The variables are listed in
[`.env.example`](../.env.example). Never commit real secrets; production secrets come from the
environment or a secret store.

This stack is for development. Outside development the API also requires a real mail backend,
and none exists yet, so it refuses to start there.

## Notes

- Migrations are not run on container start. Apply them with the `exec` command above.
- The image installs locked, non-development dependencies only, so test tools are not in it.
