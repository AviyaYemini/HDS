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

## Configuration

Compose reads these from the environment or a `.env` file, with development defaults:

| Variable | Default |
| --- | --- |
| `POSTGRES_DB`, `POSTGRES_USER` | `hds` |
| `POSTGRES_PASSWORD` | a development value |
| `JWT_SECRET_KEY` | a placeholder that the API refuses when `ENVIRONMENT=production` |

Copy [`.env.example`](../.env.example) to `.env` and replace the placeholders. Never commit real
secrets; production secrets come from the environment or a secret store.

## Notes

- Migrations are not run on container start. Apply them with the `exec` command above.
- The image installs locked, non-development dependencies only, so test tools are not in it.
