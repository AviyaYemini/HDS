# Infrastructure

The local environment, the images and the pinned versions. Nothing here is a production deployment
yet; the shared environment (specs/006-deploy-operations) adds the chart and the cluster files.

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | The local environment: PostgreSQL, the migrate step, the API, the worker, the web client |
| `Dockerfile.api` | The API image (the API, the worker and the migrations share it); runs as uid 10001 |
| `Dockerfile.web` | The web image: the Expo web export, served by an unprivileged nginx |
| `web/nginx.conf` | nginx for the web image: the app for every path that is not a file |
| `volumes/postgres` | PostgreSQL data directory (a bind mount); `make wipe` deletes it |
| `VERSIONS.md` | Every pinned tool and image version, with digests; change a row and its uses together |
| `tests/check_api_image.sh` | Checks the API image runs as non-root and carries its version (CI `lint` job) |

## The local environment

Use the `make` targets from the repository root (the root [README](../README.md#quick-start) has the
whole list): `make env` once, then `make up`, `make down`, `make wipe`, `make build`.

What `make up` does, in order:

1. Checks that its host ports are free (`scripts/ops/ports_free.sh`), skipping any port held by this
   project's own running service. A taken port stops it before anything starts, naming the port.
2. Starts PostgreSQL and waits for it.
3. Runs the one-shot `migrate` service: `alembic upgrade head`, then the development seed, which adds
   nothing that is already there. The API and the worker wait for it to succeed, so a failing
   migration leaves them stopped, and `make up` prints the migration's log.
4. Starts the API (healthy when `/readyz` answers 200), the worker (healthy when its `/readyz` on
   port 9100 answers) and, once the API is healthy, the web client.

`make up` builds the images each time, so a pull that changes the client, a dependency or a Dockerfile
is picked up; a build whose inputs did not change yields the same image (provenance is off), so a
running stack is left as it is. The API's `src/` and `alembic/` are also mounted into the migrate,
API and worker containers, so a code or migration change needs only `make down && make up`. Images
are named after the compose project (`infra-api:local`, `infra-web:local`). The port check needs
`nc` (netcat).

| Host port | Service | Variable |
| --- | --- | --- |
| 5432 | PostgreSQL | `POSTGRES_PORT` |
| 8000 | API | `API_PORT` |
| 8090 | Web client (not 8081, which the Expo dev server uses) | `WEB_PORT` |
| 9100 | API metrics | `API_METRICS_PORT` |
| 9101 | Worker health and metrics | `WORKER_METRICS_PORT` |

`scripts/ops/quickstart_check.sh` checks the whole quickstart from a clean checkout (CI's `quickstart`
job). It wipes the database, so it runs only with `QUICKSTART_THROWAWAY=1`. To run it on your own
machine beside your stack, use a fresh clone with its own `COMPOSE_PROJECT_NAME` and other ports.

## Before adding a proxy or ingress

Nothing here sits behind a proxy. The feature that adds one **must** set `TRUSTED_PROXY_COUNT` to the
number of proxies and check it in its deployment steps, because a wrong value cannot be detected at
start-up. Left at 0, every user shares the proxy's address: one bad window locks everyone out of
sign-in and silently stops reset mail for everyone. Too high, and a client can pick its own address.
See [ADR 007](../docs/adr/007-sign-in-abuse-controls.md).

## Configuration

Compose reads these from the root `.env` (which `make env` writes) or from the shell:

| Variable | Default |
| --- | --- |
| `POSTGRES_DB`, `POSTGRES_USER` | `hds` |
| `POSTGRES_PASSWORD` | the value in `.env` (a development value) |
| `JWT_SECRET_KEY` | **none, required**: compose stops when it is unset or empty, naming it |
| the port variables above | the ports above |

The API and worker containers run with `ENVIRONMENT=development` and `METRICS_PORT=9100`, set in the
compose file. Compose reads the whole file, so `JWT_SECRET_KEY` is needed even to start PostgreSQL
alone; `make down`, `make ps` and `make logs` supply a stand-in. The variables are listed in
[`.env.example`](../.env.example). Never commit real secrets; production secrets come from the
environment or a secret store.

This stack is for development, and its mail goes to the redacted outbox. Outside development the API
requires the real mail backend, `MAIL_BACKEND=smtp`, with `SMTP_HOST` and `MAIL_FROM` set and
STARTTLS on (`SMTP_STARTTLS`); the relay's password comes from the environment or Vault. See the
configuration table in [`apps/api/README.md`](../apps/api/README.md).

## Notes

- The images install locked, non-development dependencies only, so test tools are not in them.
- The web bundle calls the API at `http://localhost:$API_PORT`, baked in at build time; the
  cluster's web image is built with an empty URL and calls the API on its own origin.
