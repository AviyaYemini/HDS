# Infrastructure

The local environment, the images, the Helm charts, the release and what Flux runs in the shared
environment, and the pinned versions (specs/006-deploy-operations). The cluster's own set-up
(`make ops-cluster`) arrives with Group M.

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | The local environment: PostgreSQL, the migrate step, the API, the worker, the web client |
| `Dockerfile.api` | The API image (the API, the worker and the migrations share it); runs as uid 10001 |
| `Dockerfile.web` | The web image: the Expo web export, served by an unprivileged nginx |
| `web/nginx.conf` | nginx for the web image: the app for every path that is not a file |
| `volumes/postgres` | PostgreSQL data directory (a bind mount); `make wipe` deletes it |
| `VERSIONS.md` | Every pinned tool and image version, with digests; change a row and its uses together |
| `tests/check_api_image.sh` | Checks the API image runs as non-root and carries its version (CI `lint` job) |
| `helm/hds` | The application chart: the API, the worker, the web client, the migration hook, the ingress |
| `helm/hds-postgres` | PostgreSQL as a release of its own, installed before `hds` |
| `helm/hds-secrets` | The Vault Secrets Operator's objects, which keep the `hds-*` Secrets; installed first |
| `cluster/vault-values.yaml`, `cluster/vso-values.yaml` | Vault (one node, raft, its NetworkPolicy) and the Vault Secrets Operator, for their pinned charts |
| `cluster/vault-policy.hcl`, `cluster/vault-snapshot-policy.hcl` | Vault's two policies, written by `make ops-vault-configure` |
| `tests/check_vault_policy.sh` | Those policies and roles against a dev Vault of the cluster's version (CI `chart` job) |
| `Dockerfile.backup` | The backup image: PostgreSQL 16's client tools, rclone, jq, curl and age; runs as uid 999 |
| `backup/` | The backup scripts (`backup.sh`, `prune.sh`, `restore-test.sh`, `restore.sh`, `vault-snapshot.sh`), the remotes (`rclone.conf.tmpl`) and their test (`tests/run.sh`, CI `backup-restore`) |
| `helm/tests/assert.sh` | Lints, validates and checks the rendered charts and `flux/` (CI `chart` job, `make ci-local`) |
| `helm/tests/chart_version.sh` | Fails a pull request that changes `helm/hds-postgres` without raising its version (CI `chart` job) |
| `release/` | The release workflow's scripts: `trust.sh` (may this run publish), `scan.sh` (Trivy), `push_images.sh` (skopeo), `publish_charts.sh` (both charts) |
| `flux/clusters/local/` | What Flux runs in the cluster: an OCIRepository and a HelmRelease for each chart, and its alerts to Alertmanager |
| `tests/check_backup_jobs.sh`, `tests/check_deploy_ops.sh` | The `make ops-*` scripts against fake kubectl, flux and helm (CI `lint` job) |
| `tests/check_release.sh` | The release's scripts and workflow, without GitHub or a registry (CI `lint` job) |
| `tests/release_gates.sh` | The release's scan gates and its push, proven locally with throwaway images and registry; run by hand |

## The Helm charts

Three charts, each its own release (specs/006 research R3, amended). `hds-secrets` keeps the
Secrets, synced from Vault by the Vault Secrets Operator, and is installed first. `hds-postgres`
holds the database, so it exists before the `hds` release's `pre-install` migration hook runs, and
its data is never touched by an app upgrade or rollback. `hds` reaches it through the Service named
in its values (`database.host`).

No chart defines a Secret; `hds` and `hds-postgres` read them by name: `hds-api` (`JWT_SECRET_KEY`,
`DATABASE_PASSWORD`, which the database's password also comes from, and the optional
`THROTTLE_SECRET_KEY`), `hds-smtp` (the relay's settings, when `mail.backend` is `smtp`),
`hds-backup` and `hds-registry`. `DATABASE_PASSWORD` goes into the database URL, so it must be
URL-safe (`openssl rand -hex 32`). PostgreSQL takes it only when it first creates the database;
to change it later, change it in Vault, then set the role's password to the same value with
`\password hds` in `psql` in the pod, which sends only its hash (an `ALTER ROLE ... PASSWORD`
statement would land in psql's history on the data volume, and in the server log if statements are
logged). The order and why: [the operations runbook](../docs/runbooks/operations.md#vault). Only the API, worker, migration and backup pods carry the
`hds/database-client` label that the database's NetworkPolicy lets in; the web pods do not. The
operations port 9100 is reachable only from the `monitoring` namespace, and the ingress never
routes it.

`helm/tests/assert.sh` runs `helm lint`, kubeconform (Kubernetes v1.37.1 schemas) and assertions
on the rendered manifests: `hds` for `values-local.yaml` and `values-ci.yaml`, and `hds-postgres`
with its defaults. Every tool runs from an image pinned in `VERSIONS.md`; only Docker is needed.

## Releases

A merge to `main` that passes CI runs `.github/workflows/release.yml`: it builds the three images
once for amd64 and arm64, scans each platform with Trivy (a fixed HIGH or CRITICAL finding, or any
secret, stops it), pushes those same images, and publishes the `hds` chart as `0.1.<run number>`
with the images' digests in its values. `hds-postgres` is published only when its version is
raised. Each image and chart is signed by digest, keyless, as it is pushed, and Flux
(`flux/clusters/local/`) pulls the newest chart every 5 minutes and installs it only if it carries
the release workflow's signature (Proposed); a failed upgrade rolls back.

**What the cluster reaches out to**: GHCR, for the charts and images; Sigstore over HTTPS
(`tuf-repo-cdn.sigstore.dev` for its TUF root, `rekor.sigstore.dev` for the transparency log),
which Flux needs to verify a chart's signature; Google Drive, for the backups; the SMTP relay.
Nothing reaches in. Without Sigstore, no new version installs, and the running one keeps
running. Deploys, rollbacks and the dry run: [the operations runbook](../docs/runbooks/operations.md).

## Backups

Daily at 03:30, `backup.sh` dumps the database and writes it with a manifest (revision, row
counts, SHA-256), encrypted by rclone crypt, to the `hds-backups` volume and to Drive; `prune.sh`
then deletes what is past 30 days, never the newest valid backup. On Sundays the restore test
restores the newest Drive copy into an empty database in its own pod, compares it with the
manifest, migrates it and checks it with the code. Vault's own snapshot goes weekly, encrypted with
age, through the plain remotes. The secrets are in the `hds-backup` Secret (from Vault). Restoring,
a lost machine and the Drive set-up: [the restore runbook](../docs/runbooks/restore.md).

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
