# Data model: Deployment and operations

**Feature**: [spec.md](spec.md) | **Research**: [research.md](research.md)

This feature adds one column to the application database. Everything else it keeps lives outside the
database, in places the operator already has: backup manifests next to the backups, deploy history
in Helm and Flux, and job outcomes in Kubernetes.

## Application database

### `schedule_jobs.trace_context` (migration 024)

| Column | Type | Null | Meaning |
| --- | --- | --- | --- |
| `trace_context` | `VARCHAR(55)` | yes | The W3C `traceparent` of the request that queued the job, so the worker continues its trace (research R8) |

- **Rules**: the column is written once when the job is queued and never changed. It is null for
  jobs queued before the migration, and for requests that carry no trace; the worker then starts a
  new trace. The value is validated against the `traceparent` format, `00-<32 hex>-<16 hex>-<2 hex>`,
  and a malformed value is stored as null. It holds no personal data.
- **Migration**: 024 adds the nullable column, which is an expand step. The previous release ignores
  the column, so the two run side by side (FR-016), and the downgrade drops it.

## Kept outside the database

### Backup (the spec's "Backup")

One object pair per run, in each of the two places (`local-crypt:` and `offsite-crypt:`):

| Object | Content |
| --- | --- |
| `hds-<UTC yyyymmddTHHMMSSZ>.dump` | `pg_dump --format=custom` of the application database |
| `hds-<same>.manifest.json` | the fields below |

Manifest fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `name` | string | `hds-<timestamp>` |
| `taken_at` | RFC 3339, UTC | when the snapshot started |
| `version` | string | the `HDS_VERSION` that took it |
| `alembic_revision` | string | the database's migration version at the snapshot |
| `tables` | object, table name → integer | row counts in the snapshot |
| `size_bytes` | integer | size of the dump |
| `sha256` | hex string | of the dump, before encryption |
| `expires_on` | date | `taken_at` plus the retention (30 days) |

- **Validity**: a backup is valid only when both objects exist in a place and the dump's SHA-256
  matches the manifest. The manifest is written last, so a run cut short leaves no valid backup.
- **Retention**: a backup past `expires_on` is deleted, except that the newest valid backup in each
  place is never deleted (FR-011).
- **States**: running, then valid in both places (success); valid only locally (failure, so the
  alert fires); or nothing written (failure).

### Restore test result

This is the Kubernetes Job status of the `restore-test` CronJob. It runs weekly, and the last three
runs are kept: `successfulJobsHistoryLimit: 3`, `failedJobsHistoryLimit: 3`. kube-state-metrics
exposes it as `kube_cronjob_status_last_successful_time` and `kube_job_status_failed`. The Job's log
names the backup it restored and each check's outcome.

### Deploy record

Each deploy is a Helm release revision: revision number, chart version, `appVersion` (the commit),
time, status (`deployed`, `failed`, `superseded`) and description. Each also leaves Flux events on
the `HelmRelease`. `helm history hds` lists them, as does `make ops-status`. Helm keeps the last ten
revisions (`maxHistory: 10`).

### Release version

| Field | Meaning |
| --- | --- |
| commit SHA | the identity of the version; it tags all three images |
| image digests | what was scanned and what runs |
| chart version | `0.1.<release run number>`, increasing, so Flux picks the newest |

The version is reported as `HDS_VERSION` by `/livez` and by `hds_build_info` (FR-010).

### Health status

Health is computed when it is asked for and never stored. See [contracts/ops.md](contracts/ops.md)
for the shapes.

### Environment

| Environment | Where | `ENVIRONMENT` | Secrets from |
| --- | --- | --- | --- |
| Local | Docker Compose on a developer's machine | `development` | `.env`, written by `make env` |
| Shared | k3d cluster on the operator's machine | `production` | Vault, through the Vault Secrets Operator |
| CI | throwaway compose stack, or a k3d cluster | `test` / `production` | the workflow's environment, with throwaway values |

## Secrets (paths in Vault, KV v2)

| Path | Keys | Read by |
| --- | --- | --- |
| `hds/api` | `JWT_SECRET_KEY`, `THROTTLE_SECRET_KEY`, `DATABASE_PASSWORD` | API, worker, migration Job, PostgreSQL |
| `hds/smtp` | `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `MAIL_FROM`, `OPERATOR_EMAILS` | API, Alertmanager |
| `hds/backup` | `RCLONE_CRYPT_PASSWORD`, `RCLONE_CRYPT_SALT`, `DRIVE_TOKEN`, `DRIVE_FOLDER_ID` | backup and restore-test Jobs |
| `hds/registry` | `GHCR_READ_TOKEN` | image pull secret, when the packages are private |

Vault's unseal keys, its root token and the age private key for Vault's own snapshots are never
stored in Vault, in the cluster or in the repository. The age public key is not a secret and sits in
the chart values (research R11).
