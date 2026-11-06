# Quickstart: validating deployment and operations

**Feature**: [spec.md](spec.md) | **Contract**: [contracts/ops.md](contracts/ops.md)

Each scenario below proves one user story, end to end. CI runs scenarios 1, 2, 5 and 6 on every
pull request. Scenarios 3 and 4 need the shared environment.

## Prerequisites

- **Local** (scenario 1): Docker and make, and nothing else.
- **Shared** (scenarios 2 to 6): the above, plus k3d, kubectl, helm, the flux CLI, the vault CLI and
  rclone. `make ops-cluster` checks that each one is present and names any that is missing.
- **Accounts**, for the shared environment only: an SMTP relay and a Google account for the
  off-machine copy.

## 1. One command (US1, FR-001 to FR-005, SC-001)

```bash
git clone <repo> hds && cd hds
make env          # writes .env with a random key; run it again and it refuses
make up           # everything, from nothing
```

Expected:
- `curl -s localhost:8000/readyz` answers `{"status":"ok",…}`.
- `http://localhost:8081` shows the sign-in screen, and every sample account in the README signs in.

Then:
- `make down && make up`: the shifts created before are still there.
- `make wipe`: it asks first, then the next `make up` starts empty.
- With `JWT_SECRET_KEY` removed from `.env`, `make up` stops and names the missing setting.

## 2. Health and metrics (US2, FR-006 to FR-010)

With the local stack up:
- `curl -s localhost:8000/livez` and `curl -s localhost:8000/readyz` each answer 200 with the
  version.
- `docker compose -f infra/docker-compose.yml --env-file .env stop postgres`: now `/readyz` answers
  503 with `"failing":["database"]` within 2 seconds, while `/livez` still answers 200. Start
  PostgreSQL again and `/readyz` returns to 200.
- `curl -s localhost:9100/metrics | grep hds_http_requests_total` shows lines labelled with route
  templates only, with no ids, names or addresses.

In the cluster, `make ops-status` shows every part ready, at the deployed version. From outside the
cluster, port 9100 is not reachable through `hds.localhost`.

## 3. Backup and restore (US3, FR-011 to FR-014, SC-005, SC-006)

```bash
make ops-backup                      # now, instead of waiting for 02:30
make ops-status                      # last backup: just now, both places
rclone lsf offsite-crypt:            # the dump and its manifest
```

In the Google Drive folder itself, only encrypted names and content appear.

```bash
make ops-restore-test                # restores the off-machine copy into an empty database and checks it
make ops-restore BACKUP=hds-<ts>     # refuses: the database has data
make ops-restore BACKUP=hds-<ts> CONFIRM=replace
```

After the restore, signing in and reading shifts and pay works, and the records match those at the
backup's time. The full procedure, including a lost machine, is in `docs/runbooks/restore.md`, and
following it takes under an hour (SC-005).

To check that a failure is reported, temporarily break the Drive token in Vault and run
`make ops-backup`. The operators get an `HdsBackupFailed` e-mail, the local copy is kept, and
`make ops-status` shows the failure.

## 4. Deploy from main and roll back (US4, FR-015, FR-018, FR-021 to FR-023, SC-002, SC-008)

1. Merge a pull request with a visible change, such as a string in the web client.
2. Within 30 minutes, with the machine on, `make ops-status` shows the new commit and the change is
   visible at `http://hds.localhost:8080`. `helm history hds` lists the revision.
3. `make ops-rollback`: within 10 minutes the previous commit runs, and the change is gone. The
   database is unchanged.
4. `make ops-resume`: Flux reinstalls the newest version.

A pull request whose image has a high-severity finding fails the release workflow, and nothing is
published.

## 5. Nobody notices an update (US5, FR-016, FR-017, FR-019, FR-020, SC-003)

The CI job `rolling-update` does the following:
1. It creates a k3d cluster and installs the chart at the base commit.
2. It starts the load script: signed-in reads and writes, steadily, for the whole run.
3. It queues a schedule job.
4. It upgrades to the change's images and waits for the rollout.

Expected: zero failed requests, the same user is still signed in after the upgrade, and the schedule
job has one result. Every run upgrades across a migration, adding a throwaway one when the change has
none. The same job checks that sign-in failures from two pods inside the cluster count separately
behind Traefik, and that a forged `X-Forwarded-For` does not change the address counted. On the Mac
the client's address is lost before Traefik; this is an accepted limit (research R14).

The compatibility test runs the previous release's API tests against the new migration head, and it
passes.

## 6. No secrets (US6, FR-024 to FR-027, SC-007)

- Commit a file containing a sample key, such as an AWS-format access key: pre-commit refuses it,
  naming the file and line, and the CI `secrets` job fails on it.
- The release workflow's Trivy secret scan of the three images finds nothing.
- Start the API with `ENVIRONMENT=production` and `JWT_SECRET_KEY` set to a value from
  `.env.example`: it refuses and names `JWT_SECRET_KEY`, not the value.
- In the cluster, write a new `JWT_SECRET_KEY` to Vault at `hds/api`. Within a minute the API pods
  restart one at a time with the new key, and nothing else changes. Access tokens signed with the
  old key stop working, but refresh tokens are opaque and stored in the database, so a signed-in
  client refreshes and stays signed in.
