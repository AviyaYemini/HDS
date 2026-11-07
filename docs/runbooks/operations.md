# Runbook: operating HDS in the shared environment

The `make ops-*` targets are the ones `contracts/ops.md` §5 lists, run from the operator's machine
with flux, helm and kubectl. Background: research R3 (amended) and R4 in
`specs/006-deploy-operations/research.md`. Restoring from a backup is its own runbook,
[restore.md](restore.md).

## Deploys

**How a change reaches the cluster.** A merge to `main` runs CI. When CI succeeds on that push, the
Release workflow (`.github/workflows/release.yml`):
- builds `hds-api`, `hds-web` and `hds-backup` once, for amd64 and arm64;
- scans each platform with Trivy, where a fixed HIGH or CRITICAL vulnerability, or any secret,
  stops the release;
- pushes those same images and publishes the `hds` chart as `0.1.<run number>`, naming each image
  by digest.

Within 5 minutes Flux in the cluster sees the new chart and upgrades the `hds` release. The
`hds-postgres` chart is published only when its own version is raised, and it is installed before
`hds`. Releases never overlap.

1. Follow a deploy:

   ```bash
   flux -n flux-system get helmreleases
   helm -n hds history hds --max 5
   ```

   Expected: `hds` `Ready True` with the new chart version, and `helm history` shows one revision
   per deploy, at most 10 kept. `make ops-status` (Group K) shows the version each pod runs.

   After two merges close together, the Release run for the older one may be red with `CI ran on
   <commit>, but main is now at <commit>`. That is expected and harmless: only main's newest commit
   is published, by its own run.

2. **A failed deploy rolls itself back.** The upgrade waits up to 10 minutes for every new pod to
   be ready. If they never are, Flux rolls back to the previous revision and does not retry. The
   old pods serve throughout, because no old pod stops before a new one is ready. Flux's alert
   e-mails the operators (Group K).

   Expected: `helm -n hds history hds` shows the failed revision, then a `Rollback to <n>`
   revision with `deployed`; `flux get helmreleases` shows `hds` `Ready False` with the reason.
   The same version is not tried again: the next release, with the fix, is.

3. **Roll back a deploy that succeeded but is wrong:**

   ```bash
   make ops-rollback
   ```

   It goes back to the last deploy before the version running now, skipping failed revisions;
   run it again to go further back, or pick one with `make ops-rollback REVISION=<n>` (from
   `helm -n hds history hds`). It needs jq on the operator's machine. Expected:

   ```text
   rolling back to Helm revision <n>
   Flux is suspended for hds
   ...
   hds now runs <the previous commit> (Helm revision <m>)
   Flux stays suspended for hds. Run make ops-resume once a fixed version is released:
   Flux then installs the newest version.
   ```

   A rollback changes the code only. It runs no migration, so the database keeps the newer
   schema, which the previous release still works with (specs/006 FR-016; the `compatibility`
   CI job checks it, Group L). For a change that must be undone in the data, use a backup
   ([restore.md](restore.md)).

4. **Hand `hds` back to Flux** once the fix is released, never before: Flux installs the newest
   chart at once, the bad one if nothing newer is out.

   ```bash
   make ops-resume
   ```

   Expected: `Flux manages hds again; hds now runs <the newest commit> (Helm revision <n>)`.

   A `make ops-restore` while Flux is suspended after a rollback leaves it suspended: it sets back
   what it found.

5. **Hold a version for longer.** Set the `semver` range in
   `infra/flux/clusters/local/oci-repository.yaml` (for example `"0.1.41"`) and apply the folder
   again (`kubectl apply -k infra/flux/clusters/local`); resume Flux if it is suspended. Undo it
   by setting `">=0.1.0"` again.

**Signed releases** (Proposed). Each image and chart is signed keyless, by digest, by the Release
workflow on `main`, and Flux installs only a chart with that signature. To check one by hand (cosign
from `infra/VERSIONS.md`):

```bash
cosign verify ghcr.io/aviyayemini/hds-api@sha256:<digest> \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity https://github.com/AviyaYemini/HDS/.github/workflows/release.yml@refs/heads/main
```

Expected: `The cosign claims were validated` and the certificate's identity, as above.

- **When no new version installs**: `flux -n flux-system get sources oci` shows the source not
  Ready, and the alert e-mail carries the same message. A message about the signature (`no
  matching signatures`, `none of the expected identities matched`) means the chart is unsigned or
  signed by someone else: do not work around it, find out who pushed it. A message about fetching
  the trust root or reaching `tuf-repo-cdn.sigstore.dev` or `rekor.sigstore.dev` means the machine
  cannot reach Sigstore: the running version keeps running, and the next one installs once it can.
- **The record is public**: each signature is written to Sigstore's public transparency log, with
  the repository's and the workflow's names. That is fine while the repository is public; if it is
  ever made private, the log still names it.
- **The limit**: anyone who can push to `main` can produce a validly signed release, since the
  signature says only "the Release workflow on main". Protect `main` in GitHub's settings: a
  required pull request and passing CI before merge (recommended to the owner; not set by this
  repository).

**The Release workflow's dry run.** A manual run builds and scans and publishes nothing:

```bash
gh workflow run release.yml -f dry_run=true
```

GitHub can dispatch only a workflow that is on the default branch, so this works from the merge of
specs/006 Group I onwards. Until then, `infra/tests/release_gates.sh` proves the same gates locally,
with the same scripts and flags. A run with `dry_run=false` fails at once: only CI's success on
`main` publishes.

## Vault

Vault holds every secret the cluster runs with (data-model.md "Secrets"): one node in the namespace
`vault`, plain HTTP inside the cluster, reachable only from the Vault Secrets Operator's namespace
and the snapshot job (its NetworkPolicy; `kubectl port-forward` and `kubectl exec` go through the
kubelet and are not limited by it). The operator copies each path into the Secret of the same name
in `hds` every 60 seconds and restarts the API and the worker when `hds-api` or `hds-smtp` changes.

**Never put a secret in a file or on a command line.** The commands below take them from the
terminal without echo (`read -rs`), and hand them to Vault on its standard input: a value on a
command line shows in the process list and in shell history.

### Set-up, once

After `make ops-cluster` and before Flux installs anything (specs/006 T063, T064):

1. Initialise Vault, with one unseal key:

   ```bash
   kubectl -n vault exec -it vault-0 -- vault operator init -key-shares=1 -key-threshold=1
   ```

   Expected: `Unseal Key 1: …` and `Initial Root Token: …`. Put both in the password manager now,
   in no file, then clear the terminal's scrollback. Without the unseal key, Vault's data cannot be
   read again; it is part of the recovery kit (restore.md).

2. Unseal it: `make ops-unseal`, and paste the key at the prompt (it is not shown). Expected:
   `Vault is unsealed`.

3. Reach Vault and sign in for this shell only, with the root token from the prompt. `vault login`
   would write it to `~/.vault-token`; this keeps it in the shell's memory:

   ```bash
   kubectl -n vault port-forward vault-0 8200:8200 > /dev/null &
   export VAULT_ADDR=http://127.0.0.1:8200
   read -rs VAULT_TOKEN && export VAULT_TOKEN
   ```

4. Configure it: `make ops-vault-configure`. Expected: `Vault is configured: hds/ (KV v2),
   Kubernetes auth, the hds and vault-snapshot roles`. It is safe to run again.

5. Write the four paths. Each value is typed at a silent prompt or made by `openssl`, and reaches
   Vault as one JSON object on its standard input (`printf` is the shell's own, so no value is an
   argument). `hds/api`, with the database password URL-safe:

   ```bash
   printf '{"JWT_SECRET_KEY":"%s","THROTTLE_SECRET_KEY":"%s","DATABASE_PASSWORD":"%s"}' \
     "$(openssl rand -hex 32)" "$(openssl rand -hex 32)" "$(openssl rand -hex 32)" \
     | vault kv put -mount=hds api -
   ```

   `hds/smtp`: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `MAIL_FROM` and
   `OPERATOR_EMAILS`, the relay's password read first with `read -rs SMTP_PASSWORD`:

   ```bash
   read -rs SMTP_PASSWORD
   printf '{"SMTP_HOST":"%s","SMTP_PORT":"587","SMTP_USERNAME":"%s","SMTP_PASSWORD":"%s","MAIL_FROM":"%s","OPERATOR_EMAILS":"%s"}' \
     smtp.example.org hds@example.org "$SMTP_PASSWORD" hds@example.org ops@example.org \
     | vault kv put -mount=hds smtp - && unset SMTP_PASSWORD
   ```

   `hds/backup`: `HDS_CRYPT_PASSWORD` and `HDS_CRYPT_SALT` (each `openssl rand -hex 32`; keep both
   in the password manager too, or the backups cannot be read), `HDS_DRIVE_TOKEN` (restore.md
   section 3) and `HDS_DRIVE_FOLDER_ID`, and `HDS_DRIVE_CLIENT_ID` and `HDS_DRIVE_CLIENT_SECRET`
   with your own OAuth client. `hds/registry`: `GHCR_READ_TOKEN`, only when the packages are
   private (then set `registry.enabled` in hds-secrets and `imagePullSecrets: [hds-registry]` in
   hds).

6. Make the age key pair for Vault's snapshots: run `age-keygen` (age from `infra/VERSIONS.md`) with
   no `-o`, so it prints the pair. Put the whole output in the password manager, beside the unseal
   key. Its `# public key: age1…` line goes into `infra/helm/hds/values-local.yaml` as
   `vault.snapshotRecipient`; it is not a secret.

7. Revoke the root token, and forget it:

   ```bash
   vault token revoke -self && unset VAULT_TOKEN
   ```

   Every later change (a secret, a policy, a role) signs in the same way, with a root token made
   for it and revoked after: see "Signing in for a change" below. No standing token exists.

8. Only now let Flux install the releases: `make ops-flux` (T063, Proposed). It hands Flux
   `hds-secrets` alone, waits until `hds-api`, `hds-smtp` and `hds-backup` are synced into `hds`,
   and only then `hds-postgres` and `hds`. A path not written, or Vault sealed, stops it at the
   wait, with nothing else installed. Installed earlier, `hds-postgres` would wait for a Secret that
   does not exist yet and fail its install, which is not retried.

### Signing in for a change

The root token is revoked after set-up, and no other token can write to Vault. For a change, make
a root token with the unseal key, use it, and revoke it again:

```bash
kubectl -n vault port-forward vault-0 8200:8200 > /dev/null &
export VAULT_ADDR=http://127.0.0.1:8200
vault operator generate-root -generate-otp        # prints a one-time password: OTP below
vault operator generate-root -init -otp=OTP       # prints a nonce: NONCE below
vault operator generate-root -nonce=NONCE         # asks for the unseal key, prints an encoded token
vault operator generate-root -decode=ENCODED -otp=OTP   # prints the new root token
read -rs VAULT_TOKEN && export VAULT_TOKEN        # paste it at the silent prompt
```

The one-time password and the encoded token are not secrets on their own (the token is encrypted
with the one-time password, which is used once). When the change is done:
`vault token revoke -self && unset VAULT_TOKEN`.

### Changing a secret

**Change one key with `vault kv patch`, never `vault kv put`**, which replaces the whole path and
drops its other keys (`JWT_SECRET_KEY` and `THROTTLE_SECRET_KEY` with a new `DATABASE_PASSWORD`, say).
The value goes on standard input with `KEY=-`:

```bash
read -rs VALUE && printf '%s' "$VALUE" | vault kv patch -mount=hds smtp SMTP_PASSWORD=- && unset VALUE
```

Within about a minute the Secret changes and the API and the worker restart, one pod at a time
(FR-027). The Jobs (the migration, the backups, the restore test, Vault's snapshot) read their
Secrets when they start, so the next run has the new value.

**The database password** is the one exception: PostgreSQL reads its password only when it first
creates its data, so the role's password must change too. Vault first, then the role, straight
after:

1. `vault kv patch -mount=hds api DATABASE_PASSWORD=-`, with the new value from `openssl rand -hex
   32` on its standard input. The API and the worker start new pods, which cannot sign in to the
   database yet, so they do not become ready. The old pods keep serving on the connections they
   hold: both Deployments start a new pod before stopping an old one (maxSurge 1, maxUnavailable 0),
   so neither is ever down.
2. Set the role's password to the same value, in `psql` in the database's pod. `\password` sends
   only its hash, so the value reaches no server log and no history:

   ```bash
   kubectl -n hds exec -it hds-postgres-0 -- psql -U hds -d hds -c '\password hds'
   ```

   Expected: the new pods turn ready and the old ones stop. Between steps 1 and 2 only new
   connections of the old pods fail; a migration or a backup that starts in that gap fails and
   is run again. The database itself is never restarted for a new password.

### After a restart: unseal

Vault starts sealed after every restart of its pod or of the machine. `make ops-unseal`, with the
unseal key from the password manager. While it is sealed, the Secrets already in `hds` stay and the
pods keep running, but no change reaches them, and the weekly snapshot fails: `HdsVaultSnapshotFailed`
(Group K) is the reminder to unseal.

### Vault's snapshot

The snapshot CronJob exists only once `vault.snapshotRecipient` is set (set-up step 6). Until
then no snapshot is taken: `make ops-status` (Group K) says `vault snapshot: not configured`, marked
`FAIL`.

### Never uninstall hds-secrets

Its VaultStaticSecrets are kept by Helm on uninstall (`helm.sh/resource-policy: keep`): deleting
one makes the operator delete the Secret it made, and every pod would fail at its next start. To
remove them on purpose, knowing every Secret in `hds` goes with them: suspend Flux for the three
releases (`flux -n flux-system suspend helmrelease <name>` for `hds`, `hds-postgres` and
`hds-secrets`), then `kubectl -n hds delete vaultstaticsecret --all`, then
`helm -n hds uninstall hds-secrets`.
