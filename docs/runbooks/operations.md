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
