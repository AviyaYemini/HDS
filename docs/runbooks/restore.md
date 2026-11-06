# Runbook: restoring HDS from a backup

When to use it: the database is damaged or lost, a change must be undone from a backup, or the
machine itself is gone. It covers the Drive set-up the backups need, and the move to S3 later.
Following it end to end, a lost machine is serving again in under an hour (specs/006 SC-005).

Background: research R11 in `specs/006-deploy-operations/research.md`. A daily backup is
`hds-<UTC yyyymmddTHHMMSSZ>.dump` and its `.manifest.json`, encrypted by rclone crypt, on the
machine (`local-crypt:`, the `hds-backups` volume) and on Drive (`offsite-crypt:`). Vault's weekly
snapshot is `vault-<UTC time>.snap.age`, encrypted with age, on the plain `local:` and `offsite:`
remotes. The `make ops-*` targets are the ones `contracts/ops.md` §5 lists, run from the operator's
machine with kubectl. `make ops-restore` arrives with Group I (T049b); until then, section 1 cannot
be run in the cluster.

**The recovery kit, kept outside the machine**: Vault's unseal keys, the age private key for
Vault's snapshots, and the Google account that holds the Drive folder. Without all three, a lost
machine cannot be recovered. Check where each one is now, before you need them.

## 1. Restore a named backup in the running cluster

1. List the backups on Drive, newest last:

   ```bash
   make ops-backups
   ```

   Expected: one `hds-<time>.dump` and one `hds-<time>.manifest.json` per day, for 30 days.

2. Pick the backup by its time, `hds-20261105T033000Z` say, and restore it. `make ops-restore`
   suspends Flux for its whole run, so no release can start mid-restore, and resumes it at the end:

   ```bash
   make ops-restore BACKUP=hds-20261105T033000Z
   ```

   Expected, since the database has data: it refuses and changes nothing.

   ```text
   restore.sh: FAILED: hds on hds-postgres holds data; nothing was changed. Set CONFIRM=replace to replace it
   ```

3. Replace it, knowing that everything after the backup's time is lost:

   ```bash
   make ops-restore BACKUP=hds-20261105T033000Z CONFIRM=replace
   ```

   Expected: the API and the worker stop, the backup is restored and migrated, and they start
   again with their earlier replica counts. The restore's own lines are:

   ```text
   restore.sh: hds-20261105T033000Z: downloaded from offsite-crypt; the checksum matches
   restore.sh: hds-20261105T033000Z: restored into hds on hds-postgres; run migrate next
   ```

   T049b settles the whole output.

   A checksum mismatch stops it before anything changes; take the previous day's backup. If the
   restore itself fails, the database is empty: run step 3 again with an older backup.

4. Check the service:

   ```bash
   make ops-status
   ```

   Expected: the API `ready` on every pod, the worker `ready`, and the version you deployed.

## 2. Restore after losing the machine

1. Fetch the recovery kit: the unseal keys, the age private key (`vault-age.key` here) and the
   Google account.

2. On the new machine, build the cluster and install Vault as the repository's README says (the
   cluster section), but stop before anything reads secrets: the API, the worker and the backups
   wait for Vault.

   Expected: `kubectl get nodes` shows the node `Ready`, and `kubectl -n vault get pods` shows
   `vault-0` running but not ready (sealed).

3. In a browser, sign in to the Google account, open the Drive folder the backups use, then its
   `hds/vault` folder, and download the newest `vault-<time>.snap.age`. Its name is not encrypted.

4. Decrypt it:

   ```bash
   age -d -i vault-age.key -o vault.snap vault-20261101T030000Z.snap.age
   ls -l vault.snap
   ```

   Expected: no output from `age`, and a `vault.snap` of a few hundred kilobytes or more.

5. Initialise the new Vault, unseal it with the new keys this prints, and restore the snapshot over
   it:

   ```bash
   kubectl -n vault exec -it vault-0 -- vault operator init -key-shares=1 -key-threshold=1
   kubectl -n vault exec -it vault-0 -- vault operator unseal        # the new unseal key
   kubectl -n vault exec -it vault-0 -- vault login                  # the new root token
   kubectl -n vault cp vault.snap vault-0:/tmp/vault.snap
   kubectl -n vault exec vault-0 -- vault operator raft snapshot restore -force /tmp/vault.snap
   ```

   Expected: the restore prints nothing and exits 0. Vault now holds the old data, so it is sealed
   again under the **old** keys.

6. Unseal it with the old unseal keys from the kit:

   ```bash
   kubectl -n vault exec -it vault-0 -- vault operator unseal        # an old key, as many as needed
   kubectl -n vault exec vault-0 -- vault status
   ```

   Expected: `Sealed false`. Delete `vault.snap` and the pod's `/tmp/vault.snap` now:

   ```bash
   rm vault.snap && kubectl -n vault exec vault-0 -- rm /tmp/vault.snap
   ```

7. Let Flux install the rest (hds-postgres, then hds). The Vault Secrets Operator fills the
   Secrets (`hds-api`, `hds-smtp`, `hds-backup`) from the restored Vault. This works because
   Vault's Kubernetes auth holds only the cluster's address, `kubernetes_host`, and reads the CA
   and the reviewer token from its own pod (Group J, review R95). A snapshot carrying the old
   cluster's CA would keep every sign-in out.

   As soon as the CronJob exists, stop the new cluster's backups until step 10. A backup of the
   new, empty database would otherwise become the newest one on Drive, and the restore test would
   fail on it:

   ```bash
   kubectl -n hds patch cronjob hds-backup -p '{"spec":{"suspend":true}}'
   ```

   Expected: `kubectl -n hds get secrets hds-api hds-backup` lists both, and `kubectl -n hds get
   cronjob hds-backup` shows `SUSPEND True`. The database is new and empty, and the API's pods
   are ready once the migration hook has run.

8. Restore the newest backup from before the loss, by its name. The database is empty, so no
   `CONFIRM` is needed:

   ```bash
   make ops-backups                                   # pick the last name before the loss
   make ops-restore BACKUP=hds-20261105T033000Z
   ```

   Expected: the output of section 1, step 3.

9. Check the service, as in section 1, step 4, and sign in as a known user.

10. Start the backups again, take one now, and check it reached both places:

    ```bash
    kubectl -n hds patch cronjob hds-backup -p '{"spec":{"suspend":false}}'
    make ops-backup
    ```

    Expected: `written to local-crypt`, then `written to offsite-crypt`, then `done`.

## 3. Setting up Drive for the backups

Once, on the operator's own computer with a browser and rclone (infra/VERSIONS.md).

1. Choose the OAuth client. Either rclone's own (no set-up; shared, so slower), or your own Google
   Cloud OAuth client of type "Desktop app". **Publish your own app ("In production")**: an app
   left in "Testing" expires its refresh tokens after **7 days**, and every backup after that
   fails to reach Drive, which the `HdsBackupFailed` alert then reports.

2. Authorise rclone for the `drive.file` scope, so it can see only the files it made itself:

   ```bash
   rclone config create hds-drive drive scope=drive.file config_is_local=true
   ```

   Expected: a browser window asks to allow access; then `rclone` prints the new remote.

3. Make the folder the backups go in, and read its ID:

   ```bash
   rclone mkdir hds-drive:hds-backups
   rclone lsjson hds-drive: --dirs-only | jq -r '.[] | select(.Name == "hds-backups") | .ID'
   ```

   Expected: one ID, such as `1AbC...`.

4. Store the token and the folder in Vault, at `hds/backup`, with the crypt remotes' password and
   salt (made once, `openssl rand -hex 32` each, and kept in the recovery kit's place as well):

   ```bash
   rclone config dump | jq -c '."hds-drive".token' > drive-token.json
   vault kv put hds/backup HDS_DRIVE_TOKEN=@drive-token.json HDS_DRIVE_FOLDER_ID=1AbC... \
       HDS_CRYPT_PASSWORD=... HDS_CRYPT_SALT=...
   rm drive-token.json
   ```

   Add `HDS_DRIVE_CLIENT_ID` and `HDS_DRIVE_CLIENT_SECRET` when you use your own client. Expected:
   `Success! Data written to: hds/backup`, and within a minute the `hds-backup` Secret has the keys.

5. Run a backup and check Drive shows only encrypted names under `hds/db`:

   ```bash
   make ops-backup
   ```

   Expected: `written to offsite-crypt`; in the browser, file names that look random.

## 4. Moving the off-machine copy to S3

Only the `[offsite]` remote in `infra/backup/rclone.conf.tmpl` changes. The crypt layer, the
scripts, the manifests and the tests stay as they are.

1. Replace the `[offsite]` section with:

   ```ini
   [offsite]
   type = s3
   provider = AWS
   region = ${HDS_S3_REGION}
   access_key_id = ${HDS_S3_ACCESS_KEY_ID}
   secret_access_key = ${HDS_S3_SECRET_ACCESS_KEY}
   ```

2. Put the three values in Vault at `hds/backup`, and set the chart's `backup.offsitePath` to
   `<bucket>/hds`.

3. Release the change, then run `make ops-backup` and `make ops-restore-test`.

   Expected: `written to offsite-crypt`, then `restore test passed for hds-<time>` and `restore
   check passed`.

4. Keep the Drive copies until the S3 side holds 30 days of backups, then delete the Drive folder.
