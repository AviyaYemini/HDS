#!/bin/sh
# Vault's weekly snapshot (specs/006-deploy-operations research R11).
#
# It takes a raft snapshot through Vault's HTTP API and encrypts it on the way with age to the
# public key HDS_VAULT_RECIPIENT, so the snapshot never touches the disk unencrypted and the cluster
# can write snapshots but never read them. vault-<UTC time>.snap.age then goes to the plain local:
# and offsite: remotes, not the crypt ones: their password lives in Vault, which this must be able
# to restore without. prune.sh keeps the newest 8 on each side.
#
# Environment: VAULT_ADDR; VAULT_TOKEN, or HDS_VAULT_ROLE to sign in with the pod's ServiceAccount
# token (Vault's Kubernetes auth, Group J); HDS_VAULT_RECIPIENT, the age public key; and lib.sh's
# HDS_BACKUP_DIR, HDS_OFFSITE_PATH and the remotes' settings.
set -eu
# shellcheck source-path=SCRIPTDIR source=lib.sh
. "$(dirname "$0")/lib.sh"
: "${VAULT_ADDR:?VAULT_ADDR is not set}" "${HDS_VAULT_RECIPIENT:?HDS_VAULT_RECIPIENT is not set}"

work_dir
rclone_config plain

if [ -z "${VAULT_TOKEN:-}" ]; then
    : "${HDS_VAULT_ROLE:?set VAULT_TOKEN or HDS_VAULT_ROLE}"
    jwt=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
    VAULT_TOKEN=$(jq -n --arg role "$HDS_VAULT_ROLE" --arg jwt "$jwt" '{role: $role, jwt: $jwt}' \
        | curl -fsS --data @- "$VAULT_ADDR/v1/auth/kubernetes/login" | jq -er .auth.client_token) \
        || fail "Vault refused the ServiceAccount's sign-in for role $HDS_VAULT_ROLE"
fi

file="vault-$(date -u -d "@$(now)" +%Y%m%dT%H%M%SZ).snap.age"
# The token goes to curl on its standard input, as a header file, never as an argument. A pipeline's
# status is its last command's, and age would encrypt an empty input, so curl's failure is noted
# apart.
snapshot() {
    curl -fsS -H @- "$VAULT_ADDR/v1/sys/storage/raft/snapshot" || : > "$HDS_WORK/curl.failed"
}
printf 'X-Vault-Token: %s\n' "$VAULT_TOKEN" | snapshot \
    | age -r "$HDS_VAULT_RECIPIENT" -o "$HDS_WORK/$file" \
    || fail "the snapshot could not be encrypted"
[ ! -e "$HDS_WORK/curl.failed" ] || fail "Vault did not give a snapshot"

failed=""
for place in "local:$HDS_BACKUP_DIR/vault" "offsite:$HDS_OFFSITE_PATH/vault"; do
    if rclone copyto "$HDS_WORK/$file" "$place/$file"; then
        log "$file written to $place"
    else
        failed="$failed $place"
    fi
done
[ -z "$failed" ] || fail "$file not stored in:$failed"
