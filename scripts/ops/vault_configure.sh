#!/bin/sh
# make ops-vault-configure (specs/006-deploy-operations T052, T053; Proposed: the commands as a
# script, so they are tested): set Vault up for HDS, once after `vault operator init` and safe to
# run again. Run it with the root token, which is revoked after set-up (docs/runbooks/operations.md
# section Vault):
#
#   1. the KV v2 secrets engine at hds/ (data-model.md "Secrets");
#   2. Kubernetes auth, with only kubernetes_host in its config: Vault reads the CA and the
#      reviewer token from its own pod, so a snapshot restored onto a new cluster still lets the
#      Vault Secrets Operator and the snapshot job sign in (review R95);
#   3. the policies hds (read hds/data/*), vault-snapshot (the raft snapshot only) and alertmanager
#      (read hds/data/smtp only);
#   4. the roles, each bound to exactly one ServiceAccount, with short tokens: hds for hds-vault
#      and vault-snapshot for hds-vault-snapshot, in hds; alertmanager for hds-alertmanager-vault,
#      in monitoring (review R164).
#
# Environment: VAULT_ADDR and VAULT_TOKEN, as the vault CLI reads them; HDS_VAULT ("vault"), the
# command that runs the CLI (the checks run it in a container); HDS_KUBERNETES_HOST
# (https://kubernetes.default.svc:443).
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
k8s_host=${HDS_KUBERNETES_HOST:-https://kubernetes.default.svc:443}
fail() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }
say() { printf '%s\n' "$*"; }
v() { ${HDS_VAULT:-vault} "$@"; }

v secrets list -format=json > /dev/null || fail "Vault cannot be reached, or the token is not valid"

if v secrets list | grep -q '^hds/ '; then
    v secrets list -detailed | grep '^hds/ ' | grep -q 'version:2' \
        || fail "hds/ is mounted, but not as KV version 2"
    say "hds/ is KV v2 already"
else
    v secrets enable -path=hds -version=2 kv > /dev/null || fail "hds/ could not be mounted"
    say "mounted KV v2 at hds/"
fi

if v auth list | grep -q '^kubernetes/ '; then
    say "Kubernetes auth is enabled already"
else
    v auth enable kubernetes > /dev/null || fail "Kubernetes auth could not be enabled"
    say "enabled Kubernetes auth"
fi
# Written whole each time, so a CA or reviewer token from an earlier set-up is dropped.
v write auth/kubernetes/config kubernetes_host="$k8s_host" > /dev/null \
    || fail "auth/kubernetes/config could not be written"

v policy write hds - < "$ROOT/infra/cluster/vault-policy.hcl" > /dev/null \
    || fail "the hds policy could not be written"
v policy write vault-snapshot - < "$ROOT/infra/cluster/vault-snapshot-policy.hcl" > /dev/null \
    || fail "the vault-snapshot policy could not be written"
v policy write alertmanager - < "$ROOT/infra/cluster/vault-alertmanager-policy.hcl" > /dev/null \
    || fail "the alertmanager policy could not be written"

v write auth/kubernetes/role/hds bound_service_account_names=hds-vault \
    bound_service_account_namespaces=hds policies=hds token_ttl=10m token_max_ttl=20m \
    audience=vault > /dev/null || fail "the hds role could not be written"
v write auth/kubernetes/role/vault-snapshot bound_service_account_names=hds-vault-snapshot \
    bound_service_account_namespaces=hds policies=vault-snapshot token_ttl=5m token_max_ttl=10m \
    audience=vault > /dev/null || fail "the vault-snapshot role could not be written"
v write auth/kubernetes/role/alertmanager bound_service_account_names=hds-alertmanager-vault \
    bound_service_account_namespaces=monitoring policies=alertmanager token_ttl=10m \
    token_max_ttl=20m audience=vault > /dev/null || fail "the alertmanager role could not be written"
say "Vault is configured: hds/ (KV v2), Kubernetes auth, the hds, vault-snapshot and alertmanager roles"
