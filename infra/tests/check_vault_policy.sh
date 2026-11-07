#!/usr/bin/env bash
# Checks scripts/ops/vault_configure.sh and the policies it writes (specs/006-deploy-operations
# T052; reviews R136, R142, R143) against a real Vault in dev mode, the version the cluster runs
# (infra/VERSIONS.md). Not text: what each role's token can and cannot do. Run by the CI chart job:
#   infra/tests/check_vault_policy.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
VAULT_IMAGE="hashicorp/vault:2.0.4@sha256:5be49781ecf78bfe775c5309c6a4d9f4e9e040b6c885c99eb2b12fb69855e1a2"
# A dev server's root token, made here for this throwaway server only.
root_token="dev-$(openssl rand -hex 16)"
# What a pod's ServiceAccount mount holds, made here: with only kubernetes_host configured, Vault
# reads its CA and token from there (review R95), and it checks the CA file exists.
pod=$(mktemp -d)
openssl req -x509 -newkey rsa:2048 -nodes -subj /CN=throwaway -days 1 -keyout "$pod/key" \
    -out "$pod/ca.crt" 2> /dev/null
printf 'throwaway' > "$pod/token"
# Vault runs as its own user (uid 100), and a bind mount keeps the host's permissions on Linux, so
# the folder (mktemp's 0700) and the files must be readable by others (review R148).
chmod 755 "$pod"
chmod 644 "$pod/ca.crt" "$pod/token"
container=$(docker run -d --cap-add IPC_LOCK -e VAULT_DEV_ROOT_TOKEN_ID="$root_token" \
    -e VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200 \
    -v "$pod:/var/run/secrets/kubernetes.io/serviceaccount:ro" "$VAULT_IMAGE")
trap 'docker rm -f "$container" > /dev/null 2>&1 || true; rm -rf -- "${pod:?}"' EXIT

# vault ARGS...: the CLI in the server's container, with the token from the environment, never on
# a command line.
vault_as() {
    local token=$1
    shift
    docker exec -i -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN="$token" "$container" vault "$@"
}
for _ in $(seq 1 30); do
    vault_as "$root_token" status > /dev/null 2>&1 && break
    sleep 1
done

failures=0
pass() { printf 'ok    %s\n' "$1"; }
flunk() { printf 'FAIL  %s\n' "$1"; [ -z "${2:-}" ] || printf '      %s\n' "$2"; failures=$((failures + 1)); }
same() { if [ "$2" = "$3" ]; then pass "$1"; else flunk "$1" "wanted: $2; got: $3"; fi; }

# The configure script, as the operator runs it, with this server's CLI; twice, as it is safe to.
export VAULT_TOKEN="$root_token"
vault_cli="docker exec -i -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN $container vault"
for run in first second; do
    if [ "$run" = second ]; then
        # As an older set-up left it: a CA in the config, which a restored snapshot must not keep.
        docker exec -i -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN "$container" vault write \
            auth/kubernetes/config kubernetes_host=https://old.example:6443 \
            kubernetes_ca_cert=@/var/run/secrets/kubernetes.io/serviceaccount/ca.crt > /dev/null
    fi
    if out=$(HDS_VAULT="$vault_cli" "$ROOT/scripts/ops/vault_configure.sh" 2>&1); then
        pass "vault_configure.sh succeeds, $run run"
    else
        flunk "vault_configure.sh, $run run" "$out"
    fi
done
unset VAULT_TOKEN

echo "== the mount and the auth config"
same "hds/ is KV version 2" 2 \
    "$(vault_as "$root_token" secrets list -format=json | jq -r '."hds/".options.version')"
config=$(vault_as "$root_token" read -format=json auth/kubernetes/config | jq -c .data)
same "the Kubernetes auth config holds the cluster's address" "https://kubernetes.default.svc:443" \
    "$(jq -r .kubernetes_host <<< "$config")"
same "and no CA or reviewer token, even after an older set-up wrote one (review R95)" "" \
    "$(jq -r '[.kubernetes_ca_cert // "", .token_reviewer_jwt_set // false | tostring] | map(select(. != "" and . != "false")) | join(" ")' <<< "$config")"

echo "== the roles, each bound to exactly one ServiceAccount"
role() { vault_as "$root_token" read -format=json "auth/kubernetes/role/$1" | jq -c .data; }
for spec in "hds hds-vault hds 600 1200" "vault-snapshot hds-vault-snapshot vault-snapshot 300 600"; do
    read -r name account policy ttl max <<< "$spec"
    data=$(role "$name")
    same "role $name binds only $account in hds" "[\"$account\"] [\"hds\"]" \
        "$(jq -c .bound_service_account_names <<< "$data") $(jq -c .bound_service_account_namespaces <<< "$data")"
    same "role $name has the $policy policy and short tokens" "[\"$policy\"] $ttl $max vault" \
        "$(jq -c .token_policies <<< "$data") $(jq -r .token_ttl <<< "$data") $(jq -r .token_max_ttl <<< "$data") $(jq -r .audience <<< "$data")"
done

echo "== what each policy's token can do"
token_for() {
    vault_as "$root_token" token create -policy="$1" -ttl=5m -format=json | jq -r .auth.client_token
}
hds_token=$(token_for hds)
snapshot_token=$(token_for vault-snapshot)
caps() { vault_as "$root_token" token capabilities "$1" "$2" | tr -d '\r'; }
same "hds reads a secret's current version" read "$(caps "$hds_token" hds/data/api)"
same "hds reads every path under hds" read "$(caps "$hds_token" hds/data/backup)"
same "hds cannot read a secret's metadata or history" deny "$(caps "$hds_token" hds/metadata/api)"
same "hds cannot read anything outside hds" deny "$(caps "$hds_token" secret/data/api)"
same "hds cannot take a snapshot" deny "$(caps "$hds_token" sys/storage/raft/snapshot)"
same "vault-snapshot takes a snapshot" read "$(caps "$snapshot_token" sys/storage/raft/snapshot)"
same "vault-snapshot reads no secret" deny "$(caps "$snapshot_token" hds/data/api)"

# And in practice: a value written as the operator does, read as VSO does, never written by hds.
vault_as "$root_token" kv put -mount=hds api JWT_SECRET_KEY=throwaway > /dev/null
same "the hds token reads hds/api as a KV v2 read" throwaway \
    "$(vault_as "$hds_token" read -format=json hds/data/api | jq -r .data.data.JWT_SECRET_KEY)"
if vault_as "$hds_token" kv put -mount=hds api JWT_SECRET_KEY=changed > /dev/null 2>&1; then
    flunk "the hds token wrote hds/api"
else
    pass "the hds token cannot write hds/api"
fi

echo "== the first write in docs/runbooks/operations.md"
# `vault kv put -mount=hds PATH -` takes the whole path as a JSON object on stdin, so values made by
# the shell's own printf are never arguments.
printf '{"SMTP_HOST":"%s","SMTP_PASSWORD":"%s"}' relay.example "pw-$(openssl rand -hex 4)" \
    | vault_as "$root_token" kv put -mount=hds smtp - > /dev/null
same "kv put with a JSON object on stdin writes every key" "relay.example yes" \
    "$(vault_as "$root_token" kv get -mount=hds -format=json smtp | jq -r '.data.data | .SMTP_HOST + " " + (if (.SMTP_PASSWORD | startswith("pw-")) then "yes" else "no" end)')"

echo "== the rotation command in docs/runbooks/operations.md"
# `vault kv patch ... KEY=-` takes the value on stdin and keeps the path's other keys; `kv put`
# would replace the whole path and drop them (review R136).
vault_as "$root_token" kv put -mount=hds api JWT_SECRET_KEY=kept THROTTLE_SECRET_KEY=kept2 \
    DATABASE_PASSWORD=old > /dev/null
new="rotated-$(openssl rand -hex 8)"
printf '%s' "$new" | vault_as "$root_token" kv patch -mount=hds api DATABASE_PASSWORD=- > /dev/null
data=$(vault_as "$root_token" kv get -mount=hds -format=json api | jq -c .data.data)
same "kv patch with the value on stdin changes that key" "$new" "$(jq -r .DATABASE_PASSWORD <<< "$data")"
same "and keeps the others" "kept kept2" "$(jq -r '.JWT_SECRET_KEY + " " + .THROTTLE_SECRET_KEY' <<< "$data")"

[ "$failures" -eq 0 ] || { echo "vault policy checks: $failures failed" >&2; exit 1; }
echo "vault policy checks: all passed"
