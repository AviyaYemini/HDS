#!/bin/sh
# make ops-unseal (specs/006-deploy-operations T053): unseal Vault after a restart, from the
# operator's machine with kubectl. The unseal key is read from the terminal without echo (or from a
# pipe, such as a password manager's command line), and reaches Vault on standard input
# (`vault write sys/unseal key=-`; `vault operator unseal -` would take "-" itself as the key, review
# R141): it is never an argument, so it shows in no process list, and it is written to no file.
#
# Environment: HDS_VAULT_NAMESPACE (vault), HDS_VAULT_POD (vault-0).
set -eu
ns=${HDS_VAULT_NAMESPACE:-vault}
pod=${HDS_VAULT_POD:-vault-0}
fail() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }

if [ -t 0 ]; then
    printf 'Vault unseal key (not shown): ' >&2
    stty -echo
    trap 'stty echo' EXIT INT TERM
    IFS= read -r key || key=""
    stty echo
    printf '\n' >&2
else
    IFS= read -r key || [ -n "${key:-}" ] || key=""
fi
[ -n "$key" ] || fail "no unseal key was given; nothing was sent"

printf '%s' "$key" | kubectl -n "$ns" exec -i "$pod" -- vault write sys/unseal key=- > /dev/null \
    || fail "Vault refused the key, or $pod in $ns could not be reached"
key=""
sealed=$(kubectl -n "$ns" exec "$pod" -- vault status -format=json | sed -n 's/.*"sealed": *\([a-z]*\).*/\1/p') \
    || sealed=""
[ "$sealed" = false ] || fail "Vault is still sealed (sealed: ${sealed:-unknown})"
echo "Vault is unsealed"
