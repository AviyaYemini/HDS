# The hds role's policy (specs/006-deploy-operations T052, data-model.md "Secrets"): read-only, the
# current version of each secret under the KV v2 mount hds, nothing else. A KV v2 read is a GET on
# hds/data/<path>; the Vault Secrets Operator's VaultStaticSecret (type kv-v2) needs no more.
path "hds/data/*" {
  capabilities = ["read"]
}
