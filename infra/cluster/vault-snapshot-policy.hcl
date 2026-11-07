# The vault-snapshot role's policy (specs/006 T052, research R11): taking a raft snapshot, nothing
# else. It can read no secret.
path "sys/storage/raft/snapshot" {
  capabilities = ["read"]
}
