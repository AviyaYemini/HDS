# The alertmanager role's policy (specs/006-deploy-operations T056; review R164): the relay's
# settings and the operators' addresses, hds/smtp's current version, and nothing else.
path "hds/data/smtp" {
  capabilities = ["read"]
}
