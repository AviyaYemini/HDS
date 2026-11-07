#!/usr/bin/env bash
# The alert rules (specs/006-deploy-operations T055; contracts/ops.md section 3), with the promtool
# of the Prometheus the cluster runs (infra/VERSIONS.md). Run by the CI chart job:
#   infra/tests/check_alerts.sh
#   - promtool check rules: the rules parse, and their expressions too;
#   - promtool test rules: infra/cluster/alerts/tests.yaml, each alert firing on its series and quiet
#     on the rest;
#   - no rule reads hds_http_* (spec, Clarifications: no rules on error rates or latency).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
PROMETHEUS_IMAGE="quay.io/prometheus/prometheus:v3.15.0@sha256:efd719c99d83b060d9daefdcf00360461adf279f45ef5391f8d111892118753e"
rules="$ROOT/infra/helm/hds/files/alert-rules.yaml"
promtool() {
    docker run --rm --entrypoint promtool -v "$ROOT/infra:/infra:ro" -w /infra/cluster/alerts \
        "$PROMETHEUS_IMAGE" "$@"
}

promtool check rules /infra/helm/hds/files/alert-rules.yaml
promtool test rules tests.yaml
if grep -nE '^[^#]*hds_http_' "$rules"; then
    echo "a rule reads hds_http_*, which the spec rules out" >&2
    exit 1
fi
names=$(sed -n 's/^ *- alert: //p' "$rules" | sort | paste -sd ' ' -)
want="HdsBackupFailed HdsBackupMissing HdsRestoreTestFailed HdsRestoreTestMissing HdsVaultSnapshotFailed"
[ "$names" = "$want" ] || { echo "the alerts are '$names', not the contract's '$want'" >&2; exit 1; }
echo "alert checks: all passed"
