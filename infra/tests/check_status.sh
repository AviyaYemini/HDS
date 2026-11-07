#!/usr/bin/env bash
# Checks scripts/ops/status.sh (make ops-status; specs/006 T057; reviews R138, R161) against fake
# kubectl and helm, which answer as a cluster would. No cluster is needed; the rolling-update job
# runs it against a real one (Group L). Run by the CI lint job: infra/tests/check_status.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
fake=$(mktemp -d)
trap 'rm -rf -- "${fake:?}"' EXIT
# 2026-11-07T10:00:00Z, the time the checks run at.
NOW=$(jq -rn '"2026-11-07T10:00:00Z" | fromdateiso8601')
cat > "$fake/kubectl" <<'EOF'
#!/bin/sh
# Each FAKE_* variable replaces one answer; unset, the cluster is healthy.
case "$*" in
    "-n hds get deployments,statefulsets -o json")
        if [ -n "${FAKE_DEPLOYMENTS:-}" ]; then printf '%s' "$FAKE_DEPLOYMENTS"; else
            printf '%s' '{"items":[
                {"metadata":{"name":"hds-api","labels":{"app.kubernetes.io/version":"abc1234"}},"spec":{"replicas":2},"status":{"readyReplicas":2}},
                {"metadata":{"name":"hds-worker","labels":{"app.kubernetes.io/version":"abc1234"}},"spec":{"replicas":1},"status":{"readyReplicas":1}},
                {"metadata":{"name":"hds-postgres","labels":{"app.kubernetes.io/version":"16.15"}},"spec":{"replicas":1},"status":{"readyReplicas":1}}]}'
        fi ;;
    "-n hds get cronjob hds-backup -o json")
        printf '{"status":{"lastSuccessfulTime":"%s"}}' "${FAKE_BACKUP_LAST-2026-11-07T03:31:00Z}" ;;
    "-n hds get cronjob hds-restore-test -o json")
        printf '{"status":{"lastSuccessfulTime":"2026-11-01T04:10:00Z"}}' ;;
    "-n hds get cronjob hds-vault-snapshot") [ -z "${FAKE_NO_SNAPSHOT:-}" ] ;;
    "-n hds get cronjob hds-vault-snapshot -o json")
        printf '{"status":{"lastSuccessfulTime":"2026-11-01T03:05:00Z"}}' ;;
    "-n hds get jobs -o json")
        [ -z "${FAKE_JOBS_REFUSED:-}" ] || exit 1
        if [ -n "${FAKE_JOBS:-}" ]; then printf '%s' "$FAKE_JOBS"; else printf '%s' '{"items":[]}'; fi ;;
    "-n monitoring get pods -l app.kubernetes.io/name=prometheus "*) printf '%s' "${FAKE_PROMETHEUS-True }" ;;
    "-n monitoring get pods -l app.kubernetes.io/name=alertmanager "*) printf '%s' "${FAKE_ALERTMANAGER-True }" ;;
    "-n monitoring get pods -l app.kubernetes.io/name=kube-state-metrics "*) printf '%s' "${FAKE_KSM-True }" ;;
    "-n monitoring get secret alertmanager-hds") [ -z "${FAKE_NO_AM_SECRET:-}" ] ;;
    "get --raw /api/v1/namespaces/monitoring/services/alertmanager-operated:9093/proxy/api/v2/alerts"*)
        printf '%s' "${FAKE_ALERTS-[]}" ;;
    "-n vault exec vault-0 -- vault status -format=json") printf '{"sealed": %s}' "${FAKE_SEALED:-false}" ;;
    *) echo "fake kubectl: unexpected: $*" >&2; exit 2 ;;
esac
EOF
cat > "$fake/helm" <<'EOF'
#!/bin/sh
case "$*" in
    "-n hds history hds --max 3 -o json")
        if [ -n "${FAKE_HISTORY:-}" ]; then printf '%s' "$FAKE_HISTORY"; else
            printf '%s' '[{"revision":6,"status":"superseded","chart":"hds-0.1.41","app_version":"0ff1ce0","description":"Upgrade complete"},
                {"revision":7,"status":"deployed","chart":"hds-0.1.42","app_version":"abc1234","description":"Upgrade complete"}]'
        fi ;;
    *) echo "fake helm: unexpected: $*" >&2; exit 2 ;;
esac
EOF
chmod +x "$fake/kubectl" "$fake/helm"
export PATH="$fake:$PATH" HDS_NOW="$NOW"

failures=0
# check LABEL WANTED_STATUS WANTED_TEXT [VAR=VALUE...]
check() {
    local label=$1 want=$2 text=$3 status out
    shift 3
    set +e
    out=$(env "$@" "$ROOT/scripts/ops/status.sh" 2>&1)
    status=$?
    set -e
    if [ "$status" = "$want" ] && grep -qF -- "$text" <<< "$out"; then
        printf 'ok    %s\n' "$label"
    else
        printf 'FAIL  %s\n      status %s (wanted %s); output:\n%s\n' "$label" "$status" "$want" "$out"
        failures=$((failures + 1))
    fi
}
# quiet LABEL [VAR=VALUE...]: the run passes, with no FAIL line.
quiet() {
    local label=$1 out
    shift
    if out=$(env "$@" "$ROOT/scripts/ops/status.sh" 2>&1) && ! grep -q FAIL <<< "$out"; then
        printf 'ok    %s\n' "$label"
    else
        printf 'FAIL  %s\n%s\n' "$label" "$out"; failures=$((failures + 1))
    fi
}

quiet "a healthy cluster: every line ok, exit 0" FAKE_X=1
check "each Deployment, ready over desired, with its version" 0 "hds-api: 2/2 ready, version abc1234"
check "the last Helm revisions" 0 "revision 7: deployed, hds-0.1.42, abc1234"
check "the database's StatefulSet is a part too (review R178)" 0 "hds-postgres: 1/1 ready"
check "a failed newest deploy fails, though no alert still fires (review R173)" 1 \
    "FAIL  the newest deploy, revision 8, is failed" \
    FAKE_HISTORY='[{"revision":7,"status":"superseded","chart":"hds-0.1.42","app_version":"abc1234","description":"Upgrade complete"},{"revision":8,"status":"failed","chart":"hds-0.1.43","app_version":"bad0bad","description":"Upgrade failed: timed out"}]'
check "Jobs that cannot be read fail, not pass as no failure (review R176)" 1 \
    "FAIL  backup: its Jobs could not be read" FAKE_JOBS_REFUSED=1
check "kube-state-metrics not ready fails, since every alert reads it (review R177)" 1 \
    "FAIL  kube-state-metrics not ready" FAKE_KSM="False "
check "a database not ready fails" 1 "FAIL  hds-postgres: 0/1 ready" \
    FAKE_DEPLOYMENTS='{"items":[{"metadata":{"name":"hds-postgres","labels":{}},"spec":{"replicas":1},"status":{}}]}'
check "a Deployment short of its pods fails" 1 "FAIL  hds-api: 1/2 ready" \
    FAKE_DEPLOYMENTS='{"items":[{"metadata":{"name":"hds-api","labels":{}},"spec":{"replicas":2},"status":{"readyReplicas":1}}]}'
check "a backup older than 26 hours fails" 1 "FAIL  backup: last success 2026-11-06T03:31:00Z, too long ago" \
    FAKE_BACKUP_LAST=2026-11-06T03:31:00Z
check "a backup that never succeeded fails" 1 "FAIL  backup: never succeeded" FAKE_BACKUP_LAST=
check "the newest backup Job failed: FAIL, with its name" 1 "FAIL  backup: the newest run failed: hds-backup-29002" \
    FAKE_JOBS='{"items":[
        {"metadata":{"name":"hds-backup-29001","ownerReferences":[{"kind":"CronJob","name":"hds-backup"}]},"status":{"startTime":"2026-11-06T03:30:00Z","conditions":[{"type":"Complete","status":"True"}]}},
        {"metadata":{"name":"hds-backup-29002","ownerReferences":[{"kind":"CronJob","name":"hds-backup"}]},"status":{"startTime":"2026-11-07T03:30:00Z","conditions":[{"type":"Failed","status":"True"}]}}]}'
quiet "a failed restore Job, which no CronJob owns, is not a failed backup (review R122)" \
    FAKE_JOBS='{"items":[
        {"metadata":{"name":"hds-backup-29002","ownerReferences":[{"kind":"CronJob","name":"hds-backup"}]},"status":{"startTime":"2026-11-07T03:30:00Z","conditions":[{"type":"Complete","status":"True"}]}},
        {"metadata":{"name":"hds-restore-check-20261107","labels":{"app.kubernetes.io/component":"restore"}},"status":{"startTime":"2026-11-07T09:00:00Z","conditions":[{"type":"Failed","status":"True"}]}}]}'
check "a snapshot not configured fails, saying why (review R138)" 1 \
    "FAIL  vault snapshot: not configured (vault.snapshotRecipient is empty)" FAKE_NO_SNAPSHOT=1
check "Prometheus not ready fails (review R161)" 1 "FAIL  prometheus not ready" FAKE_PROMETHEUS="False "
check "Alertmanager not ready fails" 1 "FAIL  alertmanager not ready" FAKE_ALERTMANAGER=""
check "Alertmanager's configuration missing fails" 1 "FAIL  Alertmanager's configuration (alertmanager-hds) is missing" \
    FAKE_NO_AM_SECRET=1
check "firing alerts fail, by name" 1 "FAIL  firing: FluxHelmReleaseUpgradeFailed, HdsBackupFailed" \
    FAKE_ALERTS='[{"labels":{"alertname":"HdsBackupFailed"}},{"labels":{"alertname":"FluxHelmReleaseUpgradeFailed"}}]'
check "Alertmanager's API unreadable fails" 1 "FAIL  the firing alerts could not be read" FAKE_ALERTS=
check "a sealed Vault fails" 1 "FAIL  sealed: run make ops-unseal" FAKE_SEALED=true

[ "$failures" -eq 0 ] || { echo "status checks: $failures failed" >&2; exit 1; }
echo "status checks: all passed"
