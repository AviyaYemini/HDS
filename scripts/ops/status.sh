#!/bin/sh
# make ops-status (specs/006-deploy-operations T057, FR-007, contracts/ops.md section 5), from the
# operator's machine with kubectl, helm and jq: one line per item, and FAIL on each failing one.
#
#   - each Deployment and StatefulSet in hds (the database too): ready over desired, and the version;
#   - the backup, the restore test and Vault's snapshot: the last success (too old or never is a
#     FAIL), and the last failure of the newest Job, if it failed; a snapshot not configured yet;
#   - the last three Helm revisions of hds, the newest not deployed (failed, stuck) as a FAIL;
#   - alerting itself (reviews R161, R184): Prometheus, Alertmanager and kube-state-metrics ready,
#     Alertmanager running HDS's configuration (its receivers are exactly "operators") and the
#     operator having applied it (Reconciled), Alertmanager's configuration
#     Secret present, and the alerts firing now, read from Alertmanager's API through the API
#     server's service proxy (no port-forward to manage);
#   - Vault: sealed is a FAIL.
#
# It exits 1 when any item fails. Environment: HDS_NAMESPACE (hds), HDS_MONITORING_NAMESPACE
# (monitoring), HDS_VAULT_NAMESPACE (vault), HDS_NOW (the time, in seconds, for the checks).
set -eu
ns=${HDS_NAMESPACE:-hds}
mon=${HDS_MONITORING_NAMESPACE:-monitoring}
vault_ns=${HDS_VAULT_NAMESPACE:-vault}
now=${HDS_NOW:-$(date -u +%s)}
failed=0
work=$(mktemp -d "${TMPDIR:-/tmp}/hds-status.XXXXXX")
trap 'rm -rf -- "${work:?}"' EXIT

ok() { printf '  ok    %s\n' "$*"; }
bad() { printf '  FAIL  %s\n' "$*"; failed=1; }
section() { printf '%s\n' "$*"; }
command -v jq > /dev/null || { echo "FAILED: jq is needed" >&2; exit 1; }

section "Parts in $ns"
# The Deployments and the database's StatefulSet (review R178).
deployments=$(kubectl -n "$ns" get deployments,statefulsets -o json) \
    || { bad "the Deployments and StatefulSets could not be read"; deployments='{"items":[]}'; }
printf '%s' "$deployments" | jq -r '.items[] | [.metadata.name, (.status.readyReplicas // 0), (.spec.replicas // 0),
    (.metadata.labels["app.kubernetes.io/version"] // "unknown")] | @tsv' \
| { while IFS="$(printf '\t')" read -r name ready desired version; do
    if [ "$ready" -ge "$desired" ] && [ "$desired" -gt 0 ]; then
        printf '  ok    %s: %s/%s ready, version %s\n' "$name" "$ready" "$desired" "$version"
    else
        printf '  FAIL  %s: %s/%s ready, version %s\n' "$name" "$ready" "$desired" "$version"
    fi
done; } > "$work/deployments"
cat "$work/deployments"
if grep -q FAIL "$work/deployments"; then failed=1; fi
[ -s "$work/deployments" ] || bad "no Deployment or StatefulSet in $ns"

# The Jobs, read once for the three CronJobs; unreadable is a FAIL, not "no failure" (review R176).
jobs=$(kubectl -n "$ns" get jobs -o json) || jobs=""

# job_line CRONJOB LABEL MAX_AGE: the CronJob's last success, too old or never as a FAIL, and its
# newest Job's failure, if any (Jobs it owns: a restore's Jobs have no owner; review R122).
job_line() {
    cron=$(kubectl -n "$ns" get cronjob "$1" -o json 2> /dev/null) || cron=""
    if [ -z "$cron" ]; then
        bad "$2: no CronJob $1"
        return
    fi
    last=$(printf '%s' "$cron" | jq -r '.status.lastSuccessfulTime // ""')
    if [ -z "$last" ]; then
        bad "$2: never succeeded"
    else
        age=$((now - $(jq -rn --arg t "$last" '$t | fromdateiso8601')))
        if [ "$age" -gt "$3" ]; then bad "$2: last success $last, too long ago"; else ok "$2: last success $last"; fi
    fi
    if [ -z "$jobs" ]; then
        bad "$2: its Jobs could not be read"
        return
    fi
    newest=$(printf '%s' "$jobs" | jq -r --arg c "$1" '[.items[]
        | select(any(.metadata.ownerReferences[]?; .kind == "CronJob" and .name == $c))]
        | sort_by(.status.startTime // "") | last // empty
        | select(any(.status.conditions[]?; .type == "Failed" and .status == "True"))
        | .metadata.name + " at " + (.status.startTime // "?")') || newest=""
    [ -z "$newest" ] || bad "$2: the newest run failed: $newest"
}

section "Backups"
# The local copies' claim is made outside Helm (infra/cluster/backup-pvc.yaml; review R189), under
# the name the backup CronJob mounts (backup.storage.claimName; review R197). Missing, or Lost, is a
# FAIL; Bound, or Pending before the first backup binds it, is fine (review R194).
claim=$(kubectl -n "$ns" get cronjob hds-backup \
    -o jsonpath='{.spec.jobTemplate.spec.template.spec.volumes[?(@.name=="backups")].persistentVolumeClaim.claimName}' \
    2> /dev/null) || claim=""
claim=${claim:-hds-backups}
phase=$(kubectl -n "$ns" get pvc "$claim" -o jsonpath='{.status.phase}' 2> /dev/null) || phase=""
case $phase in
    Bound | Pending) ok "the backups' claim $claim: $phase" ;;
    "") bad "the backups' claim $claim is missing: kubectl apply -f infra/cluster/backup-pvc.yaml (or the cluster's own claim of that name)" ;;
    *) bad "the backups' claim $claim is $phase: the local copies cannot be written" ;;
esac
job_line hds-backup "backup" $((26 * 3600))
job_line hds-restore-test "restore test" $((8 * 86400))
if kubectl -n "$ns" get cronjob hds-vault-snapshot > /dev/null 2>&1; then
    job_line hds-vault-snapshot "vault snapshot" $((8 * 86400))
else
    bad "vault snapshot: not configured (vault.snapshotRecipient is empty)"
fi

section "Deploys of hds"
# The last three revisions; the newest must be deployed: a failed or stuck one is a FAIL, which no
# alert keeps showing once Flux's one event has gone (review R173).
history=$(helm -n "$ns" history hds --max 3 -o json 2> /dev/null) || history=""
if [ -z "$history" ]; then
    bad "the Helm history of hds could not be read"
else
    printf '%s' "$history" | jq -r '.[] | "  revision \(.revision): \(.status), \(.chart), \(.app_version) (\(.description))"'
    newest=$(printf '%s' "$history" | jq -r 'max_by(.revision) | "\(.revision) \(.status)"')
    case $newest in
        *" deployed") ok "the newest deploy, revision ${newest% *}, is deployed" ;;
        *) bad "the newest deploy, revision ${newest% *}, is ${newest#* }: see helm -n $ns history hds" ;;
    esac
fi

section "Alerting"
# kube-state-metrics too: every HDS alert reads its series, and an absent series fires nothing
# (review R177).
for part in prometheus alertmanager kube-state-metrics; do
    readiness=$(kubectl -n "$mon" get pods -l "app.kubernetes.io/name=$part" \
        -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{" "}{end}') || readiness=""
    case " $readiness " in
        *" True "*) ok "$part ready" ;;
        *) bad "$part not ready: alerts may not be sent" ;;
    esac
done
if kubectl -n "$mon" get secret alertmanager-hds > /dev/null 2>&1; then
    ok "Alertmanager's configuration (alertmanager-hds) is present"
else
    bad "Alertmanager's configuration (alertmanager-hds) is missing: is hds-secrets synced?"
fi
# The configuration Alertmanager runs is HDS's: without alertmanager-hds the operator runs a
# fallback with one empty receiver, which drops every alert and is Ready all the same (review
# R184). Only the receivers' names are read: the configuration holds personal addresses.
proxy="/api/v1/namespaces/$mon/services/alertmanager-operated:9093/proxy/api/v2"
receivers=$(kubectl get --raw "$proxy/receivers" 2> /dev/null | jq -c '[.[].name] | sort') || receivers=""
if [ "$receivers" = '["operators"]' ]; then
    ok "Alertmanager runs HDS's configuration (receiver operators)"
else
    bad "Alertmanager does not run HDS's configuration (receivers ${receivers:-unreadable}): alerts are dropped"
fi
# A configuration the operator refused (a bad value written to hds/smtp, say) leaves the old one
# running: the Alertmanager object's Reconciled condition is then not True (review R184).
# Read first, then judged: a refused or empty read is a FAIL, never "nothing unreconciled" (review
# R188; without pipefail a pipe from a failed kubectl would hand jq nothing and pass).
ams=$(kubectl -n "$mon" get alertmanagers -o json 2> /dev/null) || ams=""
if [ -z "$ams" ]; then
    bad "the Alertmanager objects could not be read"
elif [ "$(printf '%s' "$ams" | jq '.items | length')" = 0 ]; then
    bad "no Alertmanager object in $mon"
else
    unreconciled=$(printf '%s' "$ams" | jq -r '[.items[]
        | select(any(.status.conditions[]?; .type == "Reconciled" and .status == "True") | not)
        | .metadata.name] | join(", ")' 2> /dev/null) || unreconciled="unreadable"
    if [ -z "$unreconciled" ]; then
        ok "the operator applied Alertmanager's configuration"
    else
        bad "the operator has not applied Alertmanager's configuration ($unreconciled): see kubectl -n $mon describe alertmanager"
    fi
fi
alerts=$(kubectl get --raw "$proxy/alerts?active=true&silenced=false&inhibited=false" 2> /dev/null) \
    || alerts=""
if [ -z "$alerts" ]; then
    bad "the firing alerts could not be read from Alertmanager"
else
    firing=$(printf '%s' "$alerts" | jq -r '[.[] | .labels.alertname] | unique | join(", ")')
    if [ -n "$firing" ]; then bad "firing: $firing"; else ok "no alert firing"; fi
fi

section "Vault"
sealed=$(kubectl -n "$vault_ns" exec vault-0 -- vault status -format=json 2> /dev/null | jq -r '.sealed') || sealed=""
case $sealed in
    false) ok "unsealed" ;;
    true) bad "sealed: run make ops-unseal" ;;
    *) bad "its status could not be read" ;;
esac

exit "$failed"
