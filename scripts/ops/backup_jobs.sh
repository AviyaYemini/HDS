#!/bin/sh
# The backup targets of the shared environment (specs/006-deploy-operations T045b, contracts/ops.md
# section 5), run from the operator's machine with kubectl:
#
#   backup_jobs.sh run backup         make ops-backup: a backup now
#   backup_jobs.sh run restore-test   make ops-restore-test: the restore test now
#   backup_jobs.sh list               make ops-backups: the backups on Drive, by name
#
# `run` makes a Job from the CronJob hds-<name>, waits for it, prints its logs, and exits 1 if it
# failed. A Job made this way skips the CronJob's concurrencyPolicy: Forbid, so it refuses while a
# Job of the same CronJob, or a restore, is still running. `list` runs hds-rclone.sh in a one-off
# pod on the backup image with the hds-backup Secret, as the CronJob does: the names are encrypted
# on Drive, so a browser cannot show them.
#
# Environment: HDS_NAMESPACE (hds), HDS_JOB_TIMEOUT in seconds (1800).
set -eu
ns=${HDS_NAMESPACE:-hds}
timeout=${HDS_JOB_TIMEOUT:-1800}

say() { printf '%s\n' "$*"; }
fail() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }
k() { kubectl -n "$ns" "$@"; }

run() {
    cronjob="hds-$1"
    k get cronjob "$cronjob" > /dev/null || fail "no CronJob $cronjob in namespace $ns"
    # A restore (make ops-restore, component=restore) replaces the database: no backup or restore
    # test runs beside it (review R122).
    active=$(k get jobs -l "app.kubernetes.io/component in ($1,restore)" \
        -o jsonpath='{range .items[?(@.status.active)]}{.metadata.name}{" "}{end}') \
        || fail "the Jobs of $cronjob could not be listed"
    [ -z "$active" ] || fail "a Job of $cronjob is still running: $active; wait for it"
    job="$cronjob-manual-$(date -u +%Y%m%d%H%M%S)"
    k create job "$job" --from="cronjob/$cronjob" > /dev/null || fail "$job could not be created"
    say "created Job $job from $cronjob; waiting up to ${timeout}s"
    waited=0
    while :; do
        state=$(k get job "$job" -o jsonpath='{.status.conditions[?(@.status=="True")].type}') \
            || fail "$job could not be read"
        case " $state " in
            *" Complete "*) result=ok; break ;;
            *" Failed "*) result=failed; break ;;
        esac
        [ "$waited" -lt "$timeout" ] || { result=timeout; break; }
        sleep 5
        waited=$((waited + 5))
    done
    say "--- the logs of $job"
    k logs "job/$job" --all-containers --prefix --ignore-errors || true
    case $result in
        ok) say "$job succeeded" ;;
        failed) fail "$job failed; its pod stays for inspection: kubectl -n $ns describe job $job" ;;
        timeout) fail "$job did not finish within ${timeout}s; it is still running" ;;
    esac
}

list() {
    pod_spec=.spec.jobTemplate.spec.template.spec
    image=$(k get cronjob hds-backup -o jsonpath="{$pod_spec.containers[0].image}") \
        || fail "no CronJob hds-backup in namespace $ns"
    path=$(k get cronjob hds-backup \
        -o jsonpath="{$pod_spec.containers[0].env[?(@.name==\"HDS_OFFSITE_PATH\")].value}")
    pulls=$(k get cronjob hds-backup -o jsonpath="{$pod_spec.imagePullSecrets[*].name}")
    secrets=""
    for name in $pulls; do secrets="$secrets${secrets:+,}{\"name\":\"$name\"}"; done
    pod="hds-backups-list-$(date -u +%Y%m%d%H%M%S)"
    names=$(k run "$pod" --rm -i --quiet --restart=Never --image="$image" --overrides="{
        \"apiVersion\": \"v1\",
        \"spec\": {
            \"serviceAccountName\": \"hds-backup\",
            \"automountServiceAccountToken\": false,
            \"imagePullSecrets\": [$secrets],
            \"securityContext\": {\"runAsNonRoot\": true, \"runAsUser\": 999, \"runAsGroup\": 999,
                \"seccompProfile\": {\"type\": \"RuntimeDefault\"}},
            \"containers\": [{
                \"name\": \"list\",
                \"image\": \"$image\",
                \"command\": [\"hds-rclone.sh\", \"lsf\", \"offsite-crypt:\"],
                \"envFrom\": [{\"secretRef\": {\"name\": \"hds-backup\"}}],
                \"env\": [{\"name\": \"HDS_OFFSITE_PATH\", \"value\": \"$path\"}],
                \"securityContext\": {\"readOnlyRootFilesystem\": true,
                    \"allowPrivilegeEscalation\": false, \"capabilities\": {\"drop\": [\"ALL\"]}},
                \"volumeMounts\": [{\"name\": \"tmp\", \"mountPath\": \"/tmp\"}]
            }],
            \"volumes\": [{\"name\": \"tmp\", \"emptyDir\": {}}]
        }
    }") || fail "the backups on Drive could not be listed"
    printf '%s\n' "$names" | sort
}

case "${1:-}" in
    run)
        case "${2:-}" in
            backup | restore-test) run "$2" ;;
            *) fail "usage: $0 run backup|restore-test" ;;
        esac ;;
    list) list ;;
    *) fail "usage: $0 run backup|restore-test | list" ;;
esac
