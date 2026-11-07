#!/bin/sh
# make ops-restore BACKUP=<name> [CONFIRM=replace] (specs/006-deploy-operations T049b, FR-013,
# contracts/ops.md section 5, docs/runbooks/restore.md section 1; reviews R76, R77, R80), from the
# operator's machine with flux, helm and kubectl:
#
#   1. check: infra/backup/restore.sh --check in a Job, while the API still serves. It refuses a
#      database with data unless CONFIRM=replace, and downloads and verifies the backup. A refusal
#      stops here, having changed nothing and stopped nothing.
#   2. save the API's and the worker's replica counts and whether Flux and the hds-backup CronJob
#      are suspended already (in the ConfigMap hds-restore-saved, so a rerun after a run that
#      stopped midway sets back what ran before it); then suspend Flux for hds, so no release can
#      run its migration hook against a half-restored database or reset the replica counts, and
#      the CronJob;
#   3. scale the API and the worker to 0, and wait until no pod of either is left, counting pods
#      still in their grace period (review R80);
#   4. restore: infra/backup/restore.sh in a Job on the backup image, made from the hds-backup
#      CronJob, so it has its Secret, its database settings and its network access;
#   5. migrate: the release's own migration Job (helm get hooks hds), so it runs the API image the
#      Deployments run;
#   6. scale back to the saved counts and wait; set the CronJob's suspend back to what it was; resume
#      Flux unless it was suspended before; delete the ConfigMap.
#
# If the restore Job fails having changed nothing (restore.sh exits 3), step 6 runs and nothing is
# lost. If it fails after emptying the database, or the migration fails, the API and the worker stay
# at 0 and Flux stays suspended: starting them on a half-restored database would serve wrong data.
# The message says what to do next.
#
# Environment: BACKUP (required), CONFIRM, HDS_NAMESPACE (hds), HDS_FLUX_NAMESPACE (flux-system),
# HDS_JOB_TIMEOUT and HDS_STOP_TIMEOUT in seconds (3600, 300), HDS_POLL in seconds (5).
set -eu
ns=${HDS_NAMESPACE:-hds}
flux_ns=${HDS_FLUX_NAMESPACE:-flux-system}
job_timeout=${HDS_JOB_TIMEOUT:-3600}
stop_timeout=${HDS_STOP_TIMEOUT:-300}
poll=${HDS_POLL:-5}
backup=${BACKUP:-}
confirm=${CONFIRM:-}

say() { printf '%s\n' "$*"; }
fail() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }
k() { kubectl -n "$ns" "$@"; }

# CONFIRM goes into a JSON patch, so it is checked first (review R119).
case $confirm in '' | replace) ;; *) fail "CONFIRM is replace or unset, not '$confirm'" ;; esac
printf '%s' "$backup" | grep -Eqx 'hds-[0-9]{8}T[0-9]{6}Z' \
    || fail "usage: make ops-restore BACKUP=hds-<yyyymmdd>T<hhmmss>Z [CONFIRM=replace]; make ops-backups lists them"
work=$(mktemp -d "${TMPDIR:-/tmp}/hds-restore.XXXXXX")
trap 'rm -rf -- "${work:?}"' EXIT
stamp=$(date -u +%Y%m%d%H%M%S)

# wait_job JOB: waits for the Job and prints its logs; 0 when it completed, else the exit status of
# its container (1 when it has none: a timeout, an image that never pulled).
wait_job() {
    waited=0
    result=""
    while [ -z "$result" ]; do
        state=$(k get job "$1" -o jsonpath='{.status.conditions[?(@.status=="True")].type}') \
            || fail "$1 could not be read"
        case " $state " in
            *" Complete "*) result=0 ;;
            *" Failed "*) result=failed ;;
            *) if [ "$waited" -ge "$job_timeout" ]; then result=failed; else
                sleep "$poll"; waited=$((waited + poll)); fi ;;
        esac
    done
    say "--- the logs of $1"
    k logs "job/$1" --all-containers --prefix --ignore-errors || true
    [ "$result" = 0 ] && return 0
    code=$(k get pods -l "job-name=$1" \
        -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.exitCode}') || code=""
    case $code in '' | 0) return 1 ;; *) return "$code" ;; esac
}

# backup_job NAME ARGS...: a Job from the hds-backup CronJob's template that runs restore.sh ARGS
# instead of the backup, with CONFIRM as given. `kubectl create job --from=cronjob/...` makes the
# CronJob the Job's controller (an ownerReference and the cronjob.kubernetes.io/instantiate
# annotation), so the CronJob would adopt it: a check counted as a backup (lastSuccessfulTime), a
# refused one as a failed backup, both in its history limit. Both are removed, and the Job and its
# pod are labelled component=restore; hds/database-client, which the database lets in, stays
# (review R122). With no owner they are kept 7 days after they finish, logs and all.
backup_job() {
    name=$1
    shift
    command="\"restore.sh\""
    for arg in "$@"; do command="$command, \"$arg\""; done
    k create job "$name" --from=cronjob/hds-backup --dry-run=client -o json > "$work/job.json" \
        || fail "no CronJob hds-backup in namespace $ns to make $name from"
    kubectl patch --local -f "$work/job.json" --type=json -o json -p "[
        {\"op\": \"remove\", \"path\": \"/metadata/ownerReferences\"},
        {\"op\": \"add\", \"path\": \"/spec/ttlSecondsAfterFinished\", \"value\": 604800},
        {\"op\": \"remove\", \"path\": \"/metadata/annotations/cronjob.kubernetes.io~1instantiate\"},
        {\"op\": \"add\", \"path\": \"/metadata/labels/app.kubernetes.io~1component\", \"value\": \"restore\"},
        {\"op\": \"add\", \"path\": \"/spec/template/metadata/labels/app.kubernetes.io~1component\",
         \"value\": \"restore\"},
        {\"op\": \"replace\", \"path\": \"/spec/template/spec/containers/0/command\", \"value\": [$command]},
        {\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/env/-\",
         \"value\": {\"name\": \"CONFIRM\", \"value\": \"$confirm\"}}
    ]" > "$work/patched.json" || fail "$name could not be prepared"
    k create -f "$work/patched.json" > /dev/null || fail "$name could not be created"
    say "created Job $name"
}

# Running backups, a restore test or another restore would read or write the database while it is
# replaced.
running() {
    k get jobs -l 'app.kubernetes.io/component in (backup,restore-test,restore)' \
        -o jsonpath='{range .items[?(@.status.active)]}{.metadata.name}{" "}{end}' | sed 's/ *$//'
}
active=$(running) || fail "the Jobs in $ns could not be listed"
[ -z "$active" ] || fail "a Job is still running: $active; wait for it. Nothing was changed"

say "== 1. check $backup"
status=0
backup_job "hds-restore-check-$stamp" --check "$backup"
wait_job "hds-restore-check-$stamp" || status=$?
[ "$status" -eq 0 ] || fail "the check failed (see its logs above); nothing was changed or stopped"

say "== 2. save what runs now, then suspend Flux for hds and hds-postgres, and the hds-backup CronJob"
# What step 6 sets back: the replica counts, and whether Flux (for hds, and for hds-postgres, whose
# release would restart the database mid-restore; review R127) and the CronJob were suspended
# already (after make ops-rollback, or in runbook section 2). They are kept in the ConfigMap
# hds-restore-saved until step 6, so a run after one that stopped midway, with the API and the
# worker at 0 and everything suspended, sets back what ran before the first (review R112).
saved=$(k get configmap hds-restore-saved --ignore-not-found \
    -o jsonpath='{.data.api} {.data.worker} {.data.cron} {.data.flux} {.data.pgflux}') \
    || fail "the ConfigMap hds-restore-saved could not be read; nothing was changed"
if [ -n "$saved" ]; then
    # shellcheck disable=SC2086 # five words
    set -- $saved
    api_replicas=$1 worker_replicas=${2:-} cron_suspend=${3:-} flux_suspend=${4:-}
    pg_flux_suspend=${5:-}
    printf '%s %s' "$api_replicas" "$worker_replicas" | grep -Eqx '[0-9]+ [0-9]+' \
        && [ -n "$pg_flux_suspend" ] \
        || fail "the ConfigMap hds-restore-saved holds '$saved', not five values; fix or delete it"
    created_saved=""
    say "an earlier run stopped midway; setting back what it saved"
else
    api_replicas=$(k get deployment hds-api -o jsonpath='{.spec.replicas}') \
        || fail "no Deployment hds-api; nothing was changed"
    worker_replicas=$(k get deployment hds-worker -o jsonpath='{.spec.replicas}') \
        || fail "no Deployment hds-worker; nothing was changed"
    cron_suspend=$(k get cronjob hds-backup -o jsonpath='{.spec.suspend}') || cron_suspend=""
    flux_suspend=$(kubectl -n "$flux_ns" get helmrelease hds -o jsonpath='{.spec.suspend}') \
        || fail "the hds HelmRelease could not be read; nothing was changed"
    pg_flux_suspend=$(kubectl -n "$flux_ns" get helmrelease hds-postgres -o jsonpath='{.spec.suspend}') \
        || fail "the hds-postgres HelmRelease could not be read; nothing was changed"
    [ "$cron_suspend" = true ] || cron_suspend=false
    [ "$flux_suspend" = true ] || flux_suspend=false
    [ "$pg_flux_suspend" = true ] || pg_flux_suspend=false
    k create configmap hds-restore-saved --from-literal="api=$api_replicas" \
        --from-literal="worker=$worker_replicas" --from-literal="cron=$cron_suspend" \
        --from-literal="flux=$flux_suspend" --from-literal="pgflux=$pg_flux_suspend" > /dev/null \
        || fail "the ConfigMap hds-restore-saved could not be created; nothing was changed"
    created_saved=1
fi
say "saved: hds-api $api_replicas, hds-worker $worker_replicas, hds-backup suspend $cron_suspend," \
    "Flux suspend $flux_suspend (hds) and $pg_flux_suspend (hds-postgres)"

# hand_back: set the CronJob and Flux back to what was saved, and delete the saved state.
hand_back() {
    k patch cronjob hds-backup --type=merge -p "{\"spec\":{\"suspend\":$cron_suspend}}" > /dev/null \
        || fail "the hds-backup CronJob's suspend could not be set back to $cron_suspend"
    # Flux was suspended before (make ops-rollback): it stays so, or it would reinstall the newest
    # chart, perhaps the one rolled back from (review R113).
    if [ "$flux_suspend" = false ]; then
        flux -n "$flux_ns" resume helmrelease hds || fail "Flux could not resume hds; run make ops-resume"
        flux_state="Flux manages hds again"
    else
        flux_state="Flux stays suspended for hds, as it was; make ops-resume hands it back"
    fi
    if [ "$pg_flux_suspend" = false ]; then
        flux -n "$flux_ns" resume helmrelease hds-postgres \
            || fail "Flux could not resume hds-postgres; run flux -n $flux_ns resume helmrelease hds-postgres"
    fi
    k delete configmap hds-restore-saved > /dev/null \
        || fail "the ConfigMap hds-restore-saved could not be deleted; delete it before the next restore"
}

flux -n "$flux_ns" suspend helmrelease hds || fail "Flux could not suspend hds; nothing was changed"
[ "$(kubectl -n "$flux_ns" get helmrelease hds -o jsonpath='{.spec.suspend}')" = true ] \
    || fail "the hds HelmRelease is not suspended; nothing was changed. Run make ops-resume"
flux -n "$flux_ns" suspend helmrelease hds-postgres \
    || fail "Flux could not suspend hds-postgres; hds stays suspended. Run make ops-resume"
k patch cronjob hds-backup --type=merge -p '{"spec":{"suspend":true}}' > /dev/null \
    || fail "the hds-backup CronJob could not be suspended; Flux stays suspended. Run make ops-resume"
# A backup the CronJob started during step 1 (its check takes minutes) would run during the restore
# (review R123). Nothing is scaled yet: set everything back and stop.
active=$(running) || fail "the Jobs in $ns could not be listed; Flux and hds-backup stay suspended"
if [ -n "$active" ]; then
    if [ -n "$created_saved" ]; then
        hand_back
        fail "a backup started: $active. Wait for it and run again; nothing was changed"
    fi
    fail "a backup started: $active. Wait for it and run again; Flux and hds-backup stay suspended" \
        "from the earlier run"
fi

# start_again: scale back to the saved counts, wait, and hand everything back.
start_again() {
    if ! { k scale deployment hds-api --replicas="$api_replicas" > /dev/null \
        && k scale deployment hds-worker --replicas="$worker_replicas" > /dev/null \
        && k rollout status deployment hds-api --timeout="${stop_timeout}s" \
        && k rollout status deployment hds-worker --timeout="${stop_timeout}s"; }; then
        fail "the API and the worker did not start again; Flux and hds-backup stay suspended." \
            "See: kubectl -n $ns get pods"
    fi
    hand_back
}

say "== 3. stop the API and the worker"
k scale deployment hds-api --replicas=0 > /dev/null || fail "hds-api could not be scaled to 0"
k scale deployment hds-worker --replicas=0 > /dev/null || fail "hds-worker could not be scaled to 0"
waited=0
while :; do
    # Every pod of either, terminating ones too (.status.replicas leaves those out).
    left=$(k get pods -l 'app.kubernetes.io/instance=hds,app.kubernetes.io/component in (api,worker)' \
        -o name) || fail "the pods in $ns could not be listed"
    [ -n "$left" ] || break
    if [ "$waited" -ge "$stop_timeout" ]; then
        start_again
        fail "pods were still running after ${stop_timeout}s: $(printf '%s' "$left" | tr '\n' ' ');" \
            "nothing was changed, and everything was started again"
    fi
    sleep "$poll"
    waited=$((waited + poll))
done
say "no pod of the API or the worker is left"

say "== 4. restore $backup"
status=0
backup_job "hds-restore-run-$stamp" "$backup"
wait_job "hds-restore-run-$stamp" || status=$?
if [ "$status" -eq 3 ]; then
    start_again
    fail "the restore refused (see its logs above); nothing was changed, and everything was started again"
elif [ "$status" -ne 0 ]; then
    fail "the restore failed after the database was emptied. The API and the worker stay at 0 and" \
        "Flux stays suspended. Fix the cause and run make ops-restore BACKUP=$backup CONFIRM=replace" \
        "again, or follow docs/runbooks/restore.md"
fi

say "== 5. migrate"
helm -n "$ns" get hooks hds > "$work/hooks.yaml" || fail "the release's hooks could not be read"
kind=$(k create --dry-run=client -f "$work/hooks.yaml" -o jsonpath='{.kind}/{.metadata.name}') \
    || kind=""
[ "$kind" = Job/hds-migrate ] \
    || fail "the release's hooks are not one Job hds-migrate ($kind). The database is restored but not" \
        "migrated; the API and the worker stay at 0 and Flux stays suspended"
k create --dry-run=client -f "$work/hooks.yaml" -o json > "$work/migrate.json" \
    || fail "the migration Job could not be read"
kubectl patch --local -f "$work/migrate.json" --type=merge -o json \
    -p "{\"metadata\": {\"name\": \"hds-restore-migrate-$stamp\", \"annotations\": null}}" \
    > "$work/migrate-patched.json" || fail "the migration Job could not be prepared"
k create -f "$work/migrate-patched.json" > /dev/null || fail "the migration Job could not be created"
status=0
wait_job "hds-restore-migrate-$stamp" || status=$?
[ "$status" -eq 0 ] \
    || fail "the migration failed (see its logs above). The database is restored but not migrated;" \
        "the API and the worker stay at 0 and Flux stays suspended. See docs/runbooks/restore.md"

say "== 6. start the API and the worker again"
start_again
say "restored $backup; hds-api $api_replicas and hds-worker $worker_replicas are running, hds-backup's" \
    "suspend is $cron_suspend again. $flux_state"
