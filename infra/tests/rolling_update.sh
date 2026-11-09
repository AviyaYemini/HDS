#!/usr/bin/env bash
# The rolling-update proof (specs/006-deploy-operations T059, FR-017, FR-019, FR-020, SC-003): the
# base's release, under steady signed-in load, upgraded to the head's on a k3d cluster, with no
# failed request, no lost session and the schedule job finished once. Run by the CI rolling-update
# job, and by hand: infra/tests/rolling_update.sh [BASE]
#
#   1. A k3d cluster from infra/cluster/k3d.yaml.
#   2. The base's and the head's images, imported into it. The head's API image carries one more,
#      throwaway, additive migration (a scratch table with a nullable column), so every run upgrades
#      across a migration even when the change has none.
#   3. The hds-* Secrets from values made here; mailpit as the SMTP relay (STARTTLS required); the
#      base's hds-postgres, then its hds, with infra/tests/rolling-update-values.yaml
#      (ENVIRONMENT=production).
#   4. The first Admin; load.py through the ingress; a schedule job queued; then hds-postgres and
#      hds upgraded to the head with --wait, and the head's Deployments rolled once more, so the
#      pods that stop are the head's own; the load stopped.
#   5. It fails on any failed request or refresh, on the schedule job not ending succeeded, on a
#      mail sent, on a FAIL from make ops-status other than the ones a cluster with no monitoring,
#      Vault or backups gives (listed below), and on the forwarded-address check (ADR 007).
#   6. infra/tests/flux_verify.sh: Flux's source-controller refuses an unsigned chart and one signed
#      with another key, and takes one signed with the key it trusts (T047c).
#
# A change to hds-postgres's pod template fails this run, as expected: the upgrade restarts the only
# database pod under load, and one instance cannot roll (review R204).
#
# Every tool runs from an image pinned in infra/VERSIONS.md: k3d (with the Docker socket), kubectl
# (the k3s server's own), helm (on k3d's network). Needs Docker, git, jq, openssl and uv.
# Environment: DOCKER_SOCKET (the Docker socket's path); HDS_KEEP_CLUSTER=1 leaves the cluster.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
K3D_IMAGE="ghcr.io/k3d-io/k3d:5.9.0@sha256:6c1e2bd3b80185499ee6fd2315077e712c6cfa92279dc4bfd7645a5f992a1937"
HELM_IMAGE="alpine/helm:3.22.1@sha256:8191701ad1f82ee3695c87211fcbfe6d7b1bb7df92ac1c8de9ccdb0995a6a2c1"
MAILPIT_IMAGE="axllent/mailpit:v1.31.5@sha256:94f6b70c1436defae6799d9ac7ff0eeff9540465b842026d5cdb018f1c0234ea"
# A cluster of its own, never k3d.yaml's own `hds`, the shared environment's (T063): its name and
# host ports differ, its shape is the same (review R200; infra/tests/check_deploy_ops.sh checks).
CLUSTER=hds-rolling
PORT=18080
SERVER="k3d-$CLUSTER-server-0"
NS=hds
cd "$ROOT"
base=$(git rev-parse --verify "${1:-$(git merge-base origin/main HEAD)}^{commit}")
socket=${DOCKER_SOCKET:-$(docker context inspect --format '{{.Endpoints.docker.Host}}' | sed 's#^unix://##')}
work=$(mktemp -d "${TMPDIR:-/tmp}/hds-rolling.XXXXXX")
chmod 755 "$work"

say() { printf '\n== %s\n' "$*"; }
fail() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }
k3d() { docker run --rm -v "$socket:/var/run/docker.sock" -v "$work/k3d:/cfg:ro" "$K3D_IMAGE" "$@"; }
kubectl() { docker exec -i "$SERVER" kubectl "$@"; }
helm() {
    docker run --rm -i --network "k3d-$CLUSTER" -v "$work:/work:ro" -e KUBECONFIG=/work/kubeconfig \
        -e HELM_CACHE_HOME=/tmp -e HELM_CONFIG_HOME=/tmp -e HELM_DATA_HOME=/tmp -w /work "$HELM_IMAGE" "$@"
}
cleanup() {
    [ -n "${load_pid:-}" ] && kill "$load_pid" 2> /dev/null || true
    for tree in base head; do git -C "$ROOT" worktree remove --force "$work/$tree" > /dev/null 2>&1 || true; done
    # Only a cluster this run made is deleted.
    if [ -n "${created:-}" ] && [ "${HDS_KEEP_CLUSTER:-}" != 1 ]; then
        k3d cluster delete "$CLUSTER" > /dev/null 2>&1 || true
    fi
    rm -rf -- "${work:?}"
}
trap cleanup EXIT

say "1. a k3d cluster, $CLUSTER"
mkdir "$work/k3d"
sed -e "s/^  name: hds$/  name: $CLUSTER/" -e "s/port: 8080:80/port: $PORT:80/" -e "s/port: 8443:443/port: 18443:443/" \
    infra/cluster/k3d.yaml > "$work/k3d/k3d.yaml"
grep -q "^  name: $CLUSTER$" "$work/k3d/k3d.yaml" || fail "the cluster's name could not be set"
if k3d cluster list --no-headers 2> /dev/null | awk '{print $1}' | grep -qx "$CLUSTER"; then
    fail "a k3d cluster $CLUSTER already exists; delete it yourself (k3d cluster delete $CLUSTER), then run again"
fi
k3d cluster create --config /cfg/k3d.yaml --kubeconfig-update-default=false > /dev/null
created=1
k3d kubeconfig get "$CLUSTER" | sed "s#server: https://[^ ]*#server: https://k3d-$CLUSTER-serverlb:6443#" \
    > "$work/kubeconfig"
chmod 644 "$work/kubeconfig"
kubectl wait --for=condition=Ready node --all --timeout=120s > /dev/null

say "2. the base's ($(git rev-parse --short "$base")) and the head's images"
git worktree add --detach --quiet "$work/base" "$base"
git worktree add --detach --quiet "$work/head" HEAD
# The head's throwaway migration: additive, on a table of its own, after the head's last revision.
# The head's last revision, from the tree the head image is built from (review R195).
head_revision=$(cd "$work/head/apps/api" && uv run --quiet alembic heads | sed -n 's/^\([^ ]*\) (head)$/\1/p')
[ -n "$head_revision" ] || fail "the head's migration revision could not be read"
cat > "$work/head/apps/api/alembic/versions/999_rolling_update_scratch.py" <<EOF
"""A throwaway additive migration, built only into the rolling-update job's head image.

Revision ID: 999_rolling_update_scratch
Revises: $head_revision
"""

import sqlalchemy as sa
from alembic import op

revision = "999_rolling_update_scratch"
down_revision = "$head_revision"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add a scratch table with a nullable column, which no code reads."""
    op.create_table(
        "rolling_update_scratch",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("note", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    """Drop the scratch table."""
    op.drop_table("rolling_update_scratch")
EOF
for tree in base head; do
    for part in api web backup; do
        docker build --quiet --build-arg "HDS_VERSION=$tree" -f "$work/$tree/infra/Dockerfile.$part" \
            -t "hds-$part:rolling-$tree" "$work/$tree" > /dev/null
    done
done
k3d image import --cluster "$CLUSTER" hds-api:rolling-base hds-web:rolling-base hds-backup:rolling-base \
    hds-api:rolling-head hds-web:rolling-head hds-backup:rolling-head > /dev/null

say "3. Secrets, mailpit, and the base's release"
kubectl create namespace "$NS" > /dev/null
db_password=$(openssl rand -hex 32)
kubectl -n "$NS" create secret generic hds-api --from-literal="JWT_SECRET_KEY=$(openssl rand -hex 32)" \
    --from-literal="THROTTLE_SECRET_KEY=$(openssl rand -hex 32)" --from-literal="DATABASE_PASSWORD=$db_password" > /dev/null
kubectl -n "$NS" create secret generic hds-smtp --from-literal=SMTP_HOST=mailpit \
    --from-literal=SMTP_PORT=1025 --from-literal=SMTP_USERNAME=hds \
    --from-literal="SMTP_PASSWORD=$(openssl rand -hex 16)" --from-literal=MAIL_FROM=hds@example.org > /dev/null
kubectl -n "$NS" create secret generic hds-backup --from-literal="HDS_CRYPT_PASSWORD=$(openssl rand -hex 32)" \
    --from-literal="HDS_CRYPT_SALT=$(openssl rand -hex 32)" --from-literal=HDS_DRIVE_TOKEN='{}' \
    --from-literal=HDS_DRIVE_FOLDER_ID=none > /dev/null
kubectl -n "$NS" apply -f - > /dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: {name: mailpit}
spec:
  replicas: 1
  selector: {matchLabels: {app: mailpit}}
  template:
    metadata: {labels: {app: mailpit}}
    spec:
      containers:
        - name: mailpit
          image: $MAILPIT_IMAGE
          args: ["--smtp-tls-cert", "sans:mailpit", "--smtp-tls-key", "sans:mailpit", "--smtp-require-starttls",
                 "--smtp-auth-accept-any"]
          ports: [{containerPort: 1025}, {containerPort: 8025}]
---
apiVersion: v1
kind: Service
metadata: {name: mailpit}
spec:
  selector: {app: mailpit}
  ports: [{name: smtp, port: 1025}, {name: api, port: 8025}]
EOF
cp -R "$work/base/infra/helm" "$work/base-charts"
cp -R "$work/head/infra/helm" "$work/head-charts"
cp infra/tests/rolling-update-values.yaml "$work/values.yaml"
# images TREE: the --set words naming the imported images of that tree.
images() {
    local part
    for part in api web backup; do
        printf -- '--set images.%s.repository=hds-%s --set images.%s.tag=rolling-%s ' "$part" "$part" "$part" "$1"
    done
}
helm install hds-postgres /work/base-charts/hds-postgres --namespace "$NS" --wait --timeout 5m > /dev/null
# The backups' claim (review R189). A base whose chart still makes it (main's, until this feature
# merges) leaves it Pending under local-path, and --wait would time out on it: a throwaway pod binds
# it while the install waits, and is removed after. The upgrade keeps it (resource-policy keep) and
# the head mounts it by name. A base without it gets the claim as the cluster does: from the file.
# Read from the chart's own files, not a pipe from helm: grep -q stops at the first match, and the
# writer's SIGPIPE under pipefail would turn a match into the wrong branch (review R190).
if grep -qs '^kind: PersistentVolumeClaim' "$work"/base-charts/hds/templates/*.yaml; then
    echo "the base's chart makes the backups' claim: a throwaway pod binds it during the install"
    # shellcheck disable=SC2046
    helm install hds /work/base-charts/hds --namespace "$NS" -f /work/base-charts/hds/values-local.yaml \
        -f /work/values.yaml $(images base) --wait --timeout 10m > /dev/null &
    install_pid=$!
    for _ in $(seq 1 60); do kubectl -n "$NS" get pvc hds-backups > /dev/null 2>&1 && break; sleep 2; done
    kubectl -n "$NS" apply -f - > /dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: {name: backups-binder}
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 999
    seccompProfile: {type: RuntimeDefault}
  containers:
    - name: bind
      image: hds-backup:rolling-base
      imagePullPolicy: Never
      command: ["sleep", "3600"]
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities: {drop: ["ALL"]}
      volumeMounts: [{name: backups, mountPath: /backups}]
  volumes: [{name: backups, persistentVolumeClaim: {claimName: hds-backups}}]
EOF
    wait "$install_pid" || fail "the base's install failed"
    kubectl -n "$NS" delete pod backups-binder --wait=false > /dev/null
else
    kubectl apply -f - < infra/cluster/backup-pvc.yaml > /dev/null
    # shellcheck disable=SC2046
    helm install hds /work/base-charts/hds --namespace "$NS" -f /work/base-charts/hds/values-local.yaml \
        -f /work/values.yaml $(images base) --wait --timeout 10m > /dev/null
fi
kubectl -n "$NS" rollout status deployment/hds-api --timeout=5m > /dev/null

say "4. the first Admin, the load, a schedule job, and the upgrade"
# The first Admin, made here; its password is never an argument and never in a file: it reaches
# create_admin, load.py and curl on standard input.
admin="ops-$(openssl rand -hex 4)@example.org"
admin_password=$(openssl rand -hex 16)
printf '%s\n' "$admin_password" | kubectl -n "$NS" exec -i deployment/hds-api -- python -m hds.scripts.create_admin \
    --email "$admin" --organisation "Rolling Update" --timezone Asia/Jerusalem > /dev/null
# sign_in: an access token for the Admin, the credentials on curl's standard input.
sign_in() {
    jq -n --arg e "$admin" --arg p "$admin_password" '{email: $e, password: $p}' \
        | curl -fsS -H 'Host: hds.localhost' -H 'Content-Type: application/json' --data @- \
            http://127.0.0.1:$PORT/auth/login | jq -r .access_token
}
# The API's own environment (uv sync --extra dev; made here if this checkout has none), run with
# exec, so $! is load.py itself and the TERM below reaches it, not a wrapper.
[ -x "$ROOT/apps/api/.venv/bin/python" ] || (cd apps/api && uv sync --quiet --extra dev)
printf '%s\n' "$admin_password" | (exec "$ROOT/apps/api/.venv/bin/python" infra/tests/load.py \
    --email "$admin" --password-file - --url "http://127.0.0.1:$PORT") > "$work/load.log" 2>&1 &
load_pid=$!
sleep 10
token=$(sign_in)
today=$(date -u +%Y-%m-%d)
# made: the id of a new item, POSTed as the Admin. A run needs an active project to schedule (007,
# schedule.no_projects), and the new organisation has none: a site, a client and a project first.
made() {
    curl -fsS -H 'Host: hds.localhost' -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
        --data "$2" "http://127.0.0.1:$PORT/api/v1/$1" | jq -r .id
}
site=$(made sites '{"name": "Rolling Site", "address": "1 Rolling Street", "timezone": "Asia/Jerusalem"}')
client=$(made clients "{\"name\": \"Rolling Client\", \"contract_start\": \"$today\"}")
project=$(made projects "{\"client_id\": \"$client\", \"site_id\": \"$site\", \"name\": \"Rolling Project\", \"type\": \"security\", \"billing_rate\": \"100.00\", \"start_date\": \"$today\"}")
[ -n "$project" ] && [ "$project" != null ] || fail "the project to schedule could not be made"
job=$(curl -fsS -H 'Host: hds.localhost' -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
    --data "{\"from\": \"$today\", \"to\": \"$today\", \"solver\": \"greedy\"}" \
    http://127.0.0.1:$PORT/api/v1/schedule-jobs | jq -r .id)
[ -n "$job" ] && [ "$job" != null ] || fail "the schedule job could not be queued"
echo "queued schedule job $job"
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) the upgrade, base to head, starts"
helm upgrade hds-postgres /work/head-charts/hds-postgres --namespace "$NS" --wait --timeout 5m > /dev/null
# shellcheck disable=SC2046
helm upgrade hds /work/head-charts/hds --namespace "$NS" -f /work/head-charts/hds/values-local.yaml \
    -f /work/values.yaml $(images head) --wait --timeout 10m > /dev/null
head_roll=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "$head_roll the upgrade is done; the head rolls once more"
# Then a second rollout, head to head, under the same load: in the upgrade above the pods that
# stop are the base's, so this is what tests the head's own shutdown (its preStop; review R202).
kubectl -n "$NS" rollout restart deployment/hds-api deployment/hds-web deployment/hds-worker > /dev/null
for deployment in hds-api hds-web hds-worker; do
    kubectl -n "$NS" rollout status "deployment/$deployment" --timeout=5m > /dev/null
done
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) the head's rollout is done"
sleep 20
kill -TERM "$load_pid" 2> /dev/null || true
load_status=0
wait "$load_pid" || load_status=$?
load_pid=""
cat "$work/load.log"

say "5. the checks"
failures=0
check() { if "$@"; then printf 'ok    %s\n' "$label"; else printf 'FAIL  %s\n' "$label"; failures=$((failures + 1)); fi; }
# Every failed request fails the run, with one exception while this feature is unmerged: in the
# upgrade, base to head, the web pods that stop are the base's, and a base whose web.yaml has no
# preStop (main's, before review R201) drops a web request or two as nginx stops. Those, and only
# those, are reported and allowed; the API's, and any during the head's own rollout, never are.
grep '^  FAIL  ' "$work/load.log" | sed 's/^  FAIL  //' > "$work/failures" || true
if grep -q 'preStop:' "$work/base/infra/helm/hds/templates/web.yaml"; then
    cp "$work/failures" "$work/counted"
else
    # A failure reads "2026-11-07T16:44:23Z GET /path: why" (UTC, fixed width, so it compares as
    # a string across midnight); the path's colon is load.py's.
    awk -v roll="$head_roll" '{ path = $3; sub(/:$/, "", path) }
        !($1 < roll && $2 == "GET" && (path == "/" || path ~ /^\/_expo\//))' "$work/failures" > "$work/counted"
    allowed=$(($(wc -l < "$work/failures") - $(wc -l < "$work/counted")))
    [ "$allowed" -eq 0 ] || echo "allowed: $allowed web request(s) dropped by the base's web pods, which have no preStop (R201)"
fi
label="no request failed (counted: $(wc -l < "$work/counted" | tr -d ' ')) and the session kept refreshing"
# load.py exits non-zero on any failure, and on a sign-in that never worked (no FAIL line then).
check test ! -s "$work/counted" -a \( "$load_status" -eq 0 -o -s "$work/failures" \)
refreshes=$(sed -n 's/^load: .* \([0-9]*\) refreshes,.*/\1/p' "$work/load.log")
label="the load ran through the upgrade, refreshing at least once (${refreshes:-0})"
check test "${refreshes:-0}" -ge 1
label="the head runs: its migration was applied"
check test "$(kubectl -n "$NS" exec hds-postgres-0 -- psql -U hds -d hds -Atc 'SELECT version_num FROM alembic_version')" = 999_rolling_update_scratch
state=$(curl -fsS -H 'Host: hds.localhost' -H "Authorization: Bearer $(sign_in)" \
    "http://127.0.0.1:$PORT/api/v1/schedule-jobs/$job" | jq -r .status)
label="the schedule job ended succeeded (it was $state)"; check test "$state" = succeeded
# Recorded once: the proposal is the job row's own result, so one row holds one result; a run the
# update interrupted may be retried, within the three attempts (review R193). Not FR-020's proof:
# one attempt means the job finished before the old worker stopped; Group F's SIGTERM and
# lease-expiry tests prove the interrupted path (review R203).
recorded=$(kubectl -n "$NS" exec hds-postgres-0 -- psql -U hds -d hds -Atc \
    "SELECT (result IS NOT NULL)::text || ' ' || attempts FROM schedule_jobs WHERE id = '$job'")
label="it holds its one result, after ${recorded#* } attempt(s) (not FR-020's interrupted-run proof: Group F's tests)"
check test "${recorded% *}" = true -a "${recorded#* }" -le 3
sent=$(kubectl -n "$NS" exec deployment/mailpit -- wget -qO- http://127.0.0.1:8025/api/v1/messages | jq .total)
label="no mail was sent ($sent)"; check test "$sent" = 0

# make ops-status, with kubectl and helm as above. A cluster with no monitoring stack, Vault or
# backups fails exactly these, and nothing else (reviews R184, R188).
mkdir "$work/bin"
printf '#!/bin/sh\nexec docker exec -i %s kubectl "$@"\n' "$SERVER" > "$work/bin/kubectl"
printf '#!/bin/sh\nexec docker run --rm -i --network k3d-%s -v %s:/work:ro -e KUBECONFIG=/work/kubeconfig -e HELM_CACHE_HOME=/tmp -e HELM_CONFIG_HOME=/tmp -e HELM_DATA_HOME=/tmp %s "$@"\n' \
    "$CLUSTER" "$work" "$HELM_IMAGE" > "$work/bin/helm"
chmod +x "$work/bin/kubectl" "$work/bin/helm"
PATH="$work/bin:$PATH" scripts/ops/status.sh > "$work/status.log" 2>&1 || true
cat "$work/status.log"
grep '^  FAIL' "$work/status.log" | sed 's/^  FAIL  //' | sort > "$work/fails"
cat > "$work/expected" <<'EOF'
Alertmanager does not run HDS's configuration (receivers unreadable): alerts are dropped
Alertmanager's configuration (alertmanager-hds) is missing: is hds-secrets synced?
alertmanager not ready: alerts may not be sent
backup: never succeeded
its status could not be read
kube-state-metrics not ready: alerts may not be sent
prometheus not ready: alerts may not be sent
restore test: never succeeded
the Alertmanager objects could not be read
the firing alerts could not be read from Alertmanager
vault snapshot: not configured (vault.snapshotRecipient is empty)
EOF
sort -o "$work/expected" "$work/expected"
label="make ops-status fails only on what this cluster lacks"
check diff -u "$work/expected" "$work/fails"

say "the forwarded address (ADR 007): each pod counted under its own address"
# from_pod NAME FORGED: from a pod of its own, through Traefik's Service, 31 failed sign-ins (each
# for a new address, so no account lock interferes), with X-Forwarded-For FORGED if given. Prints
# the first status, the last, and the pod's own address. The per-network limit is 30.
from_pod() {
    kubectl -n "$NS" run "$1" --rm -i --quiet --restart=Never --image=hds-api:rolling-head \
        --image-pull-policy=Never --command -- python -c "
import json, socket, urllib.error, urllib.request, uuid
headers = {'Host': 'hds.localhost', 'Content-Type': 'application/json'}
if '$2':
    headers['X-Forwarded-For'] = '$2'
def attempt():
    body = json.dumps({'email': f'{uuid.uuid4().hex}@example.org', 'password': 'wrong'}).encode()
    request = urllib.request.Request('http://traefik.kube-system.svc/auth/login', body, headers)
    try:
        return urllib.request.urlopen(request, timeout=10).status
    except urllib.error.HTTPError as error:
        return error.code
statuses = [attempt() for _ in range(31)]
print(statuses[0], statuses[-1], socket.gethostbyname(socket.gethostname()))
"
}
read -r one_first one_last one_ip <<< "$(from_pod probe-one "")"
read -r two_first two_last _ <<< "$(from_pod probe-two "")"
read -r three_first three_last _ <<< "$(from_pod probe-three "$one_ip")"
label="one pod's 31st failure is stopped, its first is not ($one_first $one_last)"
check test "$one_first $one_last" = "401 429"
label="a second pod is counted apart, from its first ($two_first $two_last)"
check test "$two_first $two_last" = "401 429"
label="a third claiming the first's address ($one_ip) is counted under its own ($three_first $three_last)"
check test "$three_first $three_last" = "401 429"

say "the web build on the upgraded cluster (Playwright; review C1)"
# HDS_CLUSTER_SMOKE: the command that runs apps/client/tests/cluster (CI sets it); it gets the
# Admin's address and password in its environment only, never as arguments or in a file.
if [ -n "${HDS_CLUSTER_SMOKE:-}" ]; then
    label="the first Admin signs in on the web build, opens the main screens and signs out"
    check env HDS_CLUSTER_URL="http://hds.localhost:$PORT" HDS_CLUSTER_EMAIL="$admin" \
        HDS_CLUSTER_PASSWORD="$admin_password" sh -c "$HDS_CLUSTER_SMOKE"
else
    echo "skipped: HDS_CLUSTER_SMOKE is not set"
fi

say "6. Flux refuses an unsigned chart, and one signed with another key"
label="flux_verify.sh"; check infra/tests/flux_verify.sh "$CLUSTER" "$work"

[ "$failures" -eq 0 ] || { echo "rolling update: $failures failed" >&2; exit 1; }
echo "rolling update: all passed"
