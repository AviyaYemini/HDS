#!/usr/bin/env bash
# The chart checks (specs/006-deploy-operations T033, T048, T051, T052): helm lint, kubeconform and
# assertions on the rendered manifests of the three charts, hds (values-local.yaml and
# values-ci.yaml), hds-postgres and hds-secrets (research R3, amended), of Vault's, the Vault
# Secrets Operator's and kube-prometheus-stack's pinned charts with infra/cluster's values, and of
# infra/flux. Run from anywhere:
# infra/helm/tests/assert.sh
#
# Every tool runs from an image pinned by tag and digest (infra/VERSIONS.md); nothing is installed.
# kubeconform checks against the Kubernetes v1.37.1 schemas (k3s v1.37.1) at a pinned commit of
# yannh/kubernetes-json-schema, not its default, which follows master, and against three sets of
# custom-resource schemas, each pinned by SHA-256: Flux's, the Vault Secrets Operator's and
# kube-prometheus-stack's (the last two made strict from their charts' CRDs). It also renders
# kube-prometheus-stack with infra/cluster/monitoring-values.yaml. No schema may be missing
# (-strict, no -ignore-missing-schemas).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
HELM_IMAGE="alpine/helm:3.22.1@sha256:8191701ad1f82ee3695c87211fcbfe6d7b1bb7df92ac1c8de9ccdb0995a6a2c1"
KUBECONFORM_IMAGE="ghcr.io/yannh/kubeconform:v0.8.0@sha256:faffaf43f95aa6425306e1ab8d6fcad72acb9049158f38e574c085ea1ec0f64e"
# Flux's CRD schemas: crd-schemas.tar.gz of the Flux release T063 installs (infra/VERSIONS.md).
FLUX_VERSION="v2.9.6"
FLUX_SCHEMAS_SHA256="380b713bd28a2c27cfc7710219aae79e1c9ceff9bee50197c023047190532987"
# The Vault Secrets Operator's CRDs: those of its pinned chart (infra/VERSIONS.md), turned into
# strict JSON schemas, so hds-secrets is checked like the rest (review R140).
VAULT_CHART="https://helm.releases.hashicorp.com/vault-0.34.1.tgz"
VAULT_CHART_SHA256="df4c37fac32496892ff1e1d6fd7954d3a402cc7b1b2b4c2226af11b8a57564c4"
KPS_CHART="https://github.com/prometheus-community/helm-charts/releases/download/kube-prometheus-stack-92.11.0/kube-prometheus-stack-92.11.0.tgz"
KPS_CHART_SHA256="c357a259965187edcbed86591461bfbd56997591afba09be3a49a09b2bec8a2c"
VSO_CHART="https://helm.releases.hashicorp.com/vault-secrets-operator-1.6.0.tgz"
VSO_CHART_SHA256="de3e0a520918fc7281b94fa060fe8d1858c5db5b8fb6b1463024268e7c4541c0"
YQ_IMAGE="mikefarah/yq:4.54.1@sha256:4b3d9475d65571d28cbb19544d3820ec2945e4c8b2f18279394282b8dc3a592e"
SCHEMA_COMMIT="b2ba4defe30ed5a0104987433896f1fd32807304"
KUBERNETES_VERSION="1.37.1"
SCHEMAS="https://raw.githubusercontent.com/yannh/kubernetes-json-schema/${SCHEMA_COMMIT}/{{.NormalizedKubernetesVersion}}-standalone{{.StrictSuffix}}/{{.ResourceKind}}{{.KindSuffix}}.json"
DIGEST="sha256:0000000000000000000000000000000000000000000000000000000000000001"

out=$(mktemp -d)
# One yq container for every question the checks ask, started once and reached with `docker exec`:
# a container start costs about two seconds, and the checks ask yq over a hundred times, which was
# most of this script's five minutes. It reads the rendered manifests in $out as they are written.
yq_box=""
trap '[ -z "$yq_box" ] || docker rm -f "$yq_box" > /dev/null 2>&1; rm -rf "$out"' EXIT
yq_box=$(docker run -d --rm --user "$(id -u):$(id -g)" -v "$out:/m:ro" --entrypoint sh "$YQ_IMAGE" \
    -c 'trap "exit 0" TERM; while :; do sleep 3600 & wait $!; done')
failures=0

# helm_with MOUNT... -- ARGS...: helm from its pinned image, with those folders mounted; every helm
# run of this script goes through it (review R156).
helm_with() {
    local mounts=()
    while [ "$1" != -- ]; do mounts+=(-v "$1"); shift; done
    shift
    docker run --rm --user "$(id -u):$(id -g)" -e HELM_CACHE_HOME=/tmp -e HELM_CONFIG_HOME=/tmp \
        -e HELM_DATA_HOME=/tmp "${mounts[@]}" -w /charts "$HELM_IMAGE" "$@"
}
helm() { helm_with "$ROOT/infra/helm:/charts:ro" -- "$@"; }

# chart URL SHA256: a pinned chart's archive, downloaded once into a cache keyed by its SHA-256 and
# checked against it; prints the path. A new version is a new file, not a stale one (review R152).
charts_cache="${XDG_CACHE_HOME:-$HOME/.cache}/hds/charts"
chart() {
    local file="$charts_cache/$2.tgz"
    mkdir -p "$charts_cache"
    if [ ! -f "$file" ]; then
        curl -fsSL -o "$file.part" "$1" && mv "$file.part" "$file"
    fi
    echo "$2  $file" | sha256sum -c - > /dev/null \
        || { echo "$1 does not match its SHA-256" >&2; rm -f "$file"; exit 1; }
    printf '%s' "$file"
}

# yq FILE EXPRESSION: the results, one per line, over every document of the rendered file at once
# (eval-all, no document separators), so a select and a count see the whole release. It runs as
# this user, who owns the temporary folder. A failure prints YQ-FAILED, which no check expects:
# set -e does not see a failure inside an argument, and an empty result would pass a check
# that wants nothing.
yq() {
    docker exec -i "$yq_box" yq ea -N "$2" "/m/$1" | sed '/^$/d' || echo "YQ-FAILED"
}

# yq_each FILE EXPRESSION: as yq, but each document on its own (eval, not eval-all), for an
# expression that joins several of one document's fields: eval-all would mix the documents' values.
yq_each() {
    docker exec -i "$yq_box" yq e -N "$2" "/m/$1" | sed '/^$/d' || echo "YQ-FAILED"
}

# expect LABEL WANTED ACTUAL
expect() {
    if [ "$2" == "$3" ]; then
        printf 'ok    %s\n' "$1"
    else
        printf 'FAIL  %s\n      wanted: %s\n      got:    %s\n' "$1" "$2" "$3"
        failures=$((failures + 1))
    fi
}

# hds for each values file; hds-postgres has only its defaults, so it is rendered once.
for env in local ci; do
    helm lint --strict hds -f "hds/values-$env.yaml"
    helm template hds hds --namespace hds -f "hds/values-$env.yaml" > "$out/hds-$env.yaml"
done
helm lint --strict hds-postgres
helm template hds-postgres hds-postgres --namespace hds > "$out/hds-postgres.yaml"
# hds-secrets with its defaults, and with the private registry's pull secret.
helm lint --strict hds-secrets
helm template hds-secrets hds-secrets --namespace hds > "$out/hds-secrets.yaml"
helm template hds-secrets hds-secrets --namespace hds --set registry.enabled=true \
    > "$out/hds-secrets-registry.yaml"
# The local values with digests set, to check that images are then referred to by digest.
helm template hds hds --namespace hds -f hds/values-local.yaml \
    --set "images.api.digest=$DIGEST" --set "images.web.digest=$DIGEST" > "$out/hds-digest.yaml"

# crd_schemas TGZ DIR NAME: the strict JSON schemas of the CRDs in a chart's DIR, into a cache keyed
# by the chart's file (and so its SHA-256), as <kind>-<group's first part>-<version>.json, the names
# kubeconform's KindSuffix looks for; prints the cache. Strict, as kubeconform's own schemas are: no
# property the CRD does not name (reviews R140, R160). It runs inside $(...), where set -e does not
# hold, so each step checks its own status, and the cache is built aside and moved in only when
# every CRD is done: a failure leaves no cache to trust (review R175).
crd_schemas() {
    local cache building crd kind group version count=0
    cache="${XDG_CACHE_HOME:-$HOME/.cache}/hds/crd-json-schemas-$3-$(basename "$1" .tgz)"
    if [ -d "$cache" ]; then printf '%s' "$cache"; return 0; fi
    building=$(mktemp -d "$(dirname "$cache").XXXXXX") || return 1
    mkdir -p "$building/crds" || return 1
    tar -xzf "$1" -C "$building/crds" "$2" >&2 || { echo "no $2 in $1" >&2; return 1; }
    for crd in $(find "$building/crds" -name '*.yaml' | sort); do
        docker run --rm -i "$YQ_IMAGE" -o=json . < "$crd" > "$building/crd.json" || return 1
        [ "$(jq -r .kind "$building/crd.json")" = CustomResourceDefinition ] || continue
        kind=$(jq -r '.spec.names.kind | ascii_downcase' "$building/crd.json") || return 1
        group=$(jq -r '.spec.group | split(".")[0]' "$building/crd.json") || return 1
        for version in $(jq -r '.spec.versions[].name' "$building/crd.json"); do
            jq --arg v "$version" '.spec.versions[] | select(.name == $v) | .schema.openAPIV3Schema
                | walk(if type == "object" and has("properties") and (has("additionalProperties") | not)
                    then . + {additionalProperties: false} else . end)' "$building/crd.json" \
                > "$building/$kind-$group-$version.json" || return 1
            count=$((count + 1))
        done
    done
    [ "$count" -gt 0 ] || { echo "no CRD schema came from $2" >&2; return 1; }
    rm -rf -- "${building:?}/crds" "${building:?}/crd.json"
    mkdir -p "$(dirname "$cache")" && mv "$building" "$cache" || return 1
    printf '%s' "$cache"
}
vso_tgz=$(chart "$VSO_CHART" "$VSO_CHART_SHA256")
vso_cache=$(crd_schemas "$vso_tgz" vault-secrets-operator/crds vso) || exit 1
kps_tgz=$(chart "$KPS_CHART" "$KPS_CHART_SHA256")
kps_cache=$(crd_schemas "$kps_tgz" kube-prometheus-stack/charts/crds/crds kps) || exit 1
# Vault and the operator as the cluster installs them, with infra/cluster's values (T052).
vault_tgz=$(chart "$VAULT_CHART" "$VAULT_CHART_SHA256")
helm_with "$charts_cache:/c:ro" "$ROOT/infra/cluster:/v:ro" -- template vault "/c/$(basename "$vault_tgz")" \
    --namespace vault -f /v/vault-values.yaml > "$out/vault.yaml"
helm_with "$charts_cache:/c:ro" "$ROOT/infra/cluster:/v:ro" -- template vault-secrets-operator \
    "/c/$(basename "$vso_tgz")" --namespace vault-secrets-operator -f /v/vso-values.yaml \
    > "$out/vso.yaml"
# kube-prometheus-stack as the cluster installs it, with infra/cluster's values (T056).
helm_with "$charts_cache:/c:ro" "$ROOT/infra/cluster:/v:ro" -- template kube-prometheus-stack \
    "/c/$(basename "$kps_tgz")" --namespace monitoring -f /v/monitoring-values.yaml > "$out/kps.yaml"
docker run --rm -v "$out:/m:ro" -v "$vso_cache:/vso:ro" -v "$kps_cache:/kps:ro" "$KUBECONFORM_IMAGE" \
    -strict -summary -kubernetes-version "$KUBERNETES_VERSION" -schema-location "$SCHEMAS" \
    -schema-location '/vso/{{.ResourceKind}}{{.KindSuffix}}.json' \
    -schema-location '/kps/{{.ResourceKind}}{{.KindSuffix}}.json' /m

api='select(.kind == "Deployment" and .metadata.name == "hds-api")'
worker='select(.kind == "Deployment" and .metadata.name == "hds-worker")'
for env in local ci; do
    f="hds-$env.yaml"
    echo "== hds, values-$env.yaml"
    # The API Deployment (research R5, contracts/ops.md section 1).
    expect "api replicas" 2 "$(yq "$f" "$api | .spec.replicas")"
    expect "api maxUnavailable" 0 "$(yq "$f" "$api | .spec.strategy.rollingUpdate.maxUnavailable")"
    expect "api maxSurge" 1 "$(yq "$f" "$api | .spec.strategy.rollingUpdate.maxSurge")"
    c="$api | .spec.template.spec.containers[] | select(.name == \"api\")"
    expect "api readiness" "/readyz 8000 5 2" \
        "$(yq "$f" "$c | .readinessProbe | [.httpGet.path, .httpGet.port, .periodSeconds, .failureThreshold] | join(\" \")")"
    expect "api liveness" "/livez 8000 10 3" \
        "$(yq "$f" "$c | .livenessProbe | [.httpGet.path, .httpGet.port, .periodSeconds, .failureThreshold] | join(\" \")")"
    expect "api startup on /livez, up to 60 s" "/livez 60" \
        "$(yq "$f" "$c | .startupProbe | [.httpGet.path, .periodSeconds * .failureThreshold] | join(\" \")")"
    expect "api preStop sleep" 5 "$(yq "$f" "$c | .lifecycle.preStop.sleep.seconds")"
    expect "api grace" 30 "$(yq "$f" "$api | .spec.template.spec.terminationGracePeriodSeconds")"
    expect "api non-root, read-only root" "true true false" \
        "$(yq "$f" "$c | .securityContext | [.runAsNonRoot, .readOnlyRootFilesystem, .allowPrivilegeEscalation] | join(\" \")")"
    expect "api image by tag without a digest" "false" "$(yq "$f" "$c | .image | test(\"@sha256:\")")"
    # The worker Deployment (research R5, FR-020).
    expect "worker replicas" 1 "$(yq "$f" "$worker | .spec.replicas")"
    w="$worker | .spec.template.spec.containers[] | select(.name == \"worker\")"
    expect "worker probes on 9100" "9100 9100 9100" \
        "$(yq "$f" "$w | [.livenessProbe.httpGet.port, .readinessProbe.httpGet.port, .startupProbe.httpGet.port] | join(\" \")")"
    expect "worker grace" 90 "$(yq "$f" "$worker | .spec.template.spec.terminationGracePeriodSeconds")"
    expect "worker non-root, read-only root" "true true" \
        "$(yq "$f" "$w | .securityContext | [.runAsNonRoot, .readOnlyRootFilesystem] | join(\" \")")"
    # The PodDisruptionBudget (FR-017).
    expect "api PodDisruptionBudget" 1 \
        "$(yq "$f" 'select(.kind == "PodDisruptionBudget" and .metadata.name == "hds-api") | .spec.minAvailable')"
    # The migration Job: a hook on the API image (FR-016).
    job='select(.kind == "Job" and .metadata.name == "hds-migrate")'
    expect "migrate hook" "pre-install,pre-upgrade" "$(yq "$f" "$job | .metadata.annotations[\"helm.sh/hook\"]")"
    expect "migrate command" "python -m hds.scripts.migrate" \
        "$(yq "$f" "$job | .spec.template.spec.containers[0].command | join(\" \")")"
    expect "migrate on the API image" \
        "$(yq "$f" "$c | .image")" "$(yq "$f" "$job | .spec.template.spec.containers[0].image")"
    # The Ingress: the API's prefixes to the API, the rest to the web; no metrics (FR-009).
    ing='select(.kind == "Ingress") | .spec.rules[].http.paths[]'
    expect "ingress routes" "/api:hds-api:8000 /auth:hds-api:8000 /me:hds-api:8000 /:hds-web:8080" \
        "$(yq "$f" "[$ing | .path + \":\" + .backend.service.name + \":\" + (.backend.service.port.number | tostring)] | join(\" \")")"
    expect "ingress routes no /metrics and no 9100" "" \
        "$(yq "$f" "$ing | select(.path == \"/metrics\" or .backend.service.port.number == 9100) | .path")"
    # NetworkPolicy: each of the API and the worker is selected by a policy that filters ingress
    # (a policy selecting nothing, or Egress only, leaves its 9100 open to all).
    for part in api worker; do
        expect "$part has an ingress policy" 1 \
            "$(yq "$f" "(select(.kind == \"Deployment\" and .metadata.name == \"hds-$part\")
                | .spec.selector.matchLabels | to_json(0)) as \$sel
                | [select(.kind == \"NetworkPolicy\" and (.spec.podSelector.matchLabels | to_json(0)) == \$sel
                    and (.spec.policyTypes | any_c(. == \"Ingress\")))] | length")"
    done
    # Only the monitoring namespace reaches 9100. A rule naming no ports covers 9100 too, as does
    # the port's name (ops) or a range reaching it (counted from endPort alone: stricter, never
    # looser), and a rule with no sources lets everyone in, so all of them count.
    expect "9100 open to monitoring only" "monitoring" \
        "$(yq "$f" '[select(.kind == "NetworkPolicy") | .spec.ingress[]
            | select(((.ports // []) | length) == 0 or ((.ports // [])
                | any_c(.port == 9100 or .port == "ops" or (.endPort // 0) >= 9100)))
            | (.from // []) | if length == 0 then "EVERYONE"
              else .[] | .namespaceSelector.matchLabels["kubernetes.io/metadata.name"] // "OTHER" end]
            | unique | join(" ")')"
    # Only the pods that use the database carry the label hds-postgres lets in; not the web.
    expect "database clients" "api backup migrate worker" \
        "$(yq "$f" '[(.spec.template.metadata.labels // .spec.jobTemplate.spec.template.metadata.labels)
            | select(.["hds/database-client"] == "true") | .["app.kubernetes.io/component"]]
            | sort | join(" ")')"
    # No Secret is defined by the chart: they are referred to by name.
    expect "no Secret rendered" 0 "$(yq "$f" '[select(.kind == "Secret")] | length')"
    expect "DATABASE_URL names the database Service" "yes" \
        "$(yq "$f" "$c | .env[] | select(.name == \"DATABASE_URL\") | .value | test(\"@hds-postgres:5432/\") | select(.) | \"yes\"")"
done

# The backups (research R11, T043): the same rules for each of the three jobs.
for env in local ci; do
    f="hds-$env.yaml"
    echo "== hds, values-$env.yaml: the backup jobs"
    for job in backup restore-test; do
        cj="select(.kind == \"CronJob\" and .metadata.name == \"hds-$job\")"
        expect "$job deadline, no overlap, 3 runs kept" "86400 Forbid 3 3" \
            "$(yq "$f" "$cj | .spec | [.startingDeadlineSeconds, .concurrencyPolicy, .successfulJobsHistoryLimit, .failedJobsHistoryLimit] | join(\" \")")"
        expect "$job in the organisation's zone" "Asia/Jerusalem" "$(yq "$f" "$cj | .spec.timeZone")"
    done
    b='select(.kind == "CronJob" and .metadata.name == "hds-backup") | .spec'
    expect "backup at 03:30, outside the spring-forward hour" "30 3 * * *" "$(yq "$f" "$b | .schedule")"
    expect "backup prunes even when it fails, with no retry, within an hour" \
        "sh -c backup.sh; status=\$?; prune.sh || status=1; exit \$status 0 3600" \
        "$(yq "$f" "$b | .jobTemplate.spec | [(.template.spec.containers[0].command | join(\" \")), .backoffLimit, .activeDeadlineSeconds] | join(\" \")")"
    bp="$b | .jobTemplate.spec.template.spec"
    expect "backup reaches the database Service" "hds-postgres 5432" \
        "$(yq "$f" "$bp | .containers[0].env | [(.[] | select(.name == \"PGHOST\") | .value), (.[] | select(.name == \"PGPORT\") | .value)] | join(\" \")")"
    expect "backup's password from hds-api by name" "hds-api DATABASE_PASSWORD" \
        "$(yq "$f" "$bp | .containers[0].env[] | select(.name == \"PGPASSWORD\") | .valueFrom.secretKeyRef | [.name, .key] | join(\" \")")"
    expect "backup's remotes from hds-backup" "hds-backup" "$(yq "$f" "$bp | .containers[0].envFrom[0].secretRef.name")"
    expect "local copies on the hds-backups volume" "hds-backups /backups" \
        "$(yq "$f" "$bp | [(.volumes[] | select(.name == \"backups\") | .persistentVolumeClaim.claimName), (.containers[0].volumeMounts[] | select(.name == \"backups\") | .mountPath)] | join(\" \")")"
    expect "the API and the web keep serving 5 s after they are told to stop (preStop; R201)" "hds-api 5|hds-web 5" \
        "$(yq_each "$f" 'select(.kind == "Deployment" and (.metadata.name == "hds-api" or .metadata.name == "hds-web")) | .metadata.name + " " + (.spec.template.spec.containers[0].lifecycle.preStop.sleep.seconds | tostring)' | sort | paste -sd '|' -)"
    expect "the chart renders no PersistentVolumeClaim: the backup claim is made outside it (R189)" 0 \
        "$(yq "$f" '[select(.kind == "PersistentVolumeClaim")] | length')"
    expect "the backup and the snapshot mount the named claim" "hds-backups" \
        "$(yq "$f" 'select(.kind == "CronJob") | .spec.jobTemplate.spec.template.spec.volumes[]? | select(.name == "backups") | .persistentVolumeClaim.claimName' | sort -u)"
    expect "backup image non-root" "999 true" \
        "$(yq "$f" "$bp | [.securityContext.runAsUser, .containers[0].securityContext.readOnlyRootFilesystem] | join(\" \")")"
    r='select(.kind == "CronJob" and .metadata.name == "hds-restore-test") | .spec'
    expect "restore test on Sundays at 04:00, within two hours" "0 4 * * 0 7200" \
        "$(yq "$f" "$r | [.schedule, .jobTemplate.spec.activeDeadlineSeconds] | join(\" \")")"
    expect "the check runs as the API image's user" "10001" \
        "$(yq "$f" "$r | .jobTemplate.spec.template.spec.containers[0].securityContext.runAsUser")"
    rp="$r | .jobTemplate.spec.template.spec"
    expect "the empty postgres is the first init container, a native sidecar" "postgres Always" \
        "$(yq "$f" "$rp | .initContainers[0] | [.name, .restartPolicy] | join(\" \")")"
    expect "the sidecar's startup probe is pg_isready" "pg_isready -h 127.0.0.1 -U hds" \
        "$(yq "$f" "$rp | .initContainers[0].startupProbe.exec.command[2]")"
    expect "the sidecar is the cluster's postgres, on loopback only" "postgres@sha256:933463a6c8eba2b9f5cd414138738cd8d5519cd2c471214c222652f547a66a32 -c listen_addresses=127.0.0.1" \
        "$(yq "$f" "$rp | .initContainers[0] | [.image, (.args | join(\" \"))] | join(\" \")")"
    expect "then hds-backup runs restore-test.sh on the sidecar" "hds-backup restore-test.sh 127.0.0.1" \
        "$(yq "$f" "$rp | .initContainers[1] | [.name, .command[0], (.env[] | select(.name == \"PGHOST\") | .value)] | join(\" \")")"
    expect "then hds-api runs migrate and check_restore" "hds-api sh -c python -m hds.scripts.migrate && python -m hds.scripts.check_restore" \
        "$(yq "$f" "$rp | .containers[0] | [.name, (.command | join(\" \"))] | join(\" \")")"
    expect "on the release's own API image" "$(yq "$f" "$c | .image")" "$(yq "$f" "$rp | .containers[0].image")"
    expect "the check reads the sidecar, not the application's database" "postgresql+asyncpg://hds@127.0.0.1:5432/hds" \
        "$(yq "$f" "$rp | .containers[0].env[] | select(.name == \"DATABASE_URL\") | .value")"
    expect "the restore test has no local copy" 0 \
        "$(yq "$f" "$rp | [.volumes[] | select(.persistentVolumeClaim)] | length")"
    expect "the restore test has no database-client label" null \
        "$(yq "$f" "$r | .jobTemplate.spec.template.metadata.labels[\"hds/database-client\"]")"
done
echo "== hds, values-ci.yaml: Vault's snapshot"
v='select(.kind == "CronJob" and .metadata.name == "hds-vault-snapshot") | .spec'
vp="$v | .jobTemplate.spec.template.spec"
expect "vault snapshot weekly, as its own ServiceAccount" "0 3 * * 0 hds-vault-snapshot vault-snapshot.sh" \
    "$(yq hds-ci.yaml "[($v | .schedule), ($vp | .serviceAccountName), ($vp | .containers[0].command[0])] | join(\" \")")"
expect "vault snapshot signs in as the role T052 names" vault-snapshot \
    "$(yq hds-ci.yaml "$vp | .containers[0].env[] | select(.name == \"HDS_VAULT_ROLE\") | .value")"
expect "vault snapshot signs in with a 10-minute token for Vault only, no API-server token" \
    "false vault 600 /var/run/secrets/vault" \
    "$(yq hds-ci.yaml "$vp | (.automountServiceAccountToken | tostring) + \" \" + (.volumes[] | select(.name == \"vault-token\") | .projected.sources[0].serviceAccountToken | .audience + \" \" + (.expirationSeconds | tostring)) + \" \" + (.containers[0].volumeMounts[] | select(.name == \"vault-token\") | .mountPath)")"
expect "vault snapshot within 30 minutes" 1800 "$(yq hds-ci.yaml "$v | .jobTemplate.spec.activeDeadlineSeconds")"
expect "vault snapshot to the public key in the values" "age16q9ew59lene00jp303rjfh0c7t8jt9agwsctp27kez526m6hhazsfuev0c" \
    "$(yq hds-ci.yaml "$vp | .containers[0].env[] | select(.name == \"HDS_VAULT_RECIPIENT\") | .value")"
expect "no Kubernetes permission for any job" 0 \
    "$(yq hds-ci.yaml '[select(.kind == "Role" or .kind == "ClusterRole" or .kind == "RoleBinding" or .kind == "ClusterRoleBinding")] | length')"
expect "no snapshot job without a recipient" 0 \
    "$(yq hds-local.yaml '[select(.kind == "CronJob" and .metadata.name == "hds-vault-snapshot")] | length')"

echo "== hds, values-local.yaml: the settings"
lc="$api | .spec.template.spec.containers[] | select(.name == \"api\") | .env[]"
expect "ENVIRONMENT" production "$(yq hds-local.yaml "$lc | select(.name == \"ENVIRONMENT\") | .value")"
expect "TRUSTED_PROXY_COUNT" 1 "$(yq hds-local.yaml "$lc | select(.name == \"TRUSTED_PROXY_COUNT\") | .value")"
echo "== hds, with image digests set"
expect "api and web images by digest" "2" \
    "$(yq hds-digest.yaml "[select(.kind == \"Deployment\" and (.metadata.name == \"hds-api\" or .metadata.name == \"hds-web\"))
        | .spec.template.spec.containers[0].image | select(test(\"@$DIGEST\$\"))] | length")"
echo "== hds: release history"
expect "maxHistory documented as 10" 1 "$(grep -c 'maxHistory: 10' "$ROOT/infra/helm/hds/values.yaml" || true)"
# No values file holds a secret: no key named like one carries a value of its own. Secrets are
# referred to by name (a map of name and key), so such a key is a map or an empty list.
for v in "$ROOT"/infra/helm/*/values*.yaml; do
    name="values-$(basename "$(dirname "$v")")-$(basename "$v")"
    cp "$v" "$out/$name"
    expect "no literal secret in ${v#"$ROOT"/}" "" \
        "$(yq "$name" '[.. | select(kind == "scalar") | path | join(".")
            | select(test("(?i)(password|secret|token|apikey|api_key|private)[^.]*$"))] | join(" ")')"
done

f="hds-postgres.yaml"
echo "== hds-postgres"
pg='select(.kind == "StatefulSet" and .metadata.name == "hds-postgres") | .spec.template.spec'
expect "postgres non-root" "true" "$(yq "$f" "$pg | .securityContext.runAsNonRoot")"
expect "postgres probes over TCP" "pg_isready -h 127.0.0.1 -U hds|pg_isready -h 127.0.0.1 -U hds" \
    "$(yq "$f" "$pg | .containers[0] | [.readinessProbe.exec.command[2], .livenessProbe.exec.command[2]] | join(\"|\")")"
expect "postgres password from hds-api by name" "hds-api DATABASE_PASSWORD" \
    "$(yq "$f" "$pg | .containers[0].env[] | select(.name == \"POSTGRES_PASSWORD\") | .valueFrom.secretKeyRef | [.name, .key] | join(\" \")")"
# The policy selects the database's pods and filters ingress, and its rules are exactly one: the
# database clients, from this namespace only (no namespaceSelector), on 5432. A second rule, a
# rule without sources or a peer in any namespace would each change it.
expect "postgres has an ingress policy" 1 \
    "$(yq "$f" "(select(.kind == \"StatefulSet\") | .spec.selector.matchLabels | to_json(0)) as \$sel
        | [select(.kind == \"NetworkPolicy\" and (.spec.podSelector.matchLabels | to_json(0)) == \$sel
            and (.spec.policyTypes | any_c(. == \"Ingress\")))] | length")"
expect "postgres lets in only the database clients" \
    '[{"from":[{"podSelector":{"matchLabels":{"hds/database-client":"true"}}}],"ports":[{"port":5432,"protocol":"TCP"}]}]' \
    "$(yq "$f" 'select(.kind == "NetworkPolicy") | .spec.ingress | to_json(0)')"
expect "postgres listens on the Service's port" "5432 5432" \
    "$(yq "$f" "[($pg | .containers[0].env[] | select(.name == \"PGPORT\") | .value),
        (select(.kind == \"Service\") | .spec.ports[0].port)] | join(\" \")")"
expect "postgres startup probe over TCP, up to 5 min" "pg_isready -h 127.0.0.1 -U hds 300" \
    "$(yq "$f" "$pg | .containers[0].startupProbe | [.exec.command[2], .periodSeconds * .failureThreshold] | join(\" \")")"
expect "no Secret rendered" 0 "$(yq "$f" '[select(.kind == "Secret")] | length')"

# Flux's files (T048): kubeconform against the CRD schemas of the Flux release the cluster runs
# (infra/VERSIONS.md), checked by SHA-256 and kept in a cache; then the settings the runbooks rely on.
echo "== infra/flux/clusters/local"
flux_dir="$ROOT/infra/flux/clusters/local"
cache="${XDG_CACHE_HOME:-$HOME/.cache}/hds/flux-crd-schemas-$FLUX_VERSION"
if [ ! -f "$cache/helmrelease-helm-v2.json" ]; then
    mkdir -p "$cache"
    curl -fsSL -o "$cache/crd-schemas.tar.gz" \
        "https://github.com/fluxcd/flux2/releases/download/$FLUX_VERSION/crd-schemas.tar.gz"
    echo "$FLUX_SCHEMAS_SHA256  $cache/crd-schemas.tar.gz" | sha256sum -c - > /dev/null \
        || { echo "the Flux CRD schemas do not match their SHA-256" >&2; rm -rf "$cache"; exit 1; }
    tar -xzf "$cache/crd-schemas.tar.gz" -C "$cache"
fi
docker run --rm -v "$flux_dir:/f:ro" -v "$cache:/s:ro" "$KUBECONFORM_IMAGE" -strict -summary \
    -kubernetes-version "$KUBERNETES_VERSION" -schema-location "$SCHEMAS" \
    -schema-location '/s/{{.ResourceKind}}{{.KindSuffix}}.json' \
    -ignore-filename-pattern 'kustomization\.yaml' /f
expect "the kustomization lists every Flux file" \
    "$(find "$flux_dir" -maxdepth 1 -name '*.yaml' ! -name kustomization.yaml -exec basename {} \; | sort | tr '\n' ' ')" \
    "$(sed -n 's/^  - //p' "$flux_dir/kustomization.yaml" | sort | tr '\n' ' ')"
for file in "$flux_dir"/*.yaml; do
    [ "$(basename "$file")" = kustomization.yaml ] || { cat "$file"; echo "---"; }
done > "$out/flux.yaml"
f="flux.yaml"
hr='select(.kind == "HelmRelease" and .metadata.name == "hds") | .spec'
pr='select(.kind == "HelmRelease" and .metadata.name == "hds-postgres") | .spec'
sr='select(.kind == "HelmRelease" and .metadata.name == "hds-secrets") | .spec'
expect "each release's chart is its own OCIRepository" "hds hds-postgres hds-secrets" \
    "$(yq "$f" "$hr | .chartRef.name") $(yq "$f" "$pr | .chartRef.name") $(yq "$f" "$sr | .chartRef.name")"
expect "hds-postgres waits for hds-secrets, whose Secret it reads" "hds-secrets" \
    "$(yq "$f" "$pr | .dependsOn[].name")"
expect "every OCIRepository takes the newest 0.1 or later, every 5 minutes" \
    "hds >=0.1.0 5m|hds-postgres >=0.1.0 5m|hds-secrets >=0.1.0 5m" \
    "$(yq "$f" 'select(.kind == "OCIRepository") | .metadata.name + " " + .spec.ref.semver + " " + .spec.interval' | sort -u | paste -sd '|' -)"
expect "the OCIRepositories point at the release's chart paths" \
    "oci://ghcr.io/aviyayemini/charts/hds oci://ghcr.io/aviyayemini/charts/hds-postgres oci://ghcr.io/aviyayemini/charts/hds-secrets" \
    "$(yq "$f" 'select(.kind == "OCIRepository") | .spec.url' | sort | paste -sd ' ' -)"
# Signed releases only (T047c): both sources verify with cosign, for exactly one identity, the
# release workflow on main of this repository, from GitHub's issuer; both patterns anchored.
expect "every source installs only charts signed by the release workflow on main" \
    "$(printf '%s\n' 'cosign ^https://token\.actions\.githubusercontent\.com$ ^https://github\.com/AviyaYemini/HDS/\.github/workflows/release\.yml@refs/heads/main$ 1' | sort -u)" \
    "$(yq "$f" 'select(.kind == "OCIRepository") | .spec.verify.provider + " " + .spec.verify.matchOIDCIdentity[0].issuer + " " + .spec.verify.matchOIDCIdentity[0].subject + " " + (.spec.verify.matchOIDCIdentity | length | tostring)' | sort -u)"
expect "and all three sources verify" 3 "$(yq "$f" 'select(.kind == "OCIRepository" and .spec.verify.provider == "cosign") | .metadata.name' | sort -u | wc -l | tr -d ' ')"
expect "hds waits for hds-postgres" "hds-postgres" "$(yq "$f" "$hr | .dependsOn[].name")"
expect "each is a Helm release named as such, stored in hds" "hds hds hds|hds-postgres hds hds|hds-secrets hds hds" \
    "$(yq "$f" 'select(.kind == "HelmRelease") | .spec.releaseName + " " + .spec.targetNamespace + " " + .spec.storageNamespace' | sort -u | paste -sd '|' -)"
expect "a failed hds upgrade rolls back, without retries" "0 0 true rollback 10m 10" \
    "$(yq "$f" "$hr | [.install.remediation.retries, .upgrade.remediation.retries, .upgrade.remediation.remediateLastFailure, .upgrade.remediation.strategy, .timeout, .maxHistory] | join(\" \")")"
expect "drift detection is off for hds (review R97)" "disabled" \
    "$(yq "$f" "$hr | .driftDetection.mode // \"disabled\"")"
expect "hds takes values-local from the ConfigMap hds-values" "ConfigMap hds-values values.yaml" \
    "$(yq "$f" "$hr | .valuesFrom[] | [.kind, .name, .valuesKey] | join(\" \")")"
expect "release errors go to Alertmanager in monitoring" "error alertmanager alertmanager-operated.monitoring.svc:9093" \
    "$(yq "$f" '[(select(.kind == "Alert") | .spec.eventSeverity, .spec.providerRef.name), (select(.kind == "Provider") | .spec.address | sub("^http://"; "") | sub("/api.*$"; ""))] | join(" ")')"
expect "the Alert covers the three releases and their charts" \
    "HelmRelease/hds HelmRelease/hds-postgres HelmRelease/hds-secrets OCIRepository/hds OCIRepository/hds-postgres OCIRepository/hds-secrets" \
    "$(yq "$f" 'select(.kind == "Alert") | .spec.eventSources[] | .kind + "/" + .name' | sort | paste -sd ' ' -)"

f="vault.yaml"
echo "== Vault and the Vault Secrets Operator (infra/cluster)"
vs='select(.kind == "StatefulSet" and .metadata.name == "vault") | .spec'
expect "Vault: one node, the pinned image, its data on a PVC" \
    "1 hashicorp/vault:2.0.4@sha256:5be49781ecf78bfe775c5309c6a4d9f4e9e040b6c885c99eb2b12fb69855e1a2 data" \
    "$(yq "$f" "$vs | (.replicas | tostring) + \" \" + .template.spec.containers[0].image + \" \" + .volumeClaimTemplates[0].metadata.name")"
expect "Vault stores in raft" yes \
    "$(yq "$f" 'select(.kind == "ConfigMap") | .data["extraconfig-from-values.hcl"] | select(test("storage \"raft\"")) | "yes"')"
expect "no PodDisruptionBudget, which would block a drain of the one node" 0 \
    "$(yq "$f" '[select(.kind == "PodDisruptionBudget")] | length')"
expect "no injector" 0 "$(yq "$f" '[select(.metadata.name == "vault-agent-injector")] | length')"
expect "no UI" 0 "$(yq "$f" '[select(.metadata.name == "vault-ui")] | length')"
expect "Vault's API is reachable from the operator's namespace and the snapshot pods only" \
    '[{"from":[{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":"vault-secrets-operator"}}},{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":"hds"}},"podSelector":{"matchLabels":{"app.kubernetes.io/component":"vault-snapshot"}}}],"ports":[{"port":8200,"protocol":"TCP"}]}]' \
    "$(yq "$f" 'select(.kind == "NetworkPolicy") | .spec.ingress | to_json(0)')"
expect "the operator has no default connection or sign-in" 0 \
    "$(yq vso.yaml '[select(.kind == "VaultConnection" or .kind == "VaultAuth")] | length')"

f="kps.yaml"
echo "== kube-prometheus-stack (infra/cluster/monitoring-values.yaml)"
prom='select(.kind == "Prometheus") | .spec'
expect "Prometheus selects every ServiceMonitor and PrometheusRule, whatever their labels (review R160)" \
    '{} {} {} {}' \
    "$(yq_each "$f" "$prom | (.serviceMonitorSelector | to_json(0)) + \" \" + (.serviceMonitorNamespaceSelector | to_json(0)) + \" \" + (.ruleSelector | to_json(0)) + \" \" + (.ruleNamespaceSelector | to_json(0))")"
expect "none of the stack's own rules, so HDS's PrometheusRule is the only one (no Watchdog)" 0 \
    "$(yq "$f" '[select(.kind == "PrometheusRule")] | length')"
expect "no Grafana" 0 "$(yq "$f" '[select(.metadata.name | test("grafana"))] | length')"
expect "kube-state-metrics runs, filtering no metric family (review R163)" "1 none" \
    "$(yq "$f" '[select(.kind == "Deployment" and .metadata.name | test("kube-state-metrics"))] | length') $(yq "$f" 'select(.kind == "Deployment" and .metadata.name | test("kube-state-metrics")) | .spec.template.spec.containers[0].args[] | select(test("metric-(allow|deny)list"))' | head -n 1 | grep . || echo none)"
expect "Prometheus and Alertmanager by digest" \
    "quay.io/prometheus/alertmanager:v0.34.1@sha256:e9733bafb1bdef9b00e25a21f8f99dc26a22224bf16641ad754d1649f4c3357a quay.io/prometheus/prometheus:v3.15.0-distroless@sha256:b2a413d5a03ea6a76782a508d1c7947440bba3b973931a25676e278431891b01" \
    "$(yq "$f" 'select(.kind == "Prometheus" or .kind == "Alertmanager") | .spec.image' | sort | paste -sd ' ' -)"
am='select(.kind == "Alertmanager") | .spec'
expect "Alertmanager's configuration is hds-secrets' alertmanager-hds, its templates its ConfigMap" \
    "alertmanager-hds alertmanager-hds-templates" \
    "$(yq "$f" "$am | .configSecret") $(yq "$f" "$am | .configMaps[]")"
expect "the stack writes no Alertmanager configuration of its own" 0 \
    "$(yq "$f" '[select(.kind == "Secret" and (.metadata.name | test("^alertmanager-")))] | length')"
expect "and they are the names hds-secrets makes, in monitoring" "alertmanager-hds monitoring alertmanager-hds-templates monitoring" \
    "$(yq_each hds-secrets.yaml 'select(.kind == "VaultStaticSecret" and .metadata.namespace == "monitoring") | .spec.destination.name + " " + .metadata.namespace') $(yq_each hds-secrets.yaml 'select(.kind == "ConfigMap") | .metadata.name + " " + .metadata.namespace')"
expect "Alertmanager's sign-in is its own ServiceAccount and role, kept on uninstall" \
    "alertmanager hds-alertmanager-vault keep false" \
    "$(yq_each hds-secrets.yaml 'select(.kind == "VaultAuth" and .metadata.namespace == "monitoring") | .spec.kubernetes.role + " " + .spec.kubernetes.serviceAccount') $(yq_each hds-secrets.yaml 'select(.kind == "VaultStaticSecret" and .metadata.namespace == "monitoring") | .metadata.annotations["helm.sh/resource-policy"]') $(yq_each hds-secrets.yaml 'select(.kind == "ServiceAccount" and .metadata.namespace == "monitoring") | .automountServiceAccountToken')"

echo "== hds: scraping and rules (T056)"
expect "with monitoring.enabled false (CI) none of the stack's kinds renders" 0 \
    "$(yq hds-ci.yaml '[select(.apiVersion == "monitoring.coreos.com/v1")] | length')"
f="hds-local.yaml"
expect "a ServiceMonitor each for the API and the worker, on their Services' ops port" \
    "hds-api ops /metrics|hds-worker ops /metrics" \
    "$(yq_each "$f" 'select(.kind == "ServiceMonitor") | .metadata.name + " " + .spec.endpoints[0].port + " " + .spec.endpoints[0].path' | sort | paste -sd '|' -)"
expect "the Services they select expose ops on 9100" "hds-api 9100|hds-worker 9100" \
    "$(yq_each "$f" 'select(.kind == "Service" and (.metadata.name == "hds-api" or .metadata.name == "hds-worker")) | .metadata.name + " " + (.spec.ports[] | select(.name == "ops") | .port | tostring)' | sort | paste -sd '|' -)"
# monitor_selects NAME: whether ServiceMonitor NAME's matchLabels are all on Service NAME.
monitor_selects() {
    local selector labels
    selector=$(yq_each "$f" "select(.kind == \"ServiceMonitor\" and .metadata.name == \"$1\") | .spec.selector.matchLabels | to_entries | .[] | .key + \"=\" + .value")
    labels=$(yq_each "$f" "select(.kind == \"Service\" and .metadata.name == \"$1\") | .metadata.labels | to_entries | .[] | .key + \"=\" + .value")
    [ -n "$selector" ] && [ -z "$(comm -23 <(sort <<< "$selector") <(sort <<< "$labels"))" ]
}
for svc in hds-api hds-worker; do
    if monitor_selects "$svc"; then
        expect "the $svc ServiceMonitor selects the $svc Service" yes yes
    else
        expect "the $svc ServiceMonitor selects the $svc Service" yes no
    fi
done
expect "the ingress routes neither the ops port nor /metrics" "" \
    "$(yq "$f" 'select(.kind == "Ingress") | .spec.rules[].http.paths[] | select(.backend.service.port.number == 9100 or .backend.service.port.name == "ops" or (.path | test("metrics")))' )"
expect "the PrometheusRule is the rules file promtool tests, as it is" \
    "$(docker exec -i "$yq_box" yq -o=json '.groups' < "$ROOT/infra/helm/hds/files/alert-rules.yaml" | jq -c .)" \
    "$(yq_each "$f" 'select(.kind == "PrometheusRule") | .spec.groups' | docker exec -i "$yq_box" yq -o=json . | jq -c .)"

# hds-secrets (T051, research R3 amended in Group J): the operator's sign-in, one VaultStaticSecret per
# path, each kept on uninstall, and each restarting only the Deployments that read it.
f="hds-secrets.yaml"
echo "== hds-secrets"
expect "the VSO objects are hds-secrets', not the hds chart's" "0 0" \
    "$(yq hds-local.yaml '[select(.apiVersion == "secrets.hashicorp.com/v1beta1")] | length') $(yq hds-ci.yaml '[select(.apiVersion == "secrets.hashicorp.com/v1beta1")] | length')"
expect "Vault's address and the operator's sign-in" "http://vault.vault.svc:8200 kubernetes kubernetes hds hds-vault vault 600" \
    "$(yq "$f" '(select(.kind == "VaultConnection" and (.metadata.namespace // "hds") == "hds") | .spec.address) + " " + (select(.kind == "VaultAuth" and (.metadata.namespace // "hds") == "hds") | .spec | .method + " " + .mount + " " + .kubernetes.role + " " + .kubernetes.serviceAccount + " " + (.kubernetes.audiences | join(",")) + " " + (.kubernetes.tokenExpirationSeconds | tostring))' | sort -u)"
expect "its ServiceAccount mounts no token" false \
    "$(yq "$f" 'select(.kind == "ServiceAccount" and .metadata.name == "hds-vault") | .automountServiceAccountToken')"
expect "a VaultStaticSecret per path, KV v2, every 60s, into the Secret of its name" \
    "hds-api api|hds-backup backup|hds-smtp smtp" \
    "$(yq_each "$f" 'select(.kind == "VaultStaticSecret" and .spec.type == "kv-v2" and .spec.mount == "hds" and .spec.refreshAfter == "60s" and .spec.vaultAuthRef == "hds" and .spec.destination.create == true and .spec.destination.name == .metadata.name) | .metadata.name + " " + .spec.path' | sort -u | paste -sd '|' -)"
expect "each is kept on uninstall" "keep" \
    "$(yq "$f" 'select(.kind == "VaultStaticSecret") | .metadata.annotations["helm.sh/resource-policy"]' | sort -u)"
# The Deployments that read each Secret (hds.apiEnv and hds.apiEnvFrom in the hds chart): the API
# and the worker read hds-api and hds-smtp; only Jobs read hds-backup; never the database.
expect "each restarts only the Deployments that read it" \
    "hds-api: Deployment/hds-api Deployment/hds-worker|hds-backup: |hds-smtp: Deployment/hds-api Deployment/hds-worker" \
    "$(yq_each "$f" 'select(.kind == "VaultStaticSecret" and (.metadata.namespace // "hds") == "hds") | .metadata.name + ": " + ([.spec.rolloutRestartTargets[]? | .kind + "/" + .name] | join(" "))' | sort -u | paste -sd '|' -)"
expect "the hds chart's Deployments that read hds-api are those two" \
    "hds-api hds-worker" \
    "$(yq hds-local.yaml 'select(.kind == "Deployment") | select(.spec.template.spec.containers[].env[]?.valueFrom.secretKeyRef.name == "hds-api") | .metadata.name' | sort -u | paste -sd ' ' -)"
expect "and those that read hds-smtp, through envFrom (review R154)" \
    "hds-api hds-worker" \
    "$(yq hds-local.yaml 'select(.kind == "Deployment") | select(.spec.template.spec.containers[].envFrom[]?.secretRef.name == "hds-smtp") | .metadata.name' | sort -u | paste -sd ' ' -)"
expect "no pull secret unless the packages are private" 0 \
    "$(yq "$f" '[select(.metadata.name == "hds-registry")] | length')"
expect "the private registry's pull secret is a dockerconfigjson built from the token" \
    "kubernetes.io/dockerconfigjson true keep" \
    "$(yq hds-secrets-registry.yaml 'select(.kind == "VaultStaticSecret" and .metadata.name == "hds-registry") | .spec.destination.type + " " + (.spec.destination.transformation.excludeRaw | tostring) + " " + .metadata.annotations["helm.sh/resource-policy"]')"
expect "no Secret rendered" 0 "$(yq "$f" '[select(.kind == "Secret")] | length')"

if [ "$failures" -gt 0 ]; then
    echo "chart checks: $failures failed" >&2
    exit 1
fi
echo "chart checks: all passed"
