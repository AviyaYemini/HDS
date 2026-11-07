#!/usr/bin/env bash
# The chart checks (specs/006-deploy-operations T033): helm lint, kubeconform and assertions on the
# rendered manifests of both charts, hds and hds-postgres (research R3, amended), for
# values-local.yaml and values-ci.yaml. Run from anywhere: infra/helm/tests/assert.sh
#
# Every tool runs from an image pinned by tag and digest (infra/VERSIONS.md); nothing is installed.
# kubeconform checks against the Kubernetes v1.37.1 schemas (k3s v1.37.1) at a pinned commit of
# yannh/kubernetes-json-schema, not its default, which follows master. Every kind the charts render
# is a core kind, so no schema may be missing (-strict, no -ignore-missing-schemas).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
HELM_IMAGE="alpine/helm:3.22.1@sha256:8191701ad1f82ee3695c87211fcbfe6d7b1bb7df92ac1c8de9ccdb0995a6a2c1"
KUBECONFORM_IMAGE="ghcr.io/yannh/kubeconform:v0.8.0@sha256:faffaf43f95aa6425306e1ab8d6fcad72acb9049158f38e574c085ea1ec0f64e"
# Flux's CRD schemas: crd-schemas.tar.gz of the Flux release T063 installs (infra/VERSIONS.md).
FLUX_VERSION="v2.9.6"
FLUX_SCHEMAS_SHA256="380b713bd28a2c27cfc7710219aae79e1c9ceff9bee50197c023047190532987"
YQ_IMAGE="mikefarah/yq:4.54.1@sha256:4b3d9475d65571d28cbb19544d3820ec2945e4c8b2f18279394282b8dc3a592e"
SCHEMA_COMMIT="b2ba4defe30ed5a0104987433896f1fd32807304"
KUBERNETES_VERSION="1.37.1"
SCHEMAS="https://raw.githubusercontent.com/yannh/kubernetes-json-schema/${SCHEMA_COMMIT}/{{.NormalizedKubernetesVersion}}-standalone{{.StrictSuffix}}/{{.ResourceKind}}{{.KindSuffix}}.json"
DIGEST="sha256:0000000000000000000000000000000000000000000000000000000000000001"

out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
failures=0

helm() {
    docker run --rm --user "$(id -u):$(id -g)" -e HELM_CACHE_HOME=/tmp -e HELM_CONFIG_HOME=/tmp \
        -e HELM_DATA_HOME=/tmp -v "$ROOT/infra/helm:/charts:ro" -w /charts "$HELM_IMAGE" "$@"
}

# yq FILE EXPRESSION: the results, one per line, over every document of the rendered file at once
# (eval-all, no document separators), so a select and a count see the whole release. It runs as
# this user, who owns the temporary folder. A failure prints YQ-FAILED, which no check expects:
# set -e does not see a failure inside an argument, and an empty result would pass a check
# that wants nothing.
yq() {
    docker run --rm -i --user "$(id -u):$(id -g)" -v "$out:/m:ro" "$YQ_IMAGE" ea -N "$2" "/m/$1" \
        | sed '/^$/d' || echo "YQ-FAILED"
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
# The local values with digests set, to check that images are then referred to by digest.
helm template hds hds --namespace hds -f hds/values-local.yaml \
    --set "images.api.digest=$DIGEST" --set "images.web.digest=$DIGEST" > "$out/hds-digest.yaml"

docker run --rm -v "$out:/m:ro" "$KUBECONFORM_IMAGE" -strict -summary \
    -kubernetes-version "$KUBERNETES_VERSION" -schema-location "$SCHEMAS" /m

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
    expect "the hds-backups volume outlives the release" "keep" \
        "$(yq "$f" 'select(.kind == "PersistentVolumeClaim" and .metadata.name == "hds-backups") | .metadata.annotations["helm.sh/resource-policy"]')"
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
expect "each release's chart is its own OCIRepository" "hds hds-postgres" \
    "$(yq "$f" "$hr | .chartRef.name") $(yq "$f" "$pr | .chartRef.name")"
expect "both OCIRepositories take the newest 0.1 or later, every 5 minutes" \
    "hds >=0.1.0 5m|hds-postgres >=0.1.0 5m" \
    "$(yq "$f" 'select(.kind == "OCIRepository") | .metadata.name + " " + .spec.ref.semver + " " + .spec.interval' | sort -u | paste -sd '|' -)"
expect "the OCIRepositories point at the release's chart paths" \
    "oci://ghcr.io/aviyayemini/charts/hds oci://ghcr.io/aviyayemini/charts/hds-postgres" \
    "$(yq "$f" 'select(.kind == "OCIRepository") | .spec.url' | sort | paste -sd ' ' -)"
# Signed releases only (T047c): both sources verify with cosign, for exactly one identity, the
# release workflow on main of this repository, from GitHub's issuer; both patterns anchored.
expect "both sources install only charts signed by the release workflow on main" \
    "$(printf '%s\n' 'cosign ^https://token\.actions\.githubusercontent\.com$ ^https://github\.com/AviyaYemini/HDS/\.github/workflows/release\.yml@refs/heads/main$ 1' | sort -u)" \
    "$(yq "$f" 'select(.kind == "OCIRepository") | .spec.verify.provider + " " + .spec.verify.matchOIDCIdentity[0].issuer + " " + .spec.verify.matchOIDCIdentity[0].subject + " " + (.spec.verify.matchOIDCIdentity | length | tostring)' | sort -u)"
expect "and both sources verify" 2 "$(yq "$f" 'select(.kind == "OCIRepository" and .spec.verify.provider == "cosign") | .metadata.name' | sort -u | wc -l | tr -d ' ')"
expect "hds waits for hds-postgres" "hds-postgres" "$(yq "$f" "$hr | .dependsOn[].name")"
expect "both are Helm releases named as such, stored in hds" "hds hds hds|hds-postgres hds hds" \
    "$(yq "$f" 'select(.kind == "HelmRelease") | .spec.releaseName + " " + .spec.targetNamespace + " " + .spec.storageNamespace' | sort -u | paste -sd '|' -)"
expect "a failed hds upgrade rolls back, without retries" "0 0 true rollback 10m 10" \
    "$(yq "$f" "$hr | [.install.remediation.retries, .upgrade.remediation.retries, .upgrade.remediation.remediateLastFailure, .upgrade.remediation.strategy, .timeout, .maxHistory] | join(\" \")")"
expect "drift detection is off for hds (review R97)" "disabled" \
    "$(yq "$f" "$hr | .driftDetection.mode // \"disabled\"")"
expect "hds takes values-local from the ConfigMap hds-values" "ConfigMap hds-values values.yaml" \
    "$(yq "$f" "$hr | .valuesFrom[] | [.kind, .name, .valuesKey] | join(\" \")")"
expect "release errors go to Alertmanager in monitoring" "error alertmanager alertmanager-operated.monitoring.svc:9093" \
    "$(yq "$f" '[(select(.kind == "Alert") | .spec.eventSeverity, .spec.providerRef.name), (select(.kind == "Provider") | .spec.address | sub("^http://"; "") | sub("/api.*$"; ""))] | join(" ")')"
expect "the Alert covers both releases and both charts" \
    "HelmRelease/hds HelmRelease/hds-postgres OCIRepository/hds OCIRepository/hds-postgres" \
    "$(yq "$f" 'select(.kind == "Alert") | .spec.eventSources[] | .kind + "/" + .name' | sort | paste -sd ' ' -)"

if [ "$failures" -gt 0 ]; then
    echo "chart checks: $failures failed" >&2
    exit 1
fi
echo "chart checks: all passed"
