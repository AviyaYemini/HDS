#!/usr/bin/env bash
# Flux's own signature check (specs/006-deploy-operations T047c, T059; review B): on a k3d cluster,
# the source-controller of the Flux the cluster runs (v2.9.6), an OCIRepository with `verify`
# (cosign, a public key made here), and a throwaway chart in a registry on k3d's network:
#   - unsigned, the source is not Ready, for a verification error;
#   - signed with another key, it is still not Ready, so the key must match, not just be there;
#   - signed with the key it trusts, it is Ready.
# The keyless identity the cluster requires is checked statically (infra/helm/tests/assert.sh) and
# with the first real release (T064).
#
#   infra/tests/flux_verify.sh CLUSTER WORK   (called by infra/tests/rolling_update.sh)
set -euo pipefail

CLUSTER=$1 WORK=$2
FLUX_IMAGE="ghcr.io/fluxcd/flux-cli:v2.9.6@sha256:b1ac18156f227af9a524b842a96f2c20986c7a779ef721df3ba4e4540f449d76"
REGISTRY_IMAGE="registry:3@sha256:38907510915c3f1604c7e83d6ab602ab3020d092f73bdaed8696773693ff768c"
HELM_IMAGE="alpine/helm:3.22.1@sha256:8191701ad1f82ee3695c87211fcbfe6d7b1bb7df92ac1c8de9ccdb0995a6a2c1"
COSIGN_IMAGE="ghcr.io/sigstore/cosign/cosign:v3.1.4@sha256:0f06154836d2de34f887a5c273506d52ca43edaa1d9e53e6510fa78a48f69c2d"
SERVER="k3d-$CLUSTER-server-0"
network="k3d-$CLUSTER"
dir="$WORK/flux"
mkdir -p "$dir/chart/templates" "$dir/keys" "$dir/other"
chmod -R 755 "$dir"

kubectl() { docker exec -i "$SERVER" kubectl "$@"; }
on_network() { docker run --rm -i --network "$network" --user "$(id -u):$(id -g)" -v "$dir:/d" -w /d "$@"; }
registry=""
cleanup() { [ -z "$registry" ] || docker stop "$registry" > /dev/null 2>&1 || true; }
trap cleanup EXIT
failures=0
pass() { printf 'ok    %s\n' "$1"; }
flunk() { printf 'FAIL  %s\n' "$1"; [ -z "${2:-}" ] || printf '      %s\n' "$2"; failures=$((failures + 1)); }

echo "== source-controller, the registry, the keys and the chart"
docker run --rm "$FLUX_IMAGE" install --components=source-controller --export | kubectl apply -f - > /dev/null
kubectl -n flux-system rollout status deployment/source-controller --timeout=180s > /dev/null
registry=$(docker run -d --rm --network "$network" "$REGISTRY_IMAGE")
address=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$network\").IPAddress}}" "$registry"):5000
for keys in keys other; do
    on_network -e COSIGN_PASSWORD="" -w "/d/$keys" "$COSIGN_IMAGE" generate-key-pair > /dev/null 2>&1
done
kubectl -n flux-system create secret generic probe-cosign --from-file=cosign.pub=/dev/stdin \
    < "$dir/keys/cosign.pub" > /dev/null
printf 'apiVersion: v2\nname: probe\nversion: 0.1.0\n' > "$dir/chart/Chart.yaml"
printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: probe\n' > "$dir/chart/templates/configmap.yaml"
helm() { on_network -e HELM_CACHE_HOME=/tmp -e HELM_CONFIG_HOME=/tmp -e HELM_DATA_HOME=/tmp "$HELM_IMAGE" "$@"; }
helm package chart -d . > /dev/null
digest=$(helm push probe-0.1.0.tgz "oci://$address/charts" --plain-http 2>&1 | sed -n 's/^Digest: //p')
[ -n "$digest" ] || { echo "the chart could not be pushed" >&2; exit 1; }
kubectl apply -f - > /dev/null <<EOF
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: probe
  namespace: flux-system
spec:
  interval: 10s
  url: oci://$address/charts/probe
  insecure: true
  ref:
    tag: 0.1.0
  layerSelector:
    mediaType: application/vnd.cncf.helm.chart.content.v1.tar+gzip
    operation: copy
  verify:
    provider: cosign
    secretRef:
      name: probe-cosign
EOF

# settle: ask for a reconcile, wait up to 90 s until source-controller has handled that very request
# (lastHandledReconcileAt), so an earlier run's verdict is never read as this one's (review R191);
# print the Ready status and reason it then has.
settle() {
    local token got="" handled=""
    token="probe-$(date +%s)-$RANDOM"
    kubectl -n flux-system annotate --overwrite ocirepository probe "reconcile.fluxcd.io/requestedAt=$token" > /dev/null
    for _ in $(seq 1 45); do
        handled=$(kubectl -n flux-system get ocirepository probe -o jsonpath='{.status.lastHandledReconcileAt}')
        [ "$handled" = "$token" ] && break
        sleep 2
    done
    [ "$handled" = "$token" ] || { printf 'never-reconciled'; return; }
    got=$(kubectl -n flux-system get ocirepository probe \
        -o jsonpath='{.status.conditions[?(@.type=="Ready")].status} {.status.conditions[?(@.type=="Ready")].reason}')
    printf '%s' "$got"
}
sign() {
    on_network -e COSIGN_PASSWORD="" "$COSIGN_IMAGE" sign --yes --key "/d/$1/cosign.key" \
        --tlog-upload=false --use-signing-config=false --allow-http-registry \
        "$address/charts/probe@$digest" > /dev/null 2>&1
}

echo "== unsigned"
got=$(settle)
case $got in
    "False VerificationError"*) pass "an unsigned chart is refused ($got)" ;;
    *) flunk "an unsigned chart is refused" "Ready was: $got" ;;
esac
echo "== signed with another key"
sign other
got=$(settle)
case $got in
    "False VerificationError"*) pass "a chart signed with another key is refused ($got)" ;;
    *) flunk "a chart signed with another key is refused" "Ready was: $got" ;;
esac
echo "== signed with the key it trusts"
sign keys
got=$(settle)
case $got in
    "True "*) pass "a chart signed with the trusted key is taken ($got)" ;;
    *) flunk "a chart signed with the trusted key is taken" "Ready was: $got" ;;
esac

[ "$failures" -eq 0 ] || { echo "flux verify: $failures failed" >&2; exit 1; }
echo "flux verify: all passed"
