#!/bin/sh
# Package and push the three charts (specs/006-deploy-operations T047, research R3 amended, R4;
# reviews R98, R135), after the images are pushed:
#
#   publish_charts.sh OWNER VERSION SHA API_DIGEST WEB_DIGEST BACKUP_DIGEST
#
# - hds is packaged as VERSION (0.1.<run number>) with appVersion SHA, its values naming each image
#   of this release by digest, so the cluster runs exactly what was scanned (research R2). It is
#   rendered before the push, and every image must be <repository>@sha256:<digest>.
# - hds-secrets and hds-postgres keep the version in their Chart.yaml, raised by hand
#   (infra/helm/tests/chart_version.sh holds a pull request to that): hds-postgres's pod template
#   carries the chart's version, so a new version every release would restart the database. Each
#   is pushed only when the registry answers that its version is not there; any other answer (a
#   login, network or rate-limit error) fails the release, and nothing more is pushed. They go
#   first: hds, which Flux installs at once, is pushed last (review R117).
#
# Each chart is signed by its digest straight after its push (sign.sh; T047c), and the two
# fixed-version charts again each release. Helm, yq and skopeo run from the images pinned in
# infra/VERSIONS.md (HDS_HELM and HDS_SKOPEO replace them for the checks, never in GitHub Actions).
# Environment: GHCR_USER and GHCR_TOKEN to log in, when set.
set -eu

[ "$#" -eq 6 ] || { echo "usage: $0 OWNER VERSION SHA API_DIGEST WEB_DIGEST BACKUP_DIGEST" >&2; exit 2; }
owner=$1 version=$2 sha=$3 api=$4 web=$5 backup=$6
HELM_IMAGE="alpine/helm:3.22.1@sha256:8191701ad1f82ee3695c87211fcbfe6d7b1bb7df92ac1c8de9ccdb0995a6a2c1"
YQ_IMAGE="mikefarah/yq:4.54.1@sha256:4b3d9475d65571d28cbb19544d3820ec2945e4c8b2f18279394282b8dc3a592e"
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
SKOPEO_IMAGE="quay.io/skopeo/stable:v1.22.3-immutable@sha256:c0ee1f4edca5c01cb8d5611124f92f3cc47196ecab68aee5d0f90834e00574d5"
registry="oci://ghcr.io/$owner/charts"

# shellcheck source-path=SCRIPTDIR source=lib.sh
. "$(dirname "$0")/lib.sh"
no_overrides_in_ci
here=$(cd "$(dirname "$0")" && pwd)
say() { printf '%s\n' "$*"; }
for d in "$api" "$web" "$backup"; do
    printf '%s' "$d" | grep -Eqx 'sha256:[0-9a-f]{64}' || fail "'$d' is not an image digest"
done
printf '%s' "$version" | grep -Eqx '0\.1\.[0-9]+' || fail "'$version' is not 0.1.<run number>"

work=$(mktemp -d "${TMPDIR:-/tmp}/hds-charts.XXXXXX")
trap 'rm -rf -- "${work:?}"' EXIT
cp -R "$ROOT/infra/helm/hds" "$ROOT/infra/helm/hds-postgres" "$ROOT/infra/helm/hds-secrets" "$work/"
mkdir "$work/out" "$work/config"
cd "$work"

if [ -n "${HDS_HELM:-}" ]; then
    helm() { "$HDS_HELM" "$@"; }
else
    helm() {
        docker run --rm -i --user "$(id -u):$(id -g)" -e HELM_CACHE_HOME=/work/config \
            -e HELM_CONFIG_HOME=/work/config -e HELM_DATA_HOME=/work/config \
            -v "$work:/work" -w /work "$HELM_IMAGE" "$@"
    }
fi
yq() {
    docker run --rm --user "$(id -u):$(id -g)" -v "$work:/work" -w /work "$YQ_IMAGE" "$@"
}
if [ -n "${HDS_SKOPEO:-}" ]; then
    skopeo() { "$HDS_SKOPEO" "$@"; }
else
    skopeo() {
        docker run --rm -i --user "$(id -u):$(id -g)" -e REGISTRY_AUTH_FILE=/work/config/auth.json \
            -v "$work:/work" "$SKOPEO_IMAGE" "$@"
    }
fi

# signed NAME VERSION: sign the chart by the digest the registry holds for it, read back with
# skopeo rather than from helm's output (T047c).
signed() {
    skopeo inspect --raw "docker://ghcr.io/$owner/charts/$1:$2" > "$work/manifest.json" \
        || fail "$1 $2 could not be read back from the registry"
    [ -s "$work/manifest.json" ] || fail "$1 $2 read back empty from the registry"
    digest="sha256:$(sha256sum "$work/manifest.json" | cut -d ' ' -f 1)"
    HDS_SIGN=${HDS_SIGN:-$here/sign.sh} sign "ghcr.io/$owner/charts/$1@$digest" \
        || fail "$1 $2 could not be signed"
}

if [ -n "${GHCR_TOKEN:-}" ]; then
    printf '%s' "$GHCR_TOKEN" | helm registry login ghcr.io -u "${GHCR_USER:?}" --password-stdin \
        || fail "helm could not log in to ghcr.io"
    printf '%s' "$GHCR_TOKEN" | skopeo login --password-stdin -u "${GHCR_USER:?}" ghcr.io \
        || fail "skopeo could not log in to ghcr.io"
fi

# hds: this release's images, by digest.
yq -i "
    .images.api.repository = \"ghcr.io/$owner/hds-api\" | .images.api.digest = \"$api\" |
    .images.web.repository = \"ghcr.io/$owner/hds-web\" | .images.web.digest = \"$web\" |
    .images.backup.repository = \"ghcr.io/$owner/hds-backup\" | .images.backup.digest = \"$backup\" |
    .images.api.tag = \"$sha\" | .images.web.tag = \"$sha\" | .images.backup.tag = \"$sha\"
" hds/values.yaml || fail "the digests could not be written into the values"
helm package hds --version "$version" --app-version "$sha" -d out > /dev/null \
    || fail "hds could not be packaged"
helm template hds "out/hds-$version.tgz" --namespace hds -f hds/values-local.yaml > "$work/rendered.yaml" \
    || fail "the packaged hds chart does not render"
images=$(sed -n 's/^[ -]*image: *"\{0,1\}\([^"]*\)"\{0,1\} *$/\1/p' "$work/rendered.yaml" | sort -u)
[ -n "$images" ] || fail "the packaged hds chart renders no image"
for image in $images; do
    case $image in
        ghcr.io/"$owner"/hds-api@"$api" | ghcr.io/"$owner"/hds-web@"$web") ;;
        ghcr.io/"$owner"/hds-backup@"$backup" | postgres@sha256:*) ;;
        *) fail "the packaged hds chart renders $image, not an image of this release by digest" ;;
    esac
done
# The fixed-version charts, hds-secrets then hds-postgres: each only at a version the registry does
# not have yet, and signed each release.
# The list is read on descriptor 3: helm runs in `docker run -i`, which would read standard input
# and so swallow the rest of the list.
grep -v '^#' "$ROOT/infra/helm/fixed-version-charts" > "$work/fixed-version-charts"
while read -r chart <&3; do
    chart_version=$(sed -n 's/^version: *//p' "$chart/Chart.yaml")
    [ -n "$chart_version" ] || fail "no version in infra/helm/$chart/Chart.yaml"
    status=0
    answer=$(helm show chart "$registry/$chart" --version "$chart_version" 2>&1) || status=$?
    if [ "$status" -eq 0 ]; then
        say "$chart $chart_version is already in $registry; not pushed again"
        # Signed again each release, not only when pushed: a run that pushed it and then failed to
        # sign it would otherwise leave it unsigned for good, and Flux would never install it.
        signed "$chart" "$chart_version"
    elif printf '%s' "$answer" | grep -Eqi 'not found|manifest unknown|name unknown'; then
        helm package "$chart" -d out > /dev/null || fail "$chart could not be packaged"
        helm push "out/$chart-$chart_version.tgz" "$registry" \
            || fail "$chart $chart_version could not be pushed"
        say "pushed $chart $chart_version to $registry/$chart"
        signed "$chart" "$chart_version"
    else
        fail "the registry's answer for $chart $chart_version was not 'not found', so nothing was" \
            "pushed: $answer"
    fi
done 3< "$work/fixed-version-charts"

# hds last: Flux installs it as soon as it is there, so a failure above must leave it unpushed.
helm push "out/hds-$version.tgz" "$registry" || fail "hds $version could not be pushed"
say "pushed hds $version (appVersion $sha) to $registry/hds"
signed hds "$version"
