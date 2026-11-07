#!/bin/sh
# Package and push the two charts (specs/006-deploy-operations T047, research R3 amended, R4;
# review R98), after the images are pushed:
#
#   publish_charts.sh OWNER VERSION SHA API_DIGEST WEB_DIGEST BACKUP_DIGEST
#
# - hds is packaged as VERSION (0.1.<run number>) with appVersion SHA, its values naming each image
#   of this release by digest, so the cluster runs exactly what was scanned (research R2). It is
#   rendered before the push, and every image must be <repository>@sha256:<digest>.
# - hds-postgres keeps the version in its Chart.yaml, raised by hand (infra/helm/tests/
#   chart_version.sh holds a pull request to that): its pod template carries the chart's version,
#   so a new version every release would restart the database. It is pushed only when the registry
#   answers that this version is not there; any other answer (a login, network or rate-limit
#   error) fails the release, and nothing is pushed. It goes first: hds, which Flux installs at
#   once, is pushed last (review R117).
#
# Helm and yq run from the images pinned in infra/VERSIONS.md (HDS_HELM replaces helm, for the
# checks). Environment: GHCR_USER and GHCR_TOKEN to log in, when set.
set -eu

[ "$#" -eq 6 ] || { echo "usage: $0 OWNER VERSION SHA API_DIGEST WEB_DIGEST BACKUP_DIGEST" >&2; exit 2; }
owner=$1 version=$2 sha=$3 api=$4 web=$5 backup=$6
HELM_IMAGE="alpine/helm:3.22.1@sha256:8191701ad1f82ee3695c87211fcbfe6d7b1bb7df92ac1c8de9ccdb0995a6a2c1"
YQ_IMAGE="mikefarah/yq:4.54.1@sha256:4b3d9475d65571d28cbb19544d3820ec2945e4c8b2f18279394282b8dc3a592e"
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
registry="oci://ghcr.io/$owner/charts"

fail() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }
say() { printf '%s\n' "$*"; }
for d in "$api" "$web" "$backup"; do
    printf '%s' "$d" | grep -Eqx 'sha256:[0-9a-f]{64}' || fail "'$d' is not an image digest"
done
printf '%s' "$version" | grep -Eqx '0\.1\.[0-9]+' || fail "'$version' is not 0.1.<run number>"

work=$(mktemp -d "${TMPDIR:-/tmp}/hds-charts.XXXXXX")
trap 'rm -rf -- "${work:?}"' EXIT
cp -R "$ROOT/infra/helm/hds" "$ROOT/infra/helm/hds-postgres" "$work/"
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

if [ -n "${GHCR_TOKEN:-}" ]; then
    printf '%s' "$GHCR_TOKEN" | helm registry login ghcr.io -u "${GHCR_USER:?}" --password-stdin \
        || fail "helm could not log in to ghcr.io"
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
# hds-postgres: only a version the registry does not have yet.
pg_version=$(sed -n 's/^version: *//p' hds-postgres/Chart.yaml)
[ -n "$pg_version" ] || fail "no version in infra/helm/hds-postgres/Chart.yaml"
status=0
answer=$(helm show chart "$registry/hds-postgres" --version "$pg_version" 2>&1) || status=$?
if [ "$status" -eq 0 ]; then
    say "hds-postgres $pg_version is already in $registry; not pushed again"
elif printf '%s' "$answer" | grep -Eqi 'not found|manifest unknown|name unknown'; then
    helm package hds-postgres -d out > /dev/null || fail "hds-postgres could not be packaged"
    helm push "out/hds-postgres-$pg_version.tgz" "$registry" \
        || fail "hds-postgres $pg_version could not be pushed"
    say "pushed hds-postgres $pg_version to $registry/hds-postgres"
else
    fail "the registry's answer for hds-postgres $pg_version was not 'not found', so nothing was" \
        "pushed: $answer"
fi

# hds last: Flux installs it as soon as it is there, so a failure above must leave it unpushed.
helm push "out/hds-$version.tgz" "$registry" || fail "hds $version could not be pushed"
say "pushed hds $version (appVersion $sha) to $registry/hds"
