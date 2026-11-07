#!/bin/sh
# The release's scan gates (specs/006-deploy-operations T046, T047, FR-015; reviews R99, R100):
#
#   scan.sh vuln|secret LAYOUT PLATFORM
#
# LAYOUT is the OCI image layout (a folder) the release built once for both platforms; the same
# layout is what it pushes, so the bytes scanned are the bytes published. Each platform is scanned on its own.
#   vuln     a HIGH or CRITICAL vulnerability that has a fixed version fails it. Unfixed ones are
#            left out (--ignore-unfixed; the owner's reading of FR-015, research R4): nothing on our
#            side could clear them, so they would stop every release.
#   secret   any secret in the image fails it.
# The release workflow and the local proof (infra/tests/release_gates.sh) both run this script, so
# their flags cannot drift apart. Trivy runs from its image pinned by digest (infra/VERSIONS.md),
# never from trivy-action or setup-trivy, and every scan of a run shares one cache volume
# (HDS_TRIVY_CACHE, hds-trivy-cache), so the vulnerability database is downloaded once. jq picks the
# platform's image (below).
set -eu

TRIVY_IMAGE="aquasec/trivy:0.76.1@sha256:790fd66d1697395d8693d1acaa1ea702406a20f109b6fc7bac8db623eca6c06c"
usage="usage: $0 vuln|secret LAYOUT PLATFORM"
[ "$#" -eq 3 ] || { echo "$usage" >&2; exit 2; }
kind=$1 platform=$3
[ -f "$2/index.json" ] || { echo "no OCI image layout at $2" >&2; exit 2; }
layout=$(cd "$2" && pwd)

case $kind in
    vuln) flags="--scanners vuln --severity HIGH,CRITICAL --ignore-unfixed" ;;
    secret) flags="--scanners secret" ;;
    *) echo "$usage" >&2; exit 2 ;;
esac

# Trivy ignores --platform for a layout and reads its first image (review R124), so the platform's
# own image is picked here: its manifest's digest, from the image index, in a layout of its own
# beside the layout (the same file system), whose blobs are hard links to the layout's. The bytes
# are the pushed ones: the index the release pushes names them.
os=${platform%%/*} arch=${platform#*/}
top=$(jq -r '.manifests[0] | .mediaType + " " + .digest' "$layout/index.json") \
    || { echo "$layout/index.json cannot be read" >&2; exit 2; }
index="$layout/index.json"
case $top in
    application/vnd.oci.image.index.v1+json\ * | application/vnd.docker.distribution.manifest.list.v2+json\ *)
        index="$layout/blobs/sha256/${top##*sha256:}" ;;
esac
manifest=$(jq -c --arg os "$os" --arg arch "$arch" \
    '[.manifests[] | select(.platform.os == $os and .platform.architecture == $arch)]
     | if length == 1 then .[0] | {mediaType, digest, size} else error("not one") end' "$index") \
    || { echo "$layout has not exactly one image for $platform" >&2; exit 2; }
one=$(mktemp -d "$(dirname "$layout")/.scan-$(basename "$layout").XXXXXX")
trap 'rm -rf -- "${one:?}"' EXIT
printf '{"imageLayoutVersion":"1.0.0"}\n' > "$one/oci-layout"
printf '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[%s]}\n' \
    "$manifest" > "$one/index.json"
cp -Rl "$layout/blobs" "$one/blobs"

echo "== trivy $kind: $(basename "$layout") $platform ($(printf '%s' "$manifest" | jq -r .digest))"
# HDS_SCAN_REPORT names a file for Trivy's JSON report as well (the local proof reads it).
mkdir "$one/out"
report=""
[ -z "${HDS_SCAN_REPORT:-}" ] || report="--format json --output /out/report.json"
status=0
# shellcheck disable=SC2086 # the flags are words
docker run --rm -v "${HDS_TRIVY_CACHE:-hds-trivy-cache}:/cache" \
    -v "$one:/in:ro" -v "$one/out:/out" "$TRIVY_IMAGE" \
    image --cache-dir /cache --no-progress --skip-version-check --exit-code 1 $flags $report \
    --platform "$platform" --input /in || status=$?
[ -z "${HDS_SCAN_REPORT:-}" ] || cp "$one/out/report.json" "$HDS_SCAN_REPORT"
exit "$status"
