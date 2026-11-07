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
# (HDS_TRIVY_CACHE, hds-trivy-cache), so the vulnerability database is downloaded once.
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

echo "== trivy $kind: $(basename "$layout") $platform"
# shellcheck disable=SC2086 # the flags are words
docker run --rm -v "${HDS_TRIVY_CACHE:-hds-trivy-cache}:/cache" -v "$layout:/in:ro" "$TRIVY_IMAGE" \
    image --cache-dir /cache --no-progress --skip-version-check --exit-code 1 $flags \
    --platform "$platform" --input /in
