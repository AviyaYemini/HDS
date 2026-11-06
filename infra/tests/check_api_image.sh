#!/bin/sh
# Check the API image's hardening (specs/006-deploy-operations T005): it runs as a non-root user and
# carries the version it was built from, as the env HDS_VERSION and the OCI version label.
#
# Usage, from the repository root: infra/tests/check_api_image.sh [version]
# Builds infra/Dockerfile.api with that version (default "check-<random>") and exits non-zero, with
# the reason, at the first unmet rule.
set -eu

version="${1:-check-$(od -An -N4 -tx4 /dev/urandom | tr -d ' ')}"
tag="hds-api:image-check"

docker build --quiet --build-arg "HDS_VERSION=$version" -f infra/Dockerfile.api -t "$tag" . >/dev/null

uid="$(docker run --rm --entrypoint id "$tag" -u)"
if [ "$uid" = "0" ]; then
    echo "FAIL: the image runs as root (uid 0)" >&2
    exit 1
fi

env_version="$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$tag" |
    sed -n 's/^HDS_VERSION=//p')"
if [ "$env_version" != "$version" ]; then
    echo "FAIL: HDS_VERSION is '$env_version', expected '$version'" >&2
    exit 1
fi

label="$(docker inspect --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "$tag")"
if [ "$label" != "$version" ]; then
    echo "FAIL: the org.opencontainers.image.version label is '$label', expected '$version'" >&2
    exit 1
fi

echo "ok: uid $uid, version $version"
