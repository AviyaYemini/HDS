#!/bin/sh
# Push the scanned images (specs/006-deploy-operations T047; review R100): each OCI image layout
# the release built and scanned is copied as it is, both platforms and every byte
# (skopeo copy --all --preserve-digests), so what runs is what was scanned. Nothing is rebuilt.
#
#   push_images.sh OWNER SHA LAYOUT_DIR   pushes LAYOUT_DIR/hds-{api,web,backup} as
#                                         REGISTRY/OWNER/hds-<name>:SHA
#
# It then reads each pushed index back and checks its digest is the layout's, and writes
# <name>_digest=sha256:... lines to GITHUB_OUTPUT (or prints them). skopeo runs from its image
# pinned by digest (infra/VERSIONS.md). Environment: HDS_REGISTRY (ghcr.io); GHCR_USER and
# GHCR_TOKEN to log in, when set (read from stdin, never on a command line); for the local proof,
# HDS_REGISTRY_INSECURE=1 (plain HTTP) and HDS_DOCKER_NETWORK (the Docker network skopeo joins; host).
set -eu

SKOPEO_IMAGE="quay.io/skopeo/stable:v1.22.3-immutable@sha256:c0ee1f4edca5c01cb8d5611124f92f3cc47196ecab68aee5d0f90834e00574d5"
[ "$#" -eq 3 ] || { echo "usage: $0 OWNER SHA LAYOUT_DIR" >&2; exit 2; }
owner=$1 sha=$2
layouts=$(cd "$3" && pwd)
registry=${HDS_REGISTRY:-ghcr.io}
fail() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }

auth=$(mktemp -d "${TMPDIR:-/tmp}/hds-skopeo.XXXXXX")
trap 'rm -rf -- "${auth:?}"' EXIT
tls="--dest-tls-verify=true"
read_tls="--tls-verify=true"
if [ "${HDS_REGISTRY_INSECURE:-}" = 1 ]; then
    tls="--dest-tls-verify=false"
    read_tls="--tls-verify=false"
fi
skopeo() {
    docker run --rm -i --network "${HDS_DOCKER_NETWORK:-host}" --user "$(id -u):$(id -g)" -e REGISTRY_AUTH_FILE=/auth/auth.json \
        -v "$auth:/auth" -v "$layouts:/layouts:ro" "$SKOPEO_IMAGE" "$@"
}
if [ -n "${GHCR_TOKEN:-}" ]; then
    printf '%s' "$GHCR_TOKEN" | skopeo login --password-stdin -u "${GHCR_USER:?}" "$registry" \
        || fail "skopeo could not log in to $registry"
fi

for name in api web backup; do
    layout="$layouts/hds-$name"
    want=$(jq -r '.manifests | if length == 1 then .[0].digest else error("not one image") end' \
        "$layout/index.json") || fail "$layout is not a layout of one multi-platform image"
    target="$registry/$owner/hds-$name:$sha"
    skopeo copy --all --preserve-digests "$tls" "oci:/layouts/hds-$name" "docker://$target" \
        || fail "hds-$name could not be pushed"
    got="sha256:$(skopeo inspect --raw "$read_tls" "docker://$target" | sha256sum | cut -d ' ' -f 1)"
    [ "$got" = "$want" ] || fail "$target has digest $got, not the scanned $want"
    echo "pushed $target ($want)"
    line="${name}_digest=$want"
    if [ -n "${GITHUB_OUTPUT:-}" ]; then echo "$line" >> "$GITHUB_OUTPUT"; else echo "$line"; fi
done
