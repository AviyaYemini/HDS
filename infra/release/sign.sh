#!/bin/sh
# Sign one pushed artefact (specs/006-deploy-operations T047c, FR-028, Proposed; research R4):
#
#   sign.sh REGISTRY/REPOSITORY@sha256:DIGEST
#
# By digest only, never by tag: a tag can move after the signature. In GitHub Actions the signature
# is keyless: cosign takes the job's OIDC token (id-token: write), Fulcio certifies it for the
# identity "release.yml on main of this repository", and the signature is recorded in Sigstore's
# public Rekor log. No signing key exists to steal or rotate. Flux installs a chart only with such
# a signature (infra/flux/clusters/local/oci-repository*.yaml).
#
# The local proof (infra/tests/release_gates.sh) signs with a key instead (HDS_SIGN_KEY, a cosign
# key file with an empty password), and the checks replace cosign (HDS_COSIGN). Both are refused
# when GITHUB_ACTIONS=true, so in CI signing is always keyless and always happens (review R128).
#
# cosign runs from its image pinned by digest (infra/VERSIONS.md). Environment: GHCR_USER and
# GHCR_TOKEN to log in, when set (on stdin, never on a command line); HDS_REGISTRY_INSECURE=1 and
# HDS_DOCKER_NETWORK for the local proof's plain-HTTP registry.
set -eu

COSIGN_IMAGE="ghcr.io/sigstore/cosign/cosign:v3.1.4@sha256:0f06154836d2de34f887a5c273506d52ca43edaa1d9e53e6510fa78a48f69c2d"
# shellcheck source-path=SCRIPTDIR source=lib.sh
. "$(dirname "$0")/lib.sh"
no_overrides_in_ci

[ "$#" -eq 1 ] || { echo "usage: $0 REGISTRY/REPOSITORY@sha256:DIGEST" >&2; exit 2; }
ref=$1
printf '%s' "$ref" | grep -Eqx '[^@:]+(:[0-9]+)?/[^@:]+@sha256:[0-9a-f]{64}' \
    || fail "sign by digest only, not '$ref'"

if [ -n "${HDS_COSIGN:-}" ]; then
    "$HDS_COSIGN" sign --yes "$ref" || fail "$ref could not be signed"
    echo "signed $ref"
    exit 0
fi

auth=$(mktemp -d "${TMPDIR:-/tmp}/hds-cosign.XXXXXX")
trap 'rm -rf -- "${auth:?}"' EXIT
registry=${ref%%/*}
cosign() {
    docker run --rm -i --network "${HDS_DOCKER_NETWORK:-host}" --user "$(id -u):$(id -g)" \
        -e DOCKER_CONFIG=/auth -e HOME=/auth -e COSIGN_PASSWORD="" \
        -e ACTIONS_ID_TOKEN_REQUEST_URL -e ACTIONS_ID_TOKEN_REQUEST_TOKEN \
        -v "$auth:/auth" ${HDS_SIGN_KEY:+-v "$HDS_SIGN_KEY:/key/cosign.key:ro"} \
        "$COSIGN_IMAGE" "$@"
}
if [ -n "${GHCR_TOKEN:-}" ]; then
    printf '%s' "$GHCR_TOKEN" | cosign login "$registry" -u "${GHCR_USER:?}" --password-stdin \
        > /dev/null || fail "cosign could not log in to $registry"
fi

if [ -n "${HDS_SIGN_KEY:-}" ]; then
    # The local proof: a key, no transparency log, a plain-HTTP registry.
    # shellcheck disable=SC2046 # the flag is one word or none
    cosign sign --yes --key /key/cosign.key --tlog-upload=false --use-signing-config=false \
        $([ "${HDS_REGISTRY_INSECURE:-}" = 1 ] && echo --allow-http-registry) "$ref" \
        || fail "$ref could not be signed"
else
    cosign sign --yes "$ref" || fail "$ref could not be signed"
fi
echo "signed $ref"
