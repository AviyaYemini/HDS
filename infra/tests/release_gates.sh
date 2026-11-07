#!/usr/bin/env bash
# The release gates, proven locally (specs/006-deploy-operations T046; reviews R99, R100). GitHub can
# dispatch only a workflow on the default branch, so the workflow's own dry run waits for the merge;
# this runs the same scripts with the same flags (infra/release/scan.sh, push_images.sh):
#
#   (a) infra/Dockerfile.backup on an old PostgreSQL base, pinned by digest: the vuln gate fails;
#   (b) the real hds-backup image with a private key, generated here, in a layer: the secret gate
#       fails;
#   (c) hds-api, hds-web and hds-backup as they are, both platforms: each gate's result is printed;
#   (d) the three layouts pushed to a throwaway local registry: each pushed index has the digest of
#       the layout that was scanned;
#   (e) --platform picks the platform in a layout (review R124): a two-platform image whose arm64
#       half alone carries a key fails the secret gate for linux/arm64 and passes it for
#       linux/amd64, and Trivy names the architecture it read for each;
#   (f) signatures (T047c; review R128): the release's push signs each image by digest; cosign
#       verifies it with the signing key and refuses it with another key, refuses an unsigned
#       image, and refuses a tag moved to other bytes after signing. Keyless signing needs
#       GitHub's OIDC token, so this proves the mechanism with a key made here; the identity
#       Flux requires is checked in assert.sh, and Flux's own refusal in the rolling-update job.
#
# Nothing is pushed anywhere but the local registry, which is removed at the end, and nothing is
# written to the repository. It takes a while: three multi-platform builds. Run it by hand, all of
# it or some steps:
#   infra/tests/release_gates.sh [a] [b] [c] [d] [e] [f]
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
# postgres 16.0 (2023-09-14), long past its fixes: the base proof (a) builds on.
OLD_BASE="postgres:16.0@sha256:a80d0c1b119cf3d6bab27f72782f16e47ab8534ced937fa813ec2ab26e1fd81e"
REGISTRY_IMAGE="registry:3@sha256:38907510915c3f1604c7e83d6ab602ab3020d092f73bdaed8696773693ff768c"
work=$(mktemp -d "${TMPDIR:-/tmp}/hds-gates.XXXXXX")
registry=""
network="hds-gates-$$"
cleanup() {
    [ -z "$registry" ] || docker rm -f "$registry" > /dev/null 2>&1 || true
    docker network rm "$network" > /dev/null 2>&1 || true
    rm -rf -- "${work:?}"
}
trap cleanup EXIT
native="linux/$(docker version -f '{{.Server.Arch}}')"
failures=0
pass() { printf 'ok    %s\n' "$1"; }
flunk() { printf 'FAIL  %s\n' "$1"; failures=$((failures + 1)); }
cd "$ROOT"
steps=${*:-a b c d e f}
want() { case " $steps " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }
COSIGN_IMAGE=$(sed -n 's/^COSIGN_IMAGE="\(.*\)"$/\1/p' "$ROOT/infra/release/sign.sh")
# keys: two cosign key pairs made here, once: the one the proof signs with, and another. A push
# signs what it pushes (T047c); keyless signing needs GitHub's OIDC token, so the proof uses a key.
keys() {
    [ ! -d "$work/keys" ] || return 0
    mkdir "$work/keys" "$work/other"
    for dir in keys other; do
        docker run --rm --user "$(id -u):$(id -g)" -e COSIGN_PASSWORD="" -v "$work/$dir:/k" -w /k \
            "$COSIGN_IMAGE" generate-key-pair > /dev/null 2>&1
    done
}
WEB_BASE=$(sed -n 's/^FROM \(nginxinc\/nginx-unprivileged:[^ ]*\)$/\1/p' infra/Dockerfile.web)

# gate LABEL WANTED KIND LAYOUT PLATFORM: the scan's status is 0 (WANTED pass) or not (WANTED fail).
gate() {
    local label=$1 want=$2 status=0
    "$ROOT/infra/release/scan.sh" "$3" "$4" "$5" > "$work/scan.log" 2>&1 || status=$?
    grep -E '^Total:|^[^ ].*\((gobinary|debian|alpine)' "$work/scan.log" || true
    if { [ "$want" = fail ] && [ "$status" -ne 0 ]; } || { [ "$want" = pass ] && [ "$status" -eq 0 ]; }; then
        pass "$label (exit $status)"
    else
        flunk "$label (exit $status); the scan said: $(tail -n 20 "$work/scan.log")"
    fi
}

if want a; then
echo "== (a) an old base fails the vulnerability gate"
sed "s|^FROM postgres:16\.15@sha256:[0-9a-f]*|FROM $OLD_BASE|" infra/Dockerfile.backup > "$work/Dockerfile.old"
grep -qF "FROM $OLD_BASE" "$work/Dockerfile.old" || { echo "the base line was not replaced" >&2; exit 1; }
docker buildx build --quiet --platform "$native" -f "$work/Dockerfile.old" \
    --output "type=oci,dest=$work/old-base,tar=false" . > /dev/null
gate "Dockerfile.backup on $OLD_BASE" fail vuln "$work/old-base" "$native"
fi

mkdir "$work/planted"
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$work/planted/id_rsa" 2> /dev/null
if want b; then
echo "== (b) a key in the image fails the secret gate"
printf 'COPY --from=planted id_rsa /opt/hds-backup/id_rsa\n' | cat infra/Dockerfile.backup - \
    > "$work/Dockerfile.planted"
docker buildx build --quiet --platform "$native" -f "$work/Dockerfile.planted" \
    --build-context "planted=$work/planted" --output "type=oci,dest=$work/planted-image,tar=false" . \
    > /dev/null
gate "hds-backup with a generated private key" fail secret "$work/planted-image" "$native"
docker buildx build --quiet --platform "$native" -f infra/Dockerfile.backup \
    --output "type=oci,dest=$work/clean-image,tar=false" . > /dev/null
gate "hds-backup without it" pass secret "$work/clean-image" "$native"
fi

if want c || want d; then
echo "== (c) the images as they are, both platforms"
mkdir "$work/layouts"
for name in api web backup; do
    docker buildx build --quiet --platform linux/amd64,linux/arm64 --build-arg HDS_VERSION=proof \
        -f "infra/Dockerfile.$name" --output "type=oci,dest=$work/layouts/hds-$name,tar=false" . > /dev/null
    for platform in linux/amd64 linux/arm64; do
        for kind in vuln secret; do
            status=0
            "$ROOT/infra/release/scan.sh" "$kind" "$work/layouts/hds-$name" "$platform" \
                > "$work/scan.log" 2>&1 || status=$?
            printf 'hds-%s %s %s: exit %s\n' "$name" "$kind" "$platform" "$status"
            [ "$status" -eq 0 ] || grep -E '^Total:|\((gobinary|debian|alpine)' "$work/scan.log" || true
        done
    done
done

fi

if want d; then
echo "== (d) the pushed images are the scanned layouts"
docker network create "$network" > /dev/null
registry=$(docker run -d --network "$network" --network-alias registry "$REGISTRY_IMAGE")
sleep 2
keys
HDS_REGISTRY=registry:5000 HDS_REGISTRY_INSECURE=1 HDS_DOCKER_NETWORK="$network" \
    HDS_SIGN_KEY="$work/keys/cosign.key" "$ROOT/infra/release/push_images.sh" proof 0123456789abcdef0123456789abcdef01234567 \
    "$work/layouts" > "$work/push.log" 2>&1 || { cat "$work/push.log"; flunk "the local push"; }
for name in api web backup; do
    want=$(jq -r '.manifests[0].digest' "$work/layouts/hds-$name/index.json")
    if grep -qx "${name}_digest=$want" "$work/push.log"; then
        pass "hds-$name pushed as $want, the scanned layout's digest"
    else
        flunk "hds-$name: wanted $want; the push said: $(cat "$work/push.log")"
    fi
done

fi

if want e; then
echo "== (e) --platform picks the platform in a layout"
cat > "$work/Dockerfile.split" <<SPLIT
FROM $WEB_BASE
ARG TARGETARCH
USER root
RUN --mount=type=bind,from=planted,target=/planted \\
    if [ "\$TARGETARCH" = arm64 ]; then cp /planted/id_rsa /opt/id_rsa; fi
SPLIT
docker buildx build --quiet --platform linux/amd64,linux/arm64 -f "$work/Dockerfile.split" \
    --build-context "planted=$work/planted" --output "type=oci,dest=$work/split,tar=false" "$work" \
    > /dev/null
gate "the split image for linux/arm64, which carries the key" fail secret "$work/split" linux/arm64
gate "the split image for linux/amd64, which does not" pass secret "$work/split" linux/amd64
for platform in linux/amd64 linux/arm64; do
    HDS_SCAN_REPORT="$work/report.json" "$ROOT/infra/release/scan.sh" secret "$work/split" "$platform" \
        > /dev/null 2>&1 || true
    arch=$(jq -r '.Metadata.ImageConfig.architecture' "$work/report.json")
    if [ "linux/$arch" = "$platform" ]; then
        pass "Trivy read the $arch image for --platform $platform"
    else
        flunk "Trivy read '$arch' for --platform $platform"
    fi
done
fi

if want f; then
echo "== (f) signed by digest, verified, and refused when it should be"
keys
mkdir "$work/signed"
# Two small two-platform images: the one signed, and the bytes a tag is moved to afterwards.
for n in 1 2; do
    printf 'FROM %s\nLABEL proof=%s\n' "$WEB_BASE" "$n" > "$work/Dockerfile.tiny$n"
    docker buildx build --quiet --platform linux/amd64,linux/arm64 -f "$work/Dockerfile.tiny$n" \
        --output "type=oci,dest=$work/tiny$n,tar=false" "$work" > /dev/null
done
for name in api web backup; do cp -R "$work/tiny1" "$work/signed/hds-$name"; done
[ -n "$registry" ] || {
    docker network create "$network" > /dev/null
    registry=$(docker run -d --network "$network" --network-alias registry "$REGISTRY_IMAGE")
    sleep 2
}
sha=fedcba9876543210fedcba9876543210fedcba98
HDS_REGISTRY=registry:5000 HDS_REGISTRY_INSECURE=1 HDS_DOCKER_NETWORK="$network" \
    HDS_SIGN_KEY="$work/keys/cosign.key" \
    "$ROOT/infra/release/push_images.sh" signed "$sha" "$work/signed" > "$work/sign.log" 2>&1 \
    || { cat "$work/sign.log"; flunk "the signed push"; }
digest=$(jq -r '.manifests[0].digest' "$work/tiny1/index.json")
# verify KEY_DIR REF: cosign's verdict, as Flux would reach it for a key-signed artefact.
verify() {
    docker run --rm --network "$network" -v "$work/$1:/k:ro" "$COSIGN_IMAGE" verify \
        --key /k/cosign.pub --insecure-ignore-tlog=true --allow-http-registry "$2" \
        > "$work/verify.log" 2>&1
}
if verify keys "registry:5000/signed/hds-api@$digest"; then
    pass "the pushed image verifies with the signing key"
else
    flunk "the pushed image did not verify: $(tail -n 3 "$work/verify.log")"
fi
if verify other "registry:5000/signed/hds-api@$digest"; then
    flunk "it verified with another key"
else
    pass "another key is refused"
fi
SKOPEO_IMAGE=$(sed -n 's/^SKOPEO_IMAGE="\(.*\)"$/\1/p' "$ROOT/infra/release/push_images.sh")
docker run --rm --network "$network" -v "$work:/w:ro" "$SKOPEO_IMAGE" copy --all \
    --dest-tls-verify=false oci:/w/tiny2 "docker://registry:5000/signed/unsigned:$sha" > /dev/null
if verify keys "registry:5000/signed/unsigned:$sha"; then
    flunk "an unsigned image verified"
else
    pass "an unsigned image is refused"
fi
docker run --rm --network "$network" -v "$work:/w:ro" "$SKOPEO_IMAGE" copy --all \
    --dest-tls-verify=false oci:/w/tiny2 "docker://registry:5000/signed/hds-api:$sha" > /dev/null
if verify keys "registry:5000/signed/hds-api:$sha"; then
    flunk "a tag moved after signing still verified"
else
    pass "a tag moved to other bytes after signing is refused"
fi
fi

[ "$failures" -eq 0 ] || { echo "release gate proofs: $failures failed" >&2; exit 1; }
echo "release gate proofs ($steps): every gate held; (c) is printed above"
