#!/usr/bin/env bash
# Checks the release workflow's scripts (specs/006-deploy-operations T046, T047; reviews R98, R99)
# without GitHub or a registry. Run by the CI lint job: infra/tests/check_release.sh
#   - infra/release/trust.sh against workflow_run event fixtures, a fork's pull request among them;
#   - infra/helm/tests/chart_version.sh in a throwaway git repository;
#   - infra/release/publish_charts.sh with a fake helm in front of the real one: show and push are
#     played, package and template run for real.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
FIXTURES="$ROOT/infra/tests/fixtures/release"
HELM_IMAGE="alpine/helm:3.22.1@sha256:8191701ad1f82ee3695c87211fcbfe6d7b1bb7df92ac1c8de9ccdb0995a6a2c1"
scratch=$(mktemp -d)
trap 'rm -rf -- "${scratch:?}"' EXIT

failures=0
pass() { printf 'ok    %s\n' "$1"; }
flunk() { printf 'FAIL  %s\n' "$1"; [ -z "${2:-}" ] || printf '      %s\n' "$2"; failures=$((failures + 1)); }

# holds LABEL COMMAND... and never LABEL COMMAND...: the command succeeds, or fails.
holds() { local label=$1; shift; if "$@"; then pass "$label"; else flunk "$label"; fi; }
never() { local label=$1; shift; if "$@"; then flunk "$label"; else pass "$label"; fi; }
same() { if [ "$2" = "$3" ]; then pass "$1"; else flunk "$1" "wanted: $2; got: $3"; fi; }
# after A B: the first call matching A comes before the first matching B.
after() {
    local a b
    a=$(grep -n -- "$1" "$scratch/calls" | head -n 1 | cut -d: -f1)
    b=$(grep -n -- "$2" "$scratch/calls" | head -n 1 | cut -d: -f1)
    [ -n "$a" ] && [ -n "$b" ] && [ "$a" -lt "$b" ]
}

# expect LABEL WANTED_STATUS WANTED_TEXT COMMAND...: the status, and the text in the output.
expect() {
    local label=$1 want=$2 text=$3 out status
    shift 3
    set +e
    out=$("$@" 2>&1)
    status=$?
    set -e
    if [ "$status" = "$want" ] && grep -qF -- "$text" <<< "$out"; then
        pass "$label"
    else
        flunk "$label" "status $status (wanted $want); output: $out"
    fi
}

echo "== trust.sh"
trust() {
    : > "$scratch/output"
    env HDS_MAIN_TIP="${TIP-0123456789abcdef0123456789abcdef01234567}" \
        GITHUB_EVENT_NAME="$1" GITHUB_EVENT_PATH="$FIXTURES/$2" GITHUB_REPOSITORY=AviyaYemini/HDS \
        GITHUB_SHA=fedcba9876543210fedcba9876543210fedcba98 GITHUB_OUTPUT="$scratch/output" \
        DRY_RUN="${3:-}" "$ROOT/infra/release/trust.sh"
}
expect "a CI success on a push to main publishes" 0 "publish=true, commit 0123456" \
    trust workflow_run main-push.json
holds "it builds the commit CI tested" \
    grep -qx "sha=0123456789abcdef0123456789abcdef01234567" "$scratch/output"
expect "a fork's pull request from a branch named main is refused" 1 "not a push" \
    trust workflow_run fork-pr-main.json
expect "our own pull request from main is refused" 1 "not a push" trust workflow_run own-pr-main.json
expect "a push to a fork's main is refused" 1 "not of AviyaYemini/HDS" \
    trust workflow_run fork-push-main.json
expect "a push to another branch is refused" 1 "not main" trust workflow_run branch-push.json
expect "a failed CI is refused" 1 "not success" trust workflow_run main-failed.json
TIP=fedcba9876543210fedcba9876543210fedcba98 expect "an older commit of main is refused" 1 \
    "but main is now at fedcba98" trust workflow_run main-push.json
TIP="" expect "an unreadable main is refused" 1 "could not be read" trust workflow_run main-push.json
expect "a dispatched dry run builds and scans only" 0 "publish=false" \
    trust workflow_dispatch main-push.json true
expect "a dispatch with dry_run=false fails at once" 1 "publishes nothing" \
    trust workflow_dispatch main-push.json false

echo "== chart_version.sh"
repo="$scratch/repo"
mkdir -p "$repo/infra/helm/hds-postgres/templates"
git -C "$repo" init -q -b main
git -C "$repo" config user.email check@example.invalid
git -C "$repo" config user.name check
printf 'name: hds-postgres\nversion: 0.1.9\n' > "$repo/infra/helm/hds-postgres/Chart.yaml"
echo "kind: Service" > "$repo/infra/helm/hds-postgres/templates/svc.yaml"
echo "readme" > "$repo/README.md"
git -C "$repo" add -A && git -C "$repo" commit -qm base
# change WHAT VERSION: a branch from main with that change, checked against main.
change() {
    git -C "$repo" checkout -q main
    git -C "$repo" checkout -q -B "pr-$RANDOM"
    case $1 in
        template) echo "# changed" >> "$repo/infra/helm/hds-postgres/templates/svc.yaml" ;;
        other) echo "changed" >> "$repo/README.md" ;;
    esac
    [ -z "$2" ] || sed -i.bak "s/^version: .*/version: $2/" "$repo/infra/helm/hds-postgres/Chart.yaml"
    rm -f "$repo/infra/helm/hds-postgres/Chart.yaml.bak"
    git -C "$repo" commit -qam change
    (cd "$repo" && "$ROOT/infra/helm/tests/chart_version.sh" main)
}
expect "a change elsewhere needs no new version" 0 "is unchanged" change other ""
expect "a template changed without a new version fails" 1 "is not above 0.1.9" change template ""
expect "a template changed with a new version passes" 0 "from 0.1.9 to 0.1.10" change template 0.1.10
expect "a lower version fails, compared as versions" 1 "0.1.8 is not above 0.1.9" change template 0.1.8

echo "== the fixed-version charts"
listed=$(grep -v '^#' "$ROOT/infra/helm/fixed-version-charts" | sort | paste -sd ' ' -)
present=$(cd "$ROOT/infra/helm" && find . -maxdepth 2 -name Chart.yaml | cut -d/ -f2 | grep -vx hds | sort | paste -sd ' ' -)
same "every chart but hds is in infra/helm/fixed-version-charts" "$present" "$listed"

echo "== publish_charts.sh"
cat > "$scratch/helm" <<FAKE
#!/bin/sh
# FAKE_SHOW: the answer of helm show chart (exists, missing, or denied). FAKE_IMAGE: a rendered
# image to add to helm template's output.
printf 'helm %s\n' "\$*" >> "$scratch/calls"
case "\$1" in
    show)
        # As helm in docker run -i, it reads its standard input.
        cat > /dev/null
        case "\${FAKE_SHOW:-missing}" in
            exists) echo "name: hds-postgres" ;;
            missing) echo "Error: ghcr.io/x/charts/hds-postgres:0.1.0: not found" >&2; exit 1 ;;
            *) echo "Error: unexpected status code 401: unauthorized" >&2; exit 1 ;;
        esac ;;
    push | registry) ;;
    *)
        docker run --rm --user "\$(id -u):\$(id -g)" -e HELM_CACHE_HOME=/tmp -e HELM_CONFIG_HOME=/tmp \
            -e HELM_DATA_HOME=/tmp -v "\$PWD:/work" -w /work "$HELM_IMAGE" "\$@" || exit
        if [ "\$1" = template ] && [ -n "\${FAKE_IMAGE:-}" ]; then
            printf -- '---\nkind: Pod\nspec:\n  containers:\n    - image: %s\n' "\$FAKE_IMAGE"
        fi ;;
esac
FAKE
chmod +x "$scratch/helm"
# The stand-ins for skopeo (inspect --raw answers "manifest of <ref>"; copy is recorded) and for
# sign.sh (records what it signs), sharing the helm stand-in's record of calls.
cat > "$scratch/skopeo" <<FAKE
#!/bin/sh
printf 'skopeo %s\n' "\$*" >> "$scratch/calls"
case "\$1" in
    inspect) for last in "\$@"; do :; done; printf 'manifest of %s' "\$last" ;;
    copy | login) ;;
esac
FAKE
cat > "$scratch/sign" <<FAKE
#!/bin/sh
printf 'sign %s\n' "\$1" >> "$scratch/calls"
FAKE
chmod +x "$scratch/skopeo" "$scratch/sign"
# CI sets GITHUB_ACTIONS=true, and the scripts then refuse every stand-in (R128), so each run with
# stand-ins drops it (R206); the refusal cases below set it again on purpose.
# digest_of TEXT: the sha256 digest of TEXT, as the stand-in registry answers it.
digest_of() { printf 'sha256:%s' "$(printf 'manifest of %s' "$1" | sha256sum | cut -d ' ' -f 1)"; }
A="sha256:$(printf 'a%.0s' {1..64})" W="sha256:$(printf 'b%.0s' {1..64})" B="sha256:$(printf 'c%.0s' {1..64})"
publish() {
    : > "$scratch/calls"
    env -u GITHUB_ACTIONS HDS_HELM="$scratch/helm" HDS_SKOPEO="$scratch/skopeo" HDS_SIGN="$scratch/sign" \
        "$@" "$ROOT/infra/release/publish_charts.sh" aviyayemini 0.1.42 \
        0123456789abcdef0123456789abcdef01234567 "$A" "$W" "$B" < /dev/null
}
pushed() { grep -q "^helm push out/$1" "$scratch/calls"; }
expect "the three charts are pushed when the fixed versions are missing" 0 \
    "pushed hds-postgres 0.1.0" publish FAKE_SHOW=missing
holds "hds-secrets is pushed" pushed "hds-secrets-0.1.0.tgz oci://ghcr.io/aviyayemini/charts"
holds "hds-secrets goes before hds-postgres" after "^helm push out/hds-secrets" "^helm push out/hds-postgres"
holds "and hds-postgres before hds" after "^helm push out/hds-postgres" "^helm push out/hds-0.1.42"
holds "hds is pushed as 0.1.42" pushed "hds-0.1.42.tgz oci://ghcr.io/aviyayemini/charts"
HDS_DIGEST=$(digest_of "docker://ghcr.io/aviyayemini/charts/hds:0.1.42")
PG_DIGEST=$(digest_of "docker://ghcr.io/aviyayemini/charts/hds-postgres:0.1.0")
holds "hds is signed by the digest the registry holds" \
    grep -qx "sign ghcr.io/aviyayemini/charts/hds@$HDS_DIGEST" "$scratch/calls"
holds "and hds-postgres too" grep -qx "sign ghcr.io/aviyayemini/charts/hds-postgres@$PG_DIGEST" "$scratch/calls"
SECRETS_DIGEST=$(digest_of "docker://ghcr.io/aviyayemini/charts/hds-secrets:0.1.0")
holds "and hds-secrets" grep -qx "sign ghcr.io/aviyayemini/charts/hds-secrets@$SECRETS_DIGEST" "$scratch/calls"
holds "each chart is signed after its push" after "^helm push out/hds-0.1.42" "^sign .*charts/hds@"
holds "hds-postgres's too" after "^helm push out/hds-postgres" "^sign .*charts/hds-postgres@"
never "nothing is signed by tag" grep -q "^sign [^@]*$" "$scratch/calls"
expect "hds-postgres at a version already there is not pushed again" 0 "not pushed again" \
    publish FAKE_SHOW=exists
never "only hds was pushed" pushed hds-postgres
never "not hds-secrets either" pushed hds-secrets
holds "but hds-postgres is signed again, in case an earlier sign failed" \
    grep -qx "sign ghcr.io/aviyayemini/charts/hds-postgres@$PG_DIGEST" "$scratch/calls"
expect "any other registry answer fails and pushes no hds-postgres" 1 "nothing was pushed" \
    publish FAKE_SHOW=denied
never "no hds-postgres after a 401" pushed hds-postgres
never "no hds-secrets after it" pushed hds-secrets
never "and no hds after it either" pushed hds-0.1.42
never "and nothing signed" grep -q "^sign " "$scratch/calls"
expect "an image by tag in the packaged chart fails before any push" 1 "not an image of this release" \
    publish FAKE_IMAGE=ghcr.io/aviyayemini/hds-api:latest
never "nothing was pushed" grep -q "^helm push" "$scratch/calls"
expect "a digest that is not one is refused" 1 "is not an image digest" \
    env -u GITHUB_ACTIONS HDS_HELM="$scratch/helm" "$ROOT/infra/release/publish_charts.sh" aviyayemini 0.1.42 abc \
    sha256:1 "$W" "$B"

echo "== push_images.sh"
layouts="$scratch/layouts"
for name in api web backup; do
    mkdir -p "$layouts/hds-$name"
    # The stand-in registry answers "manifest of docker://<target>"; the layout names that digest.
    d=$(digest_of "docker://ghcr.io/proof/hds-$name:0123456789abcdef0123456789abcdef01234567")
    printf '{"manifests":[{"digest":"%s"}]}' "$d" > "$layouts/hds-$name/index.json"
done
push() {
    : > "$scratch/calls"
    env -u GITHUB_ACTIONS HDS_SKOPEO="$scratch/skopeo" HDS_SIGN="$scratch/sign" "$@" \
        "$ROOT/infra/release/push_images.sh" proof 0123456789abcdef0123456789abcdef01234567 "$layouts"
}
expect "the three images are pushed and their digests given" 0 "backup_digest=sha256:" push
for name in api web backup; do
    d=$(digest_of "docker://ghcr.io/proof/hds-$name:0123456789abcdef0123456789abcdef01234567")
    holds "hds-$name is signed by its digest" grep -qx "sign ghcr.io/proof/hds-$name@$d" "$scratch/calls"
    holds "after its push" after "^skopeo copy .*hds-$name:" "^sign ghcr.io/proof/hds-$name@"
done
never "no image is signed by tag" grep -q "^sign [^@]*$" "$scratch/calls"

echo "== sign.sh and the stand-ins in GitHub Actions"
expect "sign.sh refuses a tag" 1 "sign by digest only" \
    env -u GITHUB_ACTIONS HDS_COSIGN=true "$ROOT/infra/release/sign.sh" ghcr.io/aviyayemini/hds-api:0123
expect "sign.sh refuses a key in GitHub Actions" 1 "never in GitHub Actions" \
    env GITHUB_ACTIONS=true HDS_SIGN_KEY=/tmp/k "$ROOT/infra/release/sign.sh" "ghcr.io/x/y@$A"
expect "sign.sh refuses a stand-in cosign in GitHub Actions" 1 "never in GitHub Actions" \
    env GITHUB_ACTIONS=true HDS_COSIGN=true "$ROOT/infra/release/sign.sh" "ghcr.io/x/y@$A"
expect "push_images.sh refuses a stand-in signer in GitHub Actions" 1 "never in GitHub Actions" \
    push GITHUB_ACTIONS=true
expect "publish_charts.sh refuses stand-ins in GitHub Actions" 1 "never in GitHub Actions" \
    publish GITHUB_ACTIONS=true

echo "== .github/workflows/release.yml"
YQ_IMAGE="mikefarah/yq:4.54.1@sha256:4b3d9475d65571d28cbb19544d3820ec2945e4c8b2f18279394282b8dc3a592e"
wf() {
    docker run --rm -v "$ROOT/.github/workflows:/w:ro" "$YQ_IMAGE" -N "$1" /w/release.yml \
        || echo "YQ-FAILED"
}
same "it runs after CI completes on main, or by hand" "CI completed main workflow_dispatch" \
    "$(wf '[.on.workflow_run.workflows[], .on.workflow_run.types[], .on.workflow_run.branches[],
        (.on | keys | .[] | select(. == "workflow_dispatch"))] | join(" ")')"
same "one release at a time, never cancelled; a dry run apart" \
    "\${{ github.event_name == 'workflow_run' && 'release' || 'release-dry-run' }} false" \
    "$(wf '[.concurrency.group, .concurrency["cancel-in-progress"]] | join(" ")')"
same "permissions: packages write and contents read, nothing else" '{"contents":"read","packages":"write"}' \
    "$(wf '.permissions | to_json(0)')"
same "only the release job adds id-token: write, for keyless signing" \
    'release {"contents":"read","id-token":"write","packages":"write"}' \
    "$(wf '.jobs | to_entries[] | select(.value | has("permissions")) | .key + " " + (.value.permissions | to_json(0))')"
condition=$(wf '.jobs.release.if')
for part in "workflow_run.conclusion == 'success'" "workflow_run.event == 'push'" \
    "workflow_run.head_branch == 'main'" "head_repository.full_name == github.repository"; do
    holds "the job's condition holds $part" grep -qF -- "$part" <<< "$condition"
done
same "the trust check is the first step to run code" "trust/infra/release/trust.sh" \
    "$(wf '[.jobs.release.steps[] | select(has("run"))][0].run')"
# shellcheck disable=SC2016 # a GitHub expression, not the shell's
same "it builds the commit the trust check names" '${{ steps.trust.outputs.sha }}' \
    "$(wf '.jobs.release.steps[] | select(.with.path == "src") | .with.ref')"
same "only the trust check's publish=true pushes" \
    "steps.trust.outputs.publish == 'true'|steps.trust.outputs.publish == 'true'" \
    "$(wf '[.jobs.release.steps[] | select(.run // "" | test("push_images|publish_charts")) | .if] | join("|")')"
same "every action is pinned by commit" "" \
    "$(wf '[.jobs[].steps[] | select(has("uses")) | .uses | select(test("@[0-9a-f]{40}$") | not)] | join(" ")')"
same "no Trivy action" "" "$(wf '[.jobs[].steps[] | .uses // "" | select(test("trivy"))] | join(" ")')"
scans=$(wf '.jobs.release.steps[] | select(.run // "" | test("scan.sh")) | .run')
# shellcheck disable=SC2016 # the workflow's own shell text
for want in "for name in api web backup" "for platform in linux/amd64 linux/arm64" \
    'scan.sh vuln "../layouts/hds-$name" "$platform"' 'scan.sh secret "../layouts/hds-$name" "$platform"'; do
    holds "the scan step runs: $want" grep -qF -- "$want" <<< "$scans"
done

[ "$failures" -eq 0 ] || { echo "release checks: $failures failed" >&2; exit 1; }
echo "release checks: all passed"
