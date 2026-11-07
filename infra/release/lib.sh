# shellcheck shell=sh
# Shared by the release's scripts (specs/006-deploy-operations T047, T047c). Sourced, not run.
#
# The checks and the local proof replace the tools these scripts run (HDS_HELM, HDS_SKOPEO,
# HDS_SIGN, HDS_COSIGN) or sign with a key (HDS_SIGN_KEY). None of that may happen in a release:
# with GITHUB_ACTIONS=true each script refuses them before it does anything (review R128).

fail() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }

no_overrides_in_ci() {
    [ "${GITHUB_ACTIONS:-}" = true ] || return 0
    for name in HDS_HELM HDS_SKOPEO HDS_SIGN HDS_COSIGN HDS_SIGN_KEY; do
        eval "value=\${$name:-}"
        # shellcheck disable=SC2154 # set by the eval
        [ -z "$value" ] || fail "$name is for the checks and the local proof only, never in GitHub Actions"
    done
}

# sign REF: the release's signature on REF (by digest), through sign.sh or the checks' stand-in.
sign() { "${HDS_SIGN:-$(dirname "$0")/sign.sh}" "$1"; }
