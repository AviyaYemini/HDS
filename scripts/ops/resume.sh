#!/bin/sh
# make ops-resume (specs/006-deploy-operations T049, contracts/ops.md section 5): hand the hds
# HelmRelease back to Flux, which then installs the newest published version and waits for it.
#
# Environment: HDS_NAMESPACE (hds), HDS_FLUX_NAMESPACE (flux-system).
set -eu
ns=${HDS_NAMESPACE:-hds}
flux_ns=${HDS_FLUX_NAMESPACE:-flux-system}

say() { printf '%s\n' "$*"; }
fail() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }

flux -n "$flux_ns" resume helmrelease hds \
    || fail "Flux could not resume the hds HelmRelease, or its release failed. See: flux -n $flux_ns get helmrelease hds"
meta=$(helm -n "$ns" get metadata hds) || fail "Flux resumed hds, but its revision could not be read"
version=$(printf '%s\n' "$meta" | sed -n 's/^APP_VERSION: //p')
revision=$(printf '%s\n' "$meta" | sed -n 's/^REVISION: //p')
say "Flux manages hds again; hds now runs $version (Helm revision $revision)"
