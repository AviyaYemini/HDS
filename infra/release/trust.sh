#!/bin/sh
# The release workflow's first step (specs/006-deploy-operations T047, review R99): decide whether
# this run may publish, from the event GitHub hands it, and fail otherwise.
#
# A workflow_run publishes only for a CI run that succeeded on a push to main in this repository,
# for the commit main is at now.
# `branches: [main]` and head_branch match only the branch's name, which a pull request from a fork
# can share, and such a run would then publish with packages: write; so the event and the head
# repository are checked too. A workflow_dispatch is a dry run (build and scan only), and
# dry_run=false is refused at once, so nobody thinks they published.
#
# Environment (GitHub's): GITHUB_EVENT_NAME, GITHUB_EVENT_PATH, GITHUB_REPOSITORY, GITHUB_SHA,
# GITHUB_OUTPUT, GITHUB_SERVER_URL; DRY_RUN, the dispatch input; HDS_MAIN_TIP, main's newest commit
# (read with git ls-remote unless set, as the checks set it). It writes publish=true|false and sha=<commit> to
# GITHUB_OUTPUT.
set -eu

fail() { printf 'release refused: %s\n' "$*" >&2; exit 1; }
field() { jq -r "$1 // \"\"" "$GITHUB_EVENT_PATH"; }

case "${GITHUB_EVENT_NAME:-}" in
    workflow_run)
        conclusion=$(field .workflow_run.conclusion)
        event=$(field .workflow_run.event)
        branch=$(field .workflow_run.head_branch)
        repo=$(field .workflow_run.head_repository.full_name)
        sha=$(field .workflow_run.head_sha)
        [ "$conclusion" = success ] || fail "CI concluded '$conclusion', not success"
        [ "$event" = push ] || fail "CI ran for a '$event' event, not a push"
        [ "$branch" = main ] || fail "CI ran on '$branch', not main"
        [ "$repo" = "$GITHUB_REPOSITORY" ] \
            || fail "CI ran on a commit of '$repo', not of $GITHUB_REPOSITORY"
        printf '%s' "$sha" | grep -Eqx '[0-9a-f]{40}' || fail "the head commit '$sha' is not a SHA"
        # Only main's newest commit: CI re-run for an older commit, or finishing after a newer
        # one's, would otherwise publish older code as a higher version (review R114). The run is
        # then red, and harmless. ls-remote is unauthenticated, which works because the repository
        # is public; a private one needs the token (git -c http.extraHeader="Authorization: basic
        # <base64 of x-access-token:$GITHUB_TOKEN>"; review R126).
        tip=${HDS_MAIN_TIP-$(git ls-remote "${GITHUB_SERVER_URL:-https://github.com}/$GITHUB_REPOSITORY" \
            refs/heads/main | cut -f 1)}
        [ -n "$tip" ] || fail "main's newest commit could not be read"
        [ "$sha" = "$tip" ] \
            || fail "CI ran on $sha, but main is now at $tip; that commit's own release follows"
        publish=true
        ;;
    workflow_dispatch)
        [ "${DRY_RUN:-}" = true ] \
            || fail "a dispatched run is a dry run only: it builds and scans and publishes nothing." \
                "Releases publish after CI succeeds on main"
        sha=$GITHUB_SHA
        publish=false
        ;;
    *) fail "unexpected event '${GITHUB_EVENT_NAME:-}'" ;;
esac
printf 'publish=%s\nsha=%s\n' "$publish" "$sha" >> "$GITHUB_OUTPUT"
printf 'release: publish=%s, commit %s\n' "$publish" "$sha"
