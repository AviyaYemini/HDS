#!/bin/sh
# Refuse, naming the port, when a host port the local stack needs is taken by something else
# (specs/006-deploy-operations T010, the spec's "port in use" edge case). Run by `make up` before it
# starts anything, so a taken port never leaves the stack half started.
#
# Usage: scripts/ops/ports_free.sh SERVICE=PORT ...
# A port whose SERVICE is already running in this compose project is skipped: it is the stack's own,
# so `make up` on a running stack still works. RUNNING lists those services, one per line (the
# Makefile passes `docker compose ps --status running --services`).
set -eu

# Without nc every port would look free and the stack could start half-way; refuse instead.
if ! command -v nc >/dev/null 2>&1; then
    echo "make up needs nc (netcat) to check its ports; install it (macOS and most Linux have it)." >&2
    exit 1
fi

running="${RUNNING:-}"
taken=""
for pair in "$@"; do
    service="${pair%%=*}"
    port="${pair#*=}"
    if printf '%s\n' "$running" | grep -qx "$service"; then
        continue
    fi
    # A port is given up to 5 s to free itself: Docker Desktop releases a stopped stack's ports
    # a moment after its containers are gone, so `make down && make up` must not trip on them.
    tries=0
    while nc -z 127.0.0.1 "$port" >/dev/null 2>&1; do
        tries=$((tries + 1))
        if [ "$tries" -ge 5 ]; then
            taken="$taken $port ($service)"
            break
        fi
        sleep 1
    done
done

if [ -n "$taken" ]; then
    echo "Port in use:$taken. Free it, or set the port variable for it (see infra/README.md)." >&2
    exit 1
fi
