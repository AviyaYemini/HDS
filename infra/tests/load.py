"""Steady load through the ingress during a rolling update (specs/006-deploy-operations T060, T059).

Signs in once, then sends 20 requests a second until it is stopped (SIGTERM or SIGINT): reads
(``GET /api/v1/sites``) and writes (``POST /api/v1/sites``), four reads to a write, with the access
token in hand, and a refresh every 30 seconds, which rotates the refresh token, as a client would.
Any status other than the expected one, any connection error or timeout, and any refresh that fails
is counted. On stop it prints the totals and each failure, and exits 1 if there was any.

Run from ``apps/api`` (httpx is a dependency already there)::

    uv run --extra dev python ../../infra/tests/load.py --email ADMIN --password-file FILE

The password is read from a file (or standard input), never from an argument. The ingress is
reached at ``--url`` (``http://127.0.0.1:8080``) with ``Host: hds.localhost``.

This is a test tool, outside the application's layers; it imports nothing from ``hds``.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field

import httpx

RATE = 20.0  # requests a second
WRITE_EVERY = 5  # one write in five
REFRESH_EVERY = 30.0  # seconds
TIMEOUT = 5.0  # seconds per request


@dataclass
class Tally:
    """What the load saw: counts per kind, and every failure in order."""

    reads: int = 0
    writes: int = 0
    refreshes: int = 0
    failures: list[str] = field(default_factory=list)

    def fail(self, what: str) -> None:
        """Record one failure, with the time it happened."""
        self.failures.append(f"{time.strftime('%H:%M:%S')} {what}")


@dataclass
class Session:
    """The signed-in client's tokens; the refresh replaces both."""

    access: str
    refresh: str


async def sign_in(client: httpx.AsyncClient, email: str, password: str) -> Session:
    """Sign in once; any failure ends the run before the load starts.

    Raises:
        SystemExit: when the sign-in is refused or cannot be reached.
    """
    response = await client.post("/auth/login", json={"email": email, "password": password})
    if response.status_code != 200:
        raise SystemExit(f"sign-in failed: HTTP {response.status_code}")
    body = response.json()
    return Session(access=body["access_token"], refresh=body["refresh_token"])


async def one_request(client: httpx.AsyncClient, session: Session, n: int, tally: Tally) -> None:
    """Send the n-th request, a read or a write, and count its outcome."""
    headers = {"Authorization": f"Bearer {session.access}"}
    write = n % WRITE_EVERY == 0
    try:
        if write:
            response = await client.post(
                "/api/v1/sites",
                headers=headers,
                json={
                    "name": f"Load {uuid.uuid4().hex[:12]}",
                    "address": "1 Load Street",
                    "timezone": "Asia/Jerusalem",
                },
            )
            expected = 201
        else:
            response = await client.get("/api/v1/sites", headers=headers)
            expected = 200
    except httpx.HTTPError as error:
        tally.fail(f"{'POST' if write else 'GET'} /api/v1/sites: {type(error).__name__}")
        return
    if response.status_code != expected:
        method = "POST" if write else "GET"
        tally.fail(f"{method} /api/v1/sites: HTTP {response.status_code}, not {expected}")
        return
    if write:
        tally.writes += 1
    else:
        tally.reads += 1


async def refresh(client: httpx.AsyncClient, session: Session, tally: Tally) -> None:
    """Rotate the tokens, as a signed-in client does; a failure means the session is lost."""
    try:
        response = await client.post("/auth/refresh", json={"refresh_token": session.refresh})
    except httpx.HTTPError as error:
        tally.fail(f"POST /auth/refresh: {type(error).__name__}")
        return
    if response.status_code != 200:
        status = response.status_code
        tally.fail(f"POST /auth/refresh: HTTP {status}: the session no longer refreshes")
        return
    body = response.json()
    session.access, session.refresh = body["access_token"], body["refresh_token"]
    tally.refreshes += 1


async def run(args: argparse.Namespace, password: str) -> Tally:
    """Load the API until stopped, and return what it saw."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    tally = Tally()
    async with httpx.AsyncClient(
        base_url=args.url, headers={"Host": args.host}, timeout=TIMEOUT
    ) as client:
        session = await sign_in(client, args.email, password)
        print(f"signed in; {RATE:.0f} requests a second until stopped", flush=True)
        pending: set[asyncio.Task[None]] = set()
        start = time.monotonic()
        next_refresh = start + REFRESH_EVERY
        n = 0
        while not stop.is_set():
            now = time.monotonic()
            if now >= next_refresh:
                # The refresh is awaited, so no request carries a token that was just replaced.
                await refresh(client, session, tally)
                next_refresh = now + REFRESH_EVERY
            n += 1
            task = asyncio.create_task(one_request(client, session, n, tally))
            pending.add(task)
            task.add_done_callback(pending.discard)
            delay = start + n / RATE - time.monotonic()
            if delay > 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except TimeoutError:
                    pass
        if pending:
            await asyncio.gather(*pending)
    return tally


def main() -> int:
    """Parse the arguments, run the load, print the totals and the failures."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--host", default="hds.localhost")
    parser.add_argument("--email", required=True)
    parser.add_argument(
        "--password-file", default="-", help="a file holding the password, or - for standard input"
    )
    args = parser.parse_args()
    if args.password_file == "-":
        password = sys.stdin.readline().rstrip("\n")
    else:
        with open(args.password_file, encoding="utf-8") as handle:
            password = handle.read().rstrip("\n")
    tally = asyncio.run(run(args, password))
    print(
        f"load: {tally.reads} reads, {tally.writes} writes, {tally.refreshes} refreshes, "
        f"{len(tally.failures)} failures",
        flush=True,
    )
    for failure in tally.failures:
        print(f"  FAIL  {failure}")
    return 1 if tally.failures else 0


if __name__ == "__main__":
    sys.exit(main())
