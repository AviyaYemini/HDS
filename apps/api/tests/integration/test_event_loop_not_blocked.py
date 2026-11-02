"""Other requests keep moving during a burst of sign-ins (T343, F04).

Before, each Argon2 check ran on the event loop: 20 concurrent sign-ins for unknown addresses
stalled it for about 700 ms, so anonymous callers could slow every other request. A heartbeat task
measures the loop directly: each stall as long as most of one real password check counts, and a
burst may cause almost none. The check's own measured cost sets the bar, so the test holds on a slow
CI machine. The bar replaced a median of timed requests, which a mutation showed could not see the
stalls.

Covers: FR-222, SC-207.
"""

import asyncio
import statistics
import time

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core import security
from hds.core.config import get_settings
from hds.models.user import User

BURST = 20
SAMPLES = 9
ALLOWED_STALLS = 2  # scheduler noise; a check on the loop would stall it about BURST times


async def heartbeat(gaps: list[float], stop: asyncio.Event) -> None:
    """Record how long the loop took to come back after each very short sleep."""
    while not stop.is_set():
        started = time.perf_counter()
        await asyncio.sleep(0.001)
        gaps.append(time.perf_counter() - started)


async def test_the_loop_does_not_stall_during_a_burst_of_unknown_addresses(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(get_settings(), "HASH_QUEUE_LIMIT", 100)
    real = security.verify_password
    in_flight = {"now": 0, "most": 0}

    def counting(plain: str, hashed: str) -> bool:
        in_flight["now"] += 1
        in_flight["most"] = max(in_flight["most"], in_flight["now"])
        try:
            return real(plain, hashed)
        finally:
            in_flight["now"] -= 1

    durations = []
    for _ in range(SAMPLES):
        started = time.perf_counter()
        security.verify_against_dummy("not-it")
        durations.append(time.perf_counter() - started)
    one_check = statistics.median(durations)
    monkeypatch.setattr(security, "verify_password", counting)
    gaps: list[float] = []
    stop = asyncio.Event()
    beat = asyncio.create_task(heartbeat(gaps, stop))

    answers = await asyncio.gather(
        *(
            async_client.post(
                "/auth/login", json={"email": f"nobody{n}@hds.test", "password": "not-it"}
            )
            for n in range(BURST)
        )
    )
    stop.set()
    await beat

    assert {a.status_code for a in answers} == {401}
    stalls = [g for g in gaps if g >= 0.8 * one_check]
    assert len(stalls) <= ALLOWED_STALLS, (one_check, sorted(stalls))
    assert in_flight["most"] <= get_settings().HASH_CONCURRENCY
