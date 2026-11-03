"""Password hashing runs off the event loop, bounded, with a bounded queue (T342, FR-222).

Argon2 is slow on purpose. Run inside an async handler it stops every other request in the worker,
so it runs in a thread. At most ``HASH_CONCURRENCY`` run at once, at most ``HASH_QUEUE_LIMIT`` wait,
and anything beyond that is refused at once. The limiter belongs to one event loop.

Covers: FR-222.
"""

import asyncio
import threading

import pytest

from hds.core import passwords, security
from hds.core.config import get_settings
from hds.core.exceptions import ServiceBusyError


@pytest.fixture
def limits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(get_settings(), "HASH_CONCURRENCY", 2)
    monkeypatch.setattr(get_settings(), "HASH_QUEUE_LIMIT", 3)


async def test_the_results_are_those_of_the_synchronous_core() -> None:
    hashed = await passwords.hash_password("A-long-password-1")

    assert await passwords.verify_password("A-long-password-1", hashed) is True
    assert await passwords.verify_password("wrong", hashed) is False
    assert await passwords.verify_against_dummy("anything") is False
    assert security.verify_password("A-long-password-1", hashed) is True


async def test_hashing_does_not_run_on_the_event_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[threading.Thread] = []

    def record(plain: str) -> str:
        seen.append(threading.current_thread())
        return "hash"

    monkeypatch.setattr(security, "hash_password", record)

    await passwords.hash_password("x")

    assert seen and seen[0] is not threading.main_thread()


async def test_no_more_than_the_limit_run_at_once_and_the_queue_is_bounded(
    limits: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    release = threading.Event()
    lock = threading.Lock()
    running = {"now": 0, "most": 0}

    def slow(plain: str) -> str:
        with lock:
            running["now"] += 1
            running["most"] = max(running["most"], running["now"])
        release.wait(5)
        with lock:
            running["now"] -= 1
        return "hash"

    monkeypatch.setattr(security, "hash_password", slow)
    accepted = [asyncio.create_task(passwords.hash_password("x")) for _ in range(5)]  # 2 + 3
    await asyncio.sleep(0.05)

    with pytest.raises(ServiceBusyError) as refused:
        await passwords.hash_password("one too many")  # refused at once, without waiting

    assert refused.value.code == "service.busy"
    assert running["most"] == 2
    release.set()
    assert await asyncio.gather(*accepted) == ["hash"] * 5
    assert running["most"] == 2


async def test_a_failure_gives_its_place_back(
    limits: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fails(plain: str) -> str:
        raise RuntimeError("boom")

    monkeypatch.setattr(security, "hash_password", fails)
    for _ in range(10):  # more than running plus queue
        with pytest.raises(RuntimeError):
            await passwords.hash_password("x")


def test_the_limiter_belongs_to_one_event_loop(limits: None) -> None:
    """A semaphore made once and used from a second loop raises; each loop has its own."""

    async def use() -> None:
        results = await asyncio.gather(*(passwords.hash_password("x") for _ in range(5)))
        assert len(results) == 5

    asyncio.run(use())
    asyncio.run(use())
