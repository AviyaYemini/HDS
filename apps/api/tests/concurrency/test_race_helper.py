"""Tests for the concurrency helpers themselves (T202).

Every other test in this folder relies on `race` really starting its actions at the same moment
and on `session_pair` giving two independent database connections; if either quietly degraded to
running one after the other, the single-use and last-Admin tests would pass without proving
anything.

Covers: FR-208, FR-211 (helpers only).
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

Race = Callable[..., Awaitable[list[Any]]]


async def test_race_starts_both_actions_before_either_finishes(race: Race) -> None:
    """Each action waits for the other to have started; a sequential runner would time out."""
    started = [asyncio.Event(), asyncio.Event()]

    def make(me: int, other: int, name: str) -> Callable[[], Awaitable[str]]:
        async def action() -> str:
            started[me].set()
            await asyncio.wait_for(started[other].wait(), timeout=2)
            return name

        return action

    results = await race(make(0, 1, "a"), make(1, 0, "b"))
    assert results == ["a", "b"]


async def test_race_returns_exceptions_instead_of_raising(race: Race) -> None:
    """A failing action is reported in its slot so a test can assert on who lost."""

    async def ok() -> str:
        return "ok"

    async def boom() -> str:
        raise ValueError("lost the race")

    results = await race(ok, boom)
    assert results[0] == "ok"
    assert isinstance(results[1], ValueError)


async def test_session_pair_uses_two_separate_connections(
    session_pair: tuple[AsyncSession, AsyncSession],
) -> None:
    """The two sessions are different PostgreSQL backends, so they can block each other."""
    first, second = session_pair
    pid_a = (await first.execute(text("SELECT pg_backend_pid()"))).scalar_one()
    pid_b = (await second.execute(text("SELECT pg_backend_pid()"))).scalar_one()
    assert pid_a != pid_b
