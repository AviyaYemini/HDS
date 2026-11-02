"""Unit tests for the request-scoped unit of work (T204, research D1).

One transaction per request: commit once on success, roll back on any error, and run deferred
actions (sending email) only after a successful commit so no message goes out for a change that
was rolled back.

Covers: FR-210.
"""

import asyncio
import logging

import pytest

from hds.core.database import after_commit, get_unit_of_work, unit_of_work


class BoomError(Exception):
    """A domain-style error raised inside the unit of work."""


async def test_commits_exactly_once_after_success(fake_session) -> None:
    async with unit_of_work(fake_session) as session:
        assert session is fake_session
    assert fake_session.events == ["commit"]


async def test_rolls_back_and_reraises_on_any_exception(fake_session) -> None:
    with pytest.raises(BoomError):
        async with unit_of_work(fake_session):
            raise BoomError
    assert fake_session.events == ["rollback"]


async def test_rolls_back_when_the_task_is_cancelled(fake_session) -> None:
    with pytest.raises(asyncio.CancelledError):
        async with unit_of_work(fake_session):
            raise asyncio.CancelledError
    assert fake_session.events == ["rollback"]


async def test_after_commit_actions_run_after_the_commit_in_order(fake_session) -> None:
    calls: list[str] = []

    async def first() -> None:
        calls.append(f"first after {fake_session.events}")

    async def second() -> None:
        calls.append("second")

    async with unit_of_work(fake_session):
        after_commit(fake_session, first)
        after_commit(fake_session, second)
        assert calls == []  # nothing runs before the commit
    assert calls == ["first after ['commit']", "second"]


async def test_after_commit_actions_are_discarded_after_a_rollback(fake_session) -> None:
    ran: list[str] = []

    async def action() -> None:
        ran.append("ran")

    with pytest.raises(BoomError):
        async with unit_of_work(fake_session):
            after_commit(fake_session, action)
            raise BoomError
    assert ran == []
    assert not fake_session.info  # the queue does not leak into a later unit of work


async def test_failed_commit_rolls_back_discards_actions_and_raises(fake_session) -> None:
    ran: list[str] = []

    async def action() -> None:
        ran.append("ran")

    fake_session.fail_commit = True
    with pytest.raises(RuntimeError, match="commit failed"):
        async with unit_of_work(fake_session):
            after_commit(fake_session, action)
    assert fake_session.events == ["rollback"]
    assert ran == []


async def test_failing_after_commit_action_is_logged_without_secrets_and_ignored(
    fake_session, caplog
) -> None:
    ran: list[str] = []

    async def failing() -> None:
        raise RuntimeError("smtp password=s3cret-value")

    async def later() -> None:
        ran.append("later")

    caplog.set_level(logging.ERROR)
    async with unit_of_work(fake_session):
        after_commit(fake_session, failing)
        after_commit(fake_session, later)

    assert fake_session.events == ["commit"]  # the saved change stays saved
    assert ran == ["later"]  # one failure does not stop the others
    text = " ".join(r.getMessage() for r in caplog.records)
    assert "RuntimeError" in text
    assert "s3cret-value" not in text
    assert all(r.exc_info is None for r in caplog.records)  # a traceback would carry the message


async def test_dependency_commits_when_the_handler_returns(fake_session) -> None:
    gen = get_unit_of_work(fake_session)
    assert await anext(gen) is fake_session
    with pytest.raises(StopAsyncIteration):
        await anext(gen)
    assert fake_session.events == ["commit"]


async def test_dependency_rolls_back_when_the_handler_raises(fake_session) -> None:
    gen = get_unit_of_work(fake_session)
    await anext(gen)
    with pytest.raises(BoomError):
        await gen.athrow(BoomError())
    assert fake_session.events == ["rollback"]
