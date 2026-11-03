"""A rollback that fails does not hide the request's real error (T377, F12).

When the connection has gone, the rollback fails too. Letting that second error replace the first
turns a useful error into a confusing one, and a domain refusal into a server error.

Covers: FR-233.
"""

import asyncio
import logging
from typing import Any

import pytest

from hds.core.database import after_commit, unit_of_work
from tests.unit.conftest import FakeSession


class BrokenSession(FakeSession):
    """A `FakeSession` whose rollback fails, with the error it should fail with."""

    def __init__(self, error: BaseException | None = None) -> None:
        super().__init__()
        self.error = error or OSError("rollback failed: s3cret-value")
        self.rollbacks = 0

    async def rollback(self) -> None:
        self.rollbacks += 1
        raise self.error


class RefusedError(Exception):
    """Stands in for the error the request really failed with."""


async def test_the_original_error_propagates_when_the_rollback_fails_too() -> None:
    session: Any = BrokenSession()

    with pytest.raises(RefusedError) as raised:
        async with unit_of_work(session):
            raise RefusedError("the real reason")

    assert str(raised.value) == "the real reason"
    assert session.rollbacks == 1


async def test_the_failed_rollback_is_logged_by_type_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    session: Any = BrokenSession()

    with caplog.at_level(logging.ERROR, logger="hds.database"), pytest.raises(RefusedError):
        async with unit_of_work(session):
            raise RefusedError

    text = " ".join(r.getMessage() for r in caplog.records)
    assert "OSError" in text
    assert "s3cret-value" not in text
    assert all(r.exc_info is None for r in caplog.records)


async def test_a_failed_commit_propagates_when_the_rollback_fails_too(
    caplog: pytest.LogCaptureFixture,
) -> None:
    session: Any = BrokenSession()
    session.fail_commit = True

    with (
        caplog.at_level(logging.ERROR, logger="hds.database"),
        pytest.raises(RuntimeError, match="commit failed"),
    ):
        async with unit_of_work(session):
            pass

    assert session.rollbacks == 1
    assert "OSError" in " ".join(r.getMessage() for r in caplog.records)


async def test_actions_are_dropped_when_the_rollback_fails_too() -> None:
    session: Any = BrokenSession()

    async def action() -> None:
        raise AssertionError("must not run")

    with pytest.raises(RefusedError):
        async with unit_of_work(session):
            after_commit(session, action)
            raise RefusedError

    assert not session.info


async def test_cancellation_stays_a_cancellation_when_the_rollback_fails() -> None:
    session: Any = BrokenSession()

    with pytest.raises(asyncio.CancelledError):
        async with unit_of_work(session):
            raise asyncio.CancelledError

    assert session.rollbacks == 1


async def test_a_cancellation_during_the_rollback_is_never_swallowed() -> None:
    session: Any = BrokenSession(asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        async with unit_of_work(session):
            raise RefusedError
