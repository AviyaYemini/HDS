"""Fixtures shared by the unit tests that need a database session without a database.

Covers: none directly; fixtures only.
"""

from typing import Any

import pytest


class FakeSession:
    """A stand-in for ``AsyncSession`` that records commits and rollbacks in order.

    Attributes:
        info: The per-session dictionary the unit of work stores its queue in.
        events: ``"commit"`` and ``"rollback"`` in the order they were awaited.
        fail_commit: When true, ``commit`` raises instead of recording.
    """

    def __init__(self) -> None:
        self.info: dict[Any, Any] = {}
        self.events: list[str] = []
        self.fail_commit = False

    async def commit(self) -> None:
        if self.fail_commit:
            raise RuntimeError("commit failed")
        self.events.append("commit")

    async def rollback(self) -> None:
        self.events.append("rollback")


@pytest.fixture
def fake_session() -> FakeSession:
    """Return a fresh `FakeSession`."""
    return FakeSession()
