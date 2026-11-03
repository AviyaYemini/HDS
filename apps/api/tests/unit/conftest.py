"""Fixtures shared by the unit tests that need a database session without a database.

Covers: none directly; fixtures only.
"""

from typing import Any

import pytest


@pytest.fixture
def real_mail_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pretend a real mail backend is available so a production configuration can start.

    No real backend exists yet (specs/002 research D6), and production refuses to start without
    one. Tests of the other start-up rules use this fixture to get past that check. It sets the
    environment variable and the registry of real backends; both are harmless before the config
    knows about them, so tests using it pass on either side of that change.
    """
    import hds.core.config as config

    monkeypatch.setenv("MAIL_BACKEND", "test-smtp")
    monkeypatch.setattr(config, "REAL_MAIL_BACKENDS", frozenset({"test-smtp"}), raising=False)


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
