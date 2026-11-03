"""All work on a reset request's address runs after the response.

When the lookup, the link and the mail ran inside the request, a registered address took longer than
an unknown one, so timing showed who has an account. The request now only counts and answers. These
tests call the whole application directly (see ``tests/asgi_support.py``), so they can look at the
order while the job is still blocked.

Covers: FR-228, SC-212, FR-210.
"""

import asyncio
import contextlib
import logging
from collections.abc import Callable, Iterator
from datetime import datetime
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hds.core.background import RESET_JOB
from hds.core.config import get_settings
from hds.core.database import get_session_factory
from hds.core.mail import get_mail_sender
from hds.main import app
from hds.models.user import User
from hds.repositories.user import UserRepository
from tests.asgi_support import Log, post, response_end, status

EMAIL = "employee@hds.test"
UNKNOWN = "nobody@hds.test"
REQUEST = "/auth/password-reset/request"
TIMEOUT = 5


class GatedSender:
    """Logs that delivery started, then waits until the test lets it finish."""

    def __init__(self, log: Log) -> None:
        self.log = log
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.deliveries = 0

    async def send(
        self,
        to: str,
        subject: str,
        body: str,
        *,
        secret: str | None = None,
        purpose: str | None = None,
        expires_at: datetime | None = None,
    ) -> None:
        self.log.append("delivery_started")
        self.deliveries += 1
        self.started.set()
        await self.release.wait()


class FailingSender:
    """Fails with a message that holds the address and the token, as a provider's error might."""

    async def send(
        self,
        to: str,
        subject: str,
        body: str,
        *,
        secret: str | None = None,
        purpose: str | None = None,
        expires_at: datetime | None = None,
    ) -> None:
        raise RuntimeError(f"refused {to} {secret}")


@pytest.fixture
def log() -> Log:
    return []


@pytest.fixture
def gated(log: Log) -> Iterator[GatedSender]:
    sender = GatedSender(log)
    app.dependency_overrides[get_mail_sender] = lambda: sender
    yield sender
    app.dependency_overrides.pop(get_mail_sender, None)


@pytest.fixture
def lookups(log: Log, monkeypatch: pytest.MonkeyPatch) -> None:
    """Note in ``log`` each time anything looks an address up."""
    real = UserRepository.get_by_email

    async def spy(self: UserRepository, email: str) -> Any:
        log.append("lookup")
        return await real(self, email)

    monkeypatch.setattr(UserRepository, "get_by_email", spy)


async def links(db: AsyncSession) -> int:
    await db.rollback()
    return int((await db.execute(text("SELECT count(*) FROM invitation_tokens"))).scalar_one())


async def test_the_response_is_complete_before_delivery_starts(
    test_db: AsyncSession, seed_users: dict[str, User], gated: GatedSender, log: Log
) -> None:
    call = asyncio.create_task(post(REQUEST, {"email": EMAIL}, log))
    try:
        await asyncio.wait_for(gated.started.wait(), TIMEOUT)

        # Asserted while the sender is still blocked: the response did not wait for it.
        assert any(isinstance(m, dict) and m["type"] == "http.response.body" for m in log), log
        assert response_end(log) < log.index("delivery_started")
        assert status(log) == 202
    finally:
        gated.release.set()
        await asyncio.wait_for(call, TIMEOUT)

    assert gated.deliveries == 1


@pytest.mark.parametrize("email", [EMAIL, UNKNOWN], ids=["registered", "unknown"])
async def test_nothing_looks_at_the_address_before_the_response(
    test_db: AsyncSession,
    seed_users: dict[str, User],
    gated: GatedSender,
    log: Log,
    lookups: None,
    email: str,
) -> None:
    gated.release.set()

    await asyncio.wait_for(post(REQUEST, {"email": email}, log), TIMEOUT)

    assert status(log) == 202
    assert log.count("lookup") >= 1, "the job runs for every address, so the request is the same"
    assert response_end(log) < log.index("lookup")


async def test_the_job_uses_its_own_session_from_the_factory(
    test_db: AsyncSession, seed_users: dict[str, User], gated: GatedSender, log: Log
) -> None:
    real: Callable[[], async_sessionmaker[AsyncSession]] = app.dependency_overrides[
        get_session_factory
    ]

    def counted() -> Callable[[], AsyncSession]:
        def session() -> AsyncSession:
            log.append("factory")
            return real()()

        return session

    app.dependency_overrides[get_session_factory] = counted
    gated.release.set()
    try:
        await asyncio.wait_for(post(REQUEST, {"email": EMAIL}, log), TIMEOUT)
    finally:
        app.dependency_overrides[get_session_factory] = real

    assert gated.deliveries == 1
    assert await links(test_db) == 1
    assert log.count("factory") == 1


async def test_a_failed_commit_of_the_count_queues_no_job(
    test_db: AsyncSession,
    seed_users: dict[str, User],
    gated: GatedSender,
    log: Log,
    lookups: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_commit = AsyncSession.commit

    async def failing_commit(self: AsyncSession) -> None:
        await self.rollback()
        raise RuntimeError("commit failed")

    monkeypatch.setattr(AsyncSession, "commit", failing_commit)
    gated.release.set()

    with contextlib.suppress(RuntimeError):  # the server re-raises after answering 500
        await asyncio.wait_for(post(REQUEST, {"email": EMAIL}, log), TIMEOUT)

    monkeypatch.setattr(AsyncSession, "commit", real_commit)
    assert status(log) == 500
    assert "lookup" not in log
    assert gated.deliveries == 0


async def test_a_failed_commit_of_the_job_sends_nothing_and_the_answer_stands(
    test_db: AsyncSession,
    seed_users: dict[str, User],
    gated: GatedSender,
    log: Log,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    real_commit = AsyncSession.commit
    commits = {"seen": 0}

    async def failing_commit(self: AsyncSession) -> None:
        commits["seen"] += 1
        if commits["seen"] == 2:  # the first saves the count, the second the job's link
            await self.rollback()
            raise RuntimeError(f"commit failed for {EMAIL}")
        await real_commit(self)

    monkeypatch.setattr(AsyncSession, "commit", failing_commit)
    gated.release.set()

    with caplog.at_level(logging.ERROR, logger="hds.database"):
        await asyncio.wait_for(post(REQUEST, {"email": EMAIL}, log), TIMEOUT)

    monkeypatch.setattr(AsyncSession, "commit", real_commit)
    assert status(log) == 202
    assert gated.deliveries == 0
    assert await links(test_db) == 0
    text_logged = " ".join(r.getMessage() for r in caplog.records)
    assert "RuntimeError" in text_logged
    assert "reset_job" in text_logged  # an operator can tell what failed
    assert EMAIL not in text_logged


async def test_a_failed_delivery_is_logged_by_type_only_and_keeps_the_link(
    test_db: AsyncSession,
    seed_users: dict[str, User],
    log: Log,
    caplog: pytest.LogCaptureFixture,
) -> None:
    app.dependency_overrides[get_mail_sender] = FailingSender
    try:
        with caplog.at_level(logging.ERROR, logger="hds.database"):
            await asyncio.wait_for(post(REQUEST, {"email": EMAIL}, log), TIMEOUT)
    finally:
        app.dependency_overrides.pop(get_mail_sender, None)

    assert status(log) == 202
    assert await links(test_db) == 1
    records = [r for r in caplog.records if r.name == "hds.database"]
    text_logged = " ".join(r.getMessage() for r in records)
    assert "RuntimeError" in text_logged
    assert EMAIL not in text_logged
    assert "refused" not in text_logged
    assert all(r.exc_info is None for r in records)


async def test_past_the_cap_a_job_is_dropped_and_the_answer_stays_202(
    test_db: AsyncSession,
    seed_users: dict[str, User],
    gated: GatedSender,
    log: Log,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(get_settings(), "RESET_JOB_CONCURRENCY", 1)
    monkeypatch.setattr(get_settings(), "RESET_JOB_QUEUE_LIMIT", 0)
    first = asyncio.create_task(post(REQUEST, {"email": EMAIL}, log))
    try:
        await asyncio.wait_for(gated.started.wait(), TIMEOUT)
        second: Log = []
        with caplog.at_level(logging.WARNING, logger="hds.background"):
            await asyncio.wait_for(post(REQUEST, {"email": UNKNOWN}, second), TIMEOUT)
    finally:
        gated.release.set()
        await asyncio.wait_for(first, TIMEOUT)

    assert status(second) == 202
    assert gated.deliveries == 1
    records = [r for r in caplog.records if r.name == "hds.background"]
    # Exact, because the log filter would mask an address and hide one that reached this message.
    assert [r.getMessage() for r in records] == [f"background job dropped: {RESET_JOB}"]
