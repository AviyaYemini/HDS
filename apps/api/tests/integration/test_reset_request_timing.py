"""A reset request takes about the same time for every address (T360, SC-212). Opt-in.

The statistical companion of ``test_delivery_after_response.py``, skipped unless asked for:

    uv run pytest -m timing -s tests/integration/test_reset_request_timing.py

It sends 300 requests for a registered address and 300 for an unknown one, interleaved so drift
affects both equally, and compares the medians. Two things make the measurement honest:

- A pooled engine, as production has. Opening a connection per request, as the shared fixtures do,
  adds about 15 ms to both sides and hides a gap of a few milliseconds.
- The direct ASGI harness, timed to the end of the response. The httpx client waits for background
  work, so it would time the job this design moved out of the request.

The job still runs between samples, untimed. The limits stay in force but high, so the counts run as
they do in production.

Covers: FR-228, SC-212.
"""

import statistics
import time

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from hds.core.config import get_settings
from hds.core.database import get_db, get_session_factory
from hds.main import app
from hds.models.user import User
from tests.asgi_support import Log, post, response_end, status
from tests.conftest import OutboxMailSender

pytestmark = pytest.mark.timing

SAMPLES = 300


async def one_request(email: str) -> float:
    log: Log = []
    started = time.perf_counter()
    await post("/auth/password-reset/request", {"email": email}, log)
    assert status(log) == 202
    return float(log[response_end(log)]["at"]) - started


async def test_a_registered_and_an_unknown_address_take_about_the_same_time(
    migrated_test_db: str,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "RESET_REQUEST_LIMIT_PER_ADDRESS", SAMPLES * 10)
    monkeypatch.setattr(settings, "RESET_REQUEST_LIMIT_PER_NETWORK", SAMPLES * 10)
    engine = create_async_engine(migrated_test_db)  # the default pool, as in hds.core.database
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def pooled_db():  # type: ignore[no-untyped-def]
        async with factory() as db:
            yield db

    fixture_db, fixture_factory = (
        app.dependency_overrides[get_db],
        app.dependency_overrides[get_session_factory],
    )
    app.dependency_overrides[get_db] = pooled_db
    app.dependency_overrides[get_session_factory] = lambda: factory
    unknown: list[float] = []
    known: list[float] = []
    try:
        for _ in range(SAMPLES):
            unknown.append(await one_request("nobody@hds.test"))
            known.append(await one_request("employee@hds.test"))
    finally:
        app.dependency_overrides[get_db] = fixture_db
        app.dependency_overrides[get_session_factory] = fixture_factory
        await engine.dispose()

    median_unknown, median_known = statistics.median(unknown), statistics.median(known)
    ratio = max(median_unknown, median_known) / min(median_unknown, median_known)
    print(  # shown with -s: the evidence for a release
        f"\nmedian unknown {median_unknown * 1000:.2f} ms, known {median_known * 1000:.2f} ms, "
        f"ratio {ratio:.3f}"
    )
    assert len(outbox.messages) == SAMPLES, "the job must really run between samples"
    assert ratio < 1.15, f"medians differ by {ratio:.2f}x"
