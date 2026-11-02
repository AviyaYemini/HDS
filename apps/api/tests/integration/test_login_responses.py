"""A refused sign-in looks the same whichever of the reasons caused it (T278, FR-214).

Through the real application, the whole response is compared, not just the status: status line,
body (apart from the per-request id) and headers (apart from the per-request id and date). An
attacker comparing any of them must learn nothing about whether the address is registered. The
locked-account answer is the one accepted difference (feature 001, FR-005) and is not part of this
comparison. The amount of Argon2 work per request is counted as well.

Covers: FR-214, SC-207.
"""

from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core import security
from hds.models.user import User

VOLATILE_HEADERS = {"date", "x-request-id"}


def shape(response: Response) -> dict[str, Any]:
    body = response.json()
    body.pop("request_id", None)
    headers = {
        k.lower(): v for k, v in response.headers.items() if k.lower() not in VOLATILE_HEADERS
    }
    return {
        "status": response.status_code,
        "reason": response.reason_phrase,
        "body": body,
        "headers": headers,
    }


@pytest.fixture
def verifications(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []

    class Counting:
        """The real hasher with every ``verify`` recorded (the real class is read-only)."""

        def __init__(self, real: Any) -> None:
            self._real = real

        def verify(self, hashed: str, plain: str) -> bool:
            calls.append(hashed)
            return bool(self._real.verify(hashed, plain))

        def __getattr__(self, name: str) -> Any:
            return getattr(self._real, name)

    monkeypatch.setattr(security, "_ph", Counting(security._ph))
    return calls


async def test_unknown_known_and_inactive_refusals_are_identical_and_cost_the_same_work(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    verifications: list[str],
) -> None:
    await test_db.execute(
        text("UPDATE users SET is_active = false WHERE email = 'client@hds.test'")
    )
    await test_db.commit()
    attempts = {
        "unknown address": ("nobody@hds.test", "Dev1234!"),
        "known, wrong password": ("employee@hds.test", "not-the-password"),
        "inactive, right password": ("client@hds.test", "Dev1234!"),
        "inactive, wrong password": ("client@hds.test", "not-the-password"),
    }
    seen: dict[str, dict[str, Any]] = {}
    work: dict[str, int] = {}

    for name, (email, password) in attempts.items():
        before = len(verifications)
        response = await async_client.post(
            "/auth/login", json={"email": email, "password": password}
        )
        work[name] = len(verifications) - before
        seen[name] = shape(response)

    assert set(work.values()) == {1}, f"password checks per request: {work}"
    reference = seen["unknown address"]
    assert reference["status"] == 401 and reference["body"]["code"] == "auth.invalid_credentials"
    for name, observed in seen.items():
        assert observed == reference, f"{name} differs from an unknown address"
