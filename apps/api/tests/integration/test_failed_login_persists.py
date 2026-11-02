"""A refused sign-in must still be remembered (T208, research D1).

Sign-in raises an error after counting the failed attempt. With one transaction per request, an
error rolls everything back, so the count would vanish and the lockout would never trigger. The
count is therefore committed early on purpose; this test fails if that is ever lost.

Covers: FR-006b (lockout counter, feature 001), FR-210.
"""

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User


async def failed_attempts(db: AsyncSession, email: str) -> int:
    row = await db.execute(
        text("SELECT failed_login_attempts FROM users WHERE email = :e"), {"e": email}
    )
    return int(row.scalar_one())


async def test_failed_sign_in_count_survives_the_refused_request(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    email = seed_users["employee"].email
    for expected in (1, 2):
        response = await async_client.post(
            "/auth/login", json={"email": email, "password": "wrong-password-1"}
        )
        assert response.status_code == 401
        assert await failed_attempts(test_db, email) == expected
