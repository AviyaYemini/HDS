"""Sign-in does the same work for an unknown address as for a known one (T278, SC-207, FR-214).

Argon2id makes the password check the dominant cost of a sign-in. If an unknown address skips it,
the answer is much faster and reveals that the address is not registered. This test counts the
work instead of timing it: every Argon2 verification is counted, and a sign-in must cause exactly
one whatever the address, the password or the state of the account, and must return the same
error code and message. (The timing itself is checked on demand by ``test_login_timing.py``.)

Covers: FR-214, SC-207.
"""

import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core import security
from hds.core.exceptions import AuthError
from hds.core.security import hash_password
from hds.models.user import User, UserRole
from hds.services.auth import AuthService

REAL_PASSWORD = "Correct-horse-1"
HASH = hash_password(REAL_PASSWORD)


@pytest.fixture
def verifications(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every call to the Argon2 verifier, in order."""
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


def account(*, is_active: bool = True) -> User:
    user = MagicMock(spec=User)
    user.id = uuid.uuid4()
    user.organization_id = uuid.uuid4()
    user.password_hash = HASH
    user.role = UserRole.employee
    user.is_active = is_active
    user.failed_login_attempts = 0
    user.locked_until = None
    return user


def service(user: User | None) -> AuthService:
    users = AsyncMock()
    users.get_by_email.return_value = user
    users.clear_expired_lock.return_value = False
    users.register_failed_login.return_value = (1, None)
    config = MagicMock()
    config.LOGIN_MAX_FAILURES = 5
    config.LOGIN_LOCKOUT_MINUTES = 15
    return AuthService(users, AsyncMock(), config, AsyncMock())


CASES = {
    "unknown address": (None, "whatever"),
    "known, wrong password": (account(), "wrong-password"),
    "inactive, wrong password": (account(is_active=False), "wrong-password"),
    "inactive, right password": (account(is_active=False), REAL_PASSWORD),
}


@pytest.mark.parametrize("name", CASES)
async def test_every_refusal_costs_exactly_one_password_check(
    name: str, verifications: list[str]
) -> None:
    user, password = CASES[name]

    with pytest.raises(AuthError):
        await service(user).login("someone@hds.test", password)

    assert len(verifications) == 1, f"{name}: {len(verifications)} password checks"


async def test_all_refusals_carry_the_same_code_and_message() -> None:
    seen = set()
    for user, password in CASES.values():
        with pytest.raises(AuthError) as refused:
            await service(user).login("someone@hds.test", password)
        seen.add((refused.value.code, refused.value.detail))

    assert seen == {("auth.invalid_credentials", "Invalid credentials")}


async def test_the_dummy_check_uses_the_same_parameters_as_real_hashes() -> None:
    from argon2 import extract_parameters

    assert extract_parameters(security.DUMMY_PASSWORD_HASH) == extract_parameters(HASH)
    assert security.verify_against_dummy("anything") is False
